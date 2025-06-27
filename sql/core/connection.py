import os
import sqlite3
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

from utils.logger import setup_logger


class DatabaseConnection:
    """
    Manages SQLite database connections with connection pooling support.
    Implements the singleton pattern to ensure only one connection manager exists.
    """
    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        """Ensure only one instance of DatabaseConnection exists (singleton pattern)."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseConnection, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        """Initialize the database connection manager if not already initialized."""
        if self._initialized:
            return

        load_dotenv()
        self.db_path = self._get_database_path()
        self.db_logger = setup_logger('db_connection', 'sql', 'db_connection.log')

        # Initialize database schema once
        self._initialize_database()
        self.db_logger.info(f"SQLite database connection manager initialized: {self.db_path}")
        self._initialized = True

    def _get_database_path(self) -> str:
        """Get the SQLite database file path."""
        # Check for custom path in environment
        db_path = os.getenv('SQLITE_DB_PATH')

        if db_path:
            return db_path

        # Default: create in project root
        project_root = Path(__file__).parent.parent.parent
        db_dir = project_root / "data"
        db_dir.mkdir(exist_ok=True)

        return str(db_dir / "tagify.db")

    def _initialize_database(self):
        """Initialize database schema if it doesn't exist."""
        connection = self._create_new_connection()
        try:
            self._create_schema(connection)
            self.db_logger.info("Database schema initialized successfully")
        except Exception as e:
            self.db_logger.error(f"Error initializing database schema: {e}")
            raise
        finally:
            connection.close()

    def _create_schema(self, connection: sqlite3.Connection):
        """Create database tables if they don't exist."""
        cursor = connection.cursor()

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Playlists (
                    PlaylistId TEXT PRIMARY KEY,
                    PlaylistName TEXT NOT NULL,
                    MasterSyncSnapshotId TEXT DEFAULT '',
                    AssociationsSnapshotId TEXT DEFAULT '',
                    AddedDate DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Tracks (
                    Uri TEXT PRIMARY KEY,
                    TrackId TEXT,
                    TrackTitle TEXT NOT NULL,
                    Artists TEXT NOT NULL,
                    Album TEXT,
                    AddedToMaster DATETIME,
                    IsLocal INTEGER DEFAULT 0,
                    Duration INTEGER,
                    AddedDate DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS TrackPlaylists (
                    Uri TEXT,
                    PlaylistId TEXT,
                    TrackId TEXT,
                    PRIMARY KEY (Uri, PlaylistId),
                    FOREIGN KEY (Uri) REFERENCES Tracks(Uri) ON DELETE CASCADE,
                    FOREIGN KEY (PlaylistId) REFERENCES Playlists(PlaylistId) ON DELETE CASCADE
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS FileTrackMappings (
                    MappingId INTEGER PRIMARY KEY AUTOINCREMENT,
                    FilePath TEXT NOT NULL,
                    Uri TEXT,
                    FileHash TEXT,
                    FileSize INTEGER,
                    LastModified DATETIME,
                    CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
                    IsActive INTEGER DEFAULT 1
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tracks_trackid ON Tracks(TrackId)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tracks_islocal ON Tracks(IsLocal)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trackplaylists_playlistid ON TrackPlaylists(PlaylistId)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_filemappings_filepath ON FileTrackMappings(FilePath)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_filemappings_uri ON FileTrackMappings(Uri)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_filemappings_active ON FileTrackMappings(IsActive)")

            connection.commit()

        except Exception as e:
            connection.rollback()
            raise

    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool or create a new one if needed.

        Returns:
            An active database connection
        """
        try:
            connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )

            # Apply SQLite performance optimizations
            cursor = connection.cursor()

            # Essential performance settings
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.execute("PRAGMA synchronous = NORMAL")  # Faster than FULL
            cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
            cursor.execute("PRAGMA temp_store = MEMORY")  # Temp tables in memory
            cursor.execute("PRAGMA mmap_size = 134217728")  # 128MB memory map
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute("PRAGMA busy_timeout = 10000")

            cursor.close()
            connection.row_factory = sqlite3.Row
            return connection
        except Exception as e:
            self.db_logger.error(f"Failed to create connection: {e}")
            raise

    def release_connection(self, connection: sqlite3.Connection) -> None:
        """
        Close the connection immediately.
        """
        try:
            connection.close()
            self.db_logger.debug("Closed SQLite connection")
        except Exception as e:
            self.db_logger.error(f"Error closing connection: {e}")

    def _create_new_connection(self) -> sqlite3.Connection:
        """
        Create a new database connection with optimized settings.

        Returns:
            A new database connection

        Raises:
            Exception: If connection creation fails
        """
        try:
            connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,  # Allow use across threads
                timeout=30.0  # Reasonable timeout
            )

            # Basic optimizations only
            cursor = connection.cursor()

            # Enable WAL mode for better concurrency
            cursor.execute("PRAGMA journal_mode = WAL")

            # Set reasonable busy timeout
            cursor.execute("PRAGMA busy_timeout = 10000")  # 10 seconds

            # Enable foreign key constraints
            cursor.execute("PRAGMA foreign_keys = ON")

            cursor.close()

            # Set row factory to access columns by name
            connection.row_factory = sqlite3.Row
            return connection
        except Exception as e:
            self.db_logger.error(f"Failed to create database connection: {e}")
            raise
