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
        with self._lock:
            if self._initialized:
                return

            load_dotenv()

            # Configuration
            self.db_path = self._get_database_path()
            self.db_logger = setup_logger('db_connection', 'sql', 'db_connection.log')

            # Connection pool settings
            self._max_pool_size = 10
            self._connection_timeout = 30  # seconds
            self._pool = []  # Available connections
            self._in_use = {}  # Connections currently in use: {conn: timestamp}

            # Initialize database and schema
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

        Raises:
            Exception: If unable to get a connection after timeout
        """
        with self._lock:
            # First, try to get a connection from the pool
            if self._pool:
                connection = self._pool.pop()
                self._in_use[connection] = time.time()
                self.db_logger.debug(f"Reusing connection from pool. Available: {len(self._pool)}")
                return connection

            # If pool is empty but we haven't reached max connections, create a new one
            if len(self._in_use) < self._max_pool_size:
                try:
                    connection = self._create_new_connection()
                    self._in_use[connection] = time.time()
                    self.db_logger.info(f"Created new connection. Total in use: {len(self._in_use)}")
                    return connection
                except Exception as e:
                    self.db_logger.error(f"Error creating new connection: {e}")
                    raise

            # If we've reached max connections, wait for one to become available
            start_time = time.time()
            while time.time() - start_time < self._connection_timeout:
                time.sleep(0.1)  # Small delay to prevent CPU spinning

                # Try again to get a connection from the pool
                if self._pool:
                    connection = self._pool.pop()
                    self._in_use[connection] = time.time()
                    self.db_logger.debug(f"Got connection from pool after waiting")
                    return connection

            # If we get here, we timed out waiting for a connection
            self.db_logger.error(f"Timeout waiting for database connection")
            raise Exception("Timeout waiting for database connection")

    def release_connection(self, connection: sqlite3.Connection) -> None:
        """
        Return a connection to the pool.

        Args:
            connection: The connection to release
        """
        with self._lock:
            if connection in self._in_use:
                del self._in_use[connection]

                # Only return to pool if we're under the limit and the connection is still good
                if len(self._pool) < self._max_pool_size:
                    try:
                        # Test if connection is still valid
                        cursor = connection.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()

                        self._pool.append(connection)
                        self.db_logger.debug(f"Returned connection to pool. Available: {len(self._pool)}")
                    except Exception as e:
                        self.db_logger.warning(f"Closing bad connection instead of returning to pool: {e}")
                        self._close_connection(connection)
                else:
                    self._close_connection(connection)
            else:
                self.db_logger.warning(f"Attempted to release a connection that's not tracked as in-use")
                self._close_connection(connection)

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

    def _close_connection(self, connection: sqlite3.Connection) -> None:
        """
        Close a database connection.

        Args:
            connection: The connection to close
        """
        try:
            connection.close()
            self.db_logger.debug("Closed database connection")
        except Exception as e:
            self.db_logger.error(f"Error closing database connection: {e}")

    def close_all_connections(self) -> None:
        """Close all connections in the pool and in use."""
        with self._lock:
            # Close all connections in the pool
            for conn in self._pool:
                self._close_connection(conn)
            self._pool = []

            # Close all in-use connections
            for conn in list(self._in_use.keys()):
                self._close_connection(conn)
            self._in_use = {}

            self.db_logger.info("Closed all database connections")

    def get_stats(self) -> dict:
        """
        Get statistics about the connection pool.

        Returns:
            Dictionary with pool statistics
        """
        with self._lock:
            return {
                "pool_size": len(self._pool),
                "in_use": len(self._in_use),
                "max_size": self._max_pool_size
            }
