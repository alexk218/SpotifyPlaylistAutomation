from typing import Optional

from sql.core.connection import DatabaseConnection
from utils.logger import setup_logger


class UnitOfWork:
    """
    Implements the Unit of Work pattern to manage a business transaction.
    Ensures that all operations within a unit of work are treated as a single transaction.
    """

    def __init__(self, connection_provider: Optional[DatabaseConnection] = None):
        """
        Initialize a new UnitOfWork.

        Args:
            connection_provider: The DatabaseConnection to use, or None to create a new one
        """
        self.connection_provider = connection_provider or DatabaseConnection()
        self.connection = None
        self.track_repository = None
        self.playlist_repository = None
        self.track_playlist_repository = None
        self.file_track_mapping_repository = None
        self.db_logger = setup_logger('unit_of_work', 'sql', 'unit_of_work.log')
        self._repositories_initialized = False

    def __enter__(self):
        """
        Enter the context manager, starting a new transaction.

        Returns:
            self: The UnitOfWork instance
        """
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager, committing or rolling back the transaction.

        Args:
            exc_type: The exception type, if an exception was raised, otherwise None
            exc_val: The exception value, if an exception was raised, otherwise None
            exc_tb: The traceback, if an exception was raised, otherwise None
        """
        if exc_type is not None:
            # An exception occurred, roll back the transaction
            self.db_logger.error(f"Rolling back transaction due to: {exc_type.__name__}: {exc_val}")
            self.rollback()
        else:
            # No exception, commit the transaction
            self.commit()

        # Always release the connection back to the pool
        self._release_connection()

    def begin(self):
        """Begin a new transaction, acquiring a database connection."""
        if self.connection is not None:
            self.db_logger.warning("Transaction already started")
            return

        self.connection = self.connection_provider.get_connection()
        self.connection.autocommit = False  # Ensure we're in transaction mode
        self.db_logger.info("Started new transaction")

        # Initialize repositories - we do this here to ensure they share the same connection
        self._init_repositories()

    def commit(self):
        """Commit the current transaction."""
        if self.connection is None:
            self.db_logger.warning("No active transaction to commit")
            return

        try:
            self.connection.commit()
            self.db_logger.info("Transaction committed")
        except Exception as e:
            self.db_logger.error(f"Error committing transaction: {e}")
            self.rollback()
            raise

    def rollback(self):
        """Roll back the current transaction."""
        if self.connection is None:
            self.db_logger.warning("No active transaction to roll back")
            return

        try:
            self.connection.rollback()
            self.db_logger.info("Transaction rolled back")
        except Exception as e:
            self.db_logger.error(f"Error rolling back transaction: {e}")
            raise

    def _release_connection(self):
        """Release the database connection back to the pool."""
        if self.connection is not None:
            self.connection_provider.release_connection(self.connection)
            self.connection = None
            self._repositories_initialized = False
            self.db_logger.debug("Released database connection")

    def _init_repositories(self):
        """Initialize all repositories with the current connection."""
        if self._repositories_initialized:
            return

        # Import repositories here to avoid circular imports
        from sql.repositories.track_repository import TrackRepository
        from sql.repositories.playlist_repository import PlaylistRepository
        from sql.repositories.track_playlist_repository import TrackPlaylistRepository
        from sql.repositories.file_track_mapping_repository import FileTrackMappingRepository

        self.track_repository = TrackRepository(self.connection)
        self.playlist_repository = PlaylistRepository(self.connection)
        self.track_playlist_repository = TrackPlaylistRepository(self.connection)
        self.file_track_mapping_repository = FileTrackMappingRepository(self.connection)

        self._repositories_initialized = True
        self.db_logger.debug("Repositories initialized")