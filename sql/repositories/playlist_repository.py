import pyodbc
from typing import List, Optional, Dict, Any

from sql.models.playlist import Playlist
from sql.repositories.base_repository import BaseRepository


class PlaylistRepository(BaseRepository[Playlist]):
    """
    Repository for Playlist entities, handling database operations for the Playlists table.
    """

    def __init__(self, connection: pyodbc.Connection):
        """
        Initialize a new PlaylistRepository.

        Args:
            connection: Active database connection
        """
        super().__init__(connection, Playlist)
        self.table_name = "Playlists"
        self.id_column = "PlaylistId"

    def insert(self, playlist: Playlist) -> None:
        query = """
                INSERT INTO Playlists (PlaylistId, PlaylistName, AddedDate)
                VALUES (?, ?, GETDATE()) \
                """
        self.execute_non_query(query, (
            playlist.playlist_id,
            playlist.name
        ))
        self.db_logger.info(f"Inserted playlist: {playlist.playlist_id} - {playlist.name}")

    def update(self, playlist: Playlist) -> bool:
        """
        Update an existing playlist in the database.

        Args:
            playlist: The Playlist object to update

        Returns:
            True if the playlist was updated, False if it wasn't found

        Raises:
            Exception: If update fails
        """
        query = """
                UPDATE Playlists
                SET PlaylistName = ?
                WHERE PlaylistId = ? \
                """
        rows_affected = self.execute_non_query(query, (
            playlist.name,
            playlist.playlist_id
        ))

        if rows_affected > 0:
            self.db_logger.info(f"Updated playlist: {playlist.playlist_id} - {playlist.name}")
            return True
        else:
            self.db_logger.warning(f"Playlist not found for update: {playlist.playlist_id}")
            return False

    def get_by_id(self, playlist_id: str) -> Optional[Playlist]:
        """
        Get a playlist by its ID.

        Args:
            playlist_id: The playlist ID to look up

        Returns:
            Playlist object or None if not found
        """
        return super().get_by_id(playlist_id)

    def get_by_name(self, name: str) -> Optional[Playlist]:
        """
        Get a playlist by its name, with better whitespace handling.

        Args:
            name: The playlist name to look up

        Returns:
            Playlist object or None if not found
        """
        # Handle None or empty string
        if not name:
            return None

        # Normalize the name by trimming whitespace
        normalized_name = name.strip()

        # Try exact match first with normalized values
        query = """
                SELECT *
                FROM Playlists
                WHERE RTRIM(LTRIM(PlaylistName)) = ? \
                """
        result = self.fetch_one(query, (normalized_name,))

        if result:
            return self._map_to_model(result)

        # If no exact match, try case-insensitive
        query = """
                SELECT *
                FROM Playlists
                WHERE LOWER(RTRIM(LTRIM(PlaylistName))) = LOWER(?) \
                """
        result = self.fetch_one(query, (normalized_name,))

        if result:
            self.db_logger.info(f"Found playlist '{result.PlaylistName}' with case-insensitive match for '{name}'")
            return self._map_to_model(result)

        return None

    def find_by_name(self, name_part: str) -> List[Playlist]:
        """
        Find playlists by partial name match, with improved whitespace handling.

        Args:
            name_part: Part of playlist name to search for

        Returns:
            List of matching Playlist objects
        """
        if not name_part:
            return []

        # Normalize the search string
        normalized_search = name_part.strip()

        query = """
                SELECT *
                FROM Playlists
                WHERE LOWER(PlaylistName) LIKE LOWER(?) \
                """
        results = self.fetch_all(query, (f"%{normalized_search}%",))
        return [self._map_to_model(row) for row in results]

    def get_playlists_for_track(self, track_id: str) -> List[Playlist]:
        """
        Get all playlists that contain a specific track.

        Args:
            track_id: The track ID to look up

        Returns:
            List of Playlist objects containing the track
        """
        query = """
                SELECT p.*
                FROM Playlists p
                         JOIN TrackPlaylists tp ON p.PlaylistId = tp.PlaylistId
                WHERE tp.TrackId = ? \
                """
        results = self.fetch_all(query, (track_id,))
        return [self._map_to_model(row) for row in results]

    def get_playlist_count(self) -> int:
        """
        Get the total number of playlists.

        Returns:
            Count of playlists
        """
        query = "SELECT COUNT(*) AS count FROM Playlists"
        result = self.fetch_one(query)
        return result.count if result else 0

    def _map_to_model(self, row: pyodbc.Row) -> Playlist:
        """
        Map a database row to a Playlist object.

        Args:
            row: Database row from the Playlists table

        Returns:
            Playlist object with properties set from the row
        """
        playlist_id = row.PlaylistId
        name = row.PlaylistName.strip() if row.PlaylistName else ""

        return Playlist(playlist_id, name)
