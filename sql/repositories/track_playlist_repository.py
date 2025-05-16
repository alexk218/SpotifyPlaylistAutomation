import pyodbc
from typing import List, Tuple, Optional

from sql.repositories.base_repository import BaseRepository


class TrackPlaylistRepository(BaseRepository):
    """
    Repository for managing the many-to-many relationship between tracks and playlists.
    Handles database operations for the TrackPlaylists junction table.
    """

    def __init__(self, connection: pyodbc.Connection):
        """
        Initialize a new TrackPlaylistRepository.

        Args:
            connection: Active database connection
        """
        super().__init__(connection)
        self.table_name = "TrackPlaylists"

    def insert(self, track_id: str, playlist_id: str) -> None:
        """
        Associate a track with a playlist.

        Args:
            track_id: The track ID
            playlist_id: The playlist ID

        Raises:
            Exception: If insert fails
        """
        # Check if the association already exists
        if self.exists(track_id, playlist_id):
            self.db_logger.debug(f"Track {track_id} already associated with playlist {playlist_id}")
            return

        query = """
            INSERT INTO TrackPlaylists (TrackId, PlaylistId)
            VALUES (?, ?)
        """
        self.execute_non_query(query, (track_id, playlist_id))
        self.db_logger.info(f"Associated track {track_id} with playlist {playlist_id}")

    def delete(self, track_id: str, playlist_id: str) -> bool:
        """
        Remove an association between a track and a playlist.

        Args:
            track_id: The track ID
            playlist_id: The playlist ID

        Returns:
            True if the association was removed, False if it didn't exist
        """
        query = """
            DELETE FROM TrackPlaylists
            WHERE TrackId = ? AND PlaylistId = ?
        """
        rows_affected = self.execute_non_query(query, (track_id, playlist_id))

        if rows_affected > 0:
            self.db_logger.info(f"Removed association between track {track_id} and playlist {playlist_id}")
            return True
        else:
            self.db_logger.debug(f"No association found between track {track_id} and playlist {playlist_id}")
            return False

    def exists(self, track_id: str, playlist_id: str) -> bool:
        """
        Check if an association exists between a track and a playlist.

        Args:
            track_id: The track ID
            playlist_id: The playlist ID

        Returns:
            True if the association exists, False otherwise
        """
        query = """
            SELECT 1 FROM TrackPlaylists
            WHERE TrackId = ? AND PlaylistId = ?
        """
        result = self.fetch_one(query, (track_id, playlist_id))
        return result is not None

    def get_playlist_ids_for_track(self, track_id: str) -> List[str]:
        """
        Get all playlist IDs associated with a track.

        Args:
            track_id: The track ID

        Returns:
            List of playlist IDs
        """
        query = """
            SELECT PlaylistId FROM TrackPlaylists
            WHERE TrackId = ?
        """
        results = self.fetch_all(query, (track_id,))
        return [row.PlaylistId for row in results]

    def get_track_ids_for_playlist(self, playlist_id: str) -> List[str]:
        """
        Get all track IDs associated with a playlist.

        Args:
            playlist_id: The playlist ID

        Returns:
            List of track IDs
        """
        query = """
            SELECT TrackId FROM TrackPlaylists
            WHERE PlaylistId = ?
        """
        results = self.fetch_all(query, (playlist_id,))
        return [row.TrackId for row in results]

    def get_track_counts_by_playlist(self) -> List[Tuple[str, str, int]]:
        """
        Get the number of tracks in each playlist.

        Returns:
            List of tuples (playlist_id, playlist_name, track_count)
        """
        query = """
            SELECT p.PlaylistId, p.PlaylistName, COUNT(tp.TrackId) AS TrackCount
            FROM Playlists p
            LEFT JOIN TrackPlaylists tp ON p.PlaylistId = tp.PlaylistId
            GROUP BY p.PlaylistId, p.PlaylistName
            ORDER BY COUNT(tp.TrackId) DESC, p.PlaylistName
        """
        return [(row.PlaylistId, row.PlaylistName.strip(), row.TrackCount)
                for row in self.fetch_all(query)]

    def get_playlist_counts_by_track(self) -> List[Tuple[str, str, int]]:
        """
        Get the number of playlists each track is in.

        Returns:
            List of tuples (track_id, track_title, playlist_count)
        """
        query = """
            SELECT t.TrackId, t.TrackTitle, COUNT(tp.PlaylistId) AS PlaylistCount
            FROM Tracks t
            LEFT JOIN TrackPlaylists tp ON t.TrackId = tp.TrackId
            GROUP BY t.TrackId, t.TrackTitle
            ORDER BY COUNT(tp.PlaylistId) DESC, t.TrackTitle
        """
        return [(row.TrackId, row.TrackTitle, row.PlaylistCount)
                for row in self.fetch_all(query)]

    def delete_by_playlist_id(self, playlist_id: str) -> int:
        """
        Delete all track-playlist associations for a specific playlist.

        Args:
            playlist_id: The playlist ID

        Returns:
            Number of rows deleted
        """
        query = """
            DELETE FROM TrackPlaylists
            WHERE PlaylistId = ?
        """
        rows_affected = self.execute_non_query(query, (playlist_id,))

        self.db_logger.info(f"Deleted {rows_affected} track associations for playlist {playlist_id}")
        return rows_affected
