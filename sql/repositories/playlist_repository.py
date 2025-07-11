import sqlite3

from typing import List, Optional, Dict, Any

from sql.models.playlist import Playlist
from sql.repositories.base_repository import BaseRepository


class PlaylistRepository(BaseRepository[Playlist]):
    def __init__(self, connection: sqlite3.Connection):
        super().__init__(connection)
        self.table_name = "Playlists"
        self.id_column = "PlaylistId"

    def insert(self, playlist: Playlist) -> None:
        query = """
                INSERT INTO Playlists (PlaylistId, PlaylistName, MasterSyncSnapshotId, AssociationsSnapshotId, AddedDate)
                VALUES (?, ?, ?, ?, datetime('now'))
                """
        self.execute_non_query(query, (
            playlist.playlist_id,
            playlist.name,
            playlist.master_sync_snapshot_id,
            playlist.associations_snapshot_id,
        ))
        self.db_logger.info(f"Inserted playlist: {playlist.playlist_id} - {playlist.name}")

    def update(self, playlist: Playlist) -> bool:
        query = """
                UPDATE Playlists
                SET PlaylistName = ?,
                    MasterSyncSnapshotId = ?,
                    AssociationsSnapshotId = ?
                WHERE PlaylistId = ?
                """
        rows_affected = self.execute_non_query(query, (
            playlist.name,
            playlist.master_sync_snapshot_id,
            playlist.associations_snapshot_id,
            playlist.playlist_id
        ))

        if rows_affected > 0:
            self.db_logger.info(f"Updated playlist: {playlist.playlist_id} - {playlist.name}")
            return True
        else:
            self.db_logger.warning(f"Playlist not found for update: {playlist.playlist_id}")
            return False

    def delete(self, playlist_id: str) -> bool:
        # Note: Associations should be deleted separately using track_playlist_repository.delete_by_playlist_id
        # before calling this method to ensure proper order of operations

        result = self.delete_by_id(playlist_id)
        if result:
            self.db_logger.info(f"Deleted playlist with ID: {playlist_id}")
        else:
            self.db_logger.warning(f"Playlist not found for deletion: {playlist_id}")
        return result

    def get_by_id(self, playlist_id: str) -> Optional[Playlist]:
        return super().get_by_id(playlist_id)

    def get_by_name(self, name: str) -> Optional[Playlist]:
        # Handle None or empty string
        if not name:
            return None

        # Normalize the name by trimming whitespace
        normalized_name = name.strip()

        # Try exact match first with normalized values
        query = """
                SELECT *
                FROM Playlists
                WHERE TRIM(PlaylistName) = ?
                """
        result = self.fetch_one(query, (normalized_name,))

        if result:
            return self._map_to_model(result)

        # If no exact match, try case-insensitive
        query = """
                SELECT *
                FROM Playlists
                WHERE LOWER(TRIM(PlaylistName)) = LOWER(?)
                """
        result = self.fetch_one(query, (normalized_name,))

        if result:
            self.db_logger.info(f"Found playlist '{result['PlaylistName']}' with case-insensitive match for '{name}'")
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

    def get_playlists_by_ids(self, playlist_ids: List[str]) -> Dict[str, 'Playlist']:
        """
        OPTIMIZED: Get multiple playlists by their IDs in a single query.

        Args:
            playlist_ids: List of playlist IDs

        Returns:
            Dictionary mapping playlist_id to Playlist objects
        """
        if not playlist_ids:
            return {}

        placeholders = ','.join(['?' for _ in playlist_ids])
        query = f"SELECT * FROM Playlists WHERE PlaylistId IN ({placeholders})"

        results = self.fetch_all(query, playlist_ids)

        playlists_dict = {}
        for row in results:
            playlist = self._map_to_model(row)
            if playlist:
                playlists_dict[playlist.playlist_id] = playlist

        return playlists_dict

    def get_all_non_master_playlists(self) -> List['Playlist']:
        """
        OPTIMIZED: Get all playlists except MASTER in a single query.

        Returns:
            List of Playlist objects (excluding MASTER)
        """
        query = "SELECT * FROM Playlists WHERE UPPER(PlaylistName) != 'MASTER'"
        results = self.fetch_all(query)
        return [self._map_to_model(row) for row in results]

    def get_playlists_for_track(self, track_id: str) -> List[Playlist]:
        """
        Get all playlists that contain a specific track.
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
        query = "SELECT COUNT(*) AS count FROM Playlists"
        result = self.fetch_one(query)
        return result.count if result else 0

    def _map_to_model(self, row: sqlite3.Row) -> Playlist:
        """
        Map a database row to a Playlist object.

        Args:
            row: Database row from the Playlists table

        Returns:
            Playlist object with properties set from the row
        """
        playlist_id = row['PlaylistId']
        name = row['PlaylistName'].strip() if row['PlaylistName'] else ""
        master_sync_snapshot_id = row['MasterSyncSnapshotId'] if row['MasterSyncSnapshotId'] else ""
        associations_snapshot_id = row['AssociationsSnapshotId'] if row['AssociationsSnapshotId'] else ""

        return Playlist(playlist_id, name, master_sync_snapshot_id, associations_snapshot_id)
