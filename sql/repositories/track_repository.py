import pyodbc
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple

from sql.models.track import Track
from sql.repositories.base_repository import BaseRepository


class TrackRepository(BaseRepository[Track]):
    """
    Repository for Track entities, handling database operations for the Tracks table.
    """

    def __init__(self, connection: pyodbc.Connection):
        """
        Initialize a new TrackRepository.

        Args:
            connection: Active database connection
        """
        super().__init__(connection, Track)
        self.table_name = "Tracks"
        self.id_column = "TrackId"

    def insert(self, track: Track) -> None:
        """
        Insert a new track into the database.

        Args:
            track: The Track object to insert

        Raises:
            Exception: If insert fails
        """
        query = """
            INSERT INTO Tracks (TrackId, TrackTitle, Artists, Album, AddedToMaster)
            VALUES (?, ?, ?, ?, ?)
        """
        self.execute_non_query(query, (
            track.track_id,
            track.title,
            track.artists,
            track.album,
            track.added_to_master
        ))
        self.db_logger.info(f"Inserted track: {track.track_id} - {track.title}")

    def update(self, track: Track) -> bool:
        """
        Update an existing track in the database.

        Args:
            track: The Track object to update

        Returns:
            True if the track was updated, False if it wasn't found

        Raises:
            Exception: If update fails
        """
        query = """
            UPDATE Tracks 
            SET TrackTitle = ?, Artists = ?, Album = ?, AddedToMaster = ?
            WHERE TrackId = ?
        """
        rows_affected = self.execute_non_query(query, (
            track.title,
            track.artists,
            track.album,
            track.added_to_master,
            track.track_id
        ))

        if rows_affected > 0:
            self.db_logger.info(f"Updated track: {track.track_id} - {track.title}")
            return True
        else:
            self.db_logger.warning(f"Track not found for update: {track.track_id}")
            return False

    def get_by_id(self, track_id: str) -> Optional[Track]:
        """
        Get a track by its ID.

        Args:
            track_id: The track ID to look up

        Returns:
            Track object or None if not found
        """
        return super().get_by_id(track_id)

    def search_track_ids(self, track_ids: List[str]) -> List[Track]:
        """
        Get tracks matching the provided list of track IDs.

        Args:
            track_ids: List of track IDs to search for

        Returns:
            List of Track objects matching the IDs
        """
        if not track_ids:
            return []

        # Convert list of IDs to a comma-separated string for SQL IN clause
        id_string = ','.join(f"'{id}'" for id in track_ids)

        query = f"""
            SELECT * FROM Tracks
            WHERE TrackId IN ({id_string})
        """

        results = self.fetch_all(query)
        return [self._map_to_model(row) for row in results]

    def get_by_title_and_artist(self, title: str, artist: str) -> List[Track]:
        """
        Find tracks by title and artist (partial match).

        Args:
            title: Track title to search for
            artist: Artist name to search for

        Returns:
            List of matching Track objects
        """
        query = """
            SELECT * FROM Tracks
            WHERE TrackTitle LIKE ? AND Artists LIKE ?
        """
        results = self.fetch_all(query, (f"%{title}%", f"%{artist}%"))
        return [self._map_to_model(row) for row in results]

    def get_tracks_added_since(self, since_date: datetime) -> List[Track]:
        """
        Get tracks added to the MASTER playlist since a specific date.

        Args:
            since_date: The date to check against

        Returns:
            List of Track objects added since the date
        """
        query = """
            SELECT * FROM Tracks
            WHERE AddedToMaster >= ?
            ORDER BY AddedToMaster DESC
        """
        results = self.fetch_all(query, (since_date,))
        return [self._map_to_model(row) for row in results]

    def get_tracks_in_playlist(self, playlist_id: str) -> List[Track]:
        """
        Get all tracks in a specific playlist.

        Args:
            playlist_id: The playlist ID

        Returns:
            List of Track objects in the playlist
        """
        query = """
            SELECT t.* FROM Tracks t
            JOIN TrackPlaylists tp ON t.TrackId = tp.TrackId
            WHERE tp.PlaylistId = ?
        """
        results = self.fetch_all(query, (playlist_id,))
        return [self._map_to_model(row) for row in results]

    def get_tracks_not_in_playlists(self) -> List[Track]:
        """
        Get tracks that are not in any playlist except MASTER.

        Returns:
            List of Track objects not in any playlist
        """
        query = """
            SELECT t.* FROM Tracks t
            LEFT JOIN TrackPlaylists tp ON t.TrackId = tp.TrackId
            WHERE tp.TrackId IS NULL
        """
        results = self.fetch_all(query)
        return [self._map_to_model(row) for row in results]

    def get_track_count_with_id(self) -> Tuple[int, int]:
        """
        Get count of tracks with and without track IDs.

        Returns:
            Tuple of (tracks_with_ids, total_tracks)
        """
        query = """
            SELECT COUNT(*) as total FROM Tracks
        """
        result = self.fetch_one(query)
        total_tracks = result.total if result else 0

        # All tracks in the database have IDs, so this is just for API compatibility
        return total_tracks, total_tracks

    def _map_to_model(self, row: pyodbc.Row) -> Track:
        """
        Map a database row to a Track object.

        Args:
            row: Database row from the Tracks table

        Returns:
            Track object with properties set from the row
        """
        # Extract values from the row
        track_id = row.TrackId
        title = row.TrackTitle
        artists = row.Artists
        album = row.Album
        added_to_master = row.AddedToMaster if hasattr(row, 'AddedToMaster') else None

        # Create and return a Track object
        return Track(track_id, title, artists, album, added_to_master)