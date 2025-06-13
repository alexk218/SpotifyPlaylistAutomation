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
        super().__init__(connection)
        self.table_name = "Tracks"
        self.id_column = "TrackId"

    def insert(self, track: Track) -> None:
        query = """
                INSERT INTO Tracks (Uri, TrackId, TrackTitle, Artists, Album, AddedToMaster, IsLocal, AddedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE()) \
                """
        self.execute_non_query(query, (
            track.uri,
            track.track_id,
            track.title,
            track.artists,
            track.album,
            track.added_to_master,
            1 if track.is_local else 0
        ))
        self.db_logger.info(f"Inserted track: {track.track_id} - {track.title}")

    def update(self, track: Track) -> bool:
        query = """
                UPDATE Tracks
                SET Uri           = ?, \
                    TrackTitle    = ?, \
                    Artists       = ?, \
                    Album         = ?, \
                    AddedToMaster = ?, \
                    IsLocal       = ?
                WHERE TrackId = ? \
                """
        rows_affected = self.execute_non_query(query, (
            track.uri,
            track.title,
            track.artists,
            track.album,
            track.added_to_master,
            1 if track.is_local else 0,
            track.track_id
        ))

        if rows_affected > 0:
            self.db_logger.info(f"Updated track: {track.track_id} - {track.title}")
            return True
        else:
            self.db_logger.warning(f"Track not found for update: {track.track_id}")
            return False

    def delete_by_track_id(self, track_id: str) -> None:
        """Delete a track by its ID"""
        query = "DELETE FROM Tracks WHERE TrackId = ?"
        self.connection.execute(query, (track_id,))
        self.connection.commit()

    def get_by_uri(self, uri: str) -> Optional[Track]:
        """
        Get a track by its Spotify URI.

        Args:
            uri: The Spotify URI to look up

        Returns:
            Track object or None if not found
        """
        query = "SELECT * FROM Tracks WHERE Uri = ?"
        result = self.fetch_one(query, (uri,))
        return self._map_to_model(result) if result else None

    def delete_by_uri(self, uri: str) -> bool:
        """
        Delete a track by its Spotify URI.

        Args:
            uri: The Spotify URI of the track to delete

        Returns:
            True if deleted successfully, False otherwise
        """
        query = "DELETE FROM Tracks WHERE Uri = ?"
        rows_affected = self.execute_non_query(query, (uri,))

        if rows_affected > 0:
            self.db_logger.info(f"Deleted track with URI: {uri}")
            return True
        else:
            self.db_logger.warning(f"Track not found for deletion: {uri}")
            return False

    def search_uris(self, uris: List[str]) -> List[Track]:
        """
        Get tracks matching the provided list of Spotify URIs.

        Args:
            uris: List of Spotify URIs to search for

        Returns:
            List of Track objects matching the URIs
        """
        if not uris:
            return []

        # Convert list of URIs to a comma-separated string for SQL IN clause
        uri_string = ','.join(f"'{uri}'" for uri in uris)

        query = f"""
            SELECT * FROM Tracks
            WHERE Uri IN ({uri_string})
        """

        results = self.fetch_all(query)
        return [self._map_to_model(row) for row in results]

    def get_all_as_dict_by_uri(self) -> Dict[str, Track]:
        """
        Get all tracks as a dictionary indexed by URI.

        Returns:
            Dictionary of {uri: Track object}
        """
        tracks = self.get_all()
        return {track.uri: track for track in tracks if track.uri}

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
                SELECT *
                FROM Tracks
                WHERE TrackTitle LIKE ?
                  AND Artists LIKE ? \
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
                SELECT *
                FROM Tracks
                WHERE AddedToMaster >= ?
                ORDER BY AddedToMaster DESC \
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
                SELECT t.*
                FROM Tracks t
                         JOIN TrackPlaylists tp ON t.TrackId = tp.TrackId
                WHERE tp.PlaylistId = ? \
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
                SELECT t.*
                FROM Tracks t
                         LEFT JOIN TrackPlaylists tp ON t.TrackId = tp.TrackId
                WHERE tp.TrackId IS NULL \
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
                SELECT COUNT(*) as total
                FROM Tracks \
                """
        result = self.fetch_one(query)
        total_tracks = result.total if result else 0

        # All tracks in the database have IDs, so this is just for API compatibility
        return total_tracks, total_tracks

    def get_all_as_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all tracks formatted as dictionaries for API consumption.

        Returns:
            List of track data dictionaries
        """
        tracks = self.get_all()

        track_data = []
        for track in tracks:
            track_data.append({
                'id': track.track_id,
                'name': track.title,
                'artists': track.artists,
                'album': track.album,
                'added_at': track.added_to_master
            })

        self.db_logger.info(f"Retrieved {len(track_data)} tracks as dictionaries")
        return track_data

    def _map_to_model(self, row: pyodbc.Row) -> Track:
        # Extract values from the row
        uri = row.Uri if hasattr(row, 'Uri') else None
        track_id = row.TrackId
        title = row.TrackTitle
        artists = row.Artists
        album = row.Album
        added_to_master = row.AddedToMaster if hasattr(row, 'AddedToMaster') else None
        is_local = bool(row.IsLocal) if hasattr(row, 'IsLocal') else False

        # Create and return a Track object
        return Track(uri, track_id, title, artists, album, added_to_master, is_local)
