import sqlite3
from typing import List, Tuple, Dict

from sql.repositories.base_repository import BaseRepository


class TrackPlaylistRepository(BaseRepository):
    """
    Repository for managing the many-to-many relationship between tracks and playlists.
    Handles database operations for the TrackPlaylists junction table.
    """

    def __init__(self, connection: sqlite3.Connection):
        """
        Initialize a new TrackPlaylistRepository.

        Args:
            connection: Active database connection
        """
        super().__init__(connection)
        self.table_name = "TrackPlaylists"

    def insert(self, track_id: str, playlist_id: str, uri: str) -> None:
        """
        Associate a track with a playlist.

        Raises:
            Exception: If insert fails
        """
        # Check if the association already exists
        if self.exists(track_id, playlist_id):
            self.db_logger.debug(f"Track {track_id} already associated with playlist {playlist_id}")
            return

        query = """
            INSERT INTO TrackPlaylists (TrackId, PlaylistId, Uri)
            VALUES (?, ?, ?)
        """
        self.execute_non_query(query, (track_id, playlist_id, uri))
        self.db_logger.info(f"Associated track URI: {uri} with playlist {playlist_id}")

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
        return [row['PlaylistId'] for row in results]

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
        return [(row['PlaylistId'], row['PlaylistName'].strip(), row['TrackCount'])
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
        return [(row['TrackId'], row['TrackTitle'], row['PlaylistCount'])
                for row in self.fetch_all(query)]

    def get_playlist_track_counts_batch(self, playlist_ids: List[str]) -> Dict[str, int]:
        """
        Get track counts for multiple playlists in a single query.

        Args:
            playlist_ids: List of playlist IDs

        Returns:
            Dictionary mapping playlist_id to track count
        """
        if not playlist_ids:
            return {}

        placeholders = ','.join(['?' for _ in playlist_ids])
        query = f"""
            SELECT PlaylistId, COUNT(*) as TrackCount
            FROM TrackPlaylists 
            WHERE PlaylistId IN ({placeholders})
            GROUP BY PlaylistId
        """

        results = self.fetch_all(query, playlist_ids)

        # Initialize all playlists with 0 count
        counts = {pid: 0 for pid in playlist_ids}

        # Update with actual counts
        for row in results:
            counts[row['PlaylistId']] = row.TrackCount
        return counts

    def get_all_playlist_track_mappings(self) -> Dict[str, List[str]]:
        """
        Get all playlist-track mappings in a single query.

        Returns:
            Dictionary mapping playlist_id to list of track URIs
        """
        query = """
            SELECT PlaylistId, Uri 
            FROM TrackPlaylists 
            ORDER BY PlaylistId
        """

        results = self.fetch_all(query)

        playlist_mappings = {}
        for row in results:
            if row['PlaylistId'] not in playlist_mappings:
                playlist_mappings[row['PlaylistId']] = []
            playlist_mappings[row['PlaylistId']].append(row['Uri'])

        return playlist_mappings

    def batch_get_uris_for_playlists(self, playlist_ids: List[str]) -> Dict[str, List[str]]:
        """
        Get track URIs for multiple playlists in a single query.

        Args:
            playlist_ids: List of playlist IDs

        Returns:
            Dictionary mapping playlist_id to list of track URIs
        """
        if not playlist_ids:
            return {}

        placeholders = ','.join(['?' for _ in playlist_ids])
        query = f"""
            SELECT PlaylistId, Uri 
            FROM TrackPlaylists 
            WHERE PlaylistId IN ({placeholders})
            ORDER BY PlaylistId
        """

        results = self.fetch_all(query, playlist_ids)

        # Initialize all playlists with empty lists
        playlist_uris = {pid: [] for pid in playlist_ids}

        # Populate with actual URIs
        for row in results:
            if row['PlaylistId'] in playlist_uris:
                playlist_uris[row['PlaylistId']].append(row['Uri'])

        return playlist_uris

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

    def get_playlist_ids_for_uri(self, uri: str) -> List[str]:
        """
        Get all playlist IDs associated with a Spotify URI.

        Args:
            uri: The Spotify URI

        Returns:
            List of playlist IDs
        """
        query = """
            SELECT PlaylistId FROM TrackPlaylists
            WHERE Uri = ?
        """
        results = self.fetch_all(query, (uri,))
        return [row['PlaylistId'] for row in results]

    def get_uris_for_playlist(self, playlist_id: str) -> List[str]:
        """
        Get all Spotify URIs associated with a playlist.

        Args:
            playlist_id: The playlist ID

        Returns:
            List of Spotify URIs
        """
        query = """
            SELECT Uri FROM TrackPlaylists
            WHERE PlaylistId = ?
        """
        results = self.fetch_all(query, (playlist_id,))
        return [row['Uri'] for row in results]

    def insert_by_uri(self, uri: str, playlist_id: str) -> None:
        """
        Associate a Spotify URI with a playlist.

        Args:
            uri: The Spotify URI
            playlist_id: The playlist ID

        Raises:
            Exception: If insert fails
        """
        # Check if the association already exists
        if self.exists_by_uri(uri, playlist_id):
            self.db_logger.debug(f"URI {uri} already associated with playlist {playlist_id}")
            return

        query = """
            INSERT INTO TrackPlaylists (Uri, PlaylistId)
            VALUES (?, ?)
        """
        self.execute_non_query(query, (uri, playlist_id))
        self.db_logger.info(f"Associated URI {uri} with playlist {playlist_id}")

    def delete_by_uri(self, uri: str, playlist_id: str) -> bool:
        """
        Remove an association between a Spotify URI and a playlist.

        Args:
            uri: The Spotify URI
            playlist_id: The playlist ID

        Returns:
            True if the association was removed, False if it didn't exist
        """
        query = """
            DELETE FROM TrackPlaylists
            WHERE Uri = ? AND PlaylistId = ?
        """
        rows_affected = self.execute_non_query(query, (uri, playlist_id))

        if rows_affected > 0:
            self.db_logger.info(f"Removed association between URI {uri} and playlist {playlist_id}")
            return True
        else:
            self.db_logger.debug(f"No association found between URI {uri} and playlist {playlist_id}")
            return False

    def exists_by_uri(self, uri: str, playlist_id: str) -> bool:
        """
        Check if an association exists between a Spotify URI and a playlist.

        Args:
            uri: The Spotify URI
            playlist_id: The playlist ID

        Returns:
            True if the association exists, False otherwise
        """
        query = """
            SELECT 1 FROM TrackPlaylists
            WHERE Uri = ? AND PlaylistId = ?
        """
        result = self.fetch_one(query, (uri, playlist_id))
        return result is not None

    def get_uri_counts_by_playlist(self) -> List[Tuple[str, str, int]]:
        """
        Get the number of URIs in each playlist.

        Returns:
            List of tuples (playlist_id, playlist_name, uri_count)
        """
        query = """
            SELECT p.PlaylistId, p.PlaylistName, COUNT(tp.Uri) AS UriCount
            FROM Playlists p
            LEFT JOIN TrackPlaylists tp ON p.PlaylistId = tp.PlaylistId
            GROUP BY p.PlaylistId, p.PlaylistName
            ORDER BY COUNT(tp.Uri) DESC, p.PlaylistName
        """
        return [(row['PlaylistId'], row['PlaylistName'].strip(), row['UriCount'])
                for row in self.fetch_all(query)]

    def get_playlist_counts_by_uri(self) -> List[Tuple[str, str, int]]:
        """
        Get the number of playlists each URI is in.

        Returns:
            List of tuples (uri, track_title, playlist_count)
        """
        query = """
            SELECT t.Uri, t.TrackTitle, COUNT(tp.PlaylistId) AS PlaylistCount
            FROM Tracks t
            LEFT JOIN TrackPlaylists tp ON t.Uri = tp.Uri
            GROUP BY t.Uri, t.TrackTitle
            ORDER BY COUNT(tp.PlaylistId) DESC, t.TrackTitle
        """
        return [(row['Uri'], row['TrackTitle'], row['PlaylistCount'])
                for row in self.fetch_all(query)]

    def delete_by_playlist_id(self, playlist_id: str) -> int:
        """
        Delete all URI-playlist associations for a specific playlist.

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

        self.db_logger.info(f"Deleted {rows_affected} URI associations for playlist {playlist_id}")
        return rows_affected
