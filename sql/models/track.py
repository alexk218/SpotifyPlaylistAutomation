from datetime import datetime
from typing import List, Optional


class Track:
    """
    Domain model representing a track in the music library.
    Contains business logic and relationships related to tracks.
    """

    def __init__(self, track_id: str, title: str, artists: str, album: str,
                 added_to_master: Optional[datetime] = None):
        """
        Initialize a new Track instance.

        Args:
            track_id: Unique Spotify identifier for the track
            title: Title of the track
            artists: Comma-separated list of artists
            album: Album name
            added_to_master: When the track was added to the MASTER playlist
        """
        self.track_id = track_id
        self.title = title
        self.artists = artists
        self.album = album
        self.added_to_master = added_to_master
        self.playlists = []  # List of Playlist objects this track belongs to

    def add_to_playlist(self, playlist) -> None:
        """
        Add this track to a playlist if it's not already there.

        Args:
            playlist: The Playlist object to add this track to
        """
        if playlist not in self.playlists:
            self.playlists.append(playlist)

    def remove_from_playlist(self, playlist) -> bool:
        """
        Remove this track from a playlist.

        Args:
            playlist: The Playlist object to remove this track from

        Returns:
            bool: True if the track was removed, False if it wasn't in the playlist
        """
        if playlist in self.playlists:
            self.playlists.remove(playlist)
            return True
        return False

    def get_artist_list(self) -> List[str]:
        """
        Get the list of artists as individual strings.

        Returns:
            List of artist names
        """
        return [artist.strip() for artist in self.artists.split(',')]

    def get_primary_artist(self) -> str:
        """
        Get the primary (first) artist of the track.

        Returns:
            Name of the primary artist
        """
        return self.get_artist_list()[0]

    def is_in_playlist(self, playlist) -> bool:
        """
        Check if this track is in a specific playlist.

        Args:
            playlist: The Playlist object to check

        Returns:
            bool: True if track is in the playlist, False otherwise
        """
        return playlist in self.playlists

    def __str__(self) -> str:
        """String representation of the track."""
        return f"{self.artists} - {self.title}"

    def __eq__(self, other) -> bool:
        """Two tracks are equal if they have the same track_id."""
        if not isinstance(other, Track):
            return False
        return self.track_id == other.track_id

    def __hash__(self) -> int:
        """Hash based on track_id for use in sets and as dict keys."""
        return hash(self.track_id)
