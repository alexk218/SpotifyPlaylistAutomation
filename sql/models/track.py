import uuid
from datetime import datetime
from typing import List, Optional

import hashlib

from helpers.spotify_uri_helper import SpotifyUriHelper


class Track:
    def __init__(self, uri: str = None, track_id: str = None, title: str = "", artists: str = "", album: str = "",
                 added_to_master: Optional[datetime] = None, is_local: bool = False):
        """
        Initialize a Track instance.

        Args:
            uri: Spotify URI (primary identifier) - e.g., "spotify:track:id" or "spotify:local:artist:album:title:duration"
            track_id: Legacy Spotify track ID (may be None for local files)
            title: Track title
            artists: Artist names (comma-separated)
            album: Album name
            added_to_master: When track was added to master playlist
            is_local: Whether this is a local file
        """
        self.uri = uri
        self.track_id = track_id  # May be None for local files
        self.title = title
        self.artists = artists
        self.album = album
        self.added_to_master = added_to_master
        self.is_local = is_local
        self.playlists = []

        # Auto-generate local track ID if needed (for backward compatibility)
        if not self.track_id and self.is_local and self.uri:
            self.track_id = self._generate_local_track_id()

    def _generate_local_track_id(self) -> str:
        """
        Generate a legacy track ID for local files for backward compatibility.
        This uses the same logic as your existing system.
        """
        if not self.uri or not self.uri.startswith('spotify:local:'):
            return f"local_{uuid.uuid4().hex[:10]}"
            # Extract metadata from URI and generate consistent ID
        try:
            uri_info = SpotifyUriHelper.parse_uri(self.uri)

            # Normalize strings
            normalized_title = ''.join(c for c in (uri_info.title or '') if c.isalnum() or c in ' &-_')
            normalized_artist = ''.join(c for c in (uri_info.artist or '') if c.isalnum() or c in ' &-_')

            # Generate ID using same logic as your existing generate_local_track_id function
            id_string = f"{normalized_artist}_{normalized_title}".lower()
            return f"local_{hashlib.md5(id_string.encode()).hexdigest()[:16]}"
        except Exception:
            return f"local_{uuid.uuid4().hex[:10]}"

    def is_local_file(self) -> bool:
        """Check if this track is a local file based on URI."""
        return self.uri and self.uri.startswith('spotify:local:') if self.uri else self.is_local

    def is_spotify_track(self) -> bool:
        """Check if this track is a regular Spotify track."""
        return self.uri and self.uri.startswith('spotify:track:') if self.uri else False

    def get_spotify_track_id(self) -> Optional[str]:
        """
        Extract the Spotify track ID from the URI for regular tracks.

        Returns:
            Track ID for regular tracks, None for local files
        """
        if self.is_spotify_track():
            return self.uri.split(':')[2]
        return self.track_id  # Fallback to legacy track_id

    def get_local_metadata(self) -> Optional[dict]:
        """
        Extract metadata from local file URI.

        Returns:
            Dictionary with artist, album, title, duration if local file, None otherwise
        """
        if not self.is_local_file():
            return None

        try:
            uri_info = SpotifyUriHelper.parse_uri(self.uri)
            return {
                'artist': uri_info.artist or '',
                'album': uri_info.album or '',
                'title': uri_info.title or '',
                'duration': uri_info.duration
            }
        except Exception:
            return None

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
        local_indicator = " (LOCAL)" if self.is_local_file() else ""
        return f"{self.artists} - {self.title}{local_indicator}"

    def __eq__(self, other) -> bool:
        """Two tracks are equal if they have the same URI."""
        if not isinstance(other, Track):
            return False
        return self.uri == other.uri

    def __hash__(self) -> int:
        """Hash based on URI for use in sets and as dict keys."""
        return hash(self.uri) if self.uri else hash(id(self))
