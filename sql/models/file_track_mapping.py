import hashlib
import os
from datetime import datetime
from typing import Optional, Dict

from helpers.spotify_uri_helper import SpotifyUriHelper


class FileTrackMapping:
    """
    Domain model representing the mapping between local files and Spotify tracks.
    """

    def __init__(self, mapping_id: int = None, file_path: str = "", spotify_uri: str = "",
                 file_hash: str = None, file_size: int = None,
                 last_modified: datetime = None, created_at: datetime = None,
                 is_active: bool = True):
        """
        Initialize a new FileTrackMapping instance.

        Args:
            mapping_id: Unique identifier for the mapping
            file_path: Full path to the local file
            spotify_uri: Spotify URI this file maps to (e.g., spotify:track:id or spotify:local:artist:album:title:duration)
            file_hash: SHA256 hash of the file for integrity checking
            file_size: Size of the file in bytes
            last_modified: When the file was last modified
            created_at: When this mapping was created
            is_active: Whether this mapping is active (for soft deletes)
        """
        self.mapping_id = mapping_id
        self.file_path = file_path
        self.uri = spotify_uri
        self.file_hash = file_hash
        self.file_size = file_size
        self.last_modified = last_modified
        self.created_at = created_at or datetime.now()
        self.is_active = is_active

    @property
    def file_name(self) -> str:
        """Get the filename from the file path."""
        return os.path.basename(self.file_path) if self.file_path else ""

    def calculate_file_hash(self) -> Optional[str]:
        """
        Calculate and return the SHA256 hash of the file.

        Returns:
            SHA256 hash string, or None if file doesn't exist
        """
        if not os.path.exists(self.file_path):
            return None

        try:
            with open(self.file_path, 'rb') as f:
                file_hash = hashlib.sha256()
                while chunk := f.read(8192):
                    file_hash.update(chunk)
                return file_hash.hexdigest()
        except Exception:
            return None

    def update_file_info(self) -> bool:
        """
        Update file information (hash, size, last_modified) from current file state.

        Returns:
            True if successful, False if file doesn't exist
        """
        if not os.path.exists(self.file_path):
            return False

        try:
            stat = os.stat(self.file_path)
            self.file_size = stat.st_size
            self.last_modified = datetime.fromtimestamp(stat.st_mtime)
            self.file_hash = self.calculate_file_hash()
            return True
        except Exception:
            return False

    def verify_integrity(self) -> bool:
        """
        Verify that the file hasn't changed since the mapping was created.

        Returns:
            True if file hash matches stored hash, False otherwise
        """
        if not self.file_hash:
            return True  # No hash stored, assume valid

        current_hash = self.calculate_file_hash()
        return current_hash == self.file_hash if current_hash else False

    def get_file_extension(self) -> str:
        """Get the file extension in lowercase."""
        return os.path.splitext(self.file_path)[1].lower()

    def get_filename(self) -> str:
        """Get just the filename without path."""
        return os.path.basename(self.file_path)

    def file_exists(self) -> bool:
        """Check if the mapped file still exists."""
        return os.path.exists(self.file_path)

    def is_local_file_mapping(self) -> bool:
        """Check if this mapping is for a local Spotify file."""
        return self.uri.startswith('spotify:local:') if self.uri else False

    def is_track_mapping(self) -> bool:
        """Check if this mapping is for a regular Spotify track."""
        return self.uri.startswith('spotify:track:') if self.uri else False

    def get_track_id(self) -> Optional[str]:
        """
        Extract track ID from Spotify URI if it's a regular track.

        Returns:
            Track ID for regular tracks, None for local files
        """
        if self.is_track_mapping():
            return self.uri.split(':')[2]
        return None

    def get_local_metadata(self) -> Optional[Dict[str, str]]:
        """
        Extract metadata from local file URI.

        Returns:
            Dictionary with artist, album, title, duration if local file, None otherwise
        """
        if not self.is_local_file_mapping():
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

    def __str__(self) -> str:
        """String representation of the mapping."""
        return f"{self.get_filename()} -> {self.uri}"

    def __eq__(self, other) -> bool:
        """Two mappings are equal if they have the same mapping_id."""
        if not isinstance(other, FileTrackMapping):
            return False
        return self.mapping_id == other.mapping_id

    def __hash__(self) -> int:
        """Hash based on mapping_id for use in sets and as dict keys."""
        return hash(self.mapping_id)
