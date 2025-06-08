import os
import urllib.parse
import re
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass

from sql.core.unit_of_work import UnitOfWork
from sql.repositories.file_track_mapping_repository import FileTrackMappingRepository


@dataclass
class SpotifyUriInfo:
    """Information extracted from a Spotify URI"""
    uri_type: str  # 'track' or 'local'
    track_id: Optional[str] = None  # For regular tracks
    artist: Optional[str] = None  # For local files
    album: Optional[str] = None  # For local files
    title: Optional[str] = None  # For local files
    duration: Optional[int] = None  # For local files (seconds)
    raw_uri: str = ""


class SpotifyUriHelper:
    """Helper class for working with Spotify URIs"""

    @staticmethod
    def link_local_file_to_spotify_track(file_path: str, spotify_uri: str) -> Dict[str, Any]:
        """
        Link a local file to a Spotify track using URI.
        This is much simpler than the previous approach!
        """

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Verify the track exists in your Tracks table
        with UnitOfWork() as uow:
            track = uow.track_repository.get_by_uri(spotify_uri)
            if not track:
                raise ValueError(f"Track with URI '{spotify_uri}' not found in database")

        # Store the mapping in your enhanced FileTrackMappings table
        with UnitOfWork() as uow:
            mapping_repo = FileTrackMappingRepository(uow.connection)

            # Remove any existing mapping for this file
            mapping_repo.delete_by_file_path(file_path)

            # Add new mapping using URI instead of TrackId
            mapping_repo.add_mapping_by_uri(file_path, spotify_uri)

        return {
            'file_path': file_path,
            'spotify_uri': spotify_uri,
            'track_info': f"{track.artists} - {track.title}",
            'success': True
        }

    def get_spotify_uri_for_file(self, file_path: str) -> Optional[str]:
        """
        Get the Spotify URI for a local file.
        Much cleaner than the previous approach!
        """

        with UnitOfWork() as uow:
            mapping_repo = FileTrackMappingRepository(uow.connection)
            return mapping_repo.get_uri_by_file_path(file_path)

    def find_local_files_for_spotify_track(self, spotify_uri: str) -> list:
        """
        Find all local files linked to a specific Spotify track.
        """

        with UnitOfWork() as uow:
            mapping_repo = FileTrackMappingRepository(uow.connection)
            return mapping_repo.get_files_by_uri(spotify_uri)

    @staticmethod
    def parse_uri(spotify_uri: str) -> SpotifyUriInfo:
        """
        Parse a Spotify URI into its components.

        Args:
            spotify_uri: Spotify URI (e.g., 'spotify:track:id' or 'spotify:local:artist:album:title:duration')

        Returns:
            SpotifyUriInfo object with parsed components
        """
        if not spotify_uri or not spotify_uri.startswith('spotify:'):
            raise ValueError(f"Invalid Spotify URI: {spotify_uri}")

        parts = spotify_uri.split(':')

        if len(parts) < 3:
            raise ValueError(f"Invalid Spotify URI format: {spotify_uri}")

        uri_type = parts[1]

        if uri_type == 'track':
            # Regular track: spotify:track:4iV5W9uYEdYUVa79Axb7Rh
            if len(parts) != 3:
                raise ValueError(f"Invalid track URI format: {spotify_uri}")

            return SpotifyUriInfo(
                uri_type='track',
                track_id=parts[2],
                raw_uri=spotify_uri
            )

        elif uri_type == 'local':
            # Local file: spotify:local:artist:album:title:duration
            if len(parts) != 6:
                raise ValueError(f"Invalid local URI format: {spotify_uri}")

            artist = urllib.parse.unquote_plus(parts[2]) if parts[2] else ""
            album = urllib.parse.unquote_plus(parts[3]) if parts[3] else ""
            title = urllib.parse.unquote_plus(parts[4]) if parts[4] else ""

            # Parse duration (should be in seconds)
            duration = None
            try:
                duration = int(parts[5]) if parts[5] else None
            except ValueError:
                pass

            return SpotifyUriInfo(
                uri_type='local',
                artist=artist,
                album=album,
                title=title,
                duration=duration,
                raw_uri=spotify_uri
            )
        else:
            raise ValueError(f"Unsupported URI type: {uri_type}")

    @staticmethod
    def create_track_uri(track_id: str) -> str:
        """
        Create a Spotify track URI from a track ID.

        Args:
            track_id: Spotify track ID

        Returns:
            Spotify track URI
        """
        if not track_id:
            raise ValueError("Track ID cannot be empty")

        return f"spotify:track:{track_id}"

    @staticmethod
    def create_local_uri(artist: str, album: str, title: str, duration: int = None) -> str:
        """
        Create a Spotify local file URI.

        Args:
            artist: Artist name
            album: Album name (can be empty)
            title: Track title
            duration: Duration in seconds (optional)

        Returns:
            Spotify local file URI
        """
        # URL encode the components
        artist_encoded = urllib.parse.quote_plus(artist or "")
        album_encoded = urllib.parse.quote_plus(album or "")
        title_encoded = urllib.parse.quote_plus(title or "")
        duration_str = str(duration) if duration else "0"

        return f"spotify:local:{artist_encoded}:{album_encoded}:{title_encoded}:{duration_str}"

    @staticmethod
    def is_local_uri(spotify_uri: str) -> bool:
        """Check if a Spotify URI is for a local file."""
        return spotify_uri.startswith('spotify:local:')

    @staticmethod
    def is_track_uri(spotify_uri: str) -> bool:
        """Check if a Spotify URI is for a regular track."""
        return spotify_uri.startswith('spotify:track:')

    @staticmethod
    def extract_track_id(spotify_uri: str) -> Optional[str]:
        """
        Extract track ID from a regular Spotify track URI.

        Args:
            spotify_uri: Spotify URI

        Returns:
            Track ID if it's a track URI, None otherwise
        """
        if SpotifyUriHelper.is_track_uri(spotify_uri):
            return spotify_uri.split(':')[2]
        return None

    @staticmethod
    def normalize_local_metadata(artist: str, title: str) -> Tuple[str, str]:
        """
        Normalize artist and title for consistent matching.

        Args:
            artist: Artist name
            title: Track title

        Returns:
            Tuple of (normalized_artist, normalized_title)
        """

        # Remove common patterns that might cause matching issues
        def normalize_string(s: str) -> str:
            if not s:
                return ""

            # Convert to lowercase
            s = s.lower().strip()

            # Remove common brackets and parentheses content
            s = re.sub(r'\[.*?\]', '', s)  # Remove [content]
            s = re.sub(r'\(.*?\)', '', s)  # Remove (content)

            # Normalize spaces and special characters
            s = re.sub(r'\s+', ' ', s)  # Multiple spaces to single space
            s = re.sub(r'[^\w\s&-]', '', s)  # Keep only alphanumeric, spaces, &, -

            # Common replacements
            s = s.replace('&', 'and')
            s = s.replace('-', ' ')

            return s.strip()

        return normalize_string(artist), normalize_string(title)

    @staticmethod
    def generate_filename_variations(artist: str, title: str) -> list[str]:
        """
        Generate possible filename variations for a local track.

        Args:
            artist: Artist name
            title: Track title

        Returns:
            List of possible filename patterns
        """
        norm_artist, norm_title = SpotifyUriHelper.normalize_local_metadata(artist, title)

        variations = []

        # Basic combinations
        if norm_artist and norm_title:
            variations.extend([
                f"{norm_artist} - {norm_title}",
                f"{norm_title} - {norm_artist}",
                f"{norm_artist}_{norm_title}",
                f"{norm_title}_{norm_artist}",
                f"{norm_artist} {norm_title}",
                f"{norm_title} {norm_artist}",
            ])

        # Title only
        if norm_title:
            variations.append(norm_title)

        # Original versions (non-normalized)
        orig_artist = artist.lower().strip() if artist else ""
        orig_title = title.lower().strip() if title else ""

        if orig_artist and orig_title:
            variations.extend([
                f"{orig_artist} - {orig_title}",
                f"{orig_title} - {orig_artist}",
                f"{orig_artist}_{orig_title}",
                f"{orig_title}_{orig_artist}",
            ])

        if orig_title:
            variations.append(orig_title)

        # Remove duplicates and empty strings
        return list(set(v for v in variations if v))

    @staticmethod
    def match_local_file_to_uri(filename: str, local_uris: list[str],
                                threshold: float = 0.8) -> Optional[str]:
        """
        Match a filename to a local Spotify URI using fuzzy matching.

        Args:
            filename: Local filename (without extension)
            local_uris: List of local Spotify URIs to match against
            threshold: Minimum similarity threshold

        Returns:
            Best matching URI if found, None otherwise
        """
        import Levenshtein

        filename_lower = filename.lower().strip()
        best_match = None
        best_score = 0

        for uri in local_uris:
            try:
                uri_info = SpotifyUriHelper.parse_uri(uri)
                if uri_info.uri_type != 'local':
                    continue

                # Generate variations for this URI
                variations = SpotifyUriHelper.generate_filename_variations(
                    uri_info.artist or "", uri_info.title or ""
                )

                # Check each variation
                for variation in variations:
                    similarity = Levenshtein.ratio(filename_lower, variation)
                    if similarity > best_score and similarity >= threshold:
                        best_score = similarity
                        best_match = uri

            except ValueError:
                continue  # Skip invalid URIs

        return best_match


# Example usage and testing functions
def test_spotify_uri_helper():
    """Test the SpotifyUriHelper functions"""

    # Test regular track URI
    track_uri = "spotify:track:4iV5W9uYEdYUVa79Axb7Rh"
    track_info = SpotifyUriHelper.parse_uri(track_uri)
    print(f"Track URI: {track_info}")

    # Test local file URI
    local_uri = "spotify:local:DUZA::Whitney+Houston+-+Love+Will+Save+The+Day+%28DUZA+Edit%29+%5BFree+Download%5D:347"
    local_info = SpotifyUriHelper.parse_uri(local_uri)
    print(f"Local URI: {local_info}")

    # Test creating URIs
    created_track = SpotifyUriHelper.create_track_uri("4iV5W9uYEdYUVa79Axb7Rh")
    created_local = SpotifyUriHelper.create_local_uri("DUZA", "",
                                                      "Whitney Houston - Love Will Save The Day (DUZA Edit)", 347)
    print(f"Created track URI: {created_track}")
    print(f"Created local URI: {created_local}")

    # Test filename variations
    variations = SpotifyUriHelper.generate_filename_variations("DUZA",
                                                               "Whitney Houston - Love Will Save The Day (DUZA Edit)")
    print(f"Filename variations: {variations}")


if __name__ == "__main__":
    test_spotify_uri_helper()
