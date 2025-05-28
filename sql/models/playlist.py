from typing import List, Optional


class Playlist:
    """
    Domain model representing a playlist in the music library.
    Contains business logic and relationships related to playlists.
    """

    def __init__(self, playlist_id: str, name: str, snapshot_id: str = None, associations_snapshot_id: str = None):
        """
        Initialize a new Playlist instance.

        Args:
            playlist_id: Unique Spotify identifier for the playlist
            name: Name of the playlist
            snapshot_id: Spotify snapshot ID for tracking changes
            associations_snapshot_id: Snapshot ID since last time associations were synced
        """
        self.playlist_id = playlist_id
        self.name = name
        self.snapshot_id = snapshot_id
        self.associations_snapshot_id = associations_snapshot_id or ""
        self.tracks = []

    def add_track(self, track) -> None:
        """
        Add a track to this playlist if it's not already there.

        Args:
            track: The Track object to add to this playlist
        """
        if track not in self.tracks:
            self.tracks.append(track)
            # Ensure bidirectional relationship
            track.add_to_playlist(self)

    def remove_track(self, track) -> bool:
        """
        Remove a track from this playlist.

        Args:
            track: The Track object to remove from this playlist

        Returns:
            bool: True if the track was removed, False if it wasn't in the playlist
        """
        if track in self.tracks:
            self.tracks.remove(track)
            # Update the track's playlist list
            track.remove_from_playlist(self)
            return True
        return False

    def contains_track(self, track) -> bool:
        """
        Check if this playlist contains a specific track.

        Args:
            track: The Track object to check for

        Returns:
            bool: True if the playlist contains the track, False otherwise
        """
        return track in self.tracks

    def get_track_count(self) -> int:
        """
        Get the number of tracks in this playlist.

        Returns:
            int: Number of tracks
        """
        return len(self.tracks)

    def __str__(self) -> str:
        """String representation of the playlist."""
        return self.name

    def __eq__(self, other) -> bool:
        """Two playlists are equal if they have the same playlist_id."""
        if not isinstance(other, Playlist):
            return False
        return self.playlist_id == other.playlist_id

    def __hash__(self) -> int:
        """Hash based on playlist_id for use in sets and as dict keys."""
        return hash(self.playlist_id)
