"""
Helper functions for deduplicating tracks in Unchartify playlists.
"""

import re
import logging
from typing import List, Dict, Tuple, Set, Optional
import unicodedata
import Levenshtein
from spotipy import Spotify

from utils.logger import setup_logger

# Set up logging
dedup_logger = setup_logger('deduplication_helper', 'logs/deduplication.log')

# Keywords that indicate a track is likely a shorter/radio version
DEPRIORITIZE_KEYWORDS = [
    'radio', 'edit', 'short', 'radio edit', 'single', 'clean',
    'radio mix', 'radio version', 'shortened', 'album version'
]

# Keywords that indicate a track is likely an extended/full version
PRIORITIZE_KEYWORDS = [
    'extended', 'original', 'club', 'full', 'original mix', 'extended mix',
    'club mix', 'original version', 'full length', 'original club mix'
]


def is_unchartify_playlist(playlist_name: str) -> bool:
    """
    Check if a playlist is an Unchartify playlist.

    Args:
        playlist_name: Name of the playlist to check

    Returns:
        True if the playlist is an Unchartify playlist
    """
    return playlist_name.strip().upper().startswith("UNCHARTIFY:")


def normalize_track_name(track_name: str) -> str:
    """
    Normalize a track name for comparison by:
    - Converting to lowercase
    - Removing common punctuation
    - Removing extra whitespace
    - Removing accent marks

    Args:
        track_name: The track name to normalize

    Returns:
        Normalized track name
    """
    # Convert to lowercase and trim whitespace
    normalized = track_name.lower().strip()

    # Remove accent marks and normalize Unicode characters
    normalized = ''.join(
        c for c in unicodedata.normalize('NFD', normalized)
        if not unicodedata.combining(c)
    )

    # Remove common punctuation
    normalized = re.sub(r'[^\w\s]', ' ', normalized)

    # Replace multiple spaces with a single space
    normalized = re.sub(r'\s+', ' ', normalized)

    # Remove any bracketed/parenthesized content that might indicate versions
    # This helps with matching the base track name
    base_track = re.sub(r'\[.*?\]|\(.*?\)', '', normalized).strip()

    return base_track


def are_similar_tracks(track1: dict, track2: dict, similarity_threshold: float = 0.85) -> bool:
    """
    Determine if two tracks are similar enough to be considered duplicates.

    Args:
        track1: First track metadata
        track2: Second track metadata
        similarity_threshold: Threshold for Levenshtein ratio (0-1)

    Returns:
        True if tracks are similar enough to be duplicates
    """
    # Extract track names and artist names
    name1 = track1.get('name', '')
    name2 = track2.get('name', '')

    # Normalize track names
    norm_name1 = normalize_track_name(name1)
    norm_name2 = normalize_track_name(name2)

    # Get artists (use primary artist)
    artist1 = track1.get('artists', [{}])[0].get('name', '') if track1.get('artists') else ''
    artist2 = track2.get('artists', [{}])[0].get('name', '') if track2.get('artists') else ''

    # If we have different artists, they're different tracks
    if artist1.lower() != artist2.lower():
        return False

    # Calculate similarity using Levenshtein distance
    name_similarity = Levenshtein.ratio(norm_name1, norm_name2)

    # Check if names are similar enough
    return name_similarity >= similarity_threshold


def group_similar_tracks(tracks: List[dict]) -> List[List[dict]]:
    """
    Group similar tracks together into clusters.

    Args:
        tracks: List of track metadata dictionaries

    Returns:
        List of lists, where each inner list contains similar tracks
    """
    if not tracks:
        return []

    dedup_logger.info(f"Grouping {len(tracks)} tracks into similar clusters")

    # Initialize groups with the first track
    groups = [[tracks[0]]]

    # For each remaining track, check if it belongs to an existing group
    for track in tracks[1:]:
        found_group = False

        for group in groups:
            # Compare with the first track in the group
            if are_similar_tracks(track, group[0]):
                group.append(track)
                found_group = True
                break

        # If not found in any group, create a new group
        if not found_group:
            groups.append([track])

    dedup_logger.info(f"Found {len(groups)} unique tracks/groups")
    return groups


def rank_track_versions(track_group: List[dict]) -> List[dict]:
    """
    Rank different versions of the same track, prioritizing:
    1. Extended/original/club mixes
    2. Longer durations
    3. Avoiding radio edits/shortened versions

    Args:
        track_group: List of track metadata dictionaries for the same track

    Returns:
        Ranked list of tracks, best version first
    """

    def calculate_track_score(track):
        # Start with basic score based on duration
        duration_ms = track.get('duration_ms', 0)
        score = duration_ms / 1000  # Convert to seconds as base score

        name = track.get('name', '').lower()

        # Bonus for tracks with priority keywords
        for keyword in PRIORITIZE_KEYWORDS:
            if keyword.lower() in name:
                score += 120  # Add 2 minutes worth of points for prioritized versions

        # Penalty for tracks with deprioritize keywords
        for keyword in DEPRIORITIZE_KEYWORDS:
            if keyword.lower() in name:
                score -= 180  # Subtract 3 minutes worth of points for deprioritized versions

        return score

    # Sort the tracks by score, highest first
    ranked_tracks = sorted(track_group, key=calculate_track_score, reverse=True)

    # Log the ranking details
    if len(ranked_tracks) > 1:
        best = ranked_tracks[0]
        track_name = best.get('name', 'Unknown')
        artist_name = best.get('artists', [{}])[0].get('name', 'Unknown') if best.get('artists') else 'Unknown'
        dedup_logger.info(f"Best version of '{track_name}' by {artist_name}:")
        dedup_logger.info(f"  Duration: {best.get('duration_ms') / 1000:.2f}s, Score: {calculate_track_score(best)}")

        for i, track in enumerate(ranked_tracks[1:], 1):
            dedup_logger.info(f"  Alternative #{i}: {track.get('name')}")
            dedup_logger.info(
                f"    Duration: {track.get('duration_ms') / 1000:.2f}s, Score: {calculate_track_score(track)}")

    return ranked_tracks


def deduplicate_playlist(spotify_client: Spotify, playlist_id: str) -> Tuple[List[dict], List[dict]]:
    """
    Identify duplicates in a playlist and return tracks to keep and remove.

    Args:
        spotify_client: Authenticated Spotify client
        playlist_id: ID of the playlist to deduplicate

    Returns:
        Tuple of (tracks_to_keep, tracks_to_remove)
    """
    dedup_logger.info(f"Deduplicating playlist {playlist_id}")

    # Get all tracks in the playlist
    tracks = []
    offset = 0
    limit = 100  # Spotify API limit

    while True:
        results = spotify_client.playlist_items(
            playlist_id,
            offset=offset,
            limit=limit,
            fields='items(track(id,name,artists(name),duration_ms)),total'
        )

        if not results['items']:
            break

        tracks.extend([item['track'] for item in results['items'] if item['track']])
        offset += limit

        if offset >= results['total']:
            break

    dedup_logger.info(f"Found {len(tracks)} tracks in playlist")

    # Group similar tracks
    track_groups = group_similar_tracks(tracks)

    # For each group, select the best version
    tracks_to_keep = []
    tracks_to_remove = []

    for group in track_groups:
        if len(group) == 1:
            # No duplicates for this track
            tracks_to_keep.append(group[0])
        else:
            # Rank the versions and keep the best one
            ranked_tracks = rank_track_versions(group)
            tracks_to_keep.append(ranked_tracks[0])
            tracks_to_remove.extend(ranked_tracks[1:])

    dedup_logger.info(f"Keeping {len(tracks_to_keep)} tracks, removing {len(tracks_to_remove)} duplicates")
    return tracks_to_keep, tracks_to_remove


def remove_tracks_from_playlist(spotify_client: Spotify, playlist_id: str, tracks_to_remove: List[dict]) -> bool:
    """
    Remove tracks from a Spotify playlist.

    Args:
        spotify_client: Authenticated Spotify client
        playlist_id: ID of the playlist
        tracks_to_remove: List of track objects to remove

    Returns:
        True if successful
    """
    if not tracks_to_remove:
        dedup_logger.info("No tracks to remove")
        return True

    dedup_logger.info(f"Removing {len(tracks_to_remove)} tracks from playlist {playlist_id}")

    # Extract track IDs and convert to Spotify's expected format
    track_ids = [track['id'] for track in tracks_to_remove if track.get('id')]

    # Remove in batches of 100 (Spotify API limit)
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i + 100]
        try:
            spotify_client.playlist_remove_all_occurrences_of_items(playlist_id, batch)
            dedup_logger.info(f"Removed batch of {len(batch)} tracks from playlist")
        except Exception as e:
            dedup_logger.error(f"Error removing tracks from playlist: {e}")
            return False

    return True


def format_duration(ms: int) -> str:
    """
    Format milliseconds as MM:SS.

    Args:
        ms: Duration in milliseconds

    Returns:
        Formatted duration string
    """
    total_seconds = ms // 1000
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def get_track_display_info(track: dict) -> str:
    """
    Get formatted track information for display.

    Args:
        track: Track metadata

    Returns:
        Formatted track string
    """
    name = track.get('name', 'Unknown')
    artist = track.get('artists', [{}])[0].get('name', 'Unknown') if track.get('artists') else 'Unknown'
    duration = format_duration(track.get('duration_ms', 0))
    return f"{artist} - {name} ({duration})"
