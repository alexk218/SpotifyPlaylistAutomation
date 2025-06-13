# Create this file as: api/services/duplicate_track_service.py

import os
from typing import List, Dict, Any, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

import Levenshtein
from sql.core.unit_of_work import UnitOfWork
from sql.models.track import Track
from utils.logger import setup_logger

duplicate_logger = setup_logger('duplicate_tracks', 'sql', 'duplicate_tracks.log')


@dataclass
class DuplicateGroup:
    """Represents a group of duplicate tracks."""
    tracks: List[Track]
    primary_track: Track  # The track to keep (longest)
    duplicates_to_remove: List[Track]  # Tracks to remove
    playlists_to_merge: Set[str]  # All playlists these tracks belong to

    def get_track_ids_to_remove(self) -> List[str]:
        """Get track IDs of duplicates to remove."""
        return [track.track_id for track in self.duplicates_to_remove if track.track_id]

    def get_uris_to_remove(self) -> List[str]:
        """Get URIs of duplicates to remove."""
        return [track.uri for track in self.duplicates_to_remove if track.uri]


class DuplicateTrackDetector:
    """Detects and manages duplicate tracks in the database."""

    def __init__(self):
        self.similarity_threshold = 0.95  # Very high threshold for duplicates

    def find_all_duplicates(self) -> List[DuplicateGroup]:
        """
        Find all duplicate tracks in the database.

        Returns:
            List of DuplicateGroup objects representing sets of duplicate tracks
        """
        duplicate_logger.info("Starting duplicate track detection")

        try:
            with UnitOfWork() as uow:
                all_tracks = uow.track_repository.get_all()
                duplicate_logger.info(f"Retrieved {len(all_tracks)} tracks from database")

                track_playlist_map = self._get_track_playlist_mapping(uow)
                duplicate_logger.info(f"Retrieved playlist mappings for {len(track_playlist_map)} track URIs")
        except Exception as e:
            duplicate_logger.error(f"Error retrieving data from database: {e}")
            raise

        duplicate_logger.info(f"Analyzing {len(all_tracks)} tracks for duplicates")

        try:
            # Group tracks by potential duplicates
            potential_duplicates = self._group_potential_duplicates(all_tracks)
            duplicate_logger.info(f"Found {len(potential_duplicates)} potential duplicate groups")

            # Verify and create duplicate groups
            duplicate_groups = []
            for group in potential_duplicates:
                if len(group) > 1:
                    duplicate_group = self._create_duplicate_group(group, track_playlist_map)
                    if duplicate_group:
                        duplicate_groups.append(duplicate_group)

            duplicate_logger.info(f"Found {len(duplicate_groups)} confirmed duplicate groups")
            return duplicate_groups
        except Exception as e:
            duplicate_logger.error(f"Error during duplicate analysis: {e}")
            raise

    def _group_potential_duplicates(self, tracks: List[Track]) -> List[List[Track]]:
        """Group tracks that might be duplicates based on artist and title similarity."""
        groups = []
        processed = set()

        for i, track1 in enumerate(tracks):
            # Use URI as the unique identifier since TrackId might be None
            track1_key = track1.uri or f"track_{i}"
            if track1_key in processed:
                continue

            current_group = [track1]
            processed.add(track1_key)

            for j, track2 in enumerate(tracks[i + 1:], i + 1):
                track2_key = track2.uri or f"track_{j}"
                if track2_key in processed:
                    continue

                if self._are_likely_duplicates(track1, track2):
                    current_group.append(track2)
                    processed.add(track2_key)

            if len(current_group) > 1:
                groups.append(current_group)

        return groups

    def _are_likely_duplicates(self, track1: Track, track2: Track) -> bool:
        """Check if two tracks are likely duplicates."""
        # Skip if one has no data
        if not all([track1.title, track1.artists, track2.title, track2.artists]):
            return False

        # Normalize for comparison
        title1 = self._normalize_title(track1.title)
        title2 = self._normalize_title(track2.title)
        artists1 = self._normalize_artists(track1.artists)
        artists2 = self._normalize_artists(track2.artists)

        # Calculate similarities
        title_similarity = Levenshtein.ratio(title1, title2)
        artist_similarity = Levenshtein.ratio(artists1, artists2)

        # Both title and artist must be very similar
        is_duplicate = (title_similarity >= self.similarity_threshold and
                        artist_similarity >= self.similarity_threshold)

        if is_duplicate:
            duplicate_logger.debug(
                f"Potential duplicate found: '{track1.artists} - {track1.title}' vs '{track2.artists} - {track2.title}' (similarities: title={title_similarity:.3f}, artist={artist_similarity:.3f})")

        return is_duplicate

    def _normalize_title(self, title: str) -> str:
        """Normalize track title for comparison."""
        if not title:
            return ""

        # Convert to lowercase and remove common variations
        normalized = title.lower().strip()

        # Remove common parenthetical additions that might differ
        # but preserve actual remix info
        import re

        # Remove things like "(Explicit)", "(Radio Edit)", "(Clean)", etc.
        patterns_to_remove = [
            r'\s*\(explicit\)',
            r'\s*\(clean\)',
            r'\s*\(radio edit\)',
            r'\s*\(radio version\)',
            r'\s*\(album version\)',
            r'\s*\(original mix\)',
            r'\s*\(remaster\)',
            r'\s*\(remastered\)',
        ]

        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)

        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized

    def _normalize_artists(self, artists: str) -> str:
        """Normalize artist names for comparison."""
        if not artists:
            return ""

        # Convert to lowercase, sort artists alphabetically
        artist_list = [artist.strip().lower() for artist in artists.split(',')]
        artist_list.sort()

        return ', '.join(artist_list)

    def _create_duplicate_group(self, tracks: List[Track], track_playlist_map: Dict[str, Set[str]]) -> Optional[
        DuplicateGroup]:
        """Create a DuplicateGroup from a list of potential duplicate tracks."""
        if len(tracks) < 2:
            return None

        # Get track durations from Spotify if needed (for now, use a heuristic)
        # The primary track should be the one with the most complete information
        # and preferably the one that's not local

        primary_track = self._select_primary_track(tracks)
        duplicates_to_remove = [t for t in tracks if t != primary_track]

        # Get all playlists these tracks belong to
        all_playlists = set()
        for track in tracks:
            if track.uri:
                playlists = track_playlist_map.get(track.uri, set())
                all_playlists.update(playlists)

        duplicate_logger.info(
            f"Duplicate group: keeping '{primary_track.artists} - {primary_track.title}' ({primary_track.uri}), removing {len(duplicates_to_remove)} duplicates")

        return DuplicateGroup(
            tracks=tracks,
            primary_track=primary_track,
            duplicates_to_remove=duplicates_to_remove,
            playlists_to_merge=all_playlists
        )

    def _select_primary_track(self, tracks: List[Track]) -> Track:
        """Select which track to keep as the primary (longest/best quality)."""
        # Prioritize non-local tracks
        non_local_tracks = [t for t in tracks if not t.is_local]
        if non_local_tracks:
            tracks_to_consider = non_local_tracks
        else:
            tracks_to_consider = tracks

        # For now, use a simple heuristic: prefer tracks with TrackId over those without
        # and prefer tracks with more complete album information
        def track_score(track: Track) -> Tuple[int, int, int]:
            score = 0

            # Prefer tracks with TrackId
            if track.track_id:
                score += 100

            # Prefer tracks with album information
            if track.album and track.album.strip():
                score += 50

            # Prefer tracks added earlier (more likely to be the original)
            if track.added_to_master:
                # Convert to score (earlier = higher score)
                score += 10

            return (score, len(track.album or ''), len(track.title or ''))

        # Sort by score (descending) and return the best one
        tracks_to_consider.sort(key=track_score, reverse=True)

        return tracks_to_consider[0]

    def _get_track_playlist_mapping(self, uow) -> Dict[str, Set[str]]:
        """Get a mapping of track URIs to the playlists they belong to."""
        track_playlist_map = defaultdict(set)

        try:
            # Try to use the existing repository method
            all_playlist_mappings = uow.track_playlist_repository.get_all_playlist_track_mappings()

            for playlist_id, uris in all_playlist_mappings.items():
                for uri in uris:
                    track_playlist_map[uri].add(playlist_id)
        except AttributeError:
            # Fallback: iterate through all playlists and get their tracks
            duplicate_logger.info("Using fallback method to get track-playlist mappings")
            all_playlists = uow.playlist_repository.get_all()

            for playlist in all_playlists:
                try:
                    track_uris = uow.track_playlist_repository.get_uris_for_playlist(playlist.playlist_id)
                    for uri in track_uris:
                        track_playlist_map[uri].add(playlist.playlist_id)
                except Exception as e:
                    duplicate_logger.warning(f"Error getting tracks for playlist {playlist.playlist_id}: {e}")
                    continue

        return dict(track_playlist_map)


class DuplicateTrackCleaner:
    """Handles the cleanup of duplicate tracks."""

    def __init__(self):
        pass

    def cleanup_duplicates(self, duplicate_groups: List[DuplicateGroup], dry_run: bool = False) -> Dict[str, Any]:
        """
        Clean up duplicate tracks by removing duplicates and merging playlist associations.

        Args:
            duplicate_groups: List of DuplicateGroup objects to clean up
            dry_run: If True, only analyze what would be done without making changes

        Returns:
            Dictionary with cleanup results
        """
        if not duplicate_groups:
            return {
                "success": True,
                "message": "No duplicates found to clean up",
                "tracks_removed": 0,
                "playlists_merged": 0,
                "dry_run": dry_run
            }

        duplicate_logger.info(f"Starting cleanup of {len(duplicate_groups)} duplicate groups (dry_run={dry_run})")

        tracks_removed = 0
        playlists_merged = 0
        cleanup_details = []

        if not dry_run:
            with UnitOfWork() as uow:
                for group in duplicate_groups:
                    result = self._cleanup_duplicate_group(uow, group)
                    tracks_removed += result['tracks_removed']
                    playlists_merged += result['playlists_merged']
                    cleanup_details.append(result)
        else:
            # Dry run - just analyze
            for group in duplicate_groups:
                result = self._analyze_duplicate_group(group)
                tracks_removed += result['tracks_removed']
                playlists_merged += result['playlists_merged']
                cleanup_details.append(result)

        return {
            "success": True,
            "message": f"Cleaned up {len(duplicate_groups)} duplicate groups",
            "duplicate_groups_processed": len(duplicate_groups),
            "tracks_removed": tracks_removed,
            "playlists_merged": playlists_merged,
            "dry_run": dry_run,
            "details": cleanup_details[:10]  # Limit details for large operations
        }

    def _cleanup_duplicate_group(self, uow, group: DuplicateGroup) -> Dict[str, Any]:
        """Clean up a single duplicate group."""
        primary_uri = group.primary_track.uri

        # Get current playlists for the primary track
        current_primary_playlists = set(uow.track_playlist_repository.get_playlist_ids_for_uri(primary_uri))

        # Get all playlists from duplicates
        all_duplicate_playlists = set()
        for duplicate in group.duplicates_to_remove:
            if duplicate.uri:
                playlists = uow.track_playlist_repository.get_playlist_ids_for_uri(duplicate.uri)
                all_duplicate_playlists.update(playlists)

        # Find playlists to add to primary track
        playlists_to_add = all_duplicate_playlists - current_primary_playlists

        # Add primary track to additional playlists
        playlists_added = 0
        for playlist_id in playlists_to_add:
            try:
                uow.track_playlist_repository.insert_by_uri(primary_uri, playlist_id)
                playlists_added += 1
                duplicate_logger.info(f"Added primary track {primary_uri} to playlist {playlist_id}")
            except Exception as e:
                duplicate_logger.error(f"Failed to add primary track to playlist {playlist_id}: {e}")

        # Remove duplicate tracks from all playlists
        for duplicate in group.duplicates_to_remove:
            if duplicate.uri:
                # Remove from all playlists
                duplicate_playlists = uow.track_playlist_repository.get_playlist_ids_for_uri(duplicate.uri)
                for playlist_id in duplicate_playlists:
                    uow.track_playlist_repository.delete_by_uri(duplicate.uri, playlist_id)

                # Remove from tracks table
                uow.track_repository.delete_by_uri(duplicate.uri)
                duplicate_logger.info(
                    f"Removed duplicate track: {duplicate.artists} - {duplicate.title} ({duplicate.uri})")

        return {
            "primary_track": f"{group.primary_track.artists} - {group.primary_track.title}",
            "primary_uri": primary_uri,
            "tracks_removed": len(group.duplicates_to_remove),
            "playlists_merged": playlists_added,
            "removed_tracks": [f"{d.artists} - {d.title}" for d in group.duplicates_to_remove]
        }

    def _analyze_duplicate_group(self, group: DuplicateGroup) -> Dict[str, Any]:
        """Analyze what would be done for a duplicate group (dry run)."""
        return {
            "primary_track": f"{group.primary_track.artists} - {group.primary_track.title}",
            "primary_uri": group.primary_track.uri,
            "tracks_removed": len(group.duplicates_to_remove),
            "playlists_merged": len(group.playlists_to_merge),  # Approximate
            "removed_tracks": [f"{d.artists} - {d.title}" for d in group.duplicates_to_remove]
        }


def detect_and_cleanup_duplicate_tracks(dry_run: bool = False) -> Dict[str, Any]:
    """
    Main function to detect and clean up duplicate tracks.

    Args:
        dry_run: If True, only analyze what would be done without making changes

    Returns:
        Dictionary with operation results
    """
    try:
        # Detect duplicates
        detector = DuplicateTrackDetector()
        duplicate_groups = detector.find_all_duplicates()

        if not duplicate_groups:
            return {
                "success": True,
                "message": "No duplicate tracks found",
                "duplicate_groups_found": 0,
                "tracks_removed": 0,
                "playlists_merged": 0,
                "dry_run": dry_run
            }

        # Clean up duplicates
        cleaner = DuplicateTrackCleaner()
        cleanup_result = cleaner.cleanup_duplicates(duplicate_groups, dry_run=dry_run)

        cleanup_result["duplicate_groups_found"] = len(duplicate_groups)
        return cleanup_result

    except Exception as e:
        duplicate_logger.error(f"Error in duplicate track cleanup: {e}")
        return {
            "success": False,
            "message": f"Error during duplicate cleanup: {str(e)}",
            "error": str(e)
        }


def get_duplicate_tracks_report() -> Dict[str, Any]:
    """
    Generate a report of duplicate tracks without making any changes.

    Returns:
        Dictionary with duplicate tracks information
    """
    try:
        detector = DuplicateTrackDetector()
        duplicate_groups = detector.find_all_duplicates()

        if not duplicate_groups:
            return {
                "success": True,
                "message": "No duplicate tracks found",
                "duplicate_groups": [],
                "total_duplicates": 0
            }

        # Format duplicate groups for display
        formatted_groups = []
        total_duplicates = 0

        for group in duplicate_groups:
            formatted_group = {
                "primary_track": {
                    "title": group.primary_track.title,
                    "artists": group.primary_track.artists,
                    "album": group.primary_track.album,
                    "uri": group.primary_track.uri,
                    "track_id": group.primary_track.track_id
                },
                "duplicates": [],
                "playlists_affected": list(group.playlists_to_merge),
                "total_tracks_in_group": len(group.tracks)
            }

            for duplicate in group.duplicates_to_remove:
                formatted_group["duplicates"].append({
                    "title": duplicate.title,
                    "artists": duplicate.artists,
                    "album": duplicate.album,
                    "uri": duplicate.uri,
                    "track_id": duplicate.track_id
                })
                total_duplicates += 1

            formatted_groups.append(formatted_group)

        return {
            "success": True,
            "message": f"Found {len(duplicate_groups)} duplicate groups with {total_duplicates} tracks to remove",
            "duplicate_groups": formatted_groups,
            "total_groups": len(duplicate_groups),
            "total_duplicates": total_duplicates
        }

    except Exception as e:
        duplicate_logger.error(f"Error generating duplicate tracks report: {e}")
        return {
            "success": False,
            "message": f"Error generating report: {str(e)}",
            "error": str(e)
        }
