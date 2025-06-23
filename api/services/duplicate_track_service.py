import hashlib
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Any, Set, Tuple, Optional

import Levenshtein

from sql.core.unit_of_work import UnitOfWork
from sql.models.track import Track
from utils.logger import setup_logger

duplicate_logger = setup_logger('duplicate_tracks', 'sql', 'duplicate_tracks.log')


@dataclass
class DuplicateGroup:
    """Represents a group of duplicate tracks."""
    tracks: List[Track]
    primary_track: Track  # The track to keep (longest or user-selected)
    duplicates_to_remove: List[Track]  # Tracks to remove
    playlists_to_merge: Set[str]  # All playlists these tracks belong to
    requires_user_selection: bool = False  # Whether this group needs manual selection
    group_id: str = ""  # Unique identifier for this group

    def get_track_ids_to_remove(self) -> List[str]:
        """Get track IDs of duplicates to remove."""
        return [track.track_id for track in self.duplicates_to_remove if track.track_id]

    def get_uris_to_remove(self) -> List[str]:
        """Get URIs of duplicates to remove."""
        return [track.uri for track in self.duplicates_to_remove if track.uri]


class DuplicateTrackDetector:
    """Optimized duplicate track detection using fingerprinting."""

    def __init__(self):
        self.similarity_threshold = 0.95

    def find_all_duplicates(self) -> List[DuplicateGroup]:
        """Find all duplicate tracks using optimized fingerprinting approach."""
        duplicate_logger.info("Starting duplicate track detection")

        try:
            with UnitOfWork() as uow:
                all_tracks = uow.track_repository.get_all()
                duplicate_logger.info(f"Retrieved {len(all_tracks)} tracks from database")

                # Get playlist mappings in batch
                track_playlist_map = self._get_track_playlist_mapping_batch(uow)
                duplicate_logger.info(f"Retrieved playlist mappings for {len(track_playlist_map)} track URIs")
        except Exception as e:
            duplicate_logger.error(f"Error retrieving data from database: {e}")
            raise

        # Use fingerprinting for fast duplicate detection
        duplicate_groups = self._find_duplicates_with_fingerprinting(all_tracks, track_playlist_map)
        duplicate_logger.info(f"Found {len(duplicate_groups)} confirmed duplicate groups")

        return duplicate_groups

    def _find_duplicates_with_fingerprinting(self, tracks: List[Track], track_playlist_map: Dict[str, Set[str]]) -> \
            List[DuplicateGroup]:
        """Use fingerprinting to quickly identify potential duplicates."""
        # Create fingerprints for fast grouping
        fingerprint_groups = defaultdict(list)

        for track in tracks:
            if not track.title or not track.artists:
                continue

            fingerprint = self._create_track_fingerprint(track)
            fingerprint_groups[fingerprint].append(track)

        duplicate_groups = []

        # Process each fingerprint group
        for fingerprint, group_tracks in fingerprint_groups.items():
            if len(group_tracks) < 2:
                continue

            # Within each fingerprint group, do detailed similarity checking
            verified_groups = self._verify_duplicates_in_group(group_tracks)

            for verified_group in verified_groups:
                if len(verified_group) > 1:
                    duplicate_group = self._create_duplicate_group(verified_group, track_playlist_map)
                    if duplicate_group:
                        duplicate_groups.append(duplicate_group)

        return duplicate_groups

    def _create_track_fingerprint(self, track: Track) -> str:
        """Create a fingerprint for fast grouping of similar tracks."""
        # Normalize title and artist for fingerprinting
        title_normalized = self._normalize_for_fingerprint(track.title)
        artists_normalized = self._normalize_for_fingerprint(track.artists)

        # Create a hash that groups similar tracks together
        fingerprint_string = f"{artists_normalized}||{title_normalized}"

        # Use first 8 characters of hash for grouping (balances speed vs accuracy)
        return hashlib.md5(fingerprint_string.encode()).hexdigest()[:8]

    def _normalize_for_fingerprint(self, text: str) -> str:
        """Normalize text for fingerprinting (more aggressive than similarity check)."""
        if not text:
            return ""

        import re

        # Convert to lowercase
        normalized = text.lower().strip()

        # Remove common variations that shouldn't affect fingerprinting
        patterns_to_remove = [
            r'\s*\(explicit\)',
            r'\s*\(clean\)',
            r'\s*\(radio edit\)',
            r'\s*\(album version\)',
            r'\s*\(remaster\)',
            r'\s*\(remastered\)',
            r'\s*[‌\[\(].*?[‌\]\)]',  # Remove any parenthetical content
            r'[^\w\s]',  # Remove non-alphanumeric except spaces
        ]

        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)

        # Normalize whitespace and remove extra spaces
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized

    def _verify_duplicates_in_group(self, tracks: List[Track]) -> List[List[Track]]:
        """Verify which tracks in a fingerprint group are actual duplicates."""
        if len(tracks) < 2:
            return []

        verified_groups = []
        processed = set()

        for i, track1 in enumerate(tracks):
            if i in processed:
                continue

            current_group = [track1]
            processed.add(i)

            for j, track2 in enumerate(tracks[i + 1:], i + 1):
                if j in processed:
                    continue

                if self._are_duplicates_detailed(track1, track2):
                    current_group.append(track2)
                    processed.add(j)

            if len(current_group) > 1:
                verified_groups.append(current_group)

        return verified_groups

    def _are_duplicates_detailed(self, track1: Track, track2: Track) -> bool:
        """Detailed duplicate check with high precision."""
        if not all([track1.title, track1.artists, track2.title, track2.artists]):
            return False

        # Normalize for comparison
        title1 = self._normalize_title_for_comparison(track1.title)
        title2 = self._normalize_title_for_comparison(track2.title)
        artists1 = self._normalize_artists_for_comparison(track1.artists)
        artists2 = self._normalize_artists_for_comparison(track2.artists)

        # Calculate similarities
        title_similarity = Levenshtein.ratio(title1, title2)
        artist_similarity = Levenshtein.ratio(artists1, artists2)

        # Both title and artist must be very similar
        return (title_similarity >= self.similarity_threshold and
                artist_similarity >= self.similarity_threshold)

    def _normalize_title_for_comparison(self, title: str) -> str:
        """Normalize title for precise comparison."""
        if not title:
            return ""

        import re

        normalized = title.lower().strip()

        # Remove specific patterns but be more conservative than fingerprinting
        patterns_to_remove = [
            r'\s*\(explicit\)',
            r'\s*\(clean\)',
            r'\s*\(radio edit\)',
            r'\s*\(remaster(?:ed)?\)',
        ]

        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)

        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    def _normalize_artists_for_comparison(self, artists: str) -> str:
        """Normalize artist names for comparison."""
        if not artists:
            return ""

        # Split, clean, and sort artists
        artist_list = []
        for artist in artists.split(','):
            cleaned = artist.strip().lower()
            if cleaned:
                artist_list.append(cleaned)

        artist_list.sort()
        return ', '.join(artist_list)

    def _create_duplicate_group(self, tracks: List[Track], track_playlist_map: Dict[str, Set[str]]) -> Optional[
        DuplicateGroup]:
        """Create a DuplicateGroup, detecting if manual selection is needed."""
        if len(tracks) < 2:
            return None

        # Check if all tracks have same duration (requires manual selection)
        durations = [track.duration_ms or 0 for track in tracks]
        unique_durations = set(durations)
        requires_manual = len(unique_durations) <= 1 and len(tracks) > 1  # Same or no duration info

        # Create unique group ID
        track_uris = sorted([track.uri for track in tracks if track.uri])
        group_id = hashlib.md5("|".join(track_uris).encode()).hexdigest()[:8]

        if requires_manual:
            # For manual selection, pick first track as temporary primary
            primary_track = tracks[0]
            duplicates_to_remove = tracks[1:]
            duplicate_logger.info(
                f"Manual selection required for group {group_id}: {len(tracks)} tracks with same duration")
        else:
            # Auto-select longest track
            primary_track = self._select_primary_track_by_duration(tracks)
            duplicates_to_remove = [t for t in tracks if t != primary_track]

        # Get all playlists these tracks belong to
        all_playlists = set()
        for track in tracks:
            if track.uri and track.uri in track_playlist_map:
                all_playlists.update(track_playlist_map[track.uri])

        return DuplicateGroup(
            tracks=tracks,
            primary_track=primary_track,
            duplicates_to_remove=duplicates_to_remove,
            playlists_to_merge=all_playlists,
            requires_user_selection=requires_manual,
            group_id=group_id
        )

    def _select_primary_track_by_duration(self, tracks: List[Track]) -> Track:
        """Select the track with the longest duration as primary."""

        # Sort by duration (longest first), then by other quality indicators
        def track_score(track: Track) -> Tuple[int, int, int, int]:
            # Primary sort: duration (longer is better)
            duration = track.duration_ms or 0

            # Secondary sort: prefer non-local tracks
            is_not_local = 0 if track.is_local else 1

            # Tertiary sort: prefer tracks with TrackId
            has_track_id = 1 if track.track_id else 0

            # Quaternary sort: prefer tracks with album info
            has_album = len(track.album or '')

            return (duration, is_not_local, has_track_id, has_album)

        tracks_sorted = sorted(tracks, key=track_score, reverse=True)

        selected = tracks_sorted[0]
        duration_info = selected.get_duration_formatted() if selected.duration_ms else "Unknown duration"
        duplicate_logger.debug(f"Selected primary track: {selected.artists} - {selected.title} ({duration_info})")

        return selected

    def _get_track_playlist_mapping_batch(self, uow) -> Dict[str, Set[str]]:
        """Optimized batch retrieval of track-playlist mappings."""
        track_playlist_map = defaultdict(set)

        try:
            # Use existing optimized method if available
            all_playlist_mappings = uow.track_playlist_repository.get_all_playlist_track_mappings()

            for playlist_id, uris in all_playlist_mappings.items():
                for uri in uris:
                    track_playlist_map[uri].add(playlist_id)

            return dict(track_playlist_map)
        except Exception as e:
            duplicate_logger.warning(f"Error with batch mapping retrieval: {e}")
            # Fallback to individual queries (slower)
            return self._get_track_playlist_mapping_fallback(uow)

    def _get_track_playlist_mapping_fallback(self, uow) -> Dict[str, Set[str]]:
        """Fallback method for track-playlist mapping."""
        duplicate_logger.info("Using fallback method for track-playlist mappings")
        track_playlist_map = defaultdict(set)

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
                    f"Removed duplicate track: {duplicate.artists} - {duplicate.title} ({duplicate.uri})"
                )

        return {
            "primary_track": f"{group.primary_track.artists} - {group.primary_track.title}",
            "primary_uri": primary_uri,
            "primary_duration": group.primary_track.get_duration_formatted(),
            "tracks_removed": len(group.duplicates_to_remove),
            "playlists_merged": playlists_added,
            "removed_tracks": [
                {
                    "name": f"{d.artists} - {d.title}",
                    "duration": d.get_duration_formatted(),
                    "uri": d.uri
                } for d in group.duplicates_to_remove
            ]
        }

    def _analyze_duplicate_group(self, group: DuplicateGroup) -> Dict[str, Any]:
        """Analyze what would be done for a duplicate group (dry run)."""
        return {
            "primary_track": f"{group.primary_track.artists} - {group.primary_track.title}",
            "primary_uri": group.primary_track.uri,
            "primary_duration": group.primary_track.get_duration_formatted(),
            "tracks_removed": len(group.duplicates_to_remove),
            "playlists_merged": len(group.playlists_to_merge),  # Approximate
            "removed_tracks": [
                {
                    "name": f"{d.artists} - {d.title}",
                    "duration": d.get_duration_formatted(),
                    "uri": d.uri
                } for d in group.duplicates_to_remove
            ]
        }


def detect_and_cleanup_duplicate_tracks(dry_run: bool = False, user_selections: Dict[str, str] = None) -> Dict[
    str, Any]:
    """
    Main function to detect and clean up duplicate tracks.

    Args:
        dry_run: If True, only analyze what would be done
        user_selections: Dict mapping group_id to selected track URI for manual groups
    """
    try:
        # Use detector
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

        # Check if any groups require user selection
        groups_requiring_selection = [g for g in duplicate_groups if g.requires_user_selection]

        if groups_requiring_selection and not user_selections:
            return {
                "success": False,
                "message": f"{len(groups_requiring_selection)} duplicate groups require manual selection",
                "requires_user_selection": True,
                "groups_requiring_selection": len(groups_requiring_selection),
                "error": "MANUAL_SELECTION_REQUIRED"
            }

        # Apply user selections to groups that need them
        if user_selections:
            for group in groups_requiring_selection:
                if group.group_id in user_selections:
                    selected_uri = user_selections[group.group_id]
                    # Find the selected track and make it primary
                    selected_track = None
                    for track in group.tracks:
                        if track.uri == selected_uri:
                            selected_track = track
                            break

                    if selected_track:
                        group.primary_track = selected_track
                        group.duplicates_to_remove = [t for t in group.tracks if t != selected_track]
                        duplicate_logger.info(
                            f"User selected {selected_track.uri} as primary for group {group.group_id}")
                    else:
                        duplicate_logger.warning(
                            f"User selected URI {selected_uri} not found in group {group.group_id}")

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
    Generate a report of duplicate tracks with duration information and playlist names.
    """
    try:
        detector = DuplicateTrackDetector()
        duplicate_groups = detector.find_all_duplicates()

        if not duplicate_groups:
            return {
                "success": True,
                "message": "No duplicate tracks found",
                "duplicate_groups": [],
                "total_duplicates": 0,
                "requires_user_selection": False
            }

        # Get all unique playlist IDs from all groups
        all_playlist_ids = set()
        for group in duplicate_groups:
            all_playlist_ids.update(group.playlists_to_merge)

        # Get playlist names in batch
        playlist_id_to_name = {}
        if all_playlist_ids:
            with UnitOfWork() as uow:
                playlists_dict = uow.playlist_repository.get_playlists_by_ids(list(all_playlist_ids))
                for playlist_id, playlist_obj in playlists_dict.items():
                    playlist_id_to_name[playlist_id] = playlist_obj.name

        # Format duplicate groups for display with duration info and playlist names
        formatted_groups = []
        total_duplicates = 0
        groups_requiring_selection = 0

        for group in duplicate_groups:
            # Convert playlist IDs to names
            playlist_names = []
            for playlist_id in group.playlists_to_merge:
                if playlist_id in playlist_id_to_name:
                    playlist_names.append(playlist_id_to_name[playlist_id])
                else:
                    # Fallback to ID if name not found
                    playlist_names.append(f"ID: {playlist_id}")

            if group.requires_user_selection:
                groups_requiring_selection += 1

            formatted_group = {
                "group_id": group.group_id,
                "requires_user_selection": group.requires_user_selection,
                "primary_track": {
                    "title": group.primary_track.title,
                    "artists": group.primary_track.artists,
                    "album": group.primary_track.album,
                    "uri": group.primary_track.uri,
                    "track_id": group.primary_track.track_id,
                    "duration_ms": group.primary_track.duration_ms,
                    "duration_formatted": group.primary_track.get_duration_formatted(),
                    "is_local": group.primary_track.is_local
                },
                "all_tracks": [  # Include all tracks for manual selection
                    {
                        "title": track.title,
                        "artists": track.artists,
                        "album": track.album,
                        "uri": track.uri,
                        "track_id": track.track_id,
                        "duration_ms": track.duration_ms,
                        "duration_formatted": track.get_duration_formatted(),
                        "is_local": track.is_local,
                        "is_primary": track == group.primary_track
                    } for track in group.tracks
                ],
                "duplicates": [],
                "playlists_affected": playlist_names,
                "playlists_affected_count": len(playlist_names),
                "total_tracks_in_group": len(group.tracks)
            }

            for duplicate in group.duplicates_to_remove:
                formatted_group["duplicates"].append({
                    "title": duplicate.title,
                    "artists": duplicate.artists,
                    "album": duplicate.album,
                    "uri": duplicate.uri,
                    "track_id": duplicate.track_id,
                    "duration_ms": duplicate.duration_ms,
                    "duration_formatted": duplicate.get_duration_formatted(),
                    "is_local": duplicate.is_local
                })
                total_duplicates += 1

            formatted_groups.append(formatted_group)

        return {
            "success": True,
            "message": f"Found {len(duplicate_groups)} duplicate groups with {total_duplicates} tracks to remove",
            "duplicate_groups": formatted_groups,
            "total_groups": len(duplicate_groups),
            "total_duplicates": total_duplicates,
            "requires_user_selection": groups_requiring_selection > 0,
            "groups_requiring_selection": groups_requiring_selection,
            "groups_auto_resolved": len(duplicate_groups) - groups_requiring_selection
        }

    except Exception as e:
        duplicate_logger.error(f"Error generating duplicate tracks report: {e}")
        return {
            "success": False,
            "message": f"Error generating report: {str(e)}",
            "error": str(e)
        }


def apply_user_selections_and_cleanup(user_selections: Dict[str, str], dry_run: bool = False) -> Dict[str, Any]:
    """
    Apply user selections for duplicate groups and perform cleanup.

    Args:
        user_selections: Dict mapping group_id to selected track URI
        dry_run: If True, only analyze what would be done
    """
    return detect_and_cleanup_duplicate_tracks(dry_run=dry_run, user_selections=user_selections)
