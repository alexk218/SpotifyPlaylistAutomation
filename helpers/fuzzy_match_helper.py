import hashlib
import os
import re
from typing import List, Dict, Any, Tuple, Optional

import Levenshtein
from sql.models.track import Track
from utils.logger import setup_logger

fuzzy_logger = setup_logger('fuzzy_matching', 'helpers', 'fuzzy_matching.log')


# TODO: move this to api/models ?
class FuzzyMatchResult:
    """Represents a single fuzzy match result."""

    def __init__(self, track: Track, confidence: float, match_type: str, match_details: List[str] = None):
        self.track = track
        self.confidence = confidence
        self.match_type = match_type
        self.match_details = match_details or []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for API responses."""
        return {
            'track_id': self.track.track_id,
            'uri': self.track.uri,
            'artist': self.track.artists,
            'title': self.track.title,
            'album': self.track.album or ('Local Files' if self.track.is_local else 'Unknown Album'),
            'is_local': self.track.is_local,
            'confidence': self.confidence,
            'match_type': self.match_type,
            'match_details': self.match_details
        }


class FuzzyMatcher:
    """Consolidated fuzzy matching logic for tracks."""

    def __init__(self, tracks: List[Track], existing_mappings: Dict[str, str] = None):
        """
        Initialize the fuzzy matcher.

        Args:
            tracks: List of all tracks to match against
            existing_mappings: Dictionary of {file_path: spotify_uri} for already mapped files
        """
        self.tracks = tracks
        self.local_tracks = [track for track in tracks if track.is_local]
        self.regular_tracks = [track for track in tracks if not track.is_local]

        # Track which URIs are already mapped to files
        self.existing_mappings = existing_mappings or {}
        self.mapped_uris = set(self.existing_mappings.values())

        # Cache for remix/version keywords
        self.remix_keywords = {
            'remix', 'edit', 'mix', 'version', 'vip', 'bootleg', 'rework',
            'flip', 'refix', 'redo', 'extended', 'radio', 'club', 'dub'
        }

    def find_matches(self,
                     filename: str,
                     threshold: float = 0.6,
                     max_matches: int = 8,
                     exclude_track_id: str = None,
                     file_path: str = None) -> List[FuzzyMatchResult]:
        """
        Find fuzzy matches for a filename with improved logic.
        """
        # Clean filename
        filename_no_ext = os.path.splitext(filename)[0]

        # Extract artist and title from filename
        artist, title = self._extract_artist_title(filename_no_ext)

        fuzzy_logger.info(f"Matching file: {filename} -> Artist: '{artist}', Title: '{title}'")

        all_matches = []

        # 1. Try exact local file matches first (highest priority)
        local_matches = self._find_exact_local_matches(filename, filename_no_ext, file_path)
        all_matches.extend(local_matches)

        # 2. Try fuzzy matching with regular tracks
        regular_matches = self._find_fuzzy_matches_improved(artist, title, exclude_track_id, file_path)
        all_matches.extend(regular_matches)

        # 3. Try fuzzy matching with local tracks (lower priority)
        local_fuzzy_matches = self._find_local_fuzzy_matches(filename_no_ext, exclude_track_id, file_path)
        all_matches.extend(local_fuzzy_matches)

        # Filter by threshold and sort by confidence
        filtered_matches = [match for match in all_matches if match.confidence >= threshold]
        filtered_matches.sort(key=lambda x: x.confidence, reverse=True)

        # Remove duplicates (keep highest confidence)
        seen_track_ids = set()
        unique_matches = []
        for match in filtered_matches:
            if match.track.track_id not in seen_track_ids:
                seen_track_ids.add(match.track.track_id)
                unique_matches.append(match)

        return unique_matches[:max_matches]

    def find_best_match(self,
                        filename: str,
                        threshold: float = 0.75,
                        exclude_track_id: str = None,
                        file_path: str = None) -> Optional[FuzzyMatchResult]:
        """Find the single best match for a filename."""
        matches = self.find_matches(filename, threshold=0.4, max_matches=1,
                                    exclude_track_id=exclude_track_id, file_path=file_path)

        if matches and matches[0].confidence >= threshold:
            return matches[0]
        return None

    def _extract_artist_title(self, filename: str) -> Tuple[str, str]:
        """Extract artist and title from filename with better handling."""
        # Try standard "Artist - Title" format first
        if " - " in filename:
            parts = filename.split(" - ", 1)
            return parts[0].strip(), parts[1].strip()

        # Try other common separators
        for separator in [" – ", " — ", " by "]:
            if separator in filename:
                parts = filename.split(separator, 1)
                return parts[0].strip(), parts[1].strip()

        # If no separator, treat whole filename as title
        return "", filename.strip()

    def _find_exact_local_matches(self, filename: str, filename_no_ext: str, file_path: str = None) -> List[
        FuzzyMatchResult]:
        """Find exact matches with local tracks."""
        matches = []

        for track in self.local_tracks:
            if not track.title:
                continue

            # Apply mapping penalty
            confidence_penalty = self._get_mapping_penalty(track.uri, file_path)
            if confidence_penalty <= 0:
                continue  # Skip already mapped tracks

            # Try exact filename match
            if track.title == filename or track.title == filename_no_ext:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=1.0 * confidence_penalty,
                    match_type="exact_local"
                ))
                continue

            # Try normalized match
            normalized_filename = self._normalize_text(filename_no_ext)
            normalized_title = self._normalize_text(track.title)

            if normalized_title and normalized_filename == normalized_title:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=0.95 * confidence_penalty,
                    match_type="normalized_local"
                ))

        return matches

    def _find_fuzzy_matches_improved(self, artist: str, title: str, exclude_track_id: str = None,
                                     file_path: str = None) -> List[FuzzyMatchResult]:
        """Improved fuzzy matching with better remix handling."""
        matches = []

        # Normalize input
        normalized_artist = artist.lower().replace('&', 'and') if artist else ""
        normalized_title = title.lower()

        for track in self.regular_tracks:
            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            # Apply mapping penalty
            confidence_penalty = self._get_mapping_penalty(track.uri, file_path)
            if confidence_penalty <= 0:
                continue  # Skip already mapped tracks

            confidence = self._calculate_track_confidence_improved(
                normalized_artist, normalized_title, track
            )

            # Apply the mapping penalty
            final_confidence = confidence * confidence_penalty

            if final_confidence >= 0.4:  # Lower threshold for collecting matches
                match_details = []
                if confidence_penalty < 1.0:
                    match_details.append(f"mapped_penalty_{confidence_penalty:.2f}")

                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=final_confidence,
                    match_type="fuzzy_improved",
                    match_details=match_details
                ))

        return matches

    def _find_local_fuzzy_matches(self, filename_no_ext: str, exclude_track_id: str = None, file_path: str = None) -> \
            List[FuzzyMatchResult]:
        """Find fuzzy matches with local tracks."""
        matches = []

        for track in self.local_tracks:
            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            if not track.title:
                continue

            # Apply mapping penalty
            confidence_penalty = self._get_mapping_penalty(track.uri, file_path)
            if confidence_penalty <= 0:
                continue

            # Calculate similarity
            similarity = Levenshtein.ratio(filename_no_ext.lower(), track.title.lower())

            # Apply penalty
            final_similarity = similarity * confidence_penalty

            # Only include high-quality local matches to avoid noise
            if final_similarity >= 0.8:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=final_similarity,
                    match_type="local_fuzzy"
                ))

        return matches

    def _calculate_track_confidence_improved(self, local_artist: str, local_title: str, track: Track) -> float:
        """Enhanced confidence calculation with better remix handling."""
        db_artists = track.artists.lower().replace('&', 'and')
        db_title = track.title.lower()

        # Calculate artist similarity
        artist_ratio = 0.0
        if local_artist:
            # Split by comma only, don't split individual artist names
            db_artist_list = [a.strip() for a in db_artists.split(',')]

            # Check for exact match first
            if local_artist in db_artist_list:
                artist_ratio = 1.0
            else:
                # Use fuzzy matching against each artist
                artist_ratios = [Levenshtein.ratio(local_artist, db_artist) for db_artist in db_artist_list]
                artist_ratio = max(artist_ratios) if artist_ratios else 0

        # Enhanced title matching with remix awareness
        title_confidence = self._calculate_title_confidence_with_remixes(local_title, db_title)

        # Calculate weighted overall ratio
        if local_artist:
            return artist_ratio * 0.6 + title_confidence * 0.4
        else:
            return title_confidence * 0.9

    def _calculate_title_confidence_with_remixes(self, local_title: str, db_title: str) -> float:
        """Calculate title confidence with enhanced remix/version handling."""

        # Extract base titles and remix info
        local_base, local_remix_info = self._extract_remix_info(local_title)
        db_base, db_remix_info = self._extract_remix_info(db_title)

        # Calculate base title similarity
        base_similarity = Levenshtein.ratio(local_base, db_base)

        # If base titles don't match well, return low confidence
        if base_similarity < 0.7:
            return base_similarity * 0.5

        # Both have remix info - check if they match
        if local_remix_info and db_remix_info:
            remix_similarity = self._calculate_remix_similarity(local_remix_info, db_remix_info)
            # Weight base title heavily, but remix info is important for distinction
            return (base_similarity * 0.7) + (remix_similarity * 0.3)

        # One has remix info, other doesn't - penalize this
        elif local_remix_info or db_remix_info:
            # If one is a remix and the other isn't, they shouldn't match as highly
            return base_similarity * 0.6

        # Neither has remix info - just base similarity
        else:
            return base_similarity

    def _extract_remix_info(self, title: str) -> Tuple[str, str]:
        """Extract base title and remix information."""
        # Pattern to match content in parentheses or after common separators
        remix_patterns = [
            r'\s*[\(\[]([^)\]]*(?:remix|edit|mix|version|vip|bootleg|rework|flip|refix|redo|extended|radio|club|dub)[^)\]]*)\s*[\)\]]',
            r'\s*-\s*([^-]*(?:remix|edit|mix|version|vip|bootleg|rework|flip|refix|redo|extended|radio|club|dub)[^-]*)\s*$'
        ]

        for pattern in remix_patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                base_title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()
                remix_info = match.group(1).lower().strip()
                return base_title, remix_info

        # No remix info found
        return title, ""

    def _calculate_remix_similarity(self, remix1: str, remix2: str) -> float:
        """Calculate similarity between remix information."""
        if not remix1 or not remix2:
            return 0.0

        # Direct string similarity
        direct_similarity = Levenshtein.ratio(remix1, remix2)

        # Check for common remix keywords
        words1 = set(remix1.lower().split())
        words2 = set(remix2.lower().split())

        # Find remix keywords in both
        remix_words1 = words1.intersection(self.remix_keywords)
        remix_words2 = words2.intersection(self.remix_keywords)

        # If both have remix keywords, check if they match
        if remix_words1 and remix_words2:
            keyword_similarity = len(remix_words1.intersection(remix_words2)) / max(len(remix_words1),
                                                                                    len(remix_words2))
            # Combine direct similarity with keyword similarity
            return max(direct_similarity, keyword_similarity)

        return direct_similarity

    def _get_mapping_penalty(self, track_uri: str, current_file_path: str = None) -> float:
        """
        Calculate penalty for tracks that are already mapped to other files.

        Args:
            track_uri: The URI of the track being considered
            current_file_path: The path of the current file being matched (to avoid self-penalty)

        Returns:
            Penalty multiplier (0.0 to 1.0). 0.0 means completely exclude, 1.0 means no penalty
        """
        if not track_uri or track_uri not in self.mapped_uris:
            return 1.0  # No penalty if not mapped

        # Check if this track is mapped to the current file (no penalty for re-matching same file)
        if current_file_path and self.existing_mappings.get(current_file_path) == track_uri:
            return 1.0  # No penalty for existing mapping to same file

        # Heavy penalty for tracks mapped to other files
        return 0.1  # 90% confidence reduction, but don't completely exclude

    def _normalize_text(self, text: str) -> str:
        """Normalize text for matching."""
        if not text:
            return ""

        # Remove special characters and normalize spaces
        normalized = re.sub(r'[^\w\s]', '', text.lower())
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized

    def search_tracks(self, query: str, limit: int = 20) -> List[FuzzyMatchResult]:
        """Search tracks by query string with mapping awareness."""
        if not query or not query.strip():
            return []

        query_lower = query.lower().strip()
        results = []

        # Split query into words for word-level matching
        query_words = set(query_lower.split())

        for track in self.tracks:
            # Apply light mapping penalty for search (don't exclude, just de-prioritize)
            confidence_penalty = max(0.3, self._get_mapping_penalty(track.uri))

            scores = []
            match_details = []

            # Clean track data
            track_title = track.title.lower() if track.title else ""
            track_artists = track.artists.lower() if track.artists else ""
            track_combined = f"{track_artists} {track_title}".strip()

            # 1. Exact substring matches (highest priority)
            if query_lower in track_title:
                scores.append(0.95)
                match_details.append("title_exact")
            if query_lower in track_artists:
                scores.append(0.95)
                match_details.append("artist_exact")
            if query_lower in track_combined:
                scores.append(0.90)
                match_details.append("combined_exact")

            # 2. Word-level matching
            title_words = set(track_title.split())
            artist_words = set(track_artists.split())

            title_word_overlap = len(query_words.intersection(title_words)) / max(len(query_words), 1)
            artist_word_overlap = len(query_words.intersection(artist_words)) / max(len(query_words), 1)

            if title_word_overlap > 0.5:
                scores.append(0.8 * title_word_overlap)
                match_details.append(f"title_words_{title_word_overlap:.2f}")
            if artist_word_overlap > 0.5:
                scores.append(0.8 * artist_word_overlap)
                match_details.append(f"artist_words_{artist_word_overlap:.2f}")

            # 3. Fuzzy matching with Levenshtein
            title_ratio = Levenshtein.ratio(query_lower, track_title)
            artist_ratio = Levenshtein.ratio(query_lower, track_artists)
            combined_ratio = Levenshtein.ratio(query_lower, track_combined)

            scores.extend([title_ratio * 0.7, artist_ratio * 0.7, combined_ratio * 0.6])

            # Get the best score and apply penalty
            max_score = max(scores) if scores else 0
            final_score = max_score * confidence_penalty

            # Only include results above minimum threshold
            if final_score > 0.1:
                if confidence_penalty < 1.0:
                    match_details.append(f"mapped_penalty_{confidence_penalty:.2f}")

                results.append(FuzzyMatchResult(
                    track=track,
                    confidence=final_score,
                    match_type="search",
                    match_details=match_details[:3]
                ))

        # Sort by confidence and limit results
        results.sort(key=lambda x: x.confidence, reverse=True)
        return results[:limit]


# Convenience functions for backwards compatibility
def find_fuzzy_matches(filename: str, tracks: List[Track], threshold: float = 0.6,
                       max_matches: int = 8, exclude_track_id: str = None,
                       existing_mappings: Dict[str, str] = None,
                       file_path: str = None) -> List[Dict[str, Any]]:
    """Find fuzzy matches and return as dictionaries."""
    matcher = FuzzyMatcher(tracks, existing_mappings)
    matches = matcher.find_matches(filename, threshold, max_matches, exclude_track_id, file_path)
    return [match.to_dict() for match in matches]


def find_best_match(filename: str, tracks: List[Track], threshold: float = 0.75,
                    exclude_track_id: str = None, existing_mappings: Dict[str, str] = None,
                    file_path: str = None) -> Optional[Dict[str, Any]]:
    """Find the best match and return as dictionary."""
    matcher = FuzzyMatcher(tracks, existing_mappings)
    match = matcher.find_best_match(filename, threshold, exclude_track_id, file_path)
    return match.to_dict() if match else None


def search_tracks(query: str, tracks: List[Track], limit: int = 20,
                  existing_mappings: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """Search tracks and return as dictionaries."""
    matcher = FuzzyMatcher(tracks, existing_mappings)
    matches = matcher.search_tracks(query, limit)
    return [match.to_dict() for match in matches]
