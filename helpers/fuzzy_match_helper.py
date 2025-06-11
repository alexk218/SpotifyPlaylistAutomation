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
            'ratio': self.confidence,  # For backwards compatibility
            'match_type': self.match_type,
            'match_details': self.match_details
        }


class FuzzyMatcher:
    """Consolidated fuzzy matching logic for tracks."""

    def __init__(self, tracks: List[Track]):
        self.tracks = tracks
        self.local_tracks = [track for track in tracks if track.is_local]
        self.regular_tracks = [track for track in tracks if not track.is_local]

    def find_matches(self,
                     filename: str,
                     threshold: float = 0.6,
                     max_matches: int = 8,
                     exclude_track_id: str = None) -> List[FuzzyMatchResult]:
        """
        Find fuzzy matches for a filename.

        Args:
            filename: The filename to match (with or without extension)
            threshold: Minimum confidence threshold
            max_matches: Maximum number of matches to return
            exclude_track_id: Track ID to exclude from results

        Returns:
            List of FuzzyMatchResult objects, sorted by confidence
        """
        # Clean filename
        filename_no_ext = os.path.splitext(filename)[0]

        # Extract artist and title from filename
        artist, title = self._extract_artist_title(filename_no_ext)

        fuzzy_logger.info(f"Matching file: {filename} -> Artist: '{artist}', Title: '{title}'")

        all_matches = []

        # 1. Try exact local file matches first (highest priority)
        local_matches = self._find_exact_local_matches(filename, filename_no_ext)
        all_matches.extend(local_matches)

        # 2. Try fuzzy matching with regular tracks
        regular_matches = self._find_fuzzy_matches(artist, title, exclude_track_id)
        all_matches.extend(regular_matches)

        # 3. Try fuzzy matching with local tracks (lower priority)
        local_fuzzy_matches = self._find_local_fuzzy_matches(filename_no_ext, exclude_track_id)
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
                        exclude_track_id: str = None) -> Optional[FuzzyMatchResult]:
        """Find the single best match for a filename."""
        matches = self.find_matches(filename, threshold=0.4, max_matches=1, exclude_track_id=exclude_track_id)

        if matches and matches[0].confidence >= threshold:
            return matches[0]
        return None

    def search_tracks(self,
                      query: str,
                      limit: int = 20) -> List[FuzzyMatchResult]:
        """
        Search tracks by query string.

        Args:
            query: Search query
            limit: Maximum number of results

        Returns:
            List of FuzzyMatchResult objects
        """
        if not query or not query.strip():
            return []

        query_lower = query.lower().strip()
        results = []

        # Split query into words for word-level matching
        query_words = set(query_lower.split())

        for track in self.tracks:
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

            # Get the best score
            max_score = max(scores) if scores else 0

            # Only include results above minimum threshold
            if max_score > 0.1:
                results.append(FuzzyMatchResult(
                    track=track,
                    confidence=max_score,
                    match_type="search",
                    match_details=match_details[:3]
                ))

        # Sort by confidence and limit results
        results.sort(key=lambda x: x.confidence, reverse=True)
        return results[:limit]

    def _extract_artist_title(self, filename: str) -> Tuple[str, str]:
        """Extract artist and title from filename."""
        # Try standard "Artist - Title" format first
        if " - " in filename:
            parts = filename.split(" - ", 1)
            return parts[0].strip(), parts[1].strip()

        # If no separator, treat whole filename as title
        return "", filename.strip()

    def _find_exact_local_matches(self, filename: str, filename_no_ext: str) -> List[FuzzyMatchResult]:
        """Find exact matches with local tracks."""
        matches = []

        for track in self.local_tracks:
            if not track.title:
                continue

            # Try exact filename match
            if track.title == filename or track.title == filename_no_ext:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=1.0,
                    match_type="exact_local"
                ))
                continue

            # Try normalized match
            normalized_filename = self._normalize_text(filename_no_ext)
            normalized_title = self._normalize_text(track.title)

            if normalized_title and normalized_filename == normalized_title:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=0.95,
                    match_type="normalized_local"
                ))

        return matches

    def _find_fuzzy_matches(self, artist: str, title: str, exclude_track_id: str = None) -> List[FuzzyMatchResult]:
        """Find fuzzy matches with regular Spotify tracks."""
        matches = []

        # Normalize input
        normalized_artist = artist.lower().replace('&', 'and') if artist else ""
        normalized_title = title.lower()

        # Handle remix information
        clean_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()

        for track in self.regular_tracks:
            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            confidence = self._calculate_track_confidence(
                normalized_artist, clean_title, track
            )

            if confidence >= 0.4:  # Lower threshold for collecting matches
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=confidence,
                    match_type="fuzzy"
                ))

        return matches

    def _find_local_fuzzy_matches(self, filename_no_ext: str, exclude_track_id: str = None) -> List[FuzzyMatchResult]:
        """Find fuzzy matches with local tracks."""
        matches = []

        for track in self.local_tracks:
            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            if not track.title:
                continue

            # Calculate similarity
            similarity = Levenshtein.ratio(filename_no_ext.lower(), track.title.lower())

            # Only include high-quality local matches to avoid noise
            if similarity >= 0.8:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=similarity,
                    match_type="local_fuzzy"
                ))

        return matches

    def _calculate_track_confidence(self, local_artist: str, local_title: str, track: Track) -> float:
        """Calculate confidence score for track match."""
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

        # Calculate title similarity
        clean_db_title = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()
        title_ratio = Levenshtein.ratio(local_title, clean_db_title)

        # Perfect match bonus
        if local_title == clean_db_title:
            title_ratio = 1.0

        # Calculate weighted overall ratio
        if local_artist:
            return (artist_ratio * 0.6 + title_ratio * 0.4)
        else:
            return title_ratio * 0.9

    def _normalize_text(self, text: str) -> str:
        """Normalize text for matching."""
        if not text:
            return ""

        # Remove special characters and normalize spaces
        normalized = re.sub(r'[^\w\s]', '', text.lower())
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized


# Convenience functions for backwards compatibility
def find_fuzzy_matches(filename: str, tracks: List[Track], threshold: float = 0.6,
                       max_matches: int = 8, exclude_track_id: str = None) -> List[Dict[str, Any]]:
    """Find fuzzy matches and return as dictionaries."""
    matcher = FuzzyMatcher(tracks)
    matches = matcher.find_matches(filename, threshold, max_matches, exclude_track_id)
    return [match.to_dict() for match in matches]


def find_best_match(filename: str, tracks: List[Track], threshold: float = 0.75,
                    exclude_track_id: str = None) -> Optional[Dict[str, Any]]:
    """Find the best match and return as dictionary."""
    matcher = FuzzyMatcher(tracks)
    match = matcher.find_best_match(filename, threshold, exclude_track_id)
    return match.to_dict() if match else None


def search_tracks(query: str, tracks: List[Track], limit: int = 20) -> List[Dict[str, Any]]:
    """Search tracks and return as dictionaries."""
    matcher = FuzzyMatcher(tracks)
    matches = matcher.search_tracks(query, limit)
    return [match.to_dict() for match in matches]
