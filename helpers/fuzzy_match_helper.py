import os
import re
import time
from typing import List, Dict, Any, Tuple, Optional

import Levenshtein
from mutagen import File as MutagenFile

from sql.models.track import Track
from utils.logger import setup_logger

fuzzy_logger = setup_logger('fuzzy_matching', 'helpers', 'fuzzy_matching.log')

# Global debug flag - set to True for detailed timing info
DEBUG_PERFORMANCE = True


# TODO: move this to api/models ?
class AudioDurationExtractor:
    """Extract duration from audio files using mutagen."""

    @staticmethod
    def get_file_duration_ms(file_path: str) -> Optional[int]:
        """Extract duration from audio file in milliseconds."""
        if not file_path or not os.path.exists(file_path):
            return None

        try:
            audio_file = MutagenFile(file_path)
            if audio_file is None:
                return None

            if hasattr(audio_file, 'info') and hasattr(audio_file.info, 'length'):
                duration_seconds = audio_file.info.length
                return int(duration_seconds * 1000)

            return None

        except Exception as e:
            if DEBUG_PERFORMANCE:
                print(f"Warning: Could not extract duration from {file_path}: {e}")
            return None


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


class PreprocessedTrack:
    """Container for preprocessed track data to avoid repeated calculations."""

    def __init__(self, track: Track, mapping_penalty: float):
        self.track = track
        self.mapping_penalty = mapping_penalty

        # Pre-compute expensive string operations
        self.normalized_artists = track.artists.lower().replace('&', 'and') if track.artists else ""
        self.normalized_title = track.title.lower() if track.title else ""

        # Pre-extract remix info
        self.base_title, self.remix_info = self._extract_remix_info_fast(self.normalized_title)

        # Pre-compute artist words for fast filtering
        if self.normalized_artists:
            # Fast comma replacement
            clean_artists = self.normalized_artists.replace(',', ' ').replace(';', ' ').replace('&', ' ')
            self.artist_words = set(word for word in clean_artists.split() if word)
        else:
            self.artist_words = set()

        # Pre-compute combined text for search
        self.combined_text = f"{self.normalized_artists} {self.normalized_title}".strip()

    def _extract_remix_info_fast(self, title: str) -> Tuple[str, str]:
        """Fast remix info extraction using pre-compiled patterns."""
        # Use the patterns from parent class - will be set after initialization
        return title, ""  # Will be updated by parent class


class FuzzyMatcher:
    """Optimized fuzzy matching logic for tracks."""

    def __init__(self, tracks: List[Track], existing_mappings: Dict[str, str] = None):
        """
        Args:
            tracks: List of all tracks to match against
            existing_mappings: Dictionary of {file_path: spotify_uri} for already mapped files
        """
        init_start = time.time()

        self.tracks = tracks
        self.existing_mappings = existing_mappings or {}
        self.mapped_uris = set(self.existing_mappings.values())
        self.duration_extractor = AudioDurationExtractor()

        # Compile regex patterns once
        self.remix_patterns = [
            re.compile(
                r'\s*[\(\[]([^)\]]*(?:remix|edit|mix|version|vip|bootleg|rework|flip|refix|redo|extended|radio|club|dub)[^)\]]*)\s*[\)\]]',
                re.IGNORECASE),
            re.compile(
                r'\s*-\s*([^-]*(?:remix|edit|mix|version|vip|bootleg|rework|flip|refix|redo|extended|radio|club|dub)[^-]*)\s*$',
                re.IGNORECASE)
        ]

        # Cache for remix/version keywords
        self.remix_keywords = {
            'remix', 'edit', 'mix', 'version', 'vip', 'bootleg', 'rework',
            'flip', 'refix', 'redo', 'extended', 'radio', 'club', 'dub'
        }

        # Pre-process all tracks for performance
        preprocess_start = time.time()
        self.preprocessed_regular_tracks = []
        self.preprocessed_local_tracks = []
        self._preprocess_tracks()
        preprocess_time = time.time() - preprocess_start

        init_time = time.time() - init_start

        if DEBUG_PERFORMANCE:
            print(f"FuzzyMatcher initialized in {init_time:.3f}s:")
            print(f"  Total tracks: {len(tracks)}")
            print(f"  Regular tracks: {len(self.preprocessed_regular_tracks)}")
            print(f"  Local tracks: {len(self.preprocessed_local_tracks)}")
            print(f"  Existing mappings: {len(self.existing_mappings)}")
            print(f"  Preprocessing time: {preprocess_time:.3f}s")

    def _preprocess_tracks(self):
        """Pre-compute expensive string operations for all tracks."""
        skipped_mapped = 0
        skipped_invalid = 0

        for track in self.tracks:
            # Skip tracks with no title
            if not track.title or not track.title.strip():
                skipped_invalid += 1
                continue

            # Calculate mapping penalty once
            mapping_penalty = self._get_mapping_penalty_fast(track.uri)

            if mapping_penalty <= 0:
                skipped_mapped += 1
                continue

            # Create preprocessed track
            preprocessed = PreprocessedTrack(track, mapping_penalty)

            # Update remix info using our compiled patterns
            preprocessed.base_title, preprocessed.remix_info = self._extract_remix_info_fast(
                preprocessed.normalized_title)

            # Separate local and regular tracks
            if track.is_local:
                self.preprocessed_local_tracks.append(preprocessed)
            else:
                self.preprocessed_regular_tracks.append(preprocessed)

        if DEBUG_PERFORMANCE:
            print(f"  Skipped {skipped_mapped} already mapped tracks")
            print(f"  Skipped {skipped_invalid} tracks with no title")

    def _calculate_duration_confidence_boost(self, file_path: str, track_duration_ms: int) -> float:
        """Calculate confidence boost based on duration matching."""
        if not file_path or not track_duration_ms:
            return 1.0

        file_duration_ms = self.duration_extractor.get_file_duration_ms(file_path)
        if not file_duration_ms:
            return 1.0

        duration_diff_ms = abs(file_duration_ms - track_duration_ms)

        # Tolerance thresholds (in milliseconds)
        if duration_diff_ms <= 1000:  # 1 second
            return 1.25  # 25% boost
        elif duration_diff_ms <= 3000:  # 3 seconds
            return 1.20  # 20% boost
        elif duration_diff_ms <= 10000:  # 10 seconds
            return 1.15  # 15% boost
        elif duration_diff_ms <= 30000:  # 30 seconds
            return 1.10  # 10% boost
        else:
            return 1.0  # No boost

    def _find_duration_based_candidates(self, file_path: str, max_candidates: int = 50) -> List['PreprocessedTrack']:
        """Find potential matches based on duration similarity."""
        if not file_path:
            return []

        file_duration_ms = self.duration_extractor.get_file_duration_ms(file_path)
        if not file_duration_ms:
            return []

        duration_candidates = []

        for preprocessed in self.preprocessed_regular_tracks:
            track = preprocessed.track

            if not track.duration_ms:
                continue

            # Calculate duration similarity
            duration_diff_ms = abs(file_duration_ms - track.duration_ms)

            # Only consider tracks within reasonable duration range (60 seconds)
            if duration_diff_ms <= 60000:  # 1 minute tolerance
                duration_score = max(0, 1.0 - (duration_diff_ms / 60000))
                duration_candidates.append((preprocessed, duration_score, duration_diff_ms))

        # Sort by duration similarity and limit candidates
        duration_candidates.sort(key=lambda x: x[2])  # Sort by duration difference

        if DEBUG_PERFORMANCE and duration_candidates:
            print(f"    Duration-based discovery found {len(duration_candidates)} candidates")
            print(f"    Best duration match: {duration_candidates[0][2] / 1000:.1f}s difference")

        return [candidate[0] for candidate in duration_candidates[:max_candidates]]

    def find_matches(self,
                     filename: str,
                     threshold: float = 0.6,
                     max_matches: int = 8,
                     exclude_track_id: str = None,
                     file_path: str = None) -> List[FuzzyMatchResult]:
        """
        Optimized find matches with comprehensive timing.
        """
        total_start = time.time()

        # Clean filename and extract artist/title
        extract_start = time.time()
        filename_no_ext = os.path.splitext(filename)[0]
        artist, title = self._extract_artist_title(filename_no_ext)
        extract_time = time.time() - extract_start

        if DEBUG_PERFORMANCE:
            fuzzy_logger.info(f"Matching file: {filename} -> Artist: '{artist}', Title: '{title}'")

        all_matches = []

        # 1. Try exact local file matches first (usually very fast)
        local_exact_start = time.time()
        local_matches = self._find_exact_local_matches_optimized(filename, filename_no_ext, file_path)
        local_exact_time = time.time() - local_exact_start
        all_matches.extend(local_matches)

        # 2. Try fuzzy matching with regular tracks (main performance bottleneck)
        regular_start = time.time()
        regular_matches = self._find_fuzzy_matches_optimized(artist, title, exclude_track_id, file_path)
        regular_time = time.time() - regular_start
        all_matches.extend(regular_matches)

        # 3. Try fuzzy matching with local tracks (usually few matches)
        local_fuzzy_start = time.time()
        local_fuzzy_matches = self._find_local_fuzzy_matches_optimized(filename_no_ext, exclude_track_id, file_path)
        local_fuzzy_time = time.time() - local_fuzzy_start
        all_matches.extend(local_fuzzy_matches)

        # Filter and sort
        filter_start = time.time()
        filtered_matches = [match for match in all_matches if match.confidence >= threshold]
        filtered_matches.sort(key=lambda x: x.confidence, reverse=True)

        # Remove duplicates (keep highest confidence)
        seen_track_ids = set()
        unique_matches = []
        for match in filtered_matches:
            if match.track.track_id not in seen_track_ids:
                seen_track_ids.add(match.track.track_id)
                unique_matches.append(match)
        filter_time = time.time() - filter_start

        total_time = time.time() - total_start

        # Performance logging
        if DEBUG_PERFORMANCE and (total_time > 0.1 or len(all_matches) == 0):
            print(f"MATCH {filename}: {total_time:.3f}s total")
            print(f"  Extract: {extract_time:.3f}s")
            print(f"  Local exact: {local_exact_time:.3f}s ({len(local_matches)} matches)")
            print(f"  Regular fuzzy: {regular_time:.3f}s ({len(regular_matches)} matches)")
            print(f"  Local fuzzy: {local_fuzzy_time:.3f}s ({len(local_fuzzy_matches)} matches)")
            print(f"  Filter/sort: {filter_time:.3f}s")
            print(f"  Final matches: {len(unique_matches)}")
            if len(unique_matches) > 0:
                best = unique_matches[0]
                print(f"  Best: {best.track.artists} - {best.track.title} ({best.confidence:.3f})")

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

    def _find_exact_local_matches_optimized(self, filename: str, filename_no_ext: str, file_path: str = None) -> List[
        FuzzyMatchResult]:
        """Optimized exact local matches using preprocessed data."""
        matches = []

        for preprocessed in self.preprocessed_local_tracks:
            track = preprocessed.track

            # Try exact filename match
            if track.title == filename or track.title == filename_no_ext:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=1.0 * preprocessed.mapping_penalty,
                    match_type="exact_local"
                ))
                continue

            # Try normalized match
            normalized_filename = self._normalize_text(filename_no_ext)
            normalized_title = self._normalize_text(track.title)

            if normalized_title and normalized_filename == normalized_title:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=0.95 * preprocessed.mapping_penalty,
                    match_type="normalized_local"
                ))

        return matches

    def _find_fuzzy_matches_optimized(self, artist: str, title: str, exclude_track_id: str = None,
                                      file_path: str = None) -> List[FuzzyMatchResult]:
        """HEAVILY OPTIMIZED fuzzy matching using preprocessed data."""
        matching_start = time.time()

        matches = []

        # Normalize input once
        normalized_artist = artist.lower().replace('&', 'and') if artist else ""
        normalized_title = title.lower()

        # Extract remix info once for the input
        local_base, local_remix_info = self._extract_remix_info_fast(normalized_title)

        # Pre-filter candidates by artist if possible (HUGE speedup)
        if normalized_artist:
            # Fast comma replacement without regex
            clean_artist = normalized_artist.replace(',', ' ').replace(';', ' ').replace('&', ' ')
            artist_words = set(word for word in clean_artist.split() if word)

            candidate_tracks = [
                pt for pt in self.preprocessed_regular_tracks
                if not artist_words.isdisjoint(pt.artist_words)
            ]
        else:
            # No artist info - use all tracks
            candidate_tracks = self.preprocessed_regular_tracks

        # NEW: Add duration-based discovery
        # duration_candidates = []
        # if file_path:
        #     duration_candidates = self._find_duration_based_candidates(file_path)

        # NEW: Combine candidates (remove duplicates)
        # candidate_tracks = text_candidates.copy()
        # for duration_candidate in duration_candidates:
        #     if duration_candidate not in candidate_tracks:
        #         candidate_tracks.append(duration_candidate)
        # candidate_tracks = text_candidates
        # duration_candidates = []  # Empty for now
        #
        # if DEBUG_PERFORMANCE:
        #     print(
        #         f"    Combined candidates: {len(candidate_tracks)} (text: {len(text_candidates)}, duration: {len(duration_candidates)})")
        #
        # filter_time = time.time() - filter_start

        # Process only candidate tracks
        calc_start = time.time()
        processed_count = 0

        for preprocessed in candidate_tracks:
            track = preprocessed.track

            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            # Use precomputed values for fast confidence calculation
            confidence = self._calculate_confidence_fast(
                normalized_artist, local_base, local_remix_info,
                preprocessed.normalized_artists, preprocessed.base_title, preprocessed.remix_info
            )

            # Apply precomputed mapping penalty
            final_confidence = confidence * preprocessed.mapping_penalty

            # OPTIONAL: Apply duration boost only if enabled
            if file_path and track.duration_ms and hasattr(self, 'duration_extractor'):
                duration_boost = self._calculate_duration_confidence_boost(file_path, track.duration_ms)
                final_confidence = min(1.0, final_confidence * duration_boost)
                duration_boost_applied = duration_boost > 1.0
            else:
                duration_boost_applied = False

            if final_confidence >= 0.2:  # Lower threshold for collecting matches
                match_details = []
                if preprocessed.mapping_penalty < 1.0:
                    match_details.append(f"mapped_penalty_{preprocessed.mapping_penalty:.2f}")
                if duration_boost_applied:
                    match_details.append(f"duration_boost_{duration_boost:.2f}")

                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=final_confidence,
                    match_type="fuzzy_optimized",
                    match_details=match_details
                ))

        # calc_time = time.time() - calc_start
        # total_time = time.time() - matching_start
        #
        # if DEBUG_PERFORMANCE and total_time > 0.05:
        #     print(f"    Fuzzy matching details:")
        #     print(f"      Candidates: {len(candidate_tracks)}/{len(self.preprocessed_regular_tracks)}")
        #     print(f"      Processed: {processed_count}")
        #     print(f"      Calc time: {calc_time:.3f}s ({calc_time / max(processed_count, 1) * 1000:.1f}ms per track)")
        #     print(f"      Matches found: {len(matches)}")

        return matches

    def _find_local_fuzzy_matches_optimized(self, filename_no_ext: str, exclude_track_id: str = None,
                                            file_path: str = None) -> List[FuzzyMatchResult]:
        """Optimized local fuzzy matches using preprocessed data."""
        matches = []

        for preprocessed in self.preprocessed_local_tracks:
            track = preprocessed.track

            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            # Calculate similarity using precomputed normalized title
            similarity = Levenshtein.ratio(filename_no_ext.lower(), preprocessed.normalized_title)

            # Apply precomputed penalty
            final_similarity = similarity * preprocessed.mapping_penalty

            # Only include high-quality local matches to avoid noise
            if final_similarity >= 0.8:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=final_similarity,
                    match_type="local_fuzzy"
                ))

        return matches

    def _calculate_confidence_fast(self, local_artist: str, local_base_title: str, local_remix_info: str,
                                   db_artists: str, db_base_title: str, db_remix_info: str) -> float:
        """Fast confidence calculation using precomputed values."""

        # Calculate artist similarity
        # Check for exact match first (fastest)
        if local_artist in db_artists.lower():
            artist_ratio = 1.0
        else:
            # Split and check individual artists only if needed
            db_artist_list = [a.strip().lower() for a in db_artists.split(',')]

            # Check for exact match in list
            local_artist_lower = local_artist.lower()
            if local_artist_lower in db_artist_list:
                artist_ratio = 1.0
            else:
                # Fuzzy matching as fallback (only if necessary)
                artist_ratios = [Levenshtein.ratio(local_artist_lower, db_artist) for db_artist in db_artist_list]
                artist_ratio = max(artist_ratios) if artist_ratios else 0

        # Enhanced title matching with remix awareness
        title_confidence = self._calculate_title_confidence_fast(local_base_title, local_remix_info, db_base_title,
                                                                 db_remix_info)

        # Calculate weighted overall ratio
        if local_artist:
            return artist_ratio * 0.6 + title_confidence * 0.4
        else:
            return title_confidence * 0.9

    def _calculate_title_confidence_fast(self, local_base: str, local_remix_info: str, db_base: str,
                                         db_remix_info: str) -> float:
        """Fast title confidence calculation with enhanced remix/version handling."""

        # Calculate base title similarity
        base_similarity = Levenshtein.ratio(local_base, db_base)

        # If base titles don't match well, return low confidence
        if base_similarity < 0.7:
            return base_similarity * 0.5

        # Both have remix info - check if they match
        if local_remix_info and db_remix_info:
            remix_similarity = self._calculate_remix_similarity_fast(local_remix_info, db_remix_info)
            # Weight base title heavily, but remix info is important for distinction
            return (base_similarity * 0.7) + (remix_similarity * 0.3)

        # One has remix info, other doesn't - penalize this
        elif local_remix_info or db_remix_info:
            # If one is a remix and the other isn't, they shouldn't match as highly
            return base_similarity * 0.6

        # Neither has remix info - just base similarity
        else:
            return base_similarity

    def _extract_remix_info_fast(self, title: str) -> Tuple[str, str]:
        """Fast remix info extraction using pre-compiled patterns."""
        for pattern in self.remix_patterns:
            match = pattern.search(title)
            if match:
                base_title = pattern.sub('', title).strip()
                remix_info = match.group(1).lower().strip()
                return base_title, remix_info

        # No remix info found
        return title, ""

    def _calculate_remix_similarity_fast(self, remix1: str, remix2: str) -> float:
        """Fast remix similarity calculation."""
        if not remix1 or not remix2:
            return 0.0

        # Direct string similarity (fast)
        direct_similarity = Levenshtein.ratio(remix1, remix2)

        # Quick keyword check
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

    def _get_mapping_penalty_fast(self, track_uri: str, file_path: str = None) -> float:
        if not track_uri or track_uri not in self.mapped_uris:
            return 1.0  # No penalty if not mapped

        # OPTION 1: Reduce penalty severity (Quick fix)
        # return 0.7  # Changed from 0.1 to 0.7 (only 30% reduction instead of 90%)

        # OPTION 2: Smarter penalty based on file existence (Better fix)
        if file_path:
            # Only apply harsh penalty if the mapped file actually exists and is different
            existing_file = self.existing_mappings.get(track_uri)
            if existing_file and os.path.exists(existing_file) and existing_file != file_path:
                return 0.3  # Harsh penalty only for actual conflicts
            else:
                return 0.8  # Light penalty for stale/invalid mappings
        return 0.7  # Default moderate penalty

    def _normalize_text(self, text: str) -> str:
        """Fast text normalization."""
        if not text:
            return ""

        # Simple normalization - removed regex for performance
        return text.lower().strip()

    def search_tracks(self, query: str, limit: int = 20) -> List[FuzzyMatchResult]:
        """Optimized search tracks using preprocessed data."""
        search_start = time.time()

        if not query or not query.strip():
            return []

        query_lower = query.lower().strip()
        results = []

        # Split query into words for word-level matching
        query_words = set(query_lower.split())

        # Search through preprocessed regular tracks
        for preprocessed in self.preprocessed_regular_tracks:
            track = preprocessed.track

            scores = []
            match_details = []

            # 1. Exact substring matches (highest priority)
            if query_lower in preprocessed.normalized_title:
                scores.append(0.95)
                match_details.append("title_exact")
            if query_lower in preprocessed.normalized_artists:
                scores.append(0.95)
                match_details.append("artist_exact")
            if query_lower in preprocessed.combined_text:
                scores.append(0.90)
                match_details.append("combined_exact")

            # 2. Word-level matching using precomputed artist words
            title_words = set(preprocessed.normalized_title.split())

            title_word_overlap = len(query_words.intersection(title_words)) / max(len(query_words), 1)
            artist_word_overlap = len(query_words.intersection(preprocessed.artist_words)) / max(len(query_words), 1)

            if title_word_overlap > 0.5:
                scores.append(0.8 * title_word_overlap)
                match_details.append(f"title_words_{title_word_overlap:.2f}")
            if artist_word_overlap > 0.5:
                scores.append(0.8 * artist_word_overlap)
                match_details.append(f"artist_words_{artist_word_overlap:.2f}")

            # 3. Fuzzy matching with Levenshtein (only if we have some matches already)
            if scores:
                title_ratio = Levenshtein.ratio(query_lower, preprocessed.normalized_title)
                artist_ratio = Levenshtein.ratio(query_lower, preprocessed.normalized_artists)
                combined_ratio = Levenshtein.ratio(query_lower, preprocessed.combined_text)

                scores.extend([title_ratio * 0.7, artist_ratio * 0.7, combined_ratio * 0.6])

            # Get the best score and apply penalty
            if scores:
                max_score = max(scores)
                final_score = max_score * preprocessed.mapping_penalty

                # Only include results above minimum threshold
                if final_score > 0.1:
                    if preprocessed.mapping_penalty < 1.0:
                        match_details.append(f"mapped_penalty_{preprocessed.mapping_penalty:.2f}")

                    results.append(FuzzyMatchResult(
                        track=track,
                        confidence=final_score,
                        match_type="search_optimized",
                        match_details=match_details[:3]
                    ))

        # Sort by confidence and limit results
        results.sort(key=lambda x: x.confidence, reverse=True)

        search_time = time.time() - search_start

        if DEBUG_PERFORMANCE:
            print(f"Search '{query}': {search_time:.3f}s, {len(results)} results")

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
