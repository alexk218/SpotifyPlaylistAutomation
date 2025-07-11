import os
import re
import time
from typing import List, Dict, Any, Tuple, Optional

import Levenshtein
from mutagen import File as MutagenFile

from sql.models.track import Track
from utils.logger import setup_logger

fuzzy_logger = setup_logger('fuzzy_matching', 'helpers', 'fuzzy_matching.log')

# Global debug flag
DEBUG_PERFORMANCE = True

# ======================
# LEVENSHTEIN PROFILING
# ======================
original_ratio = Levenshtein.ratio
levenshtein_call_count = 0
levenshtein_total_time = 0


def counted_levenshtein_ratio(s1, s2):
    global levenshtein_call_count, levenshtein_total_time
    levenshtein_call_count += 1
    start = time.time()
    result = original_ratio(s1, s2)
    levenshtein_total_time += time.time() - start
    return result


# Apply the monkey patch
Levenshtein.ratio = counted_levenshtein_ratio


def print_levenshtein_stats():
    global levenshtein_call_count, levenshtein_total_time
    if levenshtein_call_count > 0:
        avg_time = levenshtein_total_time / levenshtein_call_count
        print(
            f"Levenshtein stats: {levenshtein_call_count} calls, {levenshtein_total_time:.3f}s total, {avg_time * 1000:.3f}ms avg")
    else:
        print("No Levenshtein calls recorded")


def reset_levenshtein_stats():
    global levenshtein_call_count, levenshtein_total_time
    levenshtein_call_count = 0
    levenshtein_total_time = 0


# TODO: move this to api/models ?
class AudioDurationExtractor:
    """Extract duration from audio files using mutagen with caching."""

    def __init__(self):
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0

    def get_file_duration_ms(self, file_path: str) -> Optional[int]:
        """Extract duration from audio file in milliseconds with caching."""
        if not file_path or not os.path.exists(file_path):
            return None

        # Check cache first
        if file_path in self.cache:
            self.cache_hits += 1
            return self.cache[file_path]

        self.cache_misses += 1
        extract_start = time.time()

        try:
            audio_file = MutagenFile(file_path)
            if audio_file is None:
                duration_ms = None
            elif hasattr(audio_file, 'info') and hasattr(audio_file.info, 'length'):
                duration_seconds = audio_file.info.length
                duration_ms = int(duration_seconds * 1000)
            else:
                duration_ms = None

            # Cache the result
            self.cache[file_path] = duration_ms

            extract_time = time.time() - extract_start
            if extract_time > 0.1:  # > 100ms
                print(f"SLOW DURATION EXTRACT: {os.path.basename(file_path)} took {extract_time:.3f}s")

            return duration_ms

        except Exception as e:
            if DEBUG_PERFORMANCE:
                print(f"Duration extract error for {file_path}: {e}")
            self.cache[file_path] = None
            return None

    def print_cache_stats(self):
        total = self.cache_hits + self.cache_misses
        if total > 0:
            hit_rate = self.cache_hits / total * 100
            print(f"Duration cache: {self.cache_hits} hits, {self.cache_misses} misses ({hit_rate:.1f}% hit rate)")
        else:
            print("No duration cache activity")


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

        print(f"[DEBUG] FuzzyMatcher created with {len(tracks)} tracks")
        print(f"[DEBUG] Existing mappings: {len(existing_mappings) if existing_mappings else 0}")
        print(
            f"[DEBUG] Session assigned URIs: {len(self.session_assigned_uris) if hasattr(self, 'session_assigned_uris') else 0}")

        self.tracks = tracks
        self.existing_mappings = existing_mappings or {}
        # Track URIs assigned during this matching session
        self.session_assigned_uris = {}  # uri -> {'file_path': str, 'confidence': float, 'file_name': str}

        # Precompute normalized data for performance
        self.preprocessed_regular_tracks = []
        self.preprocessed_local_tracks = []
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
                     file_name: str,
                     threshold: float = 0.6,
                     max_matches: int = 8,
                     exclude_track_id: str = None,
                     file_path: str = None) -> List[FuzzyMatchResult]:
        """
        Find matches with detailed performance profiling.
        """
        total_start = time.time()

        # Track each step
        steps = {}

        # Extract artist/title
        step_start = time.time()
        file_name_no_ext = os.path.splitext(file_name)[0]
        artist, title = self._extract_artist_title(file_name_no_ext)
        steps['extract'] = time.time() - step_start

        if DEBUG_PERFORMANCE:
            fuzzy_logger.info(f"Matching file: {file_name} -> Artist: '{artist}', Title: '{title}'")

        all_matches = []

        # 1. Try exact local file matches first
        step_start = time.time()
        local_matches = self._find_exact_local_matches_optimized(file_name, file_name_no_ext, file_path)
        steps['local_exact'] = time.time() - step_start
        all_matches.extend(local_matches)

        # 2. Try fuzzy matching with regular tracks (main performance bottleneck)
        step_start = time.time()
        regular_matches = self._find_fuzzy_matches_optimized(artist, title, exclude_track_id, file_path)
        steps['fuzzy_regular'] = time.time() - step_start
        all_matches.extend(regular_matches)

        # 3. Try fuzzy matching with local tracks
        step_start = time.time()
        local_fuzzy_matches = self._find_local_fuzzy_matches_optimized(file_name_no_ext, exclude_track_id, file_path)
        steps['local_fuzzy'] = time.time() - step_start
        all_matches.extend(local_fuzzy_matches)

        # 4. Filter and sort
        step_start = time.time()
        filtered_matches = [match for match in all_matches if match.confidence >= threshold]
        filtered_matches.sort(key=lambda x: x.confidence, reverse=True)

        # Remove duplicates (keep highest confidence)
        seen_track_ids = set()
        unique_matches = []
        for match in filtered_matches:
            if match.track.track_id not in seen_track_ids:
                seen_track_ids.add(match.track.track_id)
                unique_matches.append(match)
        steps['filtering'] = time.time() - step_start

        total_time = time.time() - total_start

        # Performance logging for slow matches
        if total_time > 0.05:  # > 50ms
            print(f"SLOW MATCH {file_name}: {total_time:.3f}s total")
            for step_name, duration in steps.items():
                if duration > 0.01:  # > 10ms
                    print(f"  {step_name}: {duration:.3f}s")

            if len(unique_matches) > 0:
                best = unique_matches[0]
                print(f"  Best match: {best.track.artists} - {best.track.title} ({best.confidence:.3f})")

        return unique_matches[:max_matches]

    def find_best_match(self,
                        file_name: str,
                        threshold: float = 0.75,
                        exclude_track_id: str = None,
                        file_path: str = None,
                        prevent_duplicates: bool = True) -> Optional[FuzzyMatchResult]:
        """Find the single best match for a filename with duplicate prevention."""

        # DEBUG: Log session state before matching
        if prevent_duplicates and len(self.session_assigned_uris) > 0:
            print(f"[DEBUG] Matching {file_name}: {len(self.session_assigned_uris)} URIs already assigned this session")

        matches = self.find_matches(
            file_name=file_name,
            threshold=threshold,
            max_matches=10,  # Get more matches to handle duplicates
            exclude_track_id=exclude_track_id,
            file_path=file_path
        )

        if not matches:
            return None

        # Filter out already-assigned URIs if duplicate prevention is enabled
        if prevent_duplicates:
            available_matches = []
            blocked_matches = []
            for match in matches:
                uri = match.track.uri

                # Skip if this URI is already assigned in this session
                if uri in self.session_assigned_uris:
                    existing_assignment = self.session_assigned_uris[uri]
                    blocked_matches.append((match, existing_assignment))

                    # Only consider replacing if our confidence is significantly higher
                    confidence_threshold_diff = 0.1  # Must be 10% higher to replace
                    if match.confidence > existing_assignment['confidence'] + confidence_threshold_diff:
                        # This is a better match - we'll handle the replacement later
                        available_matches.append(match)

                        # Add metadata about the potential replacement
                        match.replacement_candidate = {
                            'replaces_file': existing_assignment['file_name'],
                            'replaces_confidence': existing_assignment['confidence'],
                            'confidence_improvement': match.confidence - existing_assignment['confidence']
                        }
                    else:
                        # Skip this match - already assigned to a better or equal match
                        continue
                else:
                    # URI is available
                    available_matches.append(match)

            # DEBUG: Log duplicate prevention actions
            if blocked_matches:
                print(f"[DEBUG] File '{file_name}': {len(blocked_matches)} matches blocked by session duplicates")
                for blocked_match, existing in blocked_matches:
                    print(
                        f"[DEBUG]   - Blocked URI {blocked_match.track.uri} (conf: {blocked_match.confidence:.3f}) already assigned to '{existing['file_name']}' (conf: {existing['confidence']:.3f})")

            if not available_matches:
                print(f"[DEBUG] File '{file_name}': NO available matches after duplicate filtering")
                return None

            best_match = available_matches[0]
        else:
            best_match = matches[0]

        # Track this assignment for duplicate prevention
        if prevent_duplicates and best_match.confidence >= threshold:
            uri = best_match.track.uri

            # If this is a replacement, remove the old assignment
            if hasattr(best_match, 'replacement_candidate'):
                # The old assignment will be handled in batch processing
                pass

            # Record this assignment
            self.session_assigned_uris[uri] = {
                'file_path': file_path,
                'confidence': best_match.confidence,
                'file_name': file_name
            }

            # DEBUG: Log assignment
            print(
                f"[DEBUG] Assigned URI {uri} to '{file_name}' (conf: {best_match.confidence:.3f}). Session total: {len(self.session_assigned_uris)}")

        return best_match if best_match.confidence >= threshold else None

    def _extract_artist_title(self, file_name: str) -> Tuple[str, str]:
        """Extract artist and title from filename with better handling."""
        # Try standard "Artist - Title" format first
        if " - " in file_name:
            parts = file_name.split(" - ", 1)
            return parts[0].strip(), parts[1].strip()

        # Try other common separators
        for separator in [" – ", " — ", " by "]:
            if separator in file_name:
                parts = file_name.split(separator, 1)
                return parts[0].strip(), parts[1].strip()

        # If no separator, treat whole filename as title
        return "", file_name.strip()

    def _find_exact_local_matches_optimized(self, file_name: str, file_name_no_ext: str, file_path: str = None) -> List[
        FuzzyMatchResult]:
        """Optimized exact local matches using preprocessed data."""
        matches = []

        for preprocessed in self.preprocessed_local_tracks:
            track = preprocessed.track

            # Try exact filename match
            if track.title == file_name or track.title == file_name_no_ext:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=1.0 * preprocessed.mapping_penalty,
                    match_type="exact_local"
                ))
                continue

            # Try normalized match
            normalized_file_name = self._normalize_text(file_name_no_ext)
            normalized_title = self._normalize_text(track.title)

            if normalized_title and normalized_file_name == normalized_title:
                matches.append(FuzzyMatchResult(
                    track=track,
                    confidence=0.95 * preprocessed.mapping_penalty,
                    match_type="normalized_local"
                ))

        return matches

    def _find_fuzzy_matches_optimized(self, artist: str, title: str, exclude_track_id: str = None,
                                      file_path: str = None) -> List[FuzzyMatchResult]:
        """HEAVILY OPTIMIZED fuzzy matching with detailed profiling."""
        matching_start = time.time()

        # Track detailed steps
        steps = {
            'normalization': 0,
            'candidate_filtering': 0,
            'confidence_calculations': 0,
            'duration_calculations': 0,
            'result_processing': 0
        }

        matches = []

        # Normalization
        step_start = time.time()
        normalized_artist = artist.lower().replace('&', 'and') if artist else ""
        normalized_title = title.lower()
        local_base, local_remix_info = self._extract_remix_info_fast(normalized_title)
        steps['normalization'] = time.time() - step_start

        # Candidate filtering
        step_start = time.time()
        if normalized_artist:
            clean_artist = normalized_artist.replace(',', ' ').replace(';', ' ').replace('&', ' ')
            artist_words = set(word for word in clean_artist.split() if word)

            candidate_tracks = [
                pt for pt in self.preprocessed_regular_tracks
                if not artist_words.isdisjoint(pt.artist_words)
            ]
        else:
            candidate_tracks = self.preprocessed_regular_tracks

        steps['candidate_filtering'] = time.time() - step_start

        # Track Levenshtein calls for this file
        levenshtein_start_count = levenshtein_call_count

        # Process candidate tracks
        step_start = time.time()
        processed_count = 0
        duration_calc_time = 0

        for preprocessed in candidate_tracks:
            track = preprocessed.track

            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            # Confidence calculation
            confidence = self._calculate_confidence_fast(
                normalized_artist, local_base, local_remix_info,
                preprocessed.normalized_artists, preprocessed.base_title, preprocessed.remix_info
            )

            # Apply precomputed mapping penalty
            final_confidence = confidence * preprocessed.mapping_penalty

            # Duration boost calculation
            duration_boost_applied = False
            if file_path and track.duration_ms and hasattr(self, 'duration_extractor'):
                duration_start = time.time()
                duration_boost = self._calculate_duration_confidence_boost(file_path, track.duration_ms)
                duration_calc_time += time.time() - duration_start

                final_confidence = min(1.0, final_confidence * duration_boost)
                duration_boost_applied = duration_boost > 1.0

            processed_count += 1

            # Collect matches
            if final_confidence >= 0.2:
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

        steps['confidence_calculations'] = time.time() - step_start
        steps['duration_calculations'] = duration_calc_time

        # Result processing
        step_start = time.time()
        # Sort matches by confidence
        matches.sort(key=lambda x: x.confidence, reverse=True)
        steps['result_processing'] = time.time() - step_start

        total_time = time.time() - matching_start
        file_levenshtein_calls = levenshtein_call_count - levenshtein_start_count

        # Detailed logging for slow operations
        if total_time > 0.02 or file_levenshtein_calls > 1000:  # > 20ms or many Levenshtein calls
            print(f"  FUZZY DETAILS for '{title}' (artist: '{artist}'):")
            print(f"    Total time: {total_time:.3f}s")
            print(
                f"    Candidates: {len(candidate_tracks)}/{len(self.preprocessed_regular_tracks)} ({len(candidate_tracks) / len(self.preprocessed_regular_tracks) * 100:.1f}%)")
            print(f"    Processed: {processed_count}")
            print(f"    Levenshtein calls: {file_levenshtein_calls}")
            print(f"    Matches found: {len(matches)}")

            # Step breakdown
            for step_name, duration in steps.items():
                if duration > 0.005:  # > 5ms
                    print(f"    {step_name}: {duration:.3f}s")

            if len(matches) > 0:
                print(
                    f"    Best match: {matches[0].track.artists} - {matches[0].track.title} ({matches[0].confidence:.3f})")

        return matches

    def _find_local_fuzzy_matches_optimized(self, file_name_no_ext: str, exclude_track_id: str = None,
                                            file_path: str = None) -> List[FuzzyMatchResult]:
        """Optimized local fuzzy matches using preprocessed data."""
        matches = []

        for preprocessed in self.preprocessed_local_tracks:
            track = preprocessed.track

            if exclude_track_id and track.track_id == exclude_track_id:
                continue

            # Calculate similarity using precomputed normalized title
            similarity = Levenshtein.ratio(file_name_no_ext.lower(), preprocessed.normalized_title)

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

        # Search through all regular & local tracks
        all_preprocessed_tracks = self.preprocessed_regular_tracks + self.preprocessed_local_tracks
        for preprocessed in all_preprocessed_tracks:
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
def find_fuzzy_matches(file_name: str, tracks: List[Track], threshold: float = 0.6,
                       max_matches: int = 8, exclude_track_id: str = None,
                       existing_mappings: Dict[str, str] = None,
                       file_path: str = None) -> List[Dict[str, Any]]:
    """Find fuzzy matches and return as dictionaries."""
    matcher = FuzzyMatcher(tracks, existing_mappings)
    matches = matcher.find_matches(file_name, threshold, max_matches, exclude_track_id, file_path)
    return [match.to_dict() for match in matches]


def find_best_match(file_name: str, tracks: List[Track], threshold: float = 0.75,
                    exclude_track_id: str = None, existing_mappings: Dict[str, str] = None,
                    file_path: str = None) -> Optional[Dict[str, Any]]:
    """Find the best match and return as dictionary."""
    matcher = FuzzyMatcher(tracks, existing_mappings)
    match = matcher.find_best_match(file_name, threshold, exclude_track_id, file_path)
    return match.to_dict() if match else None


def search_tracks(query: str, tracks: List[Track], limit: int = 20,
                  existing_mappings: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """Search tracks and return as dictionaries."""
    matcher = FuzzyMatcher(tracks, existing_mappings)
    matches = matcher.search_tracks(query, limit)
    return [match.to_dict() for match in matches]
