import os
import subprocess
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

from api.constants.file_extensions import SUPPORTED_AUDIO_EXTENSIONS
from api.services.duplicate_track_service import detect_duplicate_file_mappings, resolve_duplicate_mappings, \
    analyze_existing_duplicate_mappings
from helpers.fuzzy_match_helper import search_tracks, find_fuzzy_matches, FuzzyMatcher, \
    print_levenshtein_stats, reset_levenshtein_stats
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

mapping_logger = setup_logger('file_mapping', 'sql', 'file_mapping.log')


def search_tracks_file_system(master_tracks_dir, query):
    """
    Search for tracks in file system that match the query.
    """
    if not query:
        return []

    results = []
    query = query.lower()

    # Get all file mappings and tracks from database for quick lookup
    with UnitOfWork() as uow:
        all_mappings = uow.file_track_mapping_repository.get_all()
        all_tracks = uow.track_repository.get_all()
        tracks_by_uri = uow.track_repository.get_all_as_dict_by_uri()

    # Create lookup dictionary: file_path -> mapping
    mapping_by_path = {}
    existing_mappings = {}
    for mapping in all_mappings:
        if mapping.is_active:
            normalized_path = os.path.normpath(mapping.file_path)
            mapping_by_path[normalized_path] = mapping
            existing_mappings[normalized_path] = mapping.uri

    # Scan the files in the master directory
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            # Check if file is a supported audio format
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext not in SUPPORTED_AUDIO_EXTENSIONS:
                continue

            file_path = os.path.join(str(root), str(file))
            file_name_no_ext = os.path.splitext(file)[0].lower()

            # Check if query is in filename
            if query in file_name_no_ext:
                # Look up URI and track info from FileTrackMappings
                normalized_path = os.path.normpath(file_path)
                mapping = mapping_by_path.get(normalized_path)

                uri = None
                track_id = None
                track_info = "Unknown"
                confidence = 0

                if mapping and mapping.uri:
                    uri = mapping.uri

                    # Get track info from Tracks table using URI
                    track = tracks_by_uri.get(uri)
                    if track:
                        track_id = track.get_spotify_track_id()
                        track_info = f"{track.artists} - {track.title}"

                        try:
                            # Use improved fuzzy matching with existing mappings
                            fuzzy_matches = find_fuzzy_matches(
                                file_name=file,
                                tracks=all_tracks,
                                threshold=0.0,
                                max_matches=10,
                                exclude_track_id=None,
                                existing_mappings=existing_mappings,
                                file_path=file_path
                            )

                            # Find the confidence for our specifically mapped track
                            mapped_track_confidence = None
                            for match in fuzzy_matches:
                                if match.get('uri') == uri:
                                    mapped_track_confidence = match.get('confidence', 0.7)
                                    break

                            if mapped_track_confidence is not None:
                                confidence = mapped_track_confidence
                            else:
                                confidence = 0.4  # Lower confidence for poor filename matches

                        except Exception as e:
                            print(f"Error calculating confidence for {file}: {e}")
                    else:
                        track_info = f"Mapped to: {uri}"
                        confidence = 0.5  # Has mapping but track not found - medium confidence

                results.append({
                    'file': file,
                    'uri': uri,
                    'track_id': track_id,
                    'track_info': track_info,
                    'file_name': file_name_no_ext,
                    'confidence': confidence,
                    'full_path': file_path,
                    'has_mapping': mapping is not None,
                    'file_extension': file_ext
                })

    # Sort results by relevance (higher confidence first, then by filename match position)
    results.sort(key=lambda x: (-x['confidence'], x['file'].lower().find(query)))

    print(results)
    return results


def search_tracks_db_for_matching(query: str, limit: int = 20) -> List[Dict]:
    """Advanced search specifically for file-track matching with fuzzy matching and ranking."""
    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()
        # Get existing mappings for search awareness
        all_mappings = uow.file_track_mapping_repository.get_all()
        existing_mappings = {mapping.file_path: mapping.uri for mapping in all_mappings if mapping.is_active}

    return search_tracks(query, all_tracks, limit, existing_mappings)


def fuzzy_match_track(file_name, current_track_id=None):
    """
    Find potential Spotify track matches for a local file.
    """
    # Load tracks from database for matching
    with UnitOfWork() as uow:
        try:
            tracks_db = uow.track_repository.get_all()
            # Get existing mappings to avoid conflicts
            all_mappings = uow.file_track_mapping_repository.get_all()
            existing_mappings = {mapping.file_path: mapping.uri for mapping in all_mappings if mapping.is_active}
        except Exception as e:
            print(f"Database error: {e}")
            raise ValueError(f"Database error: {str(e)}")

    # Use fuzzy matching
    matches = find_fuzzy_matches(
        file_name=file_name,
        tracks=tracks_db,
        threshold=0.45,  # Lower threshold for showing more options
        max_matches=8,
        exclude_track_id=current_track_id,
        existing_mappings=existing_mappings
    )

    # Extract artist and title for display
    file_name_no_ext = os.path.splitext(file_name)[0]
    if " - " in file_name_no_ext:
        artist, track_title = file_name_no_ext.split(" - ", 1)
    else:
        artist = ""
        track_title = file_name_no_ext

    return {
        "file_name": file_name,
        "original_artist": artist,
        "original_title": track_title,
        "matches": matches
    }


def analyze_file_mappings_with_duplicate_detection(master_tracks_dir: str,
                                                   confidence_threshold: float = 0.75) -> Dict[str, Any]:
    """
    Enhanced analysis that includes duplicate detection and prevention.
    """
    mapping_logger.info(f"Starting enhanced file mapping analysis with duplicate detection")
    start_time = time.time()

    # Perform standard analysis first
    standard_analysis = analyze_file_mappings(master_tracks_dir, confidence_threshold)

    if not standard_analysis.get('auto_matched_files'):
        # No auto-matches to check for duplicates
        return {
            **standard_analysis,
            'duplicate_detection': {
                'performed': False,
                'reason': 'No auto-matched files to check'
            }
        }

    # Extract proposed mappings from auto-matched files
    proposed_mappings = []
    for auto_match in standard_analysis['auto_matched_files']:
        proposed_mappings.append({
            'file_path': auto_match['file_path'],
            'uri': auto_match['uri'],
            'confidence': auto_match['confidence'],
            'file_name': auto_match['file_name'],
            'source': 'auto_match',
            'track_info': auto_match.get('track_info', '')
        })

    with UnitOfWork() as uow:
        all_mappings = uow.file_track_mapping_repository.get_all()
        existing_mappings = []
        for mapping in all_mappings:
            if mapping.is_active:
                existing_mappings.append({
                    'file_path': mapping.file_path,
                    'uri': mapping.uri,
                    'confidence': 1.0,  # Existing mappings are 100% confident
                    'file_name': os.path.basename(mapping.file_path),
                    'source': 'existing_database',
                    'track_info': f"Existing mapping: {mapping.uri}"
                })

    # Detect duplicates
    all_mappings_to_check = proposed_mappings + existing_mappings
    duplicate_detection = detect_duplicate_file_mappings(all_mappings_to_check)

    if duplicate_detection['needs_user_resolution']:
        # *** FIX: Filter to only include NEW proposed mappings ***
        clean_auto_matches = [
            mapping for mapping in duplicate_detection['clean_mappings_data']
            if mapping['source'] == 'auto_match'  # Only new auto-matches, not existing DB entries
        ]

        duplicate_groups = duplicate_detection['duplicate_groups_data']

        # Convert duplicate groups to files requiring user input
        additional_user_input_files = []
        for group in duplicate_groups:
            for mapping in group['mappings']:
                # *** ONLY process NEW proposed mappings in conflicts ***
                if mapping['source'] == 'auto_match':  # Skip existing DB mappings
                    user_input_entry = {
                        'file_path': mapping['file_path'],
                        'file_name': mapping['file_name'],
                        'duplicate_conflict': True,
                        'conflicting_uri': group['uri'],
                        'conflicting_track_info': group['track_info'],
                        # ... rest of the logic
                    }
                    additional_user_input_files.append(user_input_entry)

        # Update the analysis results
        updated_analysis = {
            **standard_analysis,
            'auto_matched_files': clean_auto_matches,  # Now only contains NEW auto-matches
            'files_requiring_user_input': standard_analysis['files_requiring_user_input'] + additional_user_input_files,
            'duplicate_detection': {
                'performed': True,
                'conflicts_found': duplicate_detection['duplicate_groups'],
                'clean_mappings': len(clean_auto_matches),  # Count only NEW clean mappings
                'needs_resolution': True,
                'duplicate_groups': [
                    group for group in duplicate_groups
                    if any(m['source'] == 'auto_match' for m in group['mappings'])  # Only groups with NEW mappings
                ]
            }
        }

        mapping_logger.warning(
            f"Found {duplicate_detection['duplicate_groups']} duplicate conflicts requiring user resolution")

    else:
        # No duplicates found
        updated_analysis = {
            **standard_analysis,
            'duplicate_detection': {
                'performed': True,
                'conflicts_found': 0,
                'clean_mappings': duplicate_detection['clean_mappings'],
                'needs_resolution': False
            }
        }

        mapping_logger.info("No duplicate conflicts found in auto-matched files")

    elapsed_time = time.time() - start_time
    mapping_logger.info(f"Enhanced analysis completed in {elapsed_time:.2f} seconds")

    return updated_analysis


def create_file_mappings_with_duplicate_resolution(master_tracks_dir: str,
                                                   user_selections: List[Dict[str, Any]],
                                                   precomputed_changes: Dict[str, Any] = None,
                                                   duplicate_resolutions: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Enhanced file mapping creation that handles duplicate resolution.

    Args:
        duplicate_resolutions: Dict mapping URI to selected file_path for conflicts
    """
    mapping_logger.info("Starting enhanced file mapping creation with duplicate resolution")

    # Get proposed mappings
    proposed_mappings = []

    # Add auto-matched files from precomputed changes
    if precomputed_changes and 'auto_matched_files' in precomputed_changes:
        for auto_match in precomputed_changes['auto_matched_files']:
            proposed_mappings.append({
                'file_path': auto_match['file_path'],
                'file_name': auto_match.get('file_name', auto_match.get('file_name', '')),
                'uri': auto_match['uri'],
                'confidence': auto_match['confidence'],
                'source': 'auto_match'
            })

    # Add user selections
    for selection in user_selections:
        file_path = selection.get('file_path')
        if not file_path:
            file_name = selection.get('file_name', selection.get('file_name', ''))
            if file_name:
                file_path = _find_file_path_in_directory(file_name, master_tracks_dir)

        if file_path and selection.get('uri'):
            proposed_mappings.append({
                'file_path': file_path,
                'file_name': selection.get('file_name', selection.get('file_name', os.path.basename(file_path))),
                'uri': selection['uri'],
                'confidence': selection.get('confidence', 0.0),
                'source': 'user_selection'
            })

    # Detect duplicates in final mapping set
    duplicate_detection = detect_duplicate_file_mappings(proposed_mappings)

    final_mappings = []

    if duplicate_detection['needs_user_resolution']:
        if duplicate_resolutions:
            # Resolve duplicates using user selections
            resolution_result = resolve_duplicate_mappings(
                duplicate_detection['duplicate_groups_data'],
                duplicate_resolutions
            )

            # Combine clean mappings with resolved mappings
            final_mappings = duplicate_detection['clean_mappings_data'] + resolution_result['resolved_mappings']

            mapping_logger.info(f"Resolved {resolution_result['total_resolved']} duplicate conflicts")
        else:
            # Return duplicate conflicts for user resolution
            return {
                'success': True,
                'stage': 'duplicate_resolution_required',
                'message': f"Found {duplicate_detection['duplicate_groups']} duplicate mapping conflicts that require user resolution",
                'duplicate_detection': duplicate_detection,
                'needs_duplicate_resolution': True,
                'clean_mappings_count': duplicate_detection['clean_mappings'],
                'conflicted_mappings_count': sum(
                    len(group['mappings']) for group in duplicate_detection['duplicate_groups_data'])
            }
    else:
        # No duplicates - use all clean mappings
        final_mappings = duplicate_detection['clean_mappings_data']

    # Create the mappings using existing batch creation logic
    creation_result = create_file_mappings_batch(master_tracks_dir, final_mappings)

    return {
        **creation_result,
        'stage': 'mapping_complete',
        'duplicate_detection': duplicate_detection,
        'duplicate_resolution_applied': bool(duplicate_resolutions)
    }


def get_existing_duplicate_mappings() -> Dict[str, Any]:
    """
    Get existing duplicate file mappings from the database.
    """
    return analyze_existing_duplicate_mappings()


def analyze_file_mappings(master_tracks_dir: str, confidence_threshold: float = 0.75) -> Dict[str, Any]:
    """
    Analyze which files need mapping to Spotify tracks with detailed profiling.
    """
    start_time = time.time()
    mapping_logger.info(f"Starting file mapping analysis for directory: {master_tracks_dir}")

    # Reset profiling stats
    reset_levenshtein_stats()

    # Get all tracks and existing mappings from database
    db_start = time.time()
    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()
        all_mappings = uow.file_track_mapping_repository.get_all()
        existing_mappings = {
            os.path.normpath(os.path.abspath(mapping.file_path)): mapping.uri
            for mapping in all_mappings if mapping.is_active
        }
    db_time = time.time() - db_start

    mapping_logger.info(
        f"Loaded {len(all_tracks)} tracks and {len(existing_mappings)} existing mappings in {db_time:.2f}s")
    print(
        f"Loaded {len(all_tracks)} tracks and {len(existing_mappings)} existing mappings in {db_time:.2f}s")

    # Scan for all audio files
    scan_start = time.time()
    all_audio_files = []
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in SUPPORTED_AUDIO_EXTENSIONS:
                file_path = os.path.join(root, file)
                normalized_path = os.path.normpath(os.path.abspath(file_path))
                all_audio_files.append((normalized_path, file))
    scan_time = time.time() - scan_start

    total_files = len(all_audio_files)
    mapping_logger.info(f"Found {total_files} total audio files in {scan_time:.2f}s")
    print(f"Found {total_files} total audio files in {scan_time:.2f}s")

    # Filter out files that already have mappings
    filter_start = time.time()
    unmapped_files = []
    mapped_file_count = 0

    for file_path, file in all_audio_files:
        if file_path in existing_mappings:
            mapped_file_count += 1
        else:
            unmapped_files.append((file_path, file))
    filter_time = time.time() - filter_start

    print(f"Files with existing mappings: {mapped_file_count}")
    print(f"Files needing mapping: {len(unmapped_files)}")

    # Early exit if no files need mapping
    if not unmapped_files:
        mapping_logger.info("All files already have mappings, exiting early")
        return {
            "total_files": total_files,
            "files_without_mappings": 0,
            "files_requiring_user_input": [],
            "auto_matched_files": [],
            "needs_confirmation": False,
            "requires_user_selection": False
        }

    # Create FuzzyMatcher once
    matcher_start = time.time()
    fuzzy_matcher = FuzzyMatcher(all_tracks, existing_mappings)
    matcher_time = time.time() - matcher_start
    print(f"FuzzyMatcher created in {matcher_time:.2f}s")

    # Process files with detailed tracking
    auto_matched_files = []
    files_requiring_user_input = []
    files_without_mappings = []

    # Performance tracking
    processed_count = 0
    slow_file_count = 0
    total_matching_time = 0

    match_start = time.time()

    # Process all files
    for file_path, file in unmapped_files:
        specific_match_start = time.time()
        processed_count += 1

        if processed_count % 100 == 0:
            elapsed = time.time() - match_start
            rate = processed_count / elapsed
            remaining = len(unmapped_files) - processed_count
            eta = remaining / rate if rate > 0 else 0

            mapping_logger.info(f"Processed {processed_count}/{len(unmapped_files)} files")
            print(
                f"Processed {processed_count}/{len(unmapped_files)} files ({rate:.1f} files/sec, ETA: {eta:.0f}s)")

        # Find best match
        best_match = fuzzy_matcher.find_best_match(
            file_name=file,
            threshold=confidence_threshold,
            file_path=file_path
        )

        match_time = time.time() - specific_match_start
        total_matching_time += match_time

        if best_match:
            if best_match.confidence >= confidence_threshold:
                # Auto-match high confidence files
                auto_matched_files.append({
                    'file_path': file_path,
                    'file_name': file,
                    'uri': str(best_match.track.uri),
                    'confidence': float(best_match.confidence),
                    'match_type': str(best_match.match_type),
                    'track_info': f"{best_match.track.artists} - {best_match.track.title}"
                })
            else:
                # Requires user input - get multiple matches
                all_matches = fuzzy_matcher.find_matches(
                    file_name=file,
                    threshold=0.3,
                    max_matches=5,
                    file_path=file_path
                )

                potential_matches = []
                for match in all_matches:
                    potential_matches.append({
                        'uri': str(match.track.uri),
                        'confidence': float(match.confidence),
                        'track_info': f"{match.track.artists} - {match.track.title}",
                        'match_type': str(match.match_type)
                    })

                files_requiring_user_input.append({
                    'file_path': file_path,
                    'file_name': file,
                    'potential_matches': potential_matches,
                    'top_match': {
                        'uri': str(best_match.track.uri),
                        'confidence': float(best_match.confidence),
                        'track_info': f"{best_match.track.artists} - {best_match.track.title}",
                        'match_type': str(best_match.match_type)
                    }
                })
        else:
            # No matches found
            files_requiring_user_input.append({
                'file_path': file_path,
                'file_name': file,
                'potential_matches': [],
                'top_match': None
            })

        files_without_mappings.append(file)

        # Track slow files
        if match_time > 0.1:  # > 100ms
            slow_file_count += 1
            print(f"\033[91mSLOW FILE #{slow_file_count}: {file} took {match_time:.3f}s\033[0m")

    match_time = time.time() - match_start
    elapsed_time = time.time() - start_time

    # Final performance summary
    print(f"\n=== FINAL PERFORMANCE SUMMARY ===")
    print(f"Analysis complete in {elapsed_time:.2f} seconds:")
    print(f"  Database time: {db_time:.2f}s")
    print(f"  File scanning: {scan_time:.2f}s")
    print(f"  Filtering: {filter_time:.2f}s")
    print(f"  Matcher creation: {matcher_time:.2f}s")
    print(f"  Total matching: {match_time:.2f}s")
    print(f"  Average per file: {match_time / len(unmapped_files) * 1000:.1f}ms")
    print(f"  Files >100ms: {slow_file_count}")
    print(f"  Total files: {total_files}")
    print(
        f"  Files with existing mappings: {mapped_file_count} ({mapped_file_count / total_files * 100:.1f}%)")
    print(f"  Files without mappings: {len(files_without_mappings)}")
    print(f"  Auto-matched files: {len(auto_matched_files)}")
    print(f"  Files requiring user input: {len(files_requiring_user_input)}")
    print(f"  Performance: {len(unmapped_files) / match_time:.1f} files/second")

    # Print detailed cache and operation stats
    print(f"\n=== DETAILED STATS ===")
    fuzzy_matcher.duration_extractor.print_cache_stats()
    print_levenshtein_stats()

    auto_matched_files.sort(key=lambda x: x['confidence'], reverse=True)

    print(f'FILES REQUIRING USER INPUT {files_requiring_user_input}')
    print(f'FILES REQUIRING USER INPUT {len(files_requiring_user_input)}')

    mapping_logger.info(f"Analysis completed in {elapsed_time:.2f} seconds")

    return {
        "total_files": total_files,
        "files_without_mappings": len(files_without_mappings),
        "files_requiring_user_input": files_requiring_user_input,
        "auto_matched_files": auto_matched_files,
        "needs_confirmation": len(files_requiring_user_input) > 0,
        "requires_user_selection": len(files_requiring_user_input) > 0
    }


def _find_best_match_for_file(file_name: str, local_tracks: List, regular_tracks: List,
                              confidence_threshold: float, existing_mappings: Dict[str, str] = None,
                              file_path: str = None) -> Optional[Dict[str, Any]]:
    all_tracks = local_tracks + regular_tracks

    matcher = FuzzyMatcher(all_tracks, existing_mappings)
    match = matcher.find_best_match(
        file_name=file_name,
        threshold=confidence_threshold,
        file_path=file_path
    )

    if match:
        return {
            'uri': match.track.uri,
            'confidence': match.confidence,
            'match_type': match.match_type,
            'track_info': f"{match.track.artists} - {match.track.title}",
            'artists': match.track.artists,
            'title': match.track.title,
            'album': match.track.album
        }

    return None


def orchestrate_file_mapping(master_tracks_dir: str, confirmed: bool, precomputed_changes: Dict[str, Any],
                             confidence_threshold: float, user_selections: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Handle file mapping operations with consistent response structure."""
    if not confirmed:
        # Analysis phase - return files that need mapping
        analysis_result = analyze_file_mappings(master_tracks_dir, confidence_threshold)

        return {
            "success": True,
            "stage": "analysis",
            "message": f"Found {analysis_result['files_without_mappings']} files without mappings. "
                       f"{len(analysis_result['auto_matched_files'])} can be auto-matched, "
                       f"{len(analysis_result['files_requiring_user_input'])} require user selection.",
            "needs_confirmation": analysis_result['needs_confirmation'],
            "requires_user_selection": analysis_result['requires_user_selection'],
            "details": analysis_result
        }
    else:
        # Execution phase - create the mappings
        creation_result = create_file_mappings_batch(master_tracks_dir, user_selections, precomputed_changes)

        return {
            "success": True,
            "stage": "mapping_complete",
            "message": f"File mapping completed: {creation_result['successful_mappings']} successful, "
                       f"{creation_result['failed_mappings']} failed.",
            "successful_mappings": creation_result['successful_mappings'],
            "failed_mappings": creation_result['failed_mappings'],
            "results": creation_result['results'],
            "total_processed": creation_result['total_processed']
        }


def create_file_mappings_batch(master_tracks_dir: str, user_selections: List[Dict[str, Any]],
                               precomputed_changes: Dict[str, Any] = None) -> Dict[str, Any]:
    """Create file mappings in the database using batch operations."""
    mapping_logger.info("Starting batch file mapping creation")
    batch_start = time.time()

    successful_mappings = 0
    failed_mappings = 0
    results = []

    # Process auto-matched files from precomputed changes if available
    auto_matched_files = []
    if precomputed_changes and 'auto_matched_files' in precomputed_changes:
        auto_matched_files = precomputed_changes['auto_matched_files']
        mapping_logger.info(f"Processing {len(auto_matched_files)} auto-matched files from analysis")

    # Combine auto-matches and user selections
    all_mappings = []

    # Add auto-matched files
    for auto_match in auto_matched_files:
        all_mappings.append({
            'file_path': auto_match['file_path'],
            'file_name': auto_match.get('file_name', auto_match.get('file_name', '')),
            'uri': auto_match['uri'],
            'confidence': auto_match['confidence'],
            'source': 'auto_match'
        })

    # Add user selections
    for selection in user_selections:
        file_path = selection.get('file_path')
        if not file_path:
            # Try to construct file path from filename if not provided
            file_name = selection.get('file_name', selection.get('file_name', ''))
            if file_name:
                file_path = _find_file_path_in_directory(file_name, master_tracks_dir)

        if file_path and selection.get('uri'):
            all_mappings.append({
                'file_path': file_path,
                'file_name': selection.get('file_name', selection.get('file_name', os.path.basename(file_path))),
                'uri': selection['uri'],
                'confidence': selection.get('confidence', 0.0),
                'source': 'user_selection'
            })

    if not all_mappings:
        return {
            'successful_mappings': 0,
            'failed_mappings': 0,
            'results': [],
            'total_processed': 0
        }

    mapping_logger.info(f"Processing {len(all_mappings)} total mappings")

    # OPTIMIZATION 1: Batch file existence checks
    file_check_start = time.time()
    valid_mappings = []
    for mapping in all_mappings:
        file_path = mapping['file_path']
        if os.path.exists(file_path):
            # Normalize path for consistency
            mapping['file_path'] = os.path.normpath(file_path)
            valid_mappings.append(mapping)
        else:
            failed_mappings += 1
            results.append({
                'file_name': mapping['file_name'],
                'uri': mapping['uri'],
                'success': False,
                'reason': 'File not found'
            })
            mapping_logger.error(f"File not found: {file_path}")

    file_check_time = time.time() - file_check_start
    print(f"File existence checks: {file_check_time:.3f}s for {len(all_mappings)} files")

    if not valid_mappings:
        return {
            'successful_mappings': successful_mappings,
            'failed_mappings': failed_mappings,
            'results': results,
            'total_processed': len(all_mappings)
        }

    # OPTIMIZATION 2: Batch database operations
    db_start = time.time()
    with UnitOfWork() as uow:
        # Batch 1: Check which URIs exist in tracks table
        uri_check_start = time.time()
        all_uris = [mapping['uri'] for mapping in valid_mappings]

        # Get all tracks by URI in one query
        existing_tracks = uow.track_repository.batch_get_tracks_by_uris(all_uris)
        existing_track_uris = {track.uri for track in existing_tracks}

        # Create lookup for track info
        track_info_by_uri = {
            track.uri: f"{track.artists} - {track.title}"
            for track in existing_tracks
        }
        uri_check_time = time.time() - uri_check_start

        # Batch 2: Check existing file mappings
        existing_check_start = time.time()
        all_file_paths = [mapping['file_path'] for mapping in valid_mappings]

        # Get existing mappings for all files in one query
        existing_mappings_dict = uow.file_track_mapping_repository.get_uri_mappings_batch(all_file_paths)
        existing_check_time = time.time() - existing_check_start

        print(f"URI validation: {uri_check_time:.3f}s")
        print(f"Existing mappings check: {existing_check_time:.3f}s")

        # Process mappings and prepare batch inserts
        batch_insert_start = time.time()
        mappings_to_insert = []

        for mapping in valid_mappings:
            file_path = mapping['file_path']
            uri = mapping['uri']
            file_name = mapping['file_name']

            # Check if URI exists in tracks table
            if uri not in existing_track_uris:
                failed_mappings += 1
                results.append({
                    'file_name': file_name,
                    'uri': uri,
                    'success': False,
                    'reason': 'Track URI not found in database'
                })
                mapping_logger.error(f"Track URI not found in database: {uri}")
                continue

            # Check if mapping already exists
            existing_uri = existing_mappings_dict.get(file_path)
            if existing_uri:
                if existing_uri == uri:
                    # Same mapping already exists - consider it successful
                    successful_mappings += 1
                    results.append({
                        'file_name': file_name,
                        'uri': uri,
                        'success': True,
                        'reason': 'Mapping already exists',
                        'confidence': mapping.get('confidence', 0.0),
                        'source': mapping['source'],
                        'track_info': track_info_by_uri.get(uri, 'Unknown track')
                    })
                    mapping_logger.debug(f"Mapping already exists: {file_name} -> {uri}")
                    continue
                else:
                    # Different mapping exists - this is a conflict
                    failed_mappings += 1
                    results.append({
                        'file_name': file_name,
                        'uri': uri,
                        'success': False,
                        'reason': f'File already mapped to different track: {existing_uri}'
                    })
                    mapping_logger.warning(f"Mapping conflict for {file_name}: existing={existing_uri}, new={uri}")
                    continue

            # Add to batch insert list
            mappings_to_insert.append({
                'file_path': file_path,
                'uri': uri,
                'file_name': file_name,
                'confidence': mapping.get('confidence', 0.0),
                'source': mapping['source']
            })

        # OPTIMIZATION 3: Batch insert all new mappings
        if mappings_to_insert:
            insert_success_count = uow.file_track_mapping_repository.batch_add_mappings_by_uri(mappings_to_insert)

            # Record results for successful inserts
            for mapping in mappings_to_insert:
                successful_mappings += 1
                results.append({
                    'file_name': mapping['file_name'],
                    'uri': mapping['uri'],
                    'success': True,
                    'confidence': mapping['confidence'],
                    'source': mapping['source'],
                    'track_info': track_info_by_uri.get(mapping['uri'], 'Unknown track')
                })

            mapping_logger.info(f"Batch inserted {insert_success_count} new mappings")

        batch_insert_time = time.time() - batch_insert_start
        print(f"Batch processing and insert: {batch_insert_time:.3f}s")

    db_time = time.time() - db_start
    total_time = time.time() - batch_start

    print(f"Total database time: {db_time:.3f}s")
    print(f"Total batch mapping time: {total_time:.3f}s for {len(all_mappings)} files")
    print(f"Average per file: {total_time / len(all_mappings) * 1000:.1f}ms")

    mapping_logger.info(
        f"OPTIMIZED batch mapping creation complete: {successful_mappings} successful, {failed_mappings} failed in {total_time:.3f}s")

    return {
        'successful_mappings': successful_mappings,
        'failed_mappings': failed_mappings,
        'results': results,
        'total_processed': len(all_mappings)
    }


def _is_supported_audio_file(file_name: str) -> bool:
    """Check if file is a supported audio format."""
    return os.path.splitext(file_name)[1].lower() in SUPPORTED_AUDIO_EXTENSIONS


def _find_file_path_in_directory(file_name: str, directory: str) -> str | None:
    """Find the full path of a file in the directory tree."""
    for root, _, files in os.walk(directory):
        if file_name in files:
            return os.path.join(root, file_name)
    return None


def delete_file(file_path):
    """Delete a file from the filesystem."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    file_name = os.path.basename(file_path)
    os.remove(file_path)

    return file_name


def direct_tracks_compare(master_tracks_dir):
    """Directly compare Spotify tracks with local tracks from the database using FileTrackMapping."""
    # 1. Get all tracks from the master playlist in the database
    with UnitOfWork() as uow:
        master_tracks = uow.track_repository.get_all()

        # Convert to a list of dicts for JSON serialization
        master_tracks_list = []
        for track in master_tracks:
            master_tracks_list.append({
                'uri': track.uri,
                'id': track.track_id,
                'name': track.title,
                'artists': track.artists,
                'album': track.album,
                'added_at': track.added_to_master if track.added_to_master else None
            })

        # 2. Get all file mappings from the database
        all_mappings = uow.file_track_mapping_repository.get_all()

        # Create sets for quick lookup
        mapped_uris = set()
        local_tracks_info = []

        for mapping in all_mappings:
            if not mapping.is_active:
                continue

            # Verify the file still exists
            if not os.path.exists(mapping.file_path):
                continue

            mapped_uris.add(mapping.uri)
            local_tracks_info.append({
                'path': mapping.file_path,
                'file_name': mapping.get_filename(),
                'uri': mapping.uri,
                'track_id': mapping.get_track_id(),
                'size': mapping.file_size or (
                    os.path.getsize(mapping.file_path) if os.path.exists(mapping.file_path) else 0),
                'modified': mapping.last_modified if mapping.last_modified else (
                    os.path.getmtime(mapping.file_path) if os.path.exists(mapping.file_path) else 0),
                'file_hash': mapping.file_hash,
                'is_local_file': mapping.is_local_file_mapping()
            })

        # 3. Compare to find missing tracks
        missing_tracks = []
        for track in master_tracks_list:
            # Skip tracks without a URI
            if not track['uri']:
                continue

            # If track URI is not in mapped files, it's missing
            if track['uri'] not in mapped_uris:
                missing_tracks.append(track)

        # Sort missing tracks by added_at date, newest first
        missing_tracks.sort(
            key=lambda x: x['added_at'] if x['added_at'] else '0',
            reverse=True
        )

        # 4. Return the results
        return {
            "database_time": datetime.now().isoformat(),
            "master_tracks": master_tracks_list,
            "local_tracks": {
                "count": len(mapped_uris),
                "tracks": local_tracks_info[:100]  # Limit to avoid huge payloads
            },
            "missing_tracks": missing_tracks,
            "master_tracks_dir": master_tracks_dir,
        }


def download_and_map_track(uri: str, download_dir: str):
    """Download a track using spotDL and create FileTrackMapping entry."""
    # Get track details from database
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_uri(uri)
        if not track:
            raise ValueError(f"Track URI '{uri}' not found in database")

    # Extract track ID for Spotify URL construction
    if uri.startswith('spotify:track:'):
        track_id = uri.split(':')[2]
    elif uri.startswith('spotify:local:'):
        raise ValueError(f"Cannot download local file URI: {uri}")
    else:
        raise ValueError(f"Invalid Spotify URI format: {uri}")

    # Construct Spotify URL
    spotify_url = f"https://open.spotify.com/track/{track_id}"

    # Get list of existing files BEFORE download
    existing_files = set()
    try:
        for file in os.listdir(download_dir):
            if file.endswith('.mp3'):
                existing_files.add(file)
    except Exception as e:
        print(f"Warning: Could not list existing files: {e}")
        existing_files = set()

    try:
        # Run spotDL command
        cmd = ["spotdl", spotify_url, "--output", download_dir]
        print(f"Attempting to download: {track.artists} - {track.title}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            encoding='utf-8',
            errors='replace'
        )

        print(f"spotDL exit code: {result.returncode}")
        print(f"spotDL stdout: {result.stdout}")
        if result.stderr:
            print(f"spotDL stderr: {result.stderr}")

        if result.returncode == 0:
            # Find NEW files that were created after the download
            new_files = []
            try:
                current_files = set()
                for file in os.listdir(download_dir):
                    if file.endswith('.mp3'):
                        current_files.add(file)

                newly_created_files = current_files - existing_files

                if newly_created_files:
                    print(f"Found {len(newly_created_files)} new files: {newly_created_files}")

                    # Get the newest of the new files
                    newest_time = 0
                    newest_file = None

                    for file in newly_created_files:
                        file_path = os.path.join(download_dir, file)
                        try:
                            creation_time = os.path.getctime(file_path)
                            if creation_time > newest_time:
                                newest_time = creation_time
                                newest_file = file_path
                        except Exception as e:
                            print(f"Error checking file time for {file}: {e}")

                    if newest_file:
                        new_files.append(newest_file)

            except Exception as e:
                print(f"Error finding new files: {e}")

            # If we found new files, create the file mapping
            if new_files:
                downloaded_file = new_files[0]
                print(f"Using newly downloaded file: {downloaded_file}")

                # Create FileTrackMapping entry instead of embedding metadata
                with UnitOfWork() as uow:
                    try:
                        uow.file_track_mapping_repository.add_mapping_by_uri(downloaded_file, uri)
                        mapping_success = True
                        print(f"Created file mapping: {downloaded_file} -> {uri}")
                    except Exception as e:
                        mapping_success = False
                        print(f"Failed to create file mapping: {e}")

                return {
                    "downloaded_file": downloaded_file,
                    "track_info": f"{track.artists} - {track.title}",
                    "mapping_created": mapping_success,
                    "uri": uri,
                    "spotdl_output": result.stdout[:500] if result.stdout else ""
                }
            else:
                # No new files found - download likely failed
                raise RuntimeError(
                    f"No download occurred for: {track.artists} - {track.title}. Track may not be available on YouTube.")
        else:
            # spotDL command failed
            error_output = result.stderr or result.stdout or "Unknown error"
            raise RuntimeError(f"spotDL failed for '{track.artists} - {track.title}': {error_output[:500]}")

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Download timed out after 5 minutes for: {track.artists} - {track.title}")
    except UnicodeDecodeError as e:
        raise RuntimeError(f"Encoding error during download of '{track.artists} - {track.title}': {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Download failed for '{track.artists} - {track.title}': {str(e)}")


def download_all_missing_tracks(uris: List[str], download_dir: str, progress_callback=None):
    """Download multiple tracks by URI with progress tracking."""
    total_tracks = len(uris)
    successful_downloads = []
    failed_downloads = []

    for i, uri in enumerate(uris):
        try:
            # Get track info for better progress display
            with UnitOfWork() as uow:
                track = uow.track_repository.get_by_uri(uri)
                track_name = f"{track.artists} - {track.title}" if track else f"URI {uri}"

            # Call progress callback if provided
            if progress_callback:
                progress_callback({
                    'current': i,
                    'total': total_tracks,
                    'uri': uri,
                    'track_name': track_name,
                    'status': 'downloading'
                })

            print(f"Downloading {i + 1}/{total_tracks}: {track_name}")
            result = download_and_map_track(uri, download_dir)

            successful_downloads.append({
                'uri': uri,
                'track_name': track_name,
                'result': result
            })

            if progress_callback:
                progress_callback({
                    'current': i + 1,
                    'total': total_tracks,
                    'uri': uri,
                    'track_name': track_name,
                    'status': 'completed'
                })

            print(f"✓ Successfully downloaded: {track_name}")

        except Exception as e:
            error_msg = str(e)
            failed_downloads.append({
                'uri': uri,
                'track_name': track_name if 'track_name' in locals() else f"URI {uri}",
                'error': error_msg
            })

            if progress_callback:
                progress_callback({
                    'current': i + 1,
                    'total': total_tracks,
                    'uri': uri,
                    'track_name': track_name if 'track_name' in locals() else f"URI {uri}",
                    'status': 'failed',
                    'error': error_msg
                })

            print(f"✗ Failed to download {track_name if 'track_name' in locals() else uri}: {error_msg}")

    return {
        'total_tracks': total_tracks,
        'successful_downloads': successful_downloads,
        'failed_downloads': failed_downloads,
        'success_count': len(successful_downloads),
        'failure_count': len(failed_downloads)
    }


def cleanup_stale_file_mappings() -> Dict[str, Any]:
    """
    Clean up file mappings that point to files that no longer exist.

    Returns:
        Dictionary with cleanup results
    """
    mapping_logger.info("Starting cleanup of stale file mappings")

    with UnitOfWork() as uow:
        cleanup_stats = uow.file_track_mapping_repository.cleanup_stale_mappings()

        print(f"Cleanup complete! Cleaned paths: {cleanup_stats['cleaned_paths']}")
        print(f"{cleanup_stats['cleaned_count']} stale mappings removed")
        mapping_logger.info(f"Cleanup complete: {cleanup_stats['cleaned_count']} stale mappings removed")

        return {
            "success": True,
            "message": f"Cleaned up {cleanup_stats['cleaned_count']} stale file mappings",
            "stats": cleanup_stats
        }
