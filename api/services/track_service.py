import os
import re
import subprocess
from datetime import datetime
from typing import List, Dict, Any, Tuple

import Levenshtein
from mutagen.id3 import ID3, ID3NoHeaderError
from mutagen.mp3 import MP3
from sql.core.unit_of_work import UnitOfWork
from helpers.file_helper import embed_track_id
from utils.logger import setup_logger

mapping_logger = setup_logger('file_mapping', 'sql', 'file_mapping.log')

SUPPORTED_AUDIO_EXTENSIONS = {'.mp3', '.flac', '.wav', '.m4a', '.aac', '.ogg', '.wma'}


def search_tracks(master_tracks_dir, query):
    """
    Search for tracks that match the query.

    Args:
        master_tracks_dir: Directory containing master tracks
        query: Search query string

    Returns:
        List of matching track information
    """
    if not query:
        return []

    results = []
    query = query.lower()

    # Scan the files in the master directory
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, file)
            filename_no_ext = os.path.splitext(file)[0].lower()

            # Check if query is in filename
            if query in filename_no_ext:
                # Get TrackId if present
                track_id = None
                embedded_artist_title = "Unknown"
                try:
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' in tags:
                        track_id = tags['TXXX:TRACKID'].text[0]

                    # Try to get artist and title from ID3 tags
                    artist = ""
                    title = ""
                    if 'TPE1' in tags:  # Artist
                        artist = str(tags['TPE1'])
                    if 'TIT2' in tags:  # Title
                        title = str(tags['TIT2'])

                    if artist and title:
                        embedded_artist_title = f"{artist} - {title}"
                    else:
                        embedded_artist_title = filename_no_ext
                except Exception as e:
                    print(f"Error reading ID3 tags from {file_path}: {e}")

                results.append({
                    'file': file,
                    'track_id': track_id,
                    'embedded_artist_title': embedded_artist_title,
                    'filename': filename_no_ext,
                    'confidence': 1.0 if track_id else 0,
                    'full_path': file_path
                })

    # Sort results by relevance (tracks with IDs first, then by filename match)
    results.sort(key=lambda x: (x['track_id'] is None, x['file'].lower().find(query)))

    return results


def fuzzy_match_track(file_name, current_track_id=None):
    """
    Find potential Spotify track matches for a local file.

    Args:
        file_name: Name of the file to match
        current_track_id: Current track ID if any

    Returns:
        Dictionary with match information
    """
    # Load tracks from database for matching
    with UnitOfWork() as uow:
        try:
            tracks_db = uow.track_repository.get_all()
        except Exception as e:
            print(f"Database error: {e}")
            raise ValueError(f"Database error: {str(e)}")

    # DIRECT MATCH FOR LOCAL FILES - First try an exact file name match
    exact_file_matches = []
    for track in tracks_db:
        if track.is_local:
            # Compare the file name directly
            if track.title == file_name or track.title == os.path.splitext(file_name)[0]:
                # Exact file name match - give this highest priority
                exact_file_matches.append({
                    'track_id': track.track_id,
                    'ratio': 1.0,  # Perfect match
                    'artist': track.artists or "",
                    'title': track.title,
                    'album': track.album or "Local Files",
                    'is_local': True,
                    'match_type': 'exact_file'
                })

    # If we found exact matches, return them immediately
    if exact_file_matches:
        return {
            "file_name": file_name,
            "original_artist": "",
            "original_title": os.path.splitext(file_name)[0],
            "matches": exact_file_matches
        }

    # Extract artist and title from filename
    try:
        name_part = os.path.splitext(file_name)[0]

        # Try standard "Artist - Title" format first
        if " - " in name_part:
            artist, track_title = name_part.split(" - ", 1)
        else:
            # Handle files without the separator
            artist = ""
            track_title = name_part
    except ValueError:
        artist = ""
        track_title = name_part

    # Normalize for comparison
    normalized_artist = artist.lower().replace('&', 'and')
    normalized_title = track_title.lower()

    # Original artist and title for display
    original_artist = artist
    original_title = track_title

    # Handle remix information
    remix_info = ""
    if "remix" in normalized_title.lower():
        remix_parts = normalized_title.lower().split("remix")
        normalized_title = remix_parts[0].strip()
        remix_info = "remix" + remix_parts[1] if len(remix_parts) > 1 else "remix"

    # Calculate matches
    matches = []

    # First, check for matching local files
    local_tracks = [track for track in tracks_db if track.is_local]

    # Try to match against local files first
    for track in local_tracks:
        # For local files, just compare titles since they might not have proper artist-title format
        db_title = track.title.lower()

        # Normalize both titles
        clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
        db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

        # Calculate similarity
        similarity = Levenshtein.ratio(clean_normalized_title, db_title_clean)

        # Add high similarity boost for exact file name match
        if clean_normalized_title == db_title_clean or file_name.lower() == track.title.lower():
            similarity = 1.0

        # Include if reasonable match
        if similarity >= 0.5:
            matches.append({
                'track_id': track.track_id,
                'ratio': similarity,
                'artist': track.artists or "Local File",
                'title': track.title,
                'album': track.album or "Local Files",
                'is_local': True
            })

    # Then process regular Spotify tracks
    for track in tracks_db:
        # Skip local files as we already processed them
        if track.is_local:
            continue

        # Process artists and titles
        db_artists = track.artists.lower().replace('&', 'and')
        db_title = track.title.lower()
        track_id = track.track_id

        # Skip if this is the current track ID
        if current_track_id and track_id == current_track_id:
            continue

        # Split artists and normalize
        db_artist_list = [a.strip() for a in db_artists.split(',')]
        expanded_artists = []
        for db_artist in db_artist_list:
            if ' and ' in db_artist:
                expanded_artists.extend([a.strip() for a in db_artist.split(' and ')])
            else:
                expanded_artists.append(db_artist)

        # Artist match calculation
        artist_ratios = []
        if artist:  # Only do artist matching if we have an artist name
            artist_ratios = [Levenshtein.ratio(normalized_artist, db_artist) for db_artist in expanded_artists]
            artist_ratio = max(artist_ratios) if artist_ratios else 0

            # Perfect match bonus
            if any(normalized_artist == db_artist for db_artist in expanded_artists):
                artist_ratio = 1.0
        else:
            artist_ratio = 0.0

        # Clean titles for better matching
        clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
        db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

        # Create title variations for matching
        title_variations = [
            db_title,
            db_title_clean,
            db_title.replace(' - ', ' ').replace("'s", "s")
        ]

        # Find best title match
        title_ratios = [Levenshtein.ratio(clean_normalized_title, var) for var in title_variations]
        title_ratio = max(title_ratios)

        # Perfect match bonus
        if clean_normalized_title in [var.lower() for var in title_variations]:
            title_ratio = 1.0

        # Remix bonus
        remix_bonus = 0
        if any(x in track_title.lower() for x in ['remix', 'edit', 'mix', 'version']) and \
                any(x in db_title for x in ['remix', 'edit', 'mix', 'version']):
            remix_bonus = 0.1

            # Extra bonus for same remixer
            remix_pattern = r'\(([^)]+)(remix|edit|version|mix)\)'
            local_remix_match = re.search(remix_pattern, track_title.lower())
            db_remix_match = re.search(remix_pattern, db_title.lower())

            if local_remix_match and db_remix_match and local_remix_match.group(1) == db_remix_match.group(1):
                remix_bonus += 0.1

        # Calculate overall match score
        if artist:
            overall_ratio = (artist_ratio * 0.6 + title_ratio * 0.3 + remix_bonus)
        else:
            overall_ratio = (title_ratio * 0.9 + remix_bonus)

        # Return if over threshold
        if overall_ratio >= 0.45:  # Lower threshold for showing more options
            matches.append({
                'track_id': track_id,
                'ratio': overall_ratio,
                'artist': track.artists,
                'title': track.title,
                'album': track.album,
                'is_local': False
            })

    # Sort by match quality
    matches.sort(key=lambda x: x['ratio'], reverse=True)

    # Limit to top 8 matches
    top_matches = matches[:8]

    # Return the original file info and potential matches
    return {
        "file_name": file_name,
        "original_artist": original_artist,
        "original_title": original_title,
        "matches": top_matches
    }


def orchestrate_file_mapping(master_tracks_dir: str, confirmed: bool, precomputed_changes: Dict[str, Any],
                             confidence_threshold: float, user_selections: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Handle file mapping operations with consistent response structure.

    Args:
        master_tracks_dir: Directory containing audio files
        confirmed: Whether the operation is confirmed
        precomputed_changes: Optional precomputed analysis results
        confidence_threshold: Minimum confidence for auto-matching
        user_selections: List of user-selected file/track mappings

    Returns:
        Dictionary with operation results
    """
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


def analyze_file_mappings(master_tracks_dir: str, confidence_threshold: float = 0.75) -> Dict[str, Any]:
    """
    Analyze which files need mapping to Spotify tracks.

    Args:
        master_tracks_dir: Directory containing audio files
        confidence_threshold: Minimum confidence for auto-matching

    Returns:
        Dictionary with analysis results
    """
    mapping_logger.info(f"Starting file mapping analysis for directory: {master_tracks_dir}")

    total_files = 0
    files_with_existing_mappings = 0
    files_without_mappings = []
    auto_matched_files = []
    files_requiring_user_input = []

    # Get all tracks from database
    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()
        mapping_logger.info(f"Found {len(all_tracks)} tracks in database")

        # Separate local tracks and regular tracks for different matching strategies
        local_tracks = [track for track in all_tracks if track.is_local_file()]
        regular_tracks = [track for track in all_tracks if not track.is_local_file()]

        mapping_logger.info(f"Local tracks: {len(local_tracks)}, Regular tracks: {len(regular_tracks)}")

    # Scan all audio files in the directory
    mapping_logger.info("Scanning audio files...")
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            # Check if it's a supported audio file
            if not _is_supported_audio_file(file):
                continue

            total_files += 1
            file_path = os.path.join(root, file)
            filename_no_ext = os.path.splitext(file)[0]

            # Check if file already has a mapping
            with UnitOfWork() as uow:
                existing_mapping = uow.file_track_mapping_repository.get_uri_by_file_path(file_path)

            if existing_mapping:
                files_with_existing_mappings += 1
                mapping_logger.debug(f"File already mapped: {file} -> {existing_mapping}")
                continue

            # File needs mapping - determine best match
            best_match = _find_best_match_for_file(file, filename_no_ext, local_tracks, regular_tracks)

            if best_match:
                if best_match['confidence'] >= confidence_threshold:
                    # Auto-match high confidence files
                    auto_matched_files.append({
                        'file_path': file_path,
                        'filename': file,
                        'uri': best_match['uri'],
                        'confidence': best_match['confidence'],
                        'match_type': best_match['match_type'],
                        'track_info': best_match['track_info']
                    })
                    mapping_logger.info(
                        f"Auto-matched: {file} -> {best_match['track_info']} (confidence: {best_match['confidence']:.2f})")
                else:
                    # Requires user input
                    files_requiring_user_input.append({
                        'file_path': file_path,
                        'filename': file,
                        'potential_matches': best_match.get('all_matches', [best_match]),
                        'top_match': best_match
                    })
            else:
                # No matches found
                files_requiring_user_input.append({
                    'file_path': file_path,
                    'filename': file,
                    'potential_matches': [],
                    'top_match': None
                })

            files_without_mappings.append(file)

    # Log summary
    mapping_logger.info(f"Analysis complete:")
    mapping_logger.info(f"  Total files: {total_files}")
    mapping_logger.info(f"  Files with existing mappings: {files_with_existing_mappings}")
    mapping_logger.info(f"  Files without mappings: {len(files_without_mappings)}")
    mapping_logger.info(f"  Auto-matched files: {len(auto_matched_files)}")
    mapping_logger.info(f"  Files requiring user input: {len(files_requiring_user_input)}")

    return {
        "total_files": total_files,
        "files_with_existing_mappings": files_with_existing_mappings,
        "files_without_mappings": len(files_without_mappings),
        "auto_matched_files": auto_matched_files,
        "files_requiring_user_input": files_requiring_user_input,
        "needs_confirmation": len(auto_matched_files) > 0 or len(files_requiring_user_input) > 0,
        "requires_user_selection": len(files_requiring_user_input) > 0
    }


def create_file_mappings_batch(master_tracks_dir: str, user_selections: List[Dict[str, Any]],
                               precomputed_changes: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create file mappings in the database based on user selections and auto-matches.

    Args:
        master_tracks_dir: Directory containing audio files
        user_selections: List of user-selected file/track URI pairs
        precomputed_changes: Optional precomputed analysis results

    Returns:
        Dictionary with creation results
    """
    mapping_logger.info("Starting batch file mapping creation")

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
            'filename': auto_match['filename'],
            'uri': auto_match['uri'],
            'confidence': auto_match['confidence'],
            'source': 'auto_match'
        })

    # Add user selections
    for selection in user_selections:
        file_path = selection.get('file_path')
        if not file_path:
            # Try to construct file path from filename if not provided
            filename = selection.get('filename')
            if filename:
                file_path = _find_file_path_in_directory(filename, master_tracks_dir)

        if file_path and selection.get('uri'):
            all_mappings.append({
                'file_path': file_path,
                'filename': selection.get('filename', os.path.basename(file_path)),
                'uri': selection['uri'],
                'confidence': selection.get('confidence', 0.0),
                'source': 'user_selection'
            })

    mapping_logger.info(f"Processing {len(all_mappings)} total mappings")

    # Create mappings in database
    with UnitOfWork() as uow:
        for mapping in all_mappings:
            try:
                file_path = mapping['file_path']
                uri = mapping['uri']
                filename = mapping['filename']

                # Verify file exists
                if not os.path.exists(file_path):
                    failed_mappings += 1
                    results.append({
                        'filename': filename,
                        'uri': uri,
                        'success': False,
                        'reason': 'File not found'
                    })
                    mapping_logger.error(f"File not found: {file_path}")
                    continue

                # Verify URI exists in tracks table
                track = uow.track_repository.get_by_uri(uri)
                if not track:
                    failed_mappings += 1
                    results.append({
                        'filename': filename,
                        'uri': uri,
                        'success': False,
                        'reason': 'Track URI not found in database'
                    })
                    mapping_logger.error(f"Track URI not found in database: {uri}")
                    continue

                # Check if mapping already exists
                existing_uri = uow.file_track_mapping_repository.get_uri_by_file_path(file_path)
                if existing_uri:
                    if existing_uri == uri:
                        # Same mapping already exists - consider it successful
                        successful_mappings += 1
                        results.append({
                            'filename': filename,
                            'uri': uri,
                            'success': True,
                            'reason': 'Mapping already exists'
                        })
                        mapping_logger.debug(f"Mapping already exists: {filename} -> {uri}")
                        continue
                    else:
                        # Different mapping exists - this is a conflict
                        failed_mappings += 1
                        results.append({
                            'filename': filename,
                            'uri': uri,
                            'success': False,
                            'reason': f'File already mapped to different track: {existing_uri}'
                        })
                        mapping_logger.warning(f"Mapping conflict for {filename}: existing={existing_uri}, new={uri}")
                        continue

                # Create the mapping
                uow.file_track_mapping_repository.add_mapping_by_uri(file_path, uri)
                successful_mappings += 1
                results.append({
                    'filename': filename,
                    'uri': uri,
                    'success': True,
                    'confidence': mapping.get('confidence', 0.0),
                    'source': mapping['source']
                })

                mapping_logger.info(f"Created mapping: {filename} -> {uri} (source: {mapping['source']})")

            except Exception as e:
                failed_mappings += 1
                results.append({
                    'filename': mapping.get('filename', 'unknown'),
                    'uri': mapping.get('uri', 'unknown'),
                    'success': False,
                    'reason': f'Database error: {str(e)}'
                })
                mapping_logger.error(f"Failed to create mapping for {mapping.get('filename')}: {e}")

    mapping_logger.info(f"Batch mapping creation complete: {successful_mappings} successful, {failed_mappings} failed")

    return {
        'successful_mappings': successful_mappings,
        'failed_mappings': failed_mappings,
        'results': results,
        'total_processed': len(all_mappings)
    }


def _is_supported_audio_file(filename: str) -> bool:
    """Check if file is a supported audio format."""
    return os.path.splitext(filename)[1].lower() in SUPPORTED_AUDIO_EXTENSIONS


def _find_best_match_for_file(filename: str, filename_no_ext: str, local_tracks: List, regular_tracks: List) -> dict[
                                                                                                                    str, Any] | None:
    """
    Find the best match for a file among local and regular tracks.

    Returns:
        Dictionary with match information or None if no good match found
    """
    # First try exact matching with local tracks
    local_match = _find_exact_local_match(filename, filename_no_ext, local_tracks)
    if local_match:
        return local_match

    # Then try fuzzy matching with regular tracks
    fuzzy_match = _find_fuzzy_match(filename_no_ext, regular_tracks)
    if fuzzy_match:
        return fuzzy_match

    return None


def _find_exact_local_match(filename: str, filename_no_ext: str, local_tracks: List) -> Dict[str, Any]:
    """Find exact matches with local tracks."""
    for track in local_tracks:
        # Try matching with track title
        if track.title and (track.title == filename or track.title == filename_no_ext):
            return {
                'uri': track.uri,
                'confidence': 1.0,
                'match_type': 'exact_local',
                'track_info': f"{track.artists} - {track.title}",
                'artists': track.artists,
                'title': track.title,
                'album': track.album or 'Local Files'
            }

        # Also try matching with filename variations (normalize spaces, special chars)
        normalized_filename = _normalize_for_matching(filename_no_ext)
        normalized_title = _normalize_for_matching(track.title or '')

        if normalized_title and normalized_filename == normalized_title:
            return {
                'uri': track.uri,
                'confidence': 0.95,  # Slightly lower for normalized match
                'match_type': 'normalized_local',
                'track_info': f"{track.artists} - {track.title}",
                'artists': track.artists,
                'title': track.title,
                'album': track.album or 'Local Files'
            }

    return None


def _find_fuzzy_match(filename_no_ext: str, regular_tracks: List, max_matches: int = 8) -> Dict[str, Any]:
    """Find fuzzy matches with regular Spotify tracks."""
    # Extract artist and title from filename
    artist, title = _extract_artist_title_from_filename(filename_no_ext)

    matches = []

    for track in regular_tracks:
        confidence = _calculate_track_confidence(artist, title, track)

        if confidence >= 0.4:  # Lower threshold for collecting potential matches
            matches.append({
                'uri': track.uri,
                'confidence': confidence,
                'match_type': 'fuzzy',
                'track_info': f"{track.artists} - {track.title}",
                'artists': track.artists,
                'title': track.title,
                'album': track.album
            })

    if not matches:
        return None

    # Sort by confidence and take top matches
    matches.sort(key=lambda x: x['confidence'], reverse=True)
    top_matches = matches[:max_matches]

    best_match = matches[0]
    best_match['all_matches'] = top_matches

    return best_match


def _extract_artist_title_from_filename(filename: str) -> Tuple[str, str]:
    """Extract artist and title from filename using common patterns."""
    # Try standard "Artist - Title" format first
    if " - " in filename:
        parts = filename.split(" - ", 1)
        return parts[0].strip(), parts[1].strip()

    # If no separator, treat whole filename as title
    return "", filename.strip()


def _calculate_track_confidence(local_artist: str, local_title: str, track) -> float:
    """Calculate confidence score for track match."""
    # Normalize strings for comparison
    normalized_local_artist = local_artist.lower().replace('&', 'and') if local_artist else ""
    normalized_local_title = local_title.lower()

    db_artists = track.artists.lower().replace('&', 'and')
    db_title = track.title.lower()

    # Calculate artist similarity
    artist_ratio = 0.0
    if normalized_local_artist:
        # Split database artists and find best match
        db_artist_list = [a.strip() for a in db_artists.split(',')]
        expanded_artists = []
        for db_artist in db_artist_list:
            if ' and ' in db_artist:
                expanded_artists.extend([a.strip() for a in db_artist.split(' and ')])
            else:
                expanded_artists.append(db_artist)

        artist_ratios = [Levenshtein.ratio(normalized_local_artist, db_artist) for db_artist in expanded_artists]
        artist_ratio = max(artist_ratios) if artist_ratios else 0

        # Perfect match bonus
        if any(normalized_local_artist == db_artist for db_artist in expanded_artists):
            artist_ratio = 1.0

    # Calculate title similarity
    clean_local_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_local_title).strip()
    clean_db_title = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

    title_ratio = Levenshtein.ratio(clean_local_title, clean_db_title)

    # Perfect match bonus
    if clean_local_title == clean_db_title:
        title_ratio = 1.0

    # Calculate weighted overall ratio
    if normalized_local_artist:
        overall_ratio = (artist_ratio * 0.6 + title_ratio * 0.4)
    else:
        overall_ratio = title_ratio * 0.9

    return overall_ratio


def _normalize_for_matching(text: str) -> str:
    """Normalize text for matching by removing special characters and extra spaces."""
    if not text:
        return ""

    # Remove special characters and normalize spaces
    normalized = re.sub(r'[^\w\s]', '', text.lower())
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized


def _find_file_path_in_directory(filename: str, directory: str) -> str:
    """Find the full path of a file in the directory tree."""
    for root, _, files in os.walk(directory):
        if filename in files:
            return os.path.join(root, filename)
    return None


def update_track_id(file_path, new_track_id):
    """
    Update the track ID in a file's metadata.

    Args:
        file_path: Path to the file
        new_track_id: New track ID to embed

    Returns:
        Dictionary with old and new track IDs
    """
    # Check if new track ID exists in database
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(new_track_id)
        if not track:
            raise ValueError(f"Track ID '{new_track_id}' not found in database")

    # Get existing track ID if any
    old_track_id = None
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            old_track_id = tags['TXXX:TRACKID'].text[0]
    except Exception:
        pass

    success = embed_track_id(file_path, new_track_id)

    if not success:
        raise RuntimeError(f"Failed to update TrackId in file: {file_path}")

    return {
        "old_track_id": old_track_id,
        "new_track_id": new_track_id
    }


def remove_track_id(file_path):
    """
    Remove track ID from a file's metadata.

    Args:
        file_path: Path to the file

    Returns:
        Old track ID if any
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get existing track ID if any for reporting
    old_track_id = None
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            old_track_id = tags['TXXX:TRACKID'].text[0]
            # Remove the TrackId
            tags.delall('TXXX:TRACKID')
            tags.save(file_path)
        else:
            raise ValueError(f"No TrackId found in file: {file_path}")
    except ID3NoHeaderError:
        raise ValueError(f"No ID3 tags found in file: {file_path}")

    return old_track_id


def delete_file(file_path):
    """
    Delete a file from the filesystem.

    Args:
        file_path: Path to the file to delete

    Returns:
        Name of the deleted file
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    filename = os.path.basename(file_path)
    os.remove(file_path)

    return filename


def analyze_embedding_metadata(master_tracks_dir, auto_confirm_threshold=0.75):
    """
    Analyze files that need track ID embedding.

    Args:
        master_tracks_dir: Directory containing master tracks
        auto_confirm_threshold: Threshold for auto-confirming matches

    Returns:
        Dictionary with analysis information
    """
    # Track statistics
    total_files = 0
    files_without_id = []
    auto_matched_files = []

    # Get tracks from database for auto-matching
    with UnitOfWork() as uow:
        tracks_db = uow.track_repository.get_all()

    # Scan local files
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            file_path = os.path.join(root, file)

            # Check if file has TrackId
            has_id = False
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    has_id = True
            except Exception:
                pass

            if not has_id:
                # Try to find high confidence exact matches
                exact_match = None
                for track in tracks_db:
                    if track.is_local and (track.title == file or track.title == os.path.splitext(file)[0]):
                        # Found exact match
                        exact_match = {
                            'fileName': file,
                            'trackId': track.track_id,
                            'confidence': 1.0  # 100% confidence
                        }
                        break

                if exact_match and exact_match['confidence'] >= auto_confirm_threshold:
                    # Auto-match this file - embed the TrackId immediately
                    success = embed_track_id(file_path, exact_match['trackId'])
                    if success:
                        auto_matched_files.append(exact_match)
                    else:
                        # If embedding failed, add to manual process list
                        files_without_id.append(file)
                else:
                    # No high confidence match, add to manual process list
                    files_without_id.append(file)

    return {
        "total_files": total_files,
        "files_without_id": files_without_id,
        "auto_matched_files": auto_matched_files,
        "needs_confirmation": len(files_without_id) > 0,
        "requires_fuzzy_matching": len(files_without_id) > 0
    }


def embed_metadata_batch(master_tracks_dir, user_selections):
    """
    Embed track IDs in multiple files based on user selections.

    Args:
        master_tracks_dir: Directory containing master tracks
        user_selections: List of user-selected file/track ID pairs

    Returns:
        Dictionary with embedding results
    """
    successful_embeds = 0
    failed_embeds = 0
    results = []

    for selection in user_selections:
        file_name = selection.get('fileName')
        track_id = selection.get('trackId')
        confidence = selection.get('confidence')

        # Find the full file path
        file_path = None
        for root, _, files in os.walk(master_tracks_dir):
            if file_name in files:
                file_path = os.path.join(root, file_name)
                break

        if file_path and track_id:
            # Use the embed_track_id function
            success = embed_track_id(file_path, track_id)

            if success:
                successful_embeds += 1
                results.append({
                    "file": file_name,
                    "track_id": track_id,
                    "success": True
                })
            else:
                failed_embeds += 1
                results.append({
                    "file": file_name,
                    "track_id": track_id,
                    "success": False,
                    "reason": "Failed to write to file"
                })
        else:
            failed_embeds += 1
            results.append({
                "file": file_name,
                "track_id": track_id,
                "success": False,
                "reason": "File not found or track ID missing"
            })

    return {
        "successful_embeds": successful_embeds,
        "failed_embeds": failed_embeds,
        "results": results
    }


def direct_tracks_compare(master_tracks_dir, master_playlist_id=None):
    """
    Directly compare Spotify tracks with local tracks from the database.

    Args:
        master_tracks_dir: Directory containing master tracks
        master_playlist_id: Optional ID of the master playlist

    Returns:
        Dictionary with comparison results
    """
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
                'added_at': track.added_to_master.isoformat() if track.added_to_master else None
            })

        # 2. Get all local tracks (tracks that have paths associated with them)
        # Create a set of track IDs that are verified to exist locally
        local_track_ids = set()
        local_tracks_info = []

        # Scan the master tracks directory to find which files have TrackIds
        # Scan local files to find which ones have TrackIds embedded
        for root, _, files in os.walk(master_tracks_dir):
            for filename in files:
                if not filename.lower().endswith('.mp3'):
                    continue

                file_path = os.path.join(root, filename)

                # Check if this file has a TrackId
                try:
                    try:
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]
                            local_track_ids.add(track_id)
                            local_tracks_info.append({
                                'path': file_path,
                                'filename': filename,
                                'track_id': track_id,
                                'size': os.path.getsize(file_path),
                                'modified': os.path.getmtime(file_path)
                            })
                    except ID3NoHeaderError:
                        pass
                except Exception as e:
                    print(f"Error reading ID3 tags from {file_path}: {e}")

        # 3. Compare to find missing tracks
        missing_tracks = []
        for track in master_tracks_list:
            # Skip tracks without an ID (shouldn't happen but just in case)
            if not track['id']:
                continue

            # Skip tracks that are local files
            if track['id'].startswith('local_'):
                continue

            # If track ID is not in local tracks, it's missing
            if track['id'] not in local_track_ids:
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
                "count": len(local_track_ids),
                "tracks": local_tracks_info[:100]  # Limit to avoid huge payloads
            },
            "missing_tracks": missing_tracks,
            "music_directory": master_tracks_dir,
            "master_playlist_id": master_playlist_id
        }


def download_and_embed_track(track_id: str, download_dir: str):
    """
    Download a track using spotDL and embed the TrackId metadata.
    """
    # Get track details from database first
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(track_id)
        if not track:
            raise ValueError(f"Track ID '{track_id}' not found in database")

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
        # Run spotDL command with proper encoding handling
        cmd = ["spotdl", spotify_url, "--output", download_dir]

        print(f"Attempting to download: {track.artists} - {track.title}")

        # Execute the command and capture output with UTF-8 encoding
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            encoding='utf-8',  # Force UTF-8 encoding
            errors='replace'  # Replace problematic characters instead of crashing
        )

        print(f"spotDL exit code: {result.returncode}")
        print(f"spotDL stdout: {result.stdout}")
        if result.stderr:
            print(f"spotDL stderr: {result.stderr}")

        if result.returncode == 0:
            # Check if spotDL actually downloaded something
            download_success_indicators = [
                "Downloaded",
                "download complete",
                "Successfully downloaded"
            ]

            output_text = result.stdout.lower() if result.stdout else ""
            download_indicated = any(indicator in output_text for indicator in download_success_indicators)

            # Also check for failure indicators
            failure_indicators = [
                "no results found",
                "could not find",
                "failed to download",
                "error:",
                "not found"
            ]

            failure_indicated = any(indicator in output_text for indicator in failure_indicators)

            if failure_indicated:
                raise RuntimeError(f"spotDL could not find track: {track.artists} - {track.title}")

            # Find NEW files that were created after the download
            new_files = []
            try:
                current_files = set()
                for file in os.listdir(download_dir):
                    if file.endswith('.mp3'):
                        current_files.add(file)

                # Find files that didn't exist before
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

            # If we found new files, use the newest one
            if new_files:
                downloaded_file = new_files[0]
                print(f"Using newly downloaded file: {downloaded_file}")

                # Embed the TrackId using existing helper
                from helpers.file_helper import embed_track_id
                embed_success = embed_track_id(downloaded_file, track_id)

                if embed_success:
                    return {
                        "downloaded_file": downloaded_file,
                        "track_info": f"{track.artists} - {track.title}",
                        "metadata_embedded": True,
                        "spotdl_output": result.stdout[:500] if result.stdout else ""
                    }
                else:
                    return {
                        "downloaded_file": downloaded_file,
                        "track_info": f"{track.artists} - {track.title}",
                        "metadata_embedded": False,
                        "warning": "Download successful but metadata embedding failed",
                        "spotdl_output": result.stdout[:500] if result.stdout else ""
                    }
            else:
                # No new files found - download likely failed
                if download_indicated:
                    raise RuntimeError(
                        f"spotDL indicated success but no new files found for: {track.artists} - {track.title}")
                else:
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


def download_all_missing_tracks(track_ids: List[str], download_dir: str, progress_callback=None):
    """
    Download multiple tracks with progress tracking.
    """
    total_tracks = len(track_ids)
    successful_downloads = []
    failed_downloads = []

    for i, track_id in enumerate(track_ids):
        try:
            # Get track info for better progress display
            with UnitOfWork() as uow:
                track = uow.track_repository.get_by_id(track_id)
                track_name = f"{track.artists} - {track.title}" if track else f"Track {track_id}"

            # Call progress callback if provided
            if progress_callback:
                progress_callback({
                    'current': i,
                    'total': total_tracks,
                    'track_id': track_id,
                    'track_name': track_name,
                    'status': 'downloading'
                })

            print(f"Downloading {i + 1}/{total_tracks}: {track_name}")
            result = download_and_embed_track(track_id, download_dir)

            successful_downloads.append({
                'track_id': track_id,
                'track_name': track_name,
                'result': result
            })

            if progress_callback:
                progress_callback({
                    'current': i + 1,
                    'total': total_tracks,
                    'track_id': track_id,
                    'track_name': track_name,
                    'status': 'completed'
                })

            print(f"✓ Successfully downloaded: {track_name}")

        except Exception as e:
            error_msg = str(e)
            failed_downloads.append({
                'track_id': track_id,
                'track_name': track_name if 'track_name' in locals() else f"Track {track_id}",
                'error': error_msg
            })

            if progress_callback:
                progress_callback({
                    'current': i + 1,
                    'total': total_tracks,
                    'track_id': track_id,
                    'track_name': track_name if 'track_name' in locals() else f"Track {track_id}",
                    'status': 'failed',
                    'error': error_msg
                })

            print(f"✗ Failed to download {track_name if 'track_name' in locals() else track_id}: {error_msg}")

    return {
        'total_tracks': total_tracks,
        'successful_downloads': successful_downloads,
        'failed_downloads': failed_downloads,
        'success_count': len(successful_downloads),
        'failure_count': len(failed_downloads)
    }
