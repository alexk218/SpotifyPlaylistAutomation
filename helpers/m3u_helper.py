import os
import re
from pathlib import Path
from typing import Dict, Tuple, Set, Optional, List, Any

from mutagen.aiff import AIFF
from mutagen.id3 import ID3
from mutagen.mp3 import MP3
from mutagen.wave import WAVE

from api.constants.file_extensions import SUPPORTED_AUDIO_EXTENSIONS
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

m3u_logger = setup_logger('m3u_helper', 'm3u_validation', 'm3u.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def build_uri_to_file_mapping_from_database() -> Dict[str, str]:
    """
    Build a mapping of Spotify URI -> file_path using FileTrackMappings table

    1. Uses a single query to get all active mappings
    2. Batch checks file existence instead of individual checks
    3. Returns only valid mappings

    Returns:
        Dictionary mapping Spotify URIs to file paths (only for files that exist)
    """
    with UnitOfWork() as uow:
        # Get all mappings in one query
        uri_to_file_map = uow.file_track_mapping_repository.get_all_active_uri_to_file_mappings()

        # Batch check file existence
        existing_files = batch_check_file_existence(list(uri_to_file_map.values()))

        # Filter to only existing files
        return {uri: path for uri, path in uri_to_file_map.items() if path in existing_files}


def batch_check_file_existence(file_paths: List[str]) -> Set[str]:
    """
    Check existence of multiple files efficiently.

    Args:
        file_paths: List of file paths to check

    Returns:
        Set of file paths that exist
    """
    existing_files = set()

    # Group files by directory for more efficient checking
    files_by_dir = {}
    for file_path in file_paths:
        dir_path = os.path.dirname(file_path)
        if dir_path not in files_by_dir:
            files_by_dir[dir_path] = []
        files_by_dir[dir_path].append(file_path)

    # Check files directory by directory
    for dir_path, files_in_dir in files_by_dir.items():
        if os.path.isdir(dir_path):
            try:
                # Get all files in directory at once
                actual_files = set(os.listdir(dir_path))
                for file_path in files_in_dir:
                    filename = os.path.basename(file_path)
                    if filename in actual_files:
                        existing_files.add(file_path)
            except (OSError, PermissionError):
                # Fall back to individual checks for this directory
                for file_path in files_in_dir:
                    if os.path.exists(file_path):
                        existing_files.add(file_path)

    return existing_files


def get_all_tracks_metadata_by_uri(uris: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get track metadata for multiple URIs in a single query.

    Args:
        uris: List of Spotify URIs to get metadata for

    Returns:
        Dictionary mapping URI to track metadata
    """
    with UnitOfWork() as uow:
        return uow.track_repository.get_tracks_metadata_by_uris(uris)


def get_playlists_track_uris_batch(playlist_ids: List[str]) -> Dict[str, List[str]]:
    """
    Get track URIs for multiple playlists in a single query.

    Args:
        playlist_ids: List of playlist IDs

    Returns:
        Dictionary mapping playlist_id to list of track URIs
    """
    with UnitOfWork() as uow:
        return uow.track_playlist_repository.batch_get_uris_for_playlists(playlist_ids)


def get_audio_duration(file_path: str) -> int:
    """
    Get the duration of an audio file in seconds.

    Args:
        file_path: Path to the audio file

    Returns:
        Duration in seconds, or 0 if not available
    """
    try:
        file_ext = os.path.splitext(file_path.lower())[1]

        if file_ext == '.mp3':
            audio = MP3(file_path)
            return int(audio.info.length)
        elif file_ext == '.wav':
            audio = WAVE(file_path)
            return int(audio.info.length)
        elif file_ext in ['.aiff', '.aif']:
            audio = AIFF(file_path)
            return int(audio.info.length)
        return 0
    except Exception as e:
        m3u_logger.error(f"Error getting duration for {file_path}: {e}")
        return 0


def get_m3u_track_uris_from_file(m3u_path: str, uri_to_file_map: dict) -> set:
    """
    Extract Spotify URIs from an M3U file by examining the referenced files and
    looking them up in the FileTrackMappings table.

    Args:
        m3u_path: Path to the M3U file
        uri_to_file_map: Mapping of URI -> file_path from FileTrackMappings

    Returns:
        Set of Spotify URIs found in the referenced files
    """
    track_uris = set()

    if not os.path.exists(m3u_path):
        return track_uris

    # Create reverse mapping for efficient lookup
    file_to_uri_map = {os.path.normpath(file_path): uri for uri, file_path in uri_to_file_map.items()}

    try:
        with open(m3u_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Skip comment lines and empty lines
                if line.startswith('#') or not line.strip():
                    continue

                # Get the file path and normalize it
                file_path = os.path.normpath(line.strip())

                # Look up the URI for this file path
                if file_path in file_to_uri_map:
                    track_uris.add(file_to_uri_map[file_path])

    except Exception as e:
        print(f"Error reading M3U file {m3u_path}: {e}")

    return track_uris


# TODO: DELETE? OR FIX
def get_m3u_track_ids(m3u_path: str, track_id_map: Optional[Dict[str, str]] = None) -> Set[str]:
    """
    Extract track IDs from an M3U file by examining the referenced files.

    Args:
        m3u_path: Path to the M3U file
        track_id_map: Optional mapping of track_ids to file paths for optimization

    Returns:
        Set of track IDs found in the referenced files
    """
    track_ids = set()
    reversed_map = None

    if not os.path.exists(m3u_path):
        return track_ids

    # If we have a track_id_map, create a reversed version for faster lookup
    if track_id_map is not None:
        # This might be memory-intensive but saves a lot of time
        reversed_map = {os.path.normpath(path): track_id for track_id, path in track_id_map.items()}

    try:
        with open(m3u_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Skip comment lines and empty lines
                if line.startswith('#') or not line.strip():
                    continue

                # Get the file path and normalize it
                file_path = os.path.normpath(line.strip())

                # Use reversed map for quick lookup if available
                if reversed_map is not None:
                    if file_path in reversed_map:
                        track_ids.add(reversed_map[file_path])
                elif os.path.exists(file_path):
                    file_ext = os.path.splitext(file_path.lower())[1]
                    if file_ext == '.mp3':
                        # For MP3 files, read ID3 tags
                        try:
                            tags = ID3(file_path)
                            if 'TXXX:TRACKID' in tags:
                                track_id = tags['TXXX:TRACKID'].text[0]
                                track_ids.add(track_id)
                        except Exception:
                            pass
                    elif file_ext in ['.wav', '.aiff']:
                        # For WAV/AIFF files, try to find a matching database track
                        with UnitOfWork() as uow:
                            local_tracks = [t for t in uow.track_repository.get_all() if
                                            t.track_id.startswith('local_')]

                            # Try to find a match
                            filename = os.path.basename(file_path)
                            filename_no_ext = os.path.splitext(filename)[0]

                            for track in local_tracks:
                                # Check if the filename matches the track's title or artist-title
                                if (filename_no_ext.lower() == track.title.lower() or
                                        filename_no_ext.lower() == f"{track.artists} - {track.title}".lower()):
                                    track_ids.add(track.track_id)
                                    break
    except Exception as e:
        m3u_logger.error(f"Error reading M3U file {m3u_path}: {e}")

    m3u_logger.info(f"Found {len(track_ids)} track IDs in M3U file: {m3u_path}")
    return track_ids


# TODO: FIX THIS?
def compare_playlist_with_m3u(playlist_id: str, m3u_path: str, master_tracks_dir: str,
                              track_id_map: Dict[str, str] = None) -> Tuple[bool, Set[str], Set[str]]:
    """
    Compare track IDs in a database playlist with those in an M3U file.
    Only considers tracks that actually exist locally.

    Args:
        playlist_id: Spotify ID of the playlist
        m3u_path: Path to the M3U file
        master_tracks_dir: Directory containing master tracks
        track_id_map: Optional pre-built mapping of track_ids to file paths

    Returns:
        Tuple of (has_changes, added_tracks, removed_tracks)
    """
    # If we have a track_id_map, use it for efficient comparison
    if track_id_map is not None:
        # Get track IDs from database
        with UnitOfWork() as uow:
            db_track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))

        # Get track IDs from M3U file
        m3u_track_ids = get_m3u_track_ids(m3u_path, track_id_map)

        # Track which database IDs actually exist locally
        local_db_track_ids = set()
        for track_id in db_track_ids:
            # For Spotify tracks, check if they're in the track_id_map
            if not track_id.startswith('local_'):
                if track_id in track_id_map:
                    local_db_track_ids.add(track_id)
            else:
                # For local tracks, we need a different approach
                # Get track details from the database
                with UnitOfWork() as uow:
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
                        # Search for the file in the master tracks directory
                        local_path = find_local_file_path(track.title, track.artists, master_tracks_dir)
                        if local_path:
                            # The track exists locally, so add it to our set
                            local_db_track_ids.add(track_id)

        # Now we have:
        # db_track_ids: All tracks in the playlist according to the database
        # local_db_track_ids: Tracks in the playlist that actually exist locally
        # m3u_track_ids: Tracks currently in the M3U file

        # Compare to find differences
        # Tracks that should be in M3U but aren't
        added_tracks = local_db_track_ids - m3u_track_ids
        # Tracks in M3U that shouldn't be there
        removed_tracks = m3u_track_ids - local_db_track_ids
        has_changes = bool(added_tracks or removed_tracks)

        # Log the comparison details
        m3u_logger.info(f"Playlist '{playlist_id}' comparison (using track_id_map):")
        m3u_logger.info(f" - All database tracks: {len(db_track_ids)}")
        m3u_logger.info(f" - Database tracks with local files: {len(local_db_track_ids)}")
        m3u_logger.info(f" - M3U tracks: {len(m3u_track_ids)}")
        m3u_logger.info(f" - Missing tracks (to add): {len(added_tracks)}")
        m3u_logger.info(f" - Unexpected tracks (to remove): {len(removed_tracks)}")

        return has_changes, added_tracks, removed_tracks

    # Original implementation when track_id_map is not provided
    # Get track IDs from database
    with UnitOfWork() as uow:
        db_track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))

    # Get track IDs from M3U file
    m3u_track_ids = get_m3u_track_ids(m3u_path)

    # Find which database tracks actually exist locally
    local_db_track_ids = set()

    # Scan local files to find which database tracks are available locally
    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            if not filename.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, filename)

            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    if track_id in db_track_ids:
                        local_db_track_ids.add(track_id)
            except Exception as e:
                m3u_logger.error(f"Error processing file {file_path}: {e}")
                continue

    # Only compare tracks that exist locally
    added_tracks = local_db_track_ids - m3u_track_ids  # Should be in M3U but isn't
    removed_tracks = m3u_track_ids - local_db_track_ids  # In M3U but shouldn't be
    has_changes = bool(added_tracks or removed_tracks)

    # Log the comparison details
    m3u_logger.info(f"Playlist '{playlist_id}' comparison:")
    m3u_logger.info(f" - Database tracks: {len(db_track_ids)}")
    m3u_logger.info(f" - Local tracks: {len(local_db_track_ids)}")
    m3u_logger.info(f" - M3U tracks: {len(m3u_track_ids)}")
    m3u_logger.info(f" - Added tracks: {len(added_tracks)}")
    m3u_logger.info(f" - Removed tracks: {len(removed_tracks)}")

    return has_changes, added_tracks, removed_tracks


# TODO: FIX THIS
def generate_all_m3u_playlists(
        master_tracks_dir: str,
        playlists_dir: str,
        extended: bool = True,
        skip_master: bool = True,
        overwrite: bool = True,
        only_changed: bool = True,
        changed_playlists: Optional[List[str]] = None,
        track_id_map: Dict[str, str] = None
) -> Dict[str, Any]:
    """
    Generate M3U playlists for all playlists in the database.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        extended: Whether to use extended M3U format with metadata
        skip_master: Whether to skip the MASTER playlist
        overwrite: Whether to overwrite existing playlist files
        only_changed: Only update playlists that have actually changed
        changed_playlists: List of playlist names that have changed
        track_id_map: Pre-built mapping of track_id to file_path for optimization

    Returns:
        Dictionary with statistics about the operation
    """
    stats = {
        'playlists_created': 0,
        'playlists_updated': 0,
        'playlists_unchanged': 0,
        'total_tracks_added': 0,
        'empty_playlists': []
    }

    # Create output directory if it doesn't exist
    os.makedirs(playlists_dir, exist_ok=True)

    # Get all playlists from database
    with UnitOfWork() as uow:
        playlists = uow.playlist_repository.get_all()

    # Filter out MASTER playlist if requested
    if skip_master:
        playlists = [p for p in playlists if p.name.upper() != "MASTER"]

    # If only_changed is True and we have a list of changed playlists, filter further
    if only_changed and changed_playlists is not None:
        playlists = [p for p in playlists if p.name in changed_playlists]

    for playlist in playlists:
        playlist_name = playlist.name
        playlist_id = playlist.playlist_id

        try:
            # Check if playlist already exists and handle based on overwrite flag
            safe_name = sanitize_filename(playlist_name, preserve_spaces=True)
            m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

            if os.path.exists(m3u_path) and not overwrite:
                stats['playlists_unchanged'] += 1
                m3u_logger.info(f"Skipping existing playlist: {playlist_name}")
                continue

            # Generate the M3U file
            tracks_found, tracks_added = generate_m3u_playlist(
                playlist_name=playlist_name,
                playlist_id=playlist_id,
                playlists_dir=playlists_dir,
                extended=extended,
                overwrite=overwrite,
                track_id_map=track_id_map
            )

            # Update statistics
            if os.path.exists(m3u_path):
                if tracks_added > 0:
                    stats['playlists_created'] += 1
                    stats['total_tracks_added'] += tracks_added
                else:
                    stats['playlists_unchanged'] += 1
            else:
                stats['playlists_unchanged'] += 1

            # Track empty playlists
            if tracks_found == 0:
                stats['empty_playlists'].append(playlist_name)

        except Exception as e:
            m3u_logger.error(f"Error generating M3U for playlist '{playlist_name}': {e}")
            continue

    return stats


def generate_m3u_playlist(
        playlist_name: str,
        playlist_id: str,
        m3u_path: str,
        extended: bool = True,
        overwrite: bool = True,
        uri_to_file_map: Optional[Dict[str, str]] = None,
        tracks_metadata: Optional[Dict[str, Dict[str, Any]]] = None
) -> Tuple[int, int]:
    """
    Generate an M3U playlist file using the new URI-based system.

    Args:
        playlist_name: Name of the playlist
        playlist_id: ID of the playlist
        m3u_path: Path where to save the M3U file
        extended: Whether to use extended M3U format with metadata
        overwrite: Whether to overwrite existing playlist files
        uri_to_file_map: Pre-built mapping of URI to file path
        tracks_metadata: Pre-fetched track metadata to avoid individual lookups

    Returns:
        Tuple of (tracks_found, tracks_added)
    """
    m3u_logger.info(f"Generating M3U playlist for: {playlist_name}")

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(m3u_path), exist_ok=True)

    # Check if file already exists and handle accordingly
    if os.path.exists(m3u_path):
        if not overwrite:
            m3u_logger.info(f"Playlist file already exists and overwrite=False: {m3u_path}")
            return 0, 0
        else:
            try:
                os.remove(m3u_path)
                m3u_logger.info(f"Removed existing M3U file for fresh regeneration: {m3u_path}")
            except Exception as e:
                m3u_logger.error(f"Error removing existing M3U file: {e}")

    # Build URI-to-file mapping if not provided
    if uri_to_file_map is None:
        uri_to_file_map = build_uri_to_file_mapping_from_database()

    # Get track URIs for this playlist from the database
    with UnitOfWork() as uow:
        track_uris = uow.track_playlist_repository.get_uris_for_playlist(playlist_id)

    # Get track metadata if not provided
    if tracks_metadata is None and extended:
        tracks_metadata = get_all_tracks_metadata_by_uri(track_uris)

    tracks_found = 0
    tracks_added = 0

    # Create the M3U file
    try:
        with open(m3u_path, 'w', encoding='utf-8') as m3u_file:
            if extended:
                m3u_file.write("#EXTM3U\n")

            for uri in track_uris:
                # Check if we have a file for this URI
                if uri in uri_to_file_map:
                    file_path = uri_to_file_map[uri]
                    tracks_found += 1

                    if extended and tracks_metadata and uri in tracks_metadata:
                        # Get duration for extended format
                        duration = get_audio_duration(file_path)
                        track_info = tracks_metadata[uri]

                        # Write extended M3U info line
                        m3u_file.write(f"#EXTINF:{duration},{track_info['artists']} - {track_info['title']}\n")

                    # Write the file path
                    m3u_file.write(f"{file_path}\n")
                    tracks_added += 1
                else:
                    m3u_logger.debug(f"No local file found for URI: {uri}")

    except Exception as e:
        m3u_logger.error(f"Error creating M3U file {m3u_path}: {e}")
        return tracks_found, 0

    m3u_logger.info(
        f"Generated M3U playlist '{playlist_name}': {tracks_added} tracks added out of {tracks_found} found")
    return tracks_found, tracks_added


def generate_multiple_playlists(
        playlists_to_generate: List[Dict[str, Any]],
        extended: bool = True,
        overwrite: bool = True
) -> List[Dict[str, Any]]:
    """
    Generate multiple M3U playlists efficiently with batch operations.

    Args:
        playlists_to_generate: List of playlist dicts with keys: 'id', 'name', 'm3u_path'
        extended: Whether to use extended M3U format
        overwrite: Whether to overwrite existing files

    Returns:
        List of results for each playlist
    """
    if not playlists_to_generate:
        return []

    # Extract playlist IDs
    playlist_ids = [p['id'] for p in playlists_to_generate]

    # Batch fetch all data at once
    m3u_logger.info(f"Batch loading data for {len(playlist_ids)} playlists...")

    # 1. Get URI-to-file mapping once
    uri_to_file_map = build_uri_to_file_mapping_from_database()

    # 2. Get all track URIs for all playlists in one query
    playlist_track_uris = get_playlists_track_uris_batch(playlist_ids)

    # 3. Get all unique URIs that we need metadata for
    all_uris = set()
    for uris in playlist_track_uris.values():
        all_uris.update(uris)

    # 4. Get all track metadata in one query (if extended format is needed)
    tracks_metadata = None
    if extended:
        tracks_metadata = get_all_tracks_metadata_by_uri(list(all_uris))

    # 5. Generate all playlists using pre-fetched data
    results = []
    for playlist_info in playlists_to_generate:
        playlist_id = playlist_info['id']
        playlist_name = playlist_info['name']
        m3u_path = playlist_info['m3u_path']

        # Get track URIs for this specific playlist
        track_uris = playlist_track_uris.get(playlist_id, [])

        # Generate playlist metadata subset for this playlist only
        playlist_tracks_metadata = {}
        if tracks_metadata:
            playlist_tracks_metadata = {uri: tracks_metadata[uri] for uri in track_uris if uri in tracks_metadata}

        try:
            tracks_found, tracks_added = generate_m3u_playlist(
                playlist_name=playlist_name,
                playlist_id=playlist_id,
                m3u_path=m3u_path,
                extended=extended,
                overwrite=overwrite,
                uri_to_file_map=uri_to_file_map,
                tracks_metadata=playlist_tracks_metadata
            )

            results.append({
                'id': playlist_id,
                'name': playlist_name,
                'success': True,
                'tracks_found': tracks_found,
                'tracks_added': tracks_added,
                'm3u_path': m3u_path
            })

        except Exception as e:
            m3u_logger.error(f"Failed to generate playlist {playlist_name}: {e}")
            results.append({
                'id': playlist_id,
                'name': playlist_name,
                'success': False,
                'error': str(e),
                'm3u_path': m3u_path
            })

    return results


def find_local_file_path(title: str, artists: str, music_dir: str) -> Optional[str]:
    """
    Try to find a local file in the music directory that matches the given title and artist.
    Uses multiple matching strategies to find the most likely file.

    Args:
        title: The track title to search for
        artists: The artist name(s) to search for
        music_dir: The directory to search in

    Returns:
        Path to the matching file, or None if not found
    """
    import Levenshtein
    import re

    # Clean up title and artists for comparison
    title_clean = title.lower().strip()
    artists_clean = artists.lower().strip()

    # Try to extract primary artist
    primary_artist = artists_clean.split(',')[0].strip()

    # Find all MP3 files in the directory tree first
    all_mp3_files = []
    for root, _, files in os.walk(music_dir):
        for file in files:
            if file.lower().endswith('.mp3'):
                file_path = os.path.join(root, file)
                file_name = os.path.splitext(file)[0].lower()
                all_mp3_files.append((file_path, file_name))

    # Common patterns to try for exact matches
    patterns = [
        f"{primary_artist} - {title_clean}",
        f"{title_clean} - {primary_artist}",
        f"{primary_artist}_{title_clean}",
        title_clean,
    ]

    # Step 1: Try exact matches with common patterns
    for file_path, file_name in all_mp3_files:
        for pattern in patterns:
            if pattern in file_name:
                return file_path

    # Step 2: Try more flexible matching - remove special characters and spaces
    clean_title = re.sub(r'[^\w\s]', '', title_clean).strip()
    clean_artist = re.sub(r'[^\w\s]', '', primary_artist).strip()

    for file_path, file_name in all_mp3_files:
        clean_filename = re.sub(r'[^\w\s]', '', file_name).strip()
        if f"{clean_artist} {clean_title}" in clean_filename:
            return file_path
        if f"{clean_title} {clean_artist}" in clean_filename:
            return file_path

    # Step 3: If still no match, try fuzzy matching
    best_match = None
    best_score = 0.7  # Minimum similarity threshold

    for file_path, file_name in all_mp3_files:
        # Try to match "{artist} - {title}" pattern
        expected = f"{primary_artist} - {title_clean}"
        similarity = Levenshtein.ratio(expected, file_name)

        if similarity > best_score:
            best_score = similarity
            best_match = file_path

        # Also try "{title} - {artist}" pattern
        expected = f"{title_clean} - {primary_artist}"
        similarity = Levenshtein.ratio(expected, file_name)

        if similarity > best_score:
            best_score = similarity
            best_match = file_path

        # Simple match just by title if the title is distinctive enough (longer than 4 characters)
        if len(title_clean) > 4:
            if title_clean in file_name:
                # Bonus if the title is found as a distinct word or phrase
                if re.search(r'\b' + re.escape(title_clean) + r'\b', file_name):
                    similarity = 0.85  # Higher confidence for exact title match
                    if similarity > best_score:
                        best_score = similarity
                        best_match = file_path

    return best_match


# Use existing functions from file_helper instead of duplicating code
def sanitize_filename(name: str, preserve_spaces: bool = True) -> str:
    """
    Sanitize a string for use as a filename, with option to preserve spaces.

    Args:
        name: The string to sanitize
        preserve_spaces: Whether to preserve spaces in the filename

    Returns:
        A sanitized filename string
    """
    from helpers.file_helper import sanitize_filename as original_sanitize

    try:
        # Call the original function
        sanitized = original_sanitize(name)

        # If preserve_spaces and the original removed spaces, restore them
        if preserve_spaces and ' ' in name and ' ' not in sanitized:
            # Try to handle the most common case where spaces were just removed
            sanitized = name
            # Replace only the invalid characters
            invalid_chars = '<>:"/\\|?*'
            for char in invalid_chars:
                sanitized = sanitized.replace(char, '_')

        return sanitized
    except Exception as e:
        # If there's an issue with the import, provide a basic implementation
        m3u_logger.warning(f"Error using original sanitize_filename: {e}, using built-in version")
        if preserve_spaces:
            # Replace only invalid characters, keep spaces
            invalid_chars = '<>:"/\\|?*'
            result = name
            for char in invalid_chars:
                result = result.replace(char, '_')
            return result
        else:
            # Replace spaces and invalid characters
            return re.sub(r'\s+|[<>:"/\\|?*]', '_', name)


def search_tracks_in_m3u_files(m3u_directory: str, track_paths: List[str]) -> Dict[str, List[str]]:
    """
    Search for specific tracks across all M3U files in a directory and its subdirectories.

    Args:
        m3u_directory: Root directory containing M3U files
        track_paths: List of track file paths to search for

    Returns:
        Dictionary mapping track paths to lists of M3U files that contain them
    """
    import os

    # Normalize track paths for comparison
    normalized_tracks = {}
    for track_path in track_paths:
        normalized_path = os.path.normpath(track_path).lower()
        normalized_tracks[normalized_path] = track_path

    # Results dictionary: track_path -> [list of m3u files containing it]
    results = {track: [] for track in track_paths}

    # Search all M3U files
    for root, dirs, files in os.walk(m3u_directory):
        for filename in files:
            if not filename.lower().endswith('.m3u'):
                continue

            m3u_path = os.path.join(root, filename)
            relative_m3u_path = os.path.relpath(m3u_path, m3u_directory)

            try:
                with open(m3u_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        # Skip comment lines and empty lines
                        if line.startswith('#') or not line.strip():
                            continue

                        # Get the file path and normalize it
                        file_path = line.strip()
                        normalized_file_path = os.path.normpath(file_path).lower()

                        # Check if this matches any of our target tracks
                        if normalized_file_path in normalized_tracks:
                            original_track = normalized_tracks[normalized_file_path]
                            if relative_m3u_path not in results[original_track]:
                                results[original_track].append(relative_m3u_path)

            except Exception as e:
                m3u_logger.error(f"Error reading M3U file {m3u_path}: {e}")
                continue

    return results


def search_tracks_by_title_in_m3u_files(m3u_directory: str, track_titles: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """
    Search for tracks by title across all M3U files in a directory and its subdirectories.

    Args:
        m3u_directory: Root directory containing M3U files
        track_titles: List of track titles to search for (e.g., ["Sundaland (Extended Mix)", "Plommon (Original Mix)"])

    Returns:
        Dictionary mapping track titles to lists of matches with M3U file info
    """
    import os

    # Normalize track titles for comparison
    normalized_titles = {}
    for title in track_titles:
        normalized_title = title.lower().strip()
        normalized_titles[normalized_title] = title

    # Results dictionary: track_title -> [list of matches]
    results = {title: [] for title in track_titles}

    # Search all M3U files
    for root, dirs, files in os.walk(m3u_directory):
        for filename in files:
            if not filename.lower().endswith('.m3u'):
                continue

            m3u_path = os.path.join(root, filename)
            relative_m3u_path = os.path.relpath(m3u_path, m3u_directory)

            try:
                with open(m3u_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                    for i, line in enumerate(lines):
                        # Look for EXTINF lines which contain track info
                        if line.startswith('#EXTINF:'):
                            # Parse EXTINF line: #EXTINF:duration,Artist - Title
                            try:
                                info_part = line[8:].split(',', 1)
                                if len(info_part) > 1:
                                    track_info = info_part[1].strip()

                                    # Get the file path from the next line
                                    file_path = ""
                                    if i + 1 < len(lines) and not lines[i + 1].startswith('#'):
                                        file_path = lines[i + 1].strip()

                                    # Check if any of our target titles are in this track info
                                    track_info_lower = track_info.lower()
                                    for normalized_title, original_title in normalized_titles.items():
                                        if normalized_title in track_info_lower:
                                            results[original_title].append({
                                                'playlist': relative_m3u_path,
                                                'track_info': track_info,
                                                'file_path': file_path,
                                                'playlist_full_path': m3u_path
                                            })
                            except Exception as e:
                                m3u_logger.error(f"Error parsing EXTINF line in {m3u_path}: {e}")
                                continue

            except Exception as e:
                m3u_logger.error(f"Error reading M3U file {m3u_path}: {e}")
                continue

    return results
