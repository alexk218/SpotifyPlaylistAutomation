import os
import re
from pathlib import Path
from typing import Dict, Tuple, Set, Optional, List, Any

from mutagen.id3 import ID3
from mutagen.mp3 import MP3
from mutagen.wave import WAVE
from mutagen.aiff import AIFF

from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

m3u_logger = setup_logger('m3u_helper', 'm3u_validation', 'm3u.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def build_track_id_mapping(master_tracks_dir: str) -> Dict[str, str]:
    """
    Build a mapping of track_id -> file_path for all MP3 files in the directory.
    This is an expensive operation we only want to do once.

    Returns:
        Dictionary mapping track_ids to file paths
    """
    track_id_to_path = {}
    total_files = 0
    wav_aiff_files = []

    print("Scanning music files for Track IDs...")
    m3u_logger.info(f"Building track ID mapping from {master_tracks_dir}")

    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            file_ext = os.path.splitext(filename.lower())[1]
            if file_ext not in ['.mp3', '.wav', '.aiff']:
                continue

            total_files += 1
            if total_files % 1000 == 0:
                print(f"Scanned {total_files} files...")

            file_path = os.path.join(root, filename)

            if file_ext == '.mp3':
                try:
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' in tags:
                        track_id = tags['TXXX:TRACKID'].text[0]
                        track_id_to_path[track_id] = file_path
                except Exception as e:
                    # Skip files with errors
                    m3u_logger.debug(f"Error reading ID3 tag from {file_path}: {e}")
                    pass
            else:  # WAV or AIFF file
                # For WAV/AIFF files, we'll generate a virtual track ID based on filename
                # This allows us to reference them in playlists without embedding TrackId
                normalized_filename = os.path.splitext(filename)[0].lower().replace(' ', '_')
                virtual_track_id = f"local_wav_aiff_{normalized_filename}"
                track_id_to_path[virtual_track_id] = file_path
                m3u_logger.info(f"Added WAV/AIFF file with virtual track ID: {virtual_track_id} -> {file_path}")

    print(f"Total music files: {total_files}")
    print(f"Files with valid Track IDs or virtual IDs: {len(track_id_to_path)}")
    m3u_logger.info(f"Built track ID mapping with {len(track_id_to_path)} entries from {total_files} files")
    return track_id_to_path


def get_all_playlist_track_associations(uow) -> Dict[str, Set[str]]:
    """
    Get all playlist-track associations in a single database query
    to avoid multiple queries per playlist.

    Args:
        uow: Active Unit of Work

    Returns:
        Dictionary mapping playlist_ids to sets of track_ids
    """
    all_associations = {}

    m3u_logger.info("Fetching all playlist-track associations")

    # Get all playlists
    playlists = uow.playlist_repository.get_all()

    # For each playlist, get its tracks (could be optimized further with a custom query)
    for playlist in playlists:
        track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist.playlist_id)
        all_associations[playlist.playlist_id] = set(track_ids)

    m3u_logger.info(f"Fetched associations for {len(all_associations)} playlists")
    return all_associations


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
                master_tracks_dir=master_tracks_dir,
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
        master_tracks_dir: str,
        playlists_dir: str = None,  # Keep for backward compatibility
        m3u_path: str = None,  # New parameter for explicit output path
        extended: bool = True,
        overwrite: bool = True,
        track_id_map: Dict[str, str] = None
) -> Tuple[int, int]:
    """
    Generate an M3U playlist file for a specific playlist.

    Args:
        playlist_name: Name of the playlist
        playlist_id: ID of the playlist
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in (legacy parameter)
        m3u_path: Explicit path where to save the M3U file (overrides playlists_dir)
        extended: Whether to use extended M3U format with metadata
        overwrite: Whether to overwrite existing playlist files
        track_id_map: Pre-built mapping of track ID to file path

    Returns:
        Tuple of (tracks_found, tracks_added)
    """
    m3u_logger.info(f"Generating M3U playlist for: {playlist_name}")

    # Determine the output path
    if m3u_path is None and playlists_dir is not None:
        # Backward compatibility: construct the path from playlist name and directory
        safe_name = sanitize_filename(playlist_name, preserve_spaces=True)
        m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

    if m3u_path is None:
        raise ValueError("Either output_path or playlists_dir must be provided")

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(m3u_path), exist_ok=True)

    # Log the full path we're writing to
    print(f"Writing M3U to: {m3u_path}")
    m3u_logger.info(f"Writing M3U to: {m3u_path}")

    # Check if file already exists and handle accordingly
    if os.path.exists(m3u_path):
        if not overwrite:
            m3u_logger.info(f"Playlist file already exists and overwrite=False: {m3u_path}")
            return 0, 0
        else:
            # Delete the existing file to ensure it's completely regenerated
            try:
                os.remove(m3u_path)
                m3u_logger.info(f"Removed existing M3U file for fresh regeneration: {m3u_path}")
            except Exception as e:
                m3u_logger.error(f"Error removing existing M3U file: {e}")

    # If track_id_map is provided, use it for efficient playlist generation
    if track_id_map is not None:
        # Get track IDs for this playlist from the database
        with UnitOfWork() as uow:
            track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))
            m3u_logger.info(f"Found {len(track_ids)} tracks for playlist in database")

            # Now scan for any WAV/AIFF files that match the local files in this playlist
            local_tracks = [track_id for track_id in track_ids if track_id.startswith('local_')]

            # Get track details for all tracks in the playlist
            tracks_to_add = []
            for track_id in track_ids:
                track = uow.track_repository.get_by_id(track_id)
                if track:
                    # Check if this is a local file
                    if track.is_local or track_id.startswith('local_'):
                        # For local files, check if we have the track ID in our mapping
                        if track_id in track_id_map:
                            # We have the file with the embedded TrackId (MP3) or virtual ID (WAV/AIFF)
                            file_path = track_id_map[track_id]
                            tracks_to_add.append({
                                'id': track_id,
                                'path': file_path,
                                'title': track.title,
                                'artists': track.artists,
                                'duration': get_track_duration(file_path)
                            })
                            m3u_logger.info(f"Found file for '{track.artists} - {track.title}' via track_id_map")
                        else:
                            # If not in track_id_map, try to find by filename for WAV/AIFF files
                            local_path = find_local_file_path_with_extensions(
                                track.title, track.artists, master_tracks_dir, extensions=['.wav', '.aiff', '.mp3']
                            )
                            if local_path:
                                tracks_to_add.append({
                                    'id': track_id,
                                    'path': local_path,
                                    'title': track.title,
                                    'artists': track.artists,
                                    'duration': get_track_duration(local_path)
                                })
                                m3u_logger.info(
                                    f"Found local file for '{track.artists} - {track.title}' via filename search")
                            else:
                                m3u_logger.warning(f"Could not find local file for '{track.artists} - {track.title}'")
                    elif track_id in track_id_map:
                        # Regular Spotify track with embedded TrackId
                        file_path = track_id_map[track_id]
                        tracks_to_add.append({
                            'id': track_id,
                            'path': file_path,
                            'title': track.title,
                            'artists': track.artists,
                            'duration': get_track_duration(file_path)
                        })

        # Write the M3U file
        with open(m3u_path, "w", encoding="utf-8") as m3u_file:
            # Write M3U header
            m3u_file.write("#EXTM3U\n")

            # Write each track
            for track in tracks_to_add:
                if extended:
                    # #EXTINF:duration,Artist - Title
                    m3u_file.write(f"#EXTINF:{track['duration']},{track['artists']} - {track['title']}\n")

                # Write the file path
                m3u_file.write(f"{track['path']}\n")

        tracks_found = len(track_ids.intersection(set(track_id_map.keys())))
        tracks_added = len(tracks_to_add)

        m3u_logger.info(f"Created M3U playlist '{m3u_path}' with {tracks_added} tracks")
        m3u_logger.info(f"Found {tracks_found} tracks out of {len(track_ids)} total in playlist")

        # Print additional log information
        print(f"Created M3U playlist '{m3u_path}' with {tracks_added} tracks")
        print(f"Found {tracks_found} tracks out of {len(track_ids)} total in playlist")

        return tracks_found, tracks_added

    # Original implementation when track_id_map is not provided
    # Get track IDs for this playlist from the database
    with UnitOfWork() as uow:
        track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id)
        m3u_logger.info(f"Found {len(track_ids)} tracks for playlist '{playlist_name}' in database")

        # Get track details for each track
        track_details = {}
        for track_id in track_ids:
            track = uow.track_repository.get_by_id(track_id)
            if track:
                track_details[track_id] = {
                    'title': track.title,
                    'artists': track.artists,
                    'album': track.album,
                    'is_local': track.is_local if hasattr(track, 'is_local') else False
                }

    # Find the actual files in the master directory
    tracks_found = 0
    tracks_added = 0
    track_paths = []

    # First build a set of track IDs for faster lookup
    track_id_set = set(track_ids)

    # First process local files
    for track_id in track_ids:
        if track_id.startswith('local_') and track_id in track_details:
            track_info = track_details[track_id]

            # Try to find the local file
            local_path = find_local_file_path(track_info['title'], track_info['artists'], master_tracks_dir)

            if local_path:
                tracks_found += 1
                track_paths.append({
                    'path': os.path.abspath(local_path),
                    'title': track_info['title'],
                    'artists': track_info['artists'],
                    'duration': get_track_duration(local_path)
                })
                m3u_logger.info(f"Found local file for '{track_info['artists']} - {track_info['title']}'")

    # Then process regular Spotify tracks
    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            if not filename.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, filename)

            # Check if this file has one of our track IDs
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    if track_id in track_id_set and not track_id.startswith('local_'):
                        tracks_found += 1

                        # Get track details if available
                        details = track_details.get(track_id, {})
                        title = details.get('title', os.path.splitext(filename)[0])
                        artists = details.get('artists', 'Unknown Artist')

                        # Store both the path and metadata
                        track_paths.append({
                            'path': os.path.abspath(file_path),
                            'title': title,
                            'artists': artists,
                            'duration': get_track_duration(file_path)
                        })
            except Exception as e:
                m3u_logger.error(f"Error processing file {file_path}: {e}")
                continue

    # Write the M3U file
    with open(m3u_path, "w", encoding="utf-8") as m3u_file:
        # Write header
        m3u_file.write("#EXTM3U\n")

        for track in track_paths:
            if extended:
                # Extended M3U format with track info
                # #EXTINF:duration,Artist - Title
                m3u_file.write(f"#EXTINF:{track['duration']},{track['artists']} - {track['title']}\n")

            # Write the file path
            m3u_file.write(f"{track['path']}\n")
            tracks_added += 1

    m3u_logger.info(f"Created M3U playlist '{m3u_path}' with {tracks_added} tracks")
    return tracks_found, tracks_added


def find_local_file_path_with_extensions(title: str, artists: str, music_dir: str,
                                         extensions: List[str] = ['.mp3', '.wav', '.aiff']) -> Optional[str]:
    """
    Try to find a local file in the music directory that matches the given title and artist,
    checking multiple file extensions.

    Args:
        title: The track title to search for
        artists: The artist name(s) to search for
        music_dir: The directory to search in
        extensions: List of file extensions to look for

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

    # Find all music files with the specified extensions in the directory tree
    all_music_files = []
    for root, _, files in os.walk(music_dir):
        for file in files:
            file_ext = os.path.splitext(file.lower())[1]
            if file_ext in extensions:
                file_path = os.path.join(root, file)
                file_name = os.path.splitext(file)[0].lower()
                all_music_files.append((file_path, file_name))

    # Common patterns to try for exact matches
    patterns = [
        f"{primary_artist} - {title_clean}",
        f"{title_clean} - {primary_artist}",
        f"{primary_artist}_{title_clean}",
        title_clean,
    ]

    # Step 1: Try exact matches with common patterns
    for file_path, file_name in all_music_files:
        for pattern in patterns:
            if pattern in file_name:
                return file_path

    # Step 2: Try more flexible matching - remove special characters and spaces
    clean_title = re.sub(r'[^\w\s]', '', title_clean).strip()
    clean_artist = re.sub(r'[^\w\s]', '', primary_artist).strip()

    for file_path, file_name in all_music_files:
        clean_filename = re.sub(r'[^\w\s]', '', file_name).strip()
        if f"{clean_artist} {clean_title}" in clean_filename:
            return file_path
        if f"{clean_title} {clean_artist}" in clean_filename:
            return file_path

    # Step 3: If still no match, try fuzzy matching
    best_match = None
    best_score = 0.7  # Minimum similarity threshold

    for file_path, file_name in all_music_files:
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


def get_track_duration(file_path: str) -> int:
    """
    Get the duration of a track in seconds.

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


# Use existing functions from file_helper instead of duplicating code
# Add preserve_spaces parameter if it doesn't exist
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


def regenerate_single_playlist(playlist_id: str, master_tracks_dir: str, playlists_dir: str, extended: bool = True,
                               overwrite: bool = True, track_id_map: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Regenerate a single M3U playlist for a specific playlist.

    Args:
        playlist_id: ID of the playlist to regenerate
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        extended: Whether to use extended M3U format with metadata
        overwrite: Whether to overwrite existing playlist files
        track_id_map: Pre-built mapping of track_id to file_path for optimization

    Returns:
        Dictionary with statistics about the operation
    """
    # Create output directory if it doesn't exist
    os.makedirs(playlists_dir, exist_ok=True)

    # Get the specific playlist from database
    with UnitOfWork() as uow:
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        if not playlist:
            error_msg = f'Playlist ID {playlist_id} not found in database'
            print(error_msg)
            return {
                'success': False,
                'message': error_msg
            }

    # Build track ID mapping if not provided
    if track_id_map is None:
        print(f"Building track ID mapping for {master_tracks_dir}")
        track_id_map = build_track_id_mapping(master_tracks_dir)
        print(f"Found {len(track_id_map)} track IDs in mapping")

    # Force overwrite to true to ensure changes are applied
    overwrite = True

    # Log which playlist we're regenerating
    print(f"Regenerating playlist: {playlist.name} (ID: {playlist_id})")

    # Get track IDs for this playlist
    with UnitOfWork() as uow:
        track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id)
        print(f"Found {len(track_ids)} tracks for playlist in database")

        # Check for local files
        local_track_ids = [tid for tid in track_ids if tid.startswith('local_')]
        spotify_track_ids = [tid for tid in track_ids if not tid.startswith('local_')]
        print(f"   - {len(local_track_ids)} local tracks")
        print(f"   - {len(spotify_track_ids)} Spotify tracks")

        # Get details for each track to help with local file lookup
        track_details = {}
        for track_id in track_ids:
            track = uow.track_repository.get_by_id(track_id)
            if track:
                track_details[track_id] = {
                    'title': track.title,
                    'artists': track.artists,
                    'album': track.album,
                    'is_local': track.is_local
                }

    # Special handling for local files - search the master directory for matching files
    for track_id in local_track_ids:
        # Only search if this track ID is not in our mapping already
        if track_id not in track_id_map and track_id in track_details:
            details = track_details[track_id]
            title = details['title']
            artists = details['artists']

            # Search for the file - we can call the existing find_local_file_path function
            from helpers.m3u_helper import find_local_file_path
            local_path = find_local_file_path(title, artists, master_tracks_dir)
            if local_path:
                # Add to our mapping so generate_m3u_playlist can find it
                track_id_map[track_id] = local_path
                print(f"Found local file for {artists} - {title}: {local_path}")

    # Generate the M3U file
    safe_name = sanitize_filename(playlist.name, preserve_spaces=True)
    m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

    # Delete existing file if it exists to force regeneration
    if os.path.exists(m3u_path) and overwrite:
        try:
            os.remove(m3u_path)
            print(f"Removed existing M3U file: {m3u_path}")
        except Exception as e:
            print(f"Error removing existing M3U file: {e}")
            # Continue anyway, as the file will be overwritten

    try:
        tracks_found, tracks_added = generate_m3u_playlist(
            playlist_name=playlist.name,
            playlist_id=playlist_id,
            master_tracks_dir=master_tracks_dir,
            playlists_dir=playlists_dir,
            extended=extended,
            overwrite=overwrite,
            track_id_map=track_id_map
        )
    except Exception as e:
        error_msg = f"Error generating M3U playlist: {str(e)}"
        print(error_msg)
        return {
            'success': False,
            'message': error_msg
        }

    print(f"Generated M3U with {tracks_found} tracks found, {tracks_added} tracks added")

    # Verify that the file was actually created
    if not os.path.exists(m3u_path):
        error_msg = f"M3U file was not created at: {m3u_path}"
        print(error_msg)
        return {
            'success': False,
            'message': error_msg
        }

    # Check file size to ensure it has content
    file_size = os.path.getsize(m3u_path)
    print(f"Generated M3U file size: {file_size} bytes")

    if file_size == 0:
        error_msg = f"Generated M3U file is empty: {m3u_path}"
        print(error_msg)
        return {
            'success': False,
            'message': error_msg
        }

    return {
        'success': True,
        'message': f'Successfully regenerated playlist: {playlist.name} with {tracks_added} tracks',
        'stats': {
            'playlist_name': playlist.name,
            'tracks_found': tracks_found,
            'tracks_added': tracks_added,
            'm3u_path': m3u_path,
            'file_size': file_size
        }
    }


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
    from pathlib import Path

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
