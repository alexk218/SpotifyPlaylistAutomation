import os
import re
from pathlib import Path
from typing import Dict, Tuple, Set, Optional

from mutagen.id3 import ID3
from mutagen.mp3 import MP3

from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

m3u_logger = setup_logger('m3u_helper', 'logs/m3u_validation/m3u.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def build_track_id_mapping(master_tracks_dir: str) -> Dict[str, str]:
    """
    Build a mapping of track_id -> file_path for all MP3 files in the directory.
    This is the expensive operation we only want to do once.

    Returns:
        Dictionary mapping track_ids to file paths
    """
    track_id_to_path = {}
    total_files = 0

    print("Scanning MP3 files for Track IDs...")
    m3u_logger.info(f"Building track ID mapping from {master_tracks_dir}")

    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            if not filename.lower().endswith('.mp3'):
                continue

            total_files += 1
            if total_files % 1000 == 0:
                print(f"Scanned {total_files} files...")

            file_path = os.path.join(root, filename)

            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    track_id_to_path[track_id] = file_path
            except Exception as e:
                # Skip files with errors
                m3u_logger.debug(f"Error reading ID3 tag from {file_path}: {e}")
                pass

    print(f"Total MP3 files: {total_files}")
    print(f"Files with valid Track IDs: {len(track_id_to_path)}")
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
    Extract track IDs from an M3U file by examining the referenced MP3 files.

    Args:
        m3u_path: Path to the M3U file
        track_id_map: Optional mapping of track_ids to file paths for optimization

    Returns:
        Set of track IDs found in the referenced MP3 files
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

                # Get the MP3 file path and normalize it
                file_path = os.path.normpath(line.strip())

                # Use reversed map for quick lookup if available
                if reversed_map is not None:
                    if file_path in reversed_map:
                        track_ids.add(reversed_map[file_path])
                elif os.path.exists(file_path) and file_path.lower().endswith('.mp3'):
                    # Fall back to reading ID3 tags if needed
                    try:
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]
                            track_ids.add(track_id)
                    except Exception:
                        pass
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

        # Find which database tracks actually exist locally
        local_db_track_ids = db_track_ids.intersection(track_id_map.keys())

        # Compare to find differences
        added_tracks = local_db_track_ids - m3u_track_ids
        removed_tracks = m3u_track_ids - local_db_track_ids
        has_changes = bool(added_tracks or removed_tracks)

        # Log the comparison details
        m3u_logger.info(f"Playlist '{playlist_id}' comparison (using track_id_map):")
        m3u_logger.info(f" - Database tracks: {len(db_track_ids)}")
        m3u_logger.info(f" - Local tracks: {len(local_db_track_ids)}")
        m3u_logger.info(f" - M3U tracks: {len(m3u_track_ids)}")
        m3u_logger.info(f" - Added tracks: {len(added_tracks)}")
        m3u_logger.info(f" - Removed tracks: {len(removed_tracks)}")

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


def generate_m3u_playlist(playlist_name: str, playlist_id: str, master_tracks_dir: str,
                          playlists_dir: str, extended: bool = True, overwrite: bool = True,
                          track_id_map: Dict[str, str] = None) -> Tuple[int, int]:
    """
    Generate an M3U playlist file for a specific playlist.

    Args:
        playlist_name: Name of the playlist
        playlist_id: Spotify ID of the playlist
        master_tracks_dir: Directory containing the master tracks
        playlists_dir: Directory where playlist files will be created
        extended: Whether to use extended M3U format with metadata
        overwrite: Whether to overwrite existing playlist files
        track_id_map: Optional mapping of track_id to file_path for optimization

    Returns:
        Tuple of (tracks_found, tracks_added) counts
    """
    m3u_logger.info(f"Generating M3U playlist for: {playlist_name}")

    # Ensure the playlists directory exists
    os.makedirs(playlists_dir, exist_ok=True)

    # Sanitize the playlist name for use as a filename, but preserve spaces
    safe_playlist_name = sanitize_filename(playlist_name, preserve_spaces=True)
    m3u_path = os.path.join(playlists_dir, f"{safe_playlist_name}.m3u")

    # Check if file already exists and handle accordingly
    if os.path.exists(m3u_path) and not overwrite:
        m3u_logger.info(f"Playlist file already exists and overwrite=False: {m3u_path}")
        return 0, 0

    # Get track IDs and details for this playlist from the database
    with UnitOfWork() as uow:
        track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))
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

    # If track_id_map is provided, use it for efficient playlist generation
    if track_id_map is not None:
        # Get track details for the tracks that we have files for
        tracks_to_add = []
        for track_id in track_ids:
            if track_id not in track_details:
                continue

            details = track_details[track_id]

            # Check if this is a local file
            if details['is_local'] or track_id.startswith('local_'):
                # Handle local file - try to find it in the music library
                local_path = find_local_file_path(details['title'], details['artists'], master_tracks_dir)
                if local_path:
                    tracks_to_add.append({
                        'id': track_id,
                        'path': local_path,
                        'title': details['title'],
                        'artists': details['artists'],
                        'duration': get_track_duration(local_path)
                    })
                    m3u_logger.info(f"Found local file for '{details['artists']} - {details['title']}'")
                else:
                    m3u_logger.warning(f"Could not find local file for '{details['artists']} - {details['title']}'")
            elif track_id in track_id_map:
                # Regular Spotify track
                file_path = track_id_map[track_id]
                tracks_to_add.append({
                    'id': track_id,
                    'path': file_path,
                    'title': details['title'],
                    'artists': details['artists'],
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

        return tracks_found, tracks_added

    # Original implementation when track_id_map is not provided
    tracks_found = 0
    tracks_added = 0
    track_paths = []

    # First process local files
    for track_id in track_ids:
        if track_id.startswith('local_') and track_id in track_details:
            details = track_details[track_id]

            # Try to find the local file
            local_path = find_local_file_path(details['title'], details['artists'], master_tracks_dir)

            if local_path:
                tracks_found += 1
                track_paths.append({
                    'path': os.path.abspath(local_path),
                    'title': details['title'],
                    'artists': details['artists'],
                    'duration': get_track_duration(local_path)
                })
                m3u_logger.info(f"Found local file for '{details['artists']} - {details['title']}'")

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
                    if track_id in track_ids and not track_id.startswith('local_'):
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


def generate_m3u_playlist(playlist_name: str, playlist_id: str, master_tracks_dir: str,
                          playlists_dir: str, extended: bool = True, overwrite: bool = True,
                          track_id_map: Dict[str, str] = None) -> Tuple[int, int]:
    """
    Generate an M3U playlist file for a specific playlist.

    Args:
        playlist_name: Name of the playlist
        playlist_id: Spotify ID of the playlist
        master_tracks_dir: Directory containing the master tracks
        playlists_dir: Directory where playlist files will be created
        extended: Whether to use extended M3U format with metadata
        overwrite: Whether to overwrite existing playlist files
        track_id_map: Optional mapping of track_id to file_path for optimization

    Returns:
        Tuple of (tracks_found, tracks_added) counts
    """
    m3u_logger.info(f"Generating M3U playlist for: {playlist_name}")

    # Ensure the playlists directory exists
    os.makedirs(playlists_dir, exist_ok=True)

    # Sanitize the playlist name for use as a filename, but preserve spaces
    safe_playlist_name = sanitize_filename(playlist_name, preserve_spaces=True)
    m3u_path = os.path.join(playlists_dir, f"{safe_playlist_name}.m3u")

    # Check if file already exists and handle accordingly
    if os.path.exists(m3u_path) and not overwrite:
        m3u_logger.info(f"Playlist file already exists and overwrite=False: {m3u_path}")
        return 0, 0

    # If track_id_map is provided, use it for efficient playlist generation
    if track_id_map is not None:
        # Get track IDs for this playlist from the database
        with UnitOfWork() as uow:
            track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))

            # Get track details for the tracks that we have files for
            tracks_to_add = []
            for track_id in track_ids:
                track = uow.track_repository.get_by_id(track_id)
                if track:
                    # Check if this is a local file
                    if track.is_local or track_id.startswith('local_'):
                        # Handle local file - try to find it in the music library
                        local_path = find_local_file_path(track.title, track.artists, master_tracks_dir)
                        if local_path:
                            tracks_to_add.append({
                                'id': track_id,
                                'path': local_path,
                                'title': track.title,
                                'artists': track.artists,
                                'duration': get_track_duration(local_path)
                            })
                            m3u_logger.info(f"Found local file for '{track.artists} - {track.title}'")
                        else:
                            m3u_logger.warning(f"Could not find local file for '{track.artists} - {track.title}'")
                    elif track_id in track_id_map:
                        # Regular Spotify track
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


def find_local_file_path(title: str, artists: str, music_dir: str) -> Optional[str]:
    """
    Try to find a local file in the music directory that matches the given title and artist.

    Args:
        title: The track title to search for
        artists: The artist name(s) to search for
        music_dir: The directory to search in

    Returns:
        Path to the matching file, or None if not found
    """
    import Levenshtein

    # Clean up title and artists for comparison
    title_clean = title.lower().strip()
    artists_clean = artists.lower().strip()

    # Try to extract primary artist
    primary_artist = artists_clean.split(',')[0].strip()

    # Common patterns to try
    patterns = [
        f"{primary_artist} - {title_clean}",
        f"{title_clean} - {primary_artist}",
        f"{primary_artist}_{title_clean}",
        title_clean,
    ]

    # First try exact matches with common patterns
    for root, _, files in os.walk(music_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_name = os.path.splitext(file)[0].lower()

            # Check each pattern
            for pattern in patterns:
                if pattern in file_name:
                    return os.path.join(root, file)

    # If no exact match, try fuzzy matching
    best_match = None
    best_score = 0.7  # Minimum similarity threshold

    for root, _, files in os.walk(music_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_name = os.path.splitext(file)[0].lower()

            # Try to match "{artist} - {title}" pattern
            expected = f"{primary_artist} - {title_clean}"
            similarity = Levenshtein.ratio(expected, file_name)

            if similarity > best_score:
                best_score = similarity
                best_match = os.path.join(root, file)

            # Also try "{title} - {artist}" pattern
            expected = f"{title_clean} - {primary_artist}"
            similarity = Levenshtein.ratio(expected, file_name)

            if similarity > best_score:
                best_score = similarity
                best_match = os.path.join(root, file)

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
        audio = MP3(file_path)
        return int(audio.info.length)
    except Exception:
        return 0  # Default duration if we can't read it


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
