import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Any, Set, Optional

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

    Args:
        master_tracks_dir: Directory containing master tracks

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

    # If track_id_map is provided, use it for efficient playlist generation
    if track_id_map is not None:
        # Get track IDs for this playlist from the database
        with UnitOfWork() as uow:
            track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))

            # Get track details for the tracks that we have files for
            tracks_to_add = []
            for track_id in track_ids:
                if track_id in track_id_map:
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
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
                    'album': track.album
                }

    # Find the actual files in the master directory
    tracks_found = 0
    tracks_added = 0
    track_paths = []

    # First build a set of track IDs for faster lookup
    track_id_set = set(track_ids)

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
                    if track_id in track_id_set:
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


def generate_all_m3u_playlists(master_tracks_dir: str, playlists_dir: str,
                               extended: bool = True, skip_master: bool = True,
                               overwrite: bool = True, only_changed: bool = True,
                               changed_playlists: List[str] = None,
                               track_id_map: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Generate M3U playlist files for all playlists in the database.

    Args:
        master_tracks_dir: Directory containing the master tracks
        playlists_dir: Directory where playlist files will be created
        extended: Whether to use extended M3U format with metadata
        skip_master: Whether to skip the MASTER playlist
        overwrite: Whether to overwrite existing M3U files
        only_changed: Only update playlists that have changed
        changed_playlists: List of playlist names that have changed (if None, determine automatically)
        track_id_map: Optional pre-built mapping of track_ids to file paths for optimization

    Returns:
        Dictionary with statistics about the generation process
    """
    m3u_logger.info("Starting generation of all M3U playlists")

    # Create logs directory for reports
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    m3u_logs_dir = logs_dir / 'm3u_validation'
    m3u_logs_dir.mkdir(exist_ok=True)

    # Ensure the playlists directory exists
    os.makedirs(playlists_dir, exist_ok=True)

    # Get all playlists from the database
    with UnitOfWork() as uow:
        playlists = uow.playlist_repository.get_all()
        m3u_logger.info(f"Found {len(playlists)} playlists in database")

    # Initialize statistics
    stats = {
        'total_playlists': len(playlists),
        'playlists_created': 0,
        'playlists_updated': 0,
        'playlists_unchanged': 0,
        'total_tracks_found': 0,
        'total_tracks_added': 0,
        'empty_playlists': [],
        'changed_playlists': [],  # Track which playlists changed
        'playlist_changes': {}  # Detailed changes for each playlist
    }

    # Convert changed_playlists to a set for faster lookup if provided
    changed_playlist_set = set(changed_playlists or [])

    # If no track_id_map was provided, build one (this is the expensive operation)
    if track_id_map is None and only_changed:
        track_id_map = build_track_id_mapping(master_tracks_dir)

    # Generate a playlist file for each playlist
    for playlist in playlists:
        # Skip MASTER playlist if requested
        if skip_master and playlist.name.upper() == "MASTER":
            m3u_logger.info(f"Skipping MASTER playlist")
            continue

        # Sanitize the playlist name for use as a filename
        safe_playlist_name = sanitize_filename(playlist.name, preserve_spaces=True)
        m3u_path = os.path.join(playlists_dir, f"{safe_playlist_name}.m3u")

        # Skip if only processing changed playlists and this one hasn't changed
        if only_changed and changed_playlists is not None and playlist.name not in changed_playlist_set:
            if os.path.exists(m3u_path):
                m3u_logger.info(f"Playlist '{playlist.name}' is unchanged, skipping.")
                stats['playlists_unchanged'] += 1
                continue

        # If this is a new playlist that doesn't exist yet, we should create it
        if not os.path.exists(m3u_path):
            m3u_logger.info(f"New playlist file will be created: {m3u_path}")
        else:
            m3u_logger.info(f"Updating existing playlist file: {m3u_path}")

        m3u_logger.info(f"Processing playlist: {playlist.name} (ID: {playlist.playlist_id})")

        # Generate the M3U file
        tracks_found, tracks_added = generate_m3u_playlist(
            playlist.name,
            playlist.playlist_id,
            master_tracks_dir,
            playlists_dir,
            extended,
            overwrite,
            track_id_map
        )

        stats['total_tracks_found'] += tracks_found
        stats['total_tracks_added'] += tracks_added

        if tracks_added > 0:
            if os.path.exists(m3u_path) and os.path.getmtime(m3u_path) < time.time() - 60:  # Older than 1 minute
                stats['playlists_updated'] += 1
            else:
                stats['playlists_created'] += 1

            # Mark as changed for reporting
            stats['changed_playlists'].append(playlist.name)
        else:
            stats['empty_playlists'].append(playlist.name)

    # Generate a report file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = m3u_logs_dir / f"m3u_generation_{timestamp}.log"

    with open(report_path, "w", encoding="utf-8") as report:
        report.write("M3U Playlist Generation Report\n")
        report.write("============================\n\n")
        report.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.write(f"Master tracks directory: {master_tracks_dir}\n")
        report.write(f"Playlists directory: {playlists_dir}\n\n")

        report.write("Statistics:\n")
        report.write(f"  Total playlists in database: {stats['total_playlists']}\n")
        report.write(f"  New playlists created: {stats['playlists_created']}\n")
        report.write(f"  Existing playlists updated: {stats['playlists_updated']}\n")
        report.write(f"  Playlists unchanged: {stats['playlists_unchanged']}\n")
        report.write(f"  Total tracks found: {stats['total_tracks_found']}\n")
        report.write(f"  Total tracks added to playlists: {stats['total_tracks_added']}\n")

        if stats['changed_playlists']:
            report.write("\nChanged Playlists:\n")
            for playlist_name in stats['changed_playlists']:
                changes = stats['playlist_changes'].get(playlist_name, {})
                report.write(f"  - {playlist_name}:\n")

                if changes:
                    report.write(
                        f"      {changes.get('added_tracks', 'N/A')} tracks added, "
                        f"{changes.get('removed_tracks', 'N/A')} tracks removed\n"
                    )

                    if changes.get('added_details'):
                        report.write("      Sample additions:\n")
                        for track in changes['added_details']:
                            report.write(f"        * {track}\n")
                report.write("\n")

        if stats['empty_playlists']:
            report.write("\nEmpty playlists (no tracks found):\n")
            for playlist in stats['empty_playlists']:
                report.write(f"  - {playlist}\n")

    m3u_logger.info(f"M3U playlist generation complete. Report saved to: {report_path}")
    return stats


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
