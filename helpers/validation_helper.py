"""
Optimized validation helpers that minimize Spotify API calls by using local data.

This module implements validation functions that prioritize:
1. Database data
2. Local file metadata
3. Cached API responses
4. Spotify API calls as a last resort
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional, Any, Union
from mutagen.id3 import ID3, ID3NoHeaderError

from cache_manager import spotify_cache
from drivers.spotify_client import (
    authenticate_spotify,
    get_playlist_track_ids,
    fetch_master_tracks_from_db,
    fetch_playlists
)
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

validation_logger = setup_logger('validation_helper', 'logs/validation.log')
db_logger = setup_logger('db_logger', 'sql/db.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def get_file_track_id(file_path: str) -> Optional[str]:
    """
    Get TrackId from a file's metadata if it exists.

    Args:
        file_path: Path to the audio file

    Returns:
        TrackId or None if not found
    """
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            return tags['TXXX:TRACKID'].text[0]
    except Exception as e:
        validation_logger.error(f"Error reading TrackId from {file_path}: {e}")
    return None


def validate_master_tracks(master_tracks_dir: str) -> Dict[str, int]:
    """
    Validate local tracks against MASTER playlist data from database.
    Only makes API calls if data is not in the database.

    Args:
        master_tracks_dir: Directory containing the master tracks

    Returns:
        Dictionary with validation statistics
    """
    print("\nValidating master tracks directory...")
    validation_logger.info("Starting master tracks validation")

    # Create logs directory and subdirectory
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    master_validation_dir = logs_dir / 'master_validation'
    master_validation_dir.mkdir(exist_ok=True)

    # Get tracks from database first, only use API if empty
    master_tracks = fetch_master_tracks_from_db()
    validation_logger.info(f"Retrieved {len(master_tracks)} master tracks from database")

    # If no tracks in database, we'd need to get from Spotify (not implemented here)
    if not master_tracks:
        validation_logger.warning("No master tracks found in database. Run database sync first.")
        print("\nNo master tracks found in database. Please sync your database first.")
        return {
            'total_spotify_tracks': 0,
            'total_local_files': 0,
            'files_with_valid_trackid': 0,
            'files_without_trackid': 0,
            'unmatched_files': 0,
            'missing_downloads': 0
        }

    # Extract track IDs from master tracks for quick lookup
    track_ids = {track['id'] for track in master_tracks}
    validation_logger.info(f"Validating against {len(track_ids)} track IDs")

    # Initialize tracking
    found_track_ids = set()
    missing_downloads = []
    unmatched_files = []
    files_without_trackid = []

    # Scan local files
    total_files = 0
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            file_path = os.path.join(root, file)

            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    if track_id in track_ids:
                        found_track_ids.add(track_id)
                    else:
                        unmatched_files.append({
                            'file': file,
                            'reason': 'TrackId not found in MASTER playlist',
                            'current_id': track_id
                        })
                else:
                    files_without_trackid.append(file)
            except Exception as e:
                files_without_trackid.append(file)
                validation_logger.error(f"Error reading metadata for {file}: {e}")

    # Find missing tracks - tracks in database but not locally
    missing_track_ids = track_ids - found_track_ids
    for track in master_tracks:
        if track['id'] in missing_track_ids:
            # Try to find a file that matches the name pattern
            expected_filename = f"{track['artists'].split(',')[0]} - {track['name']}.mp3"
            file_exists = False
            actual_track_id = None

            # Look for the file
            for root, _, files in os.walk(master_tracks_dir):
                for file in files:
                    if file.lower() == expected_filename.lower():
                        file_exists = True
                        file_path = os.path.join(root, file)
                        actual_track_id = get_file_track_id(file_path)
                        break
                if file_exists:
                    break

            missing_downloads.append({
                'track_id': track['id'],
                'artist': track['artists'].split(',')[0],
                'title': track['name'],
                'added_at': track['added_at'],
                'file_exists': file_exists,
                'actual_track_id': actual_track_id
            })

    # Generate report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = master_validation_dir / f'track_validation_{timestamp}.log'

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write("Track Validation Report\n")
        f.write("Validate local tracks against MASTER playlist\n")
        f.write("=====================\n\n")

        f.write(f"Total tracks in database: {len(master_tracks)}\n")
        f.write(f"Total local files: {total_files}\n")
        f.write(f"Files with valid TrackId: {len(found_track_ids)}\n")
        f.write(f"Files without TrackId: {len(files_without_trackid)}\n")
        f.write(f"Files with unmatched TrackId: {len(unmatched_files)}\n")
        f.write(f"Missing downloads: {len(missing_downloads)}\n\n")

        if missing_downloads:
            f.write("\nMissing Downloads (Sorted by Spotify Addition Date):\n")
            f.write("============================================\n")
            for track in missing_downloads:
                date_str = track['added_at'].strftime("%Y-%m-%d %H:%M:%S") if track['added_at'] else "Unknown date"
                f.write(f"• {track['artist']} - {track['title']}\n")
                f.write(f"  Added to Spotify: {date_str}\n")
                f.write(f"  Expected TrackId: {track['track_id']}\n")

                if track['file_exists']:
                    if track['actual_track_id']:
                        f.write(f"  File exists but has wrong TrackId: {track['actual_track_id']}\n")
                    else:
                        f.write(f"  File exists but has no TrackId\n")
                else:
                    f.write(f"  File not found: {track['artist']} - {track['title']}.mp3\n")
                f.write("\n")

        if files_without_trackid:
            f.write("\nFiles Without TrackId:\n")
            f.write("====================\n")
            for file in files_without_trackid:
                f.write(f"• {file}\n")

        if unmatched_files:
            f.write("\nFiles With Unmatched TrackId:\n")
            f.write("===========================\n")
            for file in unmatched_files:
                f.write(f"• {file['file']}\n")
                f.write(f"  Current TrackId: {file['current_id']}\n")
                f.write(f"  Reason: {file['reason']}\n")

    validation_logger.info(f"Validation complete. Report saved to: {log_path}")
    print(f"\nValidation complete! Report saved to: {log_path}")

    return {
        'total_spotify_tracks': len(master_tracks),
        'total_local_files': total_files,
        'files_with_valid_trackid': len(found_track_ids),
        'files_without_trackid': len(files_without_trackid),
        'unmatched_files': len(unmatched_files),
        'missing_downloads': len(missing_downloads)
    }


def check_symlink(file_path: str) -> Optional[str]:
    """
    Check if a path is a symlink and resolve it.

    Args:
        file_path: Path to check

    Returns:
        Resolved path if valid, None if broken
    """
    try:
        if not os.path.islink(file_path):
            return file_path if os.path.exists(file_path) else None

        real_path = os.path.realpath(file_path)
        return real_path if os.path.exists(real_path) else None
    except Exception:
        return None


def validate_playlist_symlinks_quick(playlists_dir: str) -> Dict[str, int]:
    """
    Quick validation of playlist symlinks against database data.
    Uses minimal API calls by prioritizing database data.

    Args:
        playlists_dir: Directory containing playlist symlinks

    Returns:
        Dictionary with validation statistics
    """
    # Create logs directory and subdirectory
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    playlist_validation_dir = logs_dir / 'playlist_validation'
    playlist_validation_dir.mkdir(exist_ok=True)

    print("\nQuick validation of playlist symlinks...")
    validation_logger.info("Starting quick validation of playlist symlinks")

    # Get playlists from database
    with UnitOfWork() as uow:
        database_playlists = uow.playlist_repository.get_all()
        db_playlist_dict = {p.name: p.playlist_id for p in database_playlists}

    validation_logger.info(f"Retrieved {len(database_playlists)} playlists from database")

    # Get Spotify authentication only if needed
    spotify_client = None

    # Track validation results
    mismatched_playlists = []
    missing_playlists = []
    extra_playlists = []

    # Get local playlist folders
    local_playlist_folders = {
        d.strip() for d in os.listdir(playlists_dir)
        if os.path.isdir(os.path.join(playlists_dir, d)) and d.upper() != "MASTER"
    }

    # Get Spotify playlist names from database, excluding MASTER
    db_playlist_names = {
        name.strip() for name in db_playlist_dict.keys()
        if name.upper() != "MASTER"
    }

    # Find missing and extra playlists
    missing_playlists = db_playlist_names - local_playlist_folders
    extra_playlists = local_playlist_folders - db_playlist_names

    # Compare each playlist's contents
    for playlist_name in db_playlist_names & local_playlist_folders:
        # Skip MASTER playlist
        if playlist_name.upper() == "MASTER":
            continue

        playlist_id = db_playlist_dict.get(playlist_name)
        local_folder = os.path.join(playlists_dir, playlist_name)

        # Get Spotify track IDs for this playlist from database
        with UnitOfWork() as uow:
            db_track_ids = set(uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))

        # If no tracks in database, initialize Spotify client and fetch from API
        if not db_track_ids and spotify_client is None:
            spotify_client = authenticate_spotify()
            # Import the function only when needed
            from drivers.spotify_client import get_playlist_track_ids
            db_track_ids = set(get_playlist_track_ids(spotify_client, playlist_id))

        validation_logger.info(f"Found {len(db_track_ids)} tracks for playlist '{playlist_name}' in database/API")

        # Get local track IDs from files
        local_track_ids = set()
        missing_trackids = []

        for file in os.listdir(local_folder):
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(local_folder, file)
            real_path = check_symlink(file_path)

            if real_path is None:
                missing_trackids.append(file)
                continue

            try:
                tags = ID3(real_path)
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    local_track_ids.add(track_id)
                else:
                    missing_trackids.append(file)
            except Exception as e:
                validation_logger.error(f"Error reading TrackId from {file}: {e}")
                missing_trackids.append(file)

        # Compare track sets
        if len(db_track_ids) != len(local_track_ids) or db_track_ids != local_track_ids or missing_trackids:
            mismatched_playlists.append({
                'name': playlist_name,
                'spotify_count': len(db_track_ids),
                'local_count': len(local_track_ids),
                'missing_tracks': db_track_ids - local_track_ids,
                'extra_tracks': local_track_ids - db_track_ids,
                'files_without_trackid': missing_trackids
            })

    # Generate report
    if mismatched_playlists or missing_playlists or extra_playlists:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = playlist_validation_dir / f'playlist_validation_quick_{timestamp}.log'

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("Quick Playlist Validation Report\n")
            f.write("Quick validation of playlist symlinks against database data\n")
            f.write("Only checks TrackIds\n")
            f.write("============================\n\n")

            if missing_playlists:
                f.write("\nMissing Playlist Folders:\n")
                f.write("========================\n")
                for playlist in sorted(missing_playlists):
                    f.write(f"• {playlist}\n")

            if extra_playlists:
                f.write("\nExtra Playlist Folders:\n")
                f.write("=====================\n")
                for playlist in sorted(extra_playlists):
                    f.write(f"• {playlist}\n")

            if mismatched_playlists:
                f.write("\nMismatched Playlists:\n")
                f.write("===================\n")
                for playlist in mismatched_playlists:
                    f.write(f"\n{playlist['name']}:\n")
                    f.write(f"  Database tracks: {playlist['spotify_count']}\n")
                    f.write(f"  Local files: {playlist['local_count']}\n")

                    if playlist['missing_tracks']:
                        f.write("\n  Missing track IDs (in database but not local):\n")
                        f.write("  -----------------------------------------\n")
                        for track_id in sorted(playlist['missing_tracks']):
                            f.write(f"  • {track_id}\n")

                    if playlist['extra_tracks']:
                        f.write("\n  Extra track IDs (local but not in database):\n")
                        f.write("  -------------------------------------\n")
                        for track_id in sorted(playlist['extra_tracks']):
                            f.write(f"  • {track_id}\n")

                    if playlist['files_without_trackid']:
                        f.write("\n  Files without TrackId:\n")
                        f.write("  --------------------\n")
                        for file in sorted(playlist['files_without_trackid']):
                            f.write(f"  • {file}\n")
                        f.write("\n")

        print(f"\nQuick validation complete! Issues found:")
        print(f"- Missing playlists: {len(missing_playlists)}")
        print(f"- Extra playlists: {len(extra_playlists)}")
        print(f"- Mismatched playlists: {len(mismatched_playlists)}")
        print(f"Report saved to: {log_path}")

    else:
        print("\nValidation complete! All playlist folders match their database data.")

    return {
        'missing_playlists': len(missing_playlists),
        'extra_playlists': len(extra_playlists),
        'mismatched_playlists': len(mismatched_playlists)
    }


def validate_playlist_symlinks(playlists_dir):
    """
    Validate playlist symlinks against Spotify playlists.

    Args:
        playlists_dir (str): Directory containing playlist symlinks

    Returns:
        Dict[str, int]: Validation statistics
    """
    # Ensure logs directory exists
    logs_dir = Path(playlists_dir).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    playlist_validation_dir = logs_dir / 'playlist_validation'
    playlist_validation_dir.mkdir(exist_ok=True)

    print("\nValidating playlist symlinks against Spotify playlists...")

    # Authenticate with Spotify
    spotify_client = authenticate_spotify()

    # Get Spotify playlists, excluding MASTER playlist
    spotify_playlists = [
        (name, owner, playlist_id)
        for name, owner, playlist_id in fetch_playlists(spotify_client)
        if playlist_id != os.getenv('MASTER_PLAYLIST_ID')
    ]

    total_playlists = len(spotify_playlists)
    print(f"Found {total_playlists} playlists to validate")
    validation_logger.info(f"Starting validation of {total_playlists} playlists")

    mismatched_playlists = []
    missing_playlists = []
    extra_playlists = []
    broken_links = []

    # Get local playlist folders
    local_playlist_folders = {
        d.strip() for d in Path(playlists_dir).iterdir()
        if d.is_dir() and d.name.upper() != "MASTER"
    }

    # Get Spotify playlist names
    spotify_playlist_names = {name.strip() for name, _, _ in spotify_playlists}

    # Find missing and extra playlists
    missing_playlists = list(spotify_playlist_names - {folder.name for folder in local_playlist_folders})
    extra_playlists = list(local_playlist_folders - spotify_playlist_names)

    # Compare each playlist's contents
    for playlist_name, _, playlist_id in spotify_playlists:
        playlist_name = playlist_name.strip()

        # Skip MASTER playlist
        if playlist_name.upper() == "MASTER":
            continue

        # Check if playlist folder exists
        local_folder = Path(playlists_dir) / playlist_name
        if not local_folder.exists():
            continue

        # Get Spotify track IDs for this playlist
        spotify_track_ids = set(get_playlist_track_ids(spotify_client, playlist_id))

        # Get local track IDs
        local_track_ids = set()
        missing_trackids = []

        # Process each MP3 file in the playlist folder
        for file_path in local_folder.glob('*.mp3'):
            # Check if symlink is valid
            real_path = check_symlink(str(file_path))
            if real_path is None:
                missing_trackids.append(file_path.name)
                broken_links.append((playlist_name, file_path.name))
                continue

            try:
                # Read TrackId from metadata
                tags = ID3(real_path)
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    local_track_ids.add(track_id)
                else:
                    missing_trackids.append(file_path.name)
            except Exception as e:
                validation_logger.error(f"Error reading TrackId from {file_path}: {e}")
                missing_trackids.append(file_path.name)

        # Check for mismatches
        if len(spotify_track_ids) != len(local_track_ids) or \
           spotify_track_ids != local_track_ids or missing_trackids:
            mismatched_playlists.append({
                'name': playlist_name,
                'spotify_count': len(spotify_track_ids),
                'local_count': len(local_track_ids),
                'missing_tracks': spotify_track_ids - local_track_ids,
                'extra_tracks': local_track_ids - spotify_track_ids,
                'files_without_trackid': missing_trackids
            })

    # Generate report if issues found
    if mismatched_playlists or missing_playlists or extra_playlists or broken_links:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = playlist_validation_dir / f'playlist_validation_{timestamp}.log'

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("Playlist Validation Report\n")
            f.write("========================\n\n")

            # Write details about missing, extra, and mismatched playlists
            # (report generation logic similar to previous implementations)
            # ... [Detailed report writing would go here]

        print(f"\nValidation complete! Issues found:")
        print(f"- Missing playlists: {len(missing_playlists)}")
        print(f"- Extra playlists: {len(extra_playlists)}")
        print(f"- Mismatched playlists: {len(mismatched_playlists)}")
        print(f"- Broken symlinks: {len(broken_links)}")
        print(f"Report saved to: {log_path}")

    else:
        print("\nValidation complete! All playlist folders match their Spotify playlists.")

    return {
        'missing_playlists': len(missing_playlists),
        'extra_playlists': len(extra_playlists),
        'mismatched_playlists': len(mismatched_playlists),
        'broken_links': len(broken_links)
    }


def get_track_info_from_db(track_id: str) -> Optional[Dict[str, Any]]:
    """
    Get track information from the database.

    Args:
        track_id: Track ID to look up

    Returns:
        Dictionary with track information or None if not found
    """
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(track_id)
        if track:
            return {
                'id': track.track_id,
                'name': track.title,
                'artists': track.artists.split(', '),
                'album': track.album
            }
    return None
