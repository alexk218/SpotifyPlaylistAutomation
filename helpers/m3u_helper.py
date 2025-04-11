import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple, Union
from mutagen.id3 import ID3, ID3NoHeaderError
from mutagen import File

from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger
from utils.symlink_tracker import tracker

m3u_logger = setup_logger('m3u_helper', 'logs/m3u_validation/m3u.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def generate_m3u_playlist(playlist_name: str, playlist_id: str, master_tracks_dir: str,
                          playlists_dir: str, extended: bool = True, overwrite: bool = True) -> Tuple[int, int]:
    """
    Generate an M3U playlist file for a specific playlist.

    Args:
        playlist_name: Name of the playlist
        playlist_id: Spotify ID of the playlist
        master_tracks_dir: Directory containing the master tracks
        playlists_dir: Directory where playlist files will be created
        extended: Whether to use extended M3U format with metadata

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
                               overwrite: bool = True) -> Dict[str, int]:
    """
    Generate M3U playlist files for all playlists in the database.

    Args:
        master_tracks_dir: Directory containing the master tracks
        playlists_dir: Directory where playlist files will be created
        extended: Whether to use extended M3U format with metadata
        skip_master: Whether to skip the MASTER playlist

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
        'total_tracks_found': 0,
        'total_tracks_added': 0,
        'empty_playlists': []
    }

    # Generate a playlist file for each playlist
    for playlist in playlists:
        # Skip MASTER playlist if requested
        if skip_master and playlist.name.upper() == "MASTER":
            m3u_logger.info(f"Skipping MASTER playlist")
            continue

        m3u_logger.info(f"Processing playlist: {playlist.name} (ID: {playlist.playlist_id})")

        # Log the detailed information for each playlist
        with UnitOfWork() as uow:
            track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist.playlist_id)
            m3u_logger.info(f"Found {len(track_ids)} track IDs in database for playlist '{playlist.name}'")

            if track_ids:
                # Log the first few track IDs for debugging purposes
                sample_ids = track_ids[:5]
                m3u_logger.info(f"Sample track IDs: {', '.join(sample_ids)}")

                # Get track details for the first few tracks
                for i, track_id in enumerate(sample_ids):
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
                        m3u_logger.info(f"  Track {i + 1}: {track.artists} - {track.title}")

        tracks_found, tracks_added = generate_m3u_playlist(
            playlist.name,
            playlist.playlist_id,
            master_tracks_dir,
            playlists_dir,
            extended,
            overwrite
        )

        stats['total_tracks_found'] += tracks_found
        stats['total_tracks_added'] += tracks_added

        if tracks_added > 0:
            stats['playlists_created'] += 1
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
        report.write(f"  Playlists created: {stats['playlists_created']}\n")
        report.write(f"  Total tracks found: {stats['total_tracks_found']}\n")
        report.write(f"  Total tracks added to playlists: {stats['total_tracks_added']}\n")

        if stats['empty_playlists']:
            report.write("\nEmpty playlists (no tracks found):\n")
            for playlist in stats['empty_playlists']:
                report.write(f"  - {playlist}\n")

    m3u_logger.info(f"M3U playlist generation complete. Report saved to: {report_path}")
    return stats


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


def get_track_duration(file_path: str) -> int:
    """
    Get the duration of a track in seconds.

    Args:
        file_path: Path to the audio file

    Returns:
        Duration in seconds, or 0 if not available
    """
    try:
        from mutagen.mp3 import MP3
        audio = MP3(file_path)
        return int(audio.info.length)
    except Exception:
        return 0  # Default duration if we can't read it
