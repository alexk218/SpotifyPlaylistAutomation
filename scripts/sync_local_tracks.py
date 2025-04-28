#!/usr/bin/env python
import argparse
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Set, Tuple
from dotenv import load_dotenv

load_dotenv()

# Import the necessary modules from your existing codebase
from sql.core.unit_of_work import UnitOfWork
from sql.models.track import Track
from utils.logger import setup_logger
from mutagen.id3 import ID3, ID3NoHeaderError

# Set up logging
logger = setup_logger('sync_local_tracks', 'logs/sync_local_tracks.log')

# Default paths
LOCAL_TRACKS_CACHE_DIRECTORY = os.getenv("LOCAL_TRACKS_CACHE_DIRECTORY")
MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
MASTER_PLAYLIST_ID = os.getenv("MASTER_PLAYLIST_ID")


def extract_track_id(file_path):
    """Extract Spotify TrackId from MP3 file metadata."""
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            return tags['TXXX:TRACKID'].text[0]
        return None
    except ID3NoHeaderError:
        return None
    except Exception as e:
        logger.error(f"Error extracting TrackId from {file_path}: {e}")
        return None


def scan_directory(directory: str) -> Dict[str, str]:
    """
    Scan directory for MP3 files and extract Spotify TrackIds.
    Returns a dictionary mapping track IDs to file paths.
    """
    track_id_map = {}
    total_files = 0
    files_with_track_id = 0

    print(f"Scanning directory: {directory}")
    logger.info(f"Scanning directory: {directory}")

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.mp3'):
                total_files += 1
                file_path = os.path.join(root, file)

                # Extract track ID
                track_id = extract_track_id(file_path)

                if track_id:
                    files_with_track_id += 1
                    track_id_map[track_id] = file_path

                # Show progress for large libraries
                if total_files % 100 == 0:
                    print(f"Processed {total_files} files, found {files_with_track_id} with TrackIds")

    print(f"Scan complete: {total_files} total files, {files_with_track_id} with TrackIds")
    logger.info(f"Scan complete: {total_files} total files, {files_with_track_id} with TrackIds")
    return track_id_map


def get_all_tracks_from_db() -> List[Track]:
    """Get all tracks from the database."""
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()
        logger.info(f"Retrieved {len(tracks)} tracks from database")
        return tracks


def get_master_playlist_tracks_from_db() -> List[Track]:
    """Get all tracks in the MASTER playlist from the database."""
    with UnitOfWork() as uow:
        if not MASTER_PLAYLIST_ID:
            logger.error("MASTER_PLAYLIST_ID not set in environment variables")
            return []

        tracks = uow.track_repository.get_tracks_in_playlist(MASTER_PLAYLIST_ID)
        logger.info(f"Retrieved {len(tracks)} tracks from MASTER playlist")
        return tracks


def generate_cache_file(music_directory: str, output_dir: str, track_id_map: Dict[str, str],
                        db_tracks: List[Track]) -> str:
    """
    Generate a cache file of all local tracks with their Spotify TrackIds.
    Maps the track_id_map (from files) to track data from the database.

    Returns the path to the generated cache file.
    """
    start_time = time.time()

    # Create lookup dictionary for faster access to track data
    track_data_map = {track.track_id: track for track in db_tracks}

    # Prepare local tracks data
    local_tracks = []
    for track_id, file_path in track_id_map.items():
        # Get file stats
        try:
            file_stats = os.stat(file_path)
            file_size = file_stats.st_size
            modified_time = file_stats.st_mtime
        except Exception as e:
            logger.error(f"Error getting stats for {file_path}: {e}")
            continue

        # Get base filename
        filename = os.path.basename(file_path)

        # Get track data from database if available
        db_track = track_data_map.get(track_id)

        # Extract artist and title from filename or database
        if db_track:
            artist = db_track.artists
            title = db_track.title
            album = db_track.album
        else:
            # Extract from filename (fallback)
            filename_base = os.path.splitext(filename)[0]
            artist = ""
            title = filename_base
            album = ""

            # Common pattern: "Artist - Title"
            if " - " in filename_base:
                parts = filename_base.split(" - ", 1)
                artist = parts[0].strip()
                title = parts[1].strip()

        # Add to local tracks list
        local_tracks.append({
            "path": file_path,
            "filename": filename,
            "track_id": track_id,
            "size": file_size,
            "modified": modified_time,
            "artist": artist,
            "title": title,
            "album": album
        })

    # Prepare output data
    cache_data = {
        "generated": datetime.now().isoformat(),
        "music_directory": music_directory,
        "total_files": len(track_id_map),
        "files_with_track_id": len(track_id_map),
        "tracks": local_tracks
    }

    # Determine output filename
    date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"local_tracks_cache_{date_suffix}.json"
    output_file = os.path.join(output_dir, filename)

    # Latest version file (always overwritten)
    latest_file = os.path.join(output_dir, "local_tracks_cache.json")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2)

    # Write to latest file
    with open(latest_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2)

    elapsed_time = time.time() - start_time
    print(f"Cache generated in {elapsed_time:.2f} seconds")
    print(f"Cache files saved to: {output_file} and {latest_file}")
    logger.info(f"Cache generated in {elapsed_time:.2f} seconds")
    logger.info(f"Cache files saved to: {output_file} and {latest_file}")

    return output_file


def analyze_missing_tracks(db_tracks: List[Track], local_track_ids: Set[str]) -> Tuple[List[Track], int]:
    """
    Analyze which tracks from the database are missing in local files.

    Returns:
        Tuple of (missing_tracks_list, missing_count)
    """
    missing_tracks = []

    for track in db_tracks:
        if track.track_id not in local_track_ids:
            missing_tracks.append(track)

    return missing_tracks, len(missing_tracks)


def main():
    parser = argparse.ArgumentParser(description="Sync local tracks with Spotify database and generate cache")
    parser.add_argument("--music-dir", type=str, default=MASTER_TRACKS_DIRECTORY,
                        help=f"Directory containing music files (default: {MASTER_TRACKS_DIRECTORY})")
    parser.add_argument("--output-dir", type=str, default=LOCAL_TRACKS_CACHE_DIRECTORY,
                        help=f"Directory to save cache files (default: {LOCAL_TRACKS_CACHE_DIRECTORY})")
    parser.add_argument("--master-only", action="store_true",
                        help="Only include tracks from MASTER playlist in analysis")
    parser.add_argument("--report", action="store_true",
                        help="Generate a detailed missing tracks report")
    parser.add_argument("--server", action="store_true",
                        help="Start the local tracks server after generating cache")

    args = parser.parse_args()

    # Validate directories
    music_dir = args.music_dir
    if not music_dir or not os.path.isdir(music_dir):
        print(f"Error: Music directory '{music_dir}' not found")
        return

    output_dir = args.output_dir
    if not output_dir:
        print("Error: Output directory not specified")
        return

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Scan directory for MP3 files with TrackIds
    track_id_map = scan_directory(music_dir)
    local_track_ids = set(track_id_map.keys())

    # Get tracks from database
    if args.master_only:
        db_tracks = get_master_playlist_tracks_from_db()
    else:
        db_tracks = get_all_tracks_from_db()

    # Generate cache file
    cache_file = generate_cache_file(music_dir, output_dir, track_id_map, db_tracks)

    # Analyze missing tracks
    missing_tracks, missing_count = analyze_missing_tracks(db_tracks, local_track_ids)

    # Print summary
    print("\nSUMMARY:")
    print(f"Total tracks in database: {len(db_tracks)}")
    print(f"Total local tracks with TrackId: {len(local_track_ids)}")
    print(f"Missing tracks: {missing_count}")

    # Generate detailed report if requested
    if args.report and missing_count > 0:
        report_file = os.path.join(output_dir, f"missing_tracks_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("MISSING TRACKS REPORT\n")
            f.write("====================\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Music directory: {music_dir}\n")
            f.write(f"Total tracks in database: {len(db_tracks)}\n")
            f.write(f"Total local tracks with TrackId: {len(local_track_ids)}\n")
            f.write(f"Missing tracks: {missing_count}\n\n")

            f.write("MISSING TRACKS:\n")
            f.write("==============\n\n")

            # Sort missing tracks by artist
            missing_tracks.sort(key=lambda t: (t.artists, t.title))

            for track in missing_tracks:
                f.write(f"{track.artists} - {track.title}\n")
                f.write(f"  Album: {track.album}\n")
                f.write(f"  Track ID: {track.track_id}\n")
                # Add playlist info if available
                with UnitOfWork() as uow:
                    playlists = uow.playlist_repository.get_playlists_for_track(track.track_id)
                    if playlists:
                        playlist_names = [p.name for p in playlists]
                        f.write(f"  Playlists: {', '.join(playlist_names)}\n")
                f.write("\n")

        print(f"\nDetailed missing tracks report saved to: {report_file}")

    # Start local tracks server if requested
    if args.server:
        from scripts.local_tracks_server import run_server
        print("\nStarting local tracks server...")
        run_server(8765, output_dir)


if __name__ == "__main__":
    main()
