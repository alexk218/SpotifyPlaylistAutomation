import os
from datetime import datetime
from os import PathLike
from pathlib import Path
from typing import Union
from mutagen.id3 import ID3, ID3NoHeaderError
from drivers.spotify_client import authenticate_spotify, fetch_master_tracks_for_validation, fetch_playlists, \
    get_playlist_track_ids
from helpers.file_helper import create_symlink, cleanup_broken_symlinks
from sql.helpers.db_helper import fetch_playlists_for_track, fetch_all_playlists_db
from utils.logger import setup_logger
from utils.symlink_tracker import tracker

MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_logger', 'sql/db.log')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent

# Organizes all songs from 'tracks_master' by creating symlinks in playlist folders based on db association
# ! CREATES FOLDERS WITH SYMLINKS
def organize_songs_into_playlists(
        master_tracks_dir: Union[str, PathLike[str]],
        playlists_dir: Union[str, PathLike[str]],
        dry_run: bool = False
) -> None:
    with tracker.tracking_session():
        print("Organizing songs into playlist folders with symlinks...")
        db_logger.info("Starting to organize songs into playlists.")

        # First, clean up any broken symlinks
        cleanup_broken_symlinks(playlists_dir, dry_run)

        # Get all playlists from database to create directories
        playlists = fetch_all_playlists_db()

        # Create playlist directories if they don't exist
        for playlist in playlists:
            playlist_id, playlist_name = playlist
            playlist_path = os.path.join(playlists_dir, playlist_name)

            if not dry_run:
                if os.path.exists(playlist_path):
                    db_logger.info(f"Playlist directory already exists: {playlist_path}")
                else:
                    os.makedirs(playlist_path)
                    db_logger.info(f"Created new playlist directory: {playlist_path}")
            else:
                if os.path.exists(playlist_path):
                    db_logger.info(f"[DRY RUN] Playlist directory already exists: {playlist_path}")
                else:
                    db_logger.info(f"[DRY RUN] Would create new playlist directory: {playlist_path}")

        # Process each track in the master directory
        for root, _, files in os.walk(master_tracks_dir):
            for filename in files:
                if not filename.lower().endswith('.mp3'):
                    continue

                file_path = os.path.join(root, filename)

                # Extract TrackId from metadata
                try:
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' not in tags:
                        db_logger.warning(f"No TrackId found in metadata for: {filename}")
                        continue

                    track_id = tags['TXXX:TRACKID'].text[0]
                    db_logger.info(f"Found TrackId in {filename}: {track_id}")

                    # Get associated playlists for this track
                    associated_playlists = fetch_playlists_for_track(track_id)

                    if not associated_playlists:
                        db_logger.warning(f"No playlist associations found for track: {filename} (ID: {track_id})")
                        continue

                    # Create symlinks in each associated playlist directory
                    for playlist_name in associated_playlists:
                        playlist_path = os.path.join(playlists_dir, playlist_name)
                        symlink_path = os.path.join(playlist_path, filename)

                        if dry_run:
                            db_logger.info(f"[DRY RUN] Would create symlink: {symlink_path} -> {file_path}")
                        else:
                            create_symlink(file_path, symlink_path)

                except ID3NoHeaderError:
                    db_logger.warning(f"No ID3 tags found in: {filename}")
                    continue
                except Exception as e:
                    db_logger.error(f"Error processing {filename}: {e}")
                    continue

        db_logger.info("Playlist organization complete!")


# Get TrackId from a file if it exists
def get_file_track_id(file_path):
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            return tags['TXXX:TRACKID'].text[0]
    except Exception as e:
        db_logger.error(f"Error reading TrackId from {file_path}: {e}")
    return None


# Validate local tracks against Spotify MASTER playlist
def validate_master_tracks(master_tracks_dir):
    print("\nValidating master tracks directory...")

    # Create logs directory and subdirectory
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    master_validation_dir = logs_dir / 'master_validation'
    master_validation_dir.mkdir(exist_ok=True)

    # Get tracks from Spotify
    spotify_client = authenticate_spotify()
    spotify_tracks = fetch_master_tracks_for_validation(spotify_client, MASTER_PLAYLIST_ID)
    track_ids = {track['id'] for track in spotify_tracks}

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
                            'reason': 'TrackId not found in Spotify playlist',
                            'current_id': track_id
                        })
                else:
                    files_without_trackid.append(file)
            except Exception as e:
                files_without_trackid.append(file)
                db_logger.error(f"Error reading metadata for {file}: {e}")

    # Enhanced missing tracks check
    missing_track_ids = track_ids - found_track_ids
    for track in spotify_tracks:
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
        f.write("Validate local tracks against Spotify MASTER playlist\n")
        f.write("=====================\n\n")

        f.write(f"Total tracks in Spotify playlist: {len(spotify_tracks)}\n")
        f.write(f"Total local files: {total_files}\n")
        f.write(f"Files with valid TrackId: {len(found_track_ids)}\n")
        f.write(f"Files without TrackId: {len(files_without_trackid)}\n")
        f.write(f"Files with unmatched TrackId: {len(unmatched_files)}\n")
        f.write(f"Missing downloads: {len(missing_downloads)}\n\n")

        if missing_downloads:
            f.write("\nMissing Downloads (Sorted by Spotify Addition Date):\n")
            f.write("============================================\n")
            for track in missing_downloads:
                date_str = track['added_at'].strftime("%Y-%m-%d %H:%M:%S")
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

    print(f"\nValidation complete! Report saved to: {log_path}")

    return {
        'total_spotify_tracks': len(spotify_tracks),
        'total_local_files': total_files,
        'files_with_valid_trackid': len(found_track_ids),
        'files_without_trackid': len(files_without_trackid),
        'unmatched_files': len(unmatched_files),
        'missing_downloads': len(missing_downloads)
    }


def check_symlink(file_path):
    try:
        if not os.path.islink(file_path):
            return file_path if os.path.exists(file_path) else None

        real_path = os.path.realpath(file_path)
        return real_path if os.path.exists(real_path) else None
    except Exception:
        return None


def get_track_info(spotify_client, track_id, timeout=5):
    try:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError
        with ThreadPoolExecutor() as executor:
            future = executor.submit(spotify_client.track, track_id)
            track = future.result(timeout=timeout)  # 5 second timeout
            return track
    except Exception as e:
        db_logger.error(f"Failed to fetch track info for {track_id}: {e}")
        return None


# Validate that symlinked playlist folders match Spotify playlists.
# Compares both track count and actual tracks using TrackIds.
def validate_playlist_symlinks(playlists_dir):
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    playlist_validation_dir = logs_dir / 'playlist_validation'
    playlist_validation_dir.mkdir(exist_ok=True)

    print("\nValidating playlist symlinks against Spotify playlists...")

    spotify_client = authenticate_spotify()

    if not MASTER_PLAYLIST_ID:
        db_logger.error("MASTER_PLAYLIST_ID environment variable not set!")
        return

    spotify_playlists = [
        (name, owner, playlist_id)
        for name, owner, playlist_id in fetch_playlists(spotify_client)
        if playlist_id != MASTER_PLAYLIST_ID
    ]

    total_playlists = len(spotify_playlists)
    print(f"Found {total_playlists} playlists to validate")
    db_logger.info(f"Starting validation of {total_playlists} playlists")

    mismatched_playlists = []
    missing_playlists = []
    extra_playlists = []
    broken_links = []

    local_playlist_folders = {
        d.strip() for d in os.listdir(playlists_dir)
        if os.path.isdir(os.path.join(playlists_dir, d)) and d.upper() != "MASTER"
    }

    spotify_playlist_names = {name.strip() for name, _, _ in spotify_playlists}

    missing_playlists = spotify_playlist_names - local_playlist_folders
    extra_playlists = local_playlist_folders - spotify_playlist_names

    print(f"Comparing playlists and their tracks...")

    for index, (playlist_name, _, playlist_id) in enumerate(spotify_playlists, 1):
        print(f"Processing playlist {index}/{total_playlists}: {playlist_name}")
        db_logger.info(f"Processing playlist {index}/{total_playlists}: {playlist_name}")

        playlist_name = playlist_name.strip()

        if playlist_name.upper() == "MASTER":
            continue

        matching_folder = None
        for folder in local_playlist_folders:
            if folder.strip() == playlist_name:
                matching_folder = folder
                break

        if not matching_folder:
            continue

        local_folder = os.path.join(playlists_dir, matching_folder)

        try:
            spotify_track_ids = set(get_playlist_track_ids(spotify_client, playlist_id))
            db_logger.info(f"Found {len(spotify_track_ids)} tracks in Spotify playlist '{playlist_name}'")
        except Exception as e:
            db_logger.error(f"Failed to fetch Spotify tracks for playlist '{playlist_name}': {e}")
            continue

        local_track_ids = set()
        missing_trackids = []
        local_files = {}

        try:
            for file in os.listdir(local_folder):
                if not file.lower().endswith('.mp3'):
                    continue

                file_path = os.path.join(local_folder, file)
                real_path = check_symlink(file_path)

                if real_path is None:
                    db_logger.warning(f"Broken symlink or missing file found in {playlist_name}: {file}")
                    broken_links.append((playlist_name, file))
                    missing_trackids.append(file)
                    continue

                try:
                    tags = ID3(real_path)
                    if 'TXXX:TRACKID' in tags:
                        track_id = tags['TXXX:TRACKID'].text[0]
                        local_track_ids.add(track_id)
                        local_files[track_id] = file
                    else:
                        missing_trackids.append(file)
                except Exception as e:
                    db_logger.error(f"Error reading TrackId from {file}: {e}")
                    missing_trackids.append(file)

        except Exception as e:
            db_logger.error(f"Error accessing playlist folder {playlist_name}: {e}")
            continue

        if len(spotify_track_ids) != len(local_track_ids) or spotify_track_ids != local_track_ids or missing_trackids:
            missing_tracks = spotify_track_ids - local_track_ids
            extra_tracks = {track_id: local_files[track_id] for track_id in (local_track_ids - spotify_track_ids)}

            mismatched_playlists.append({
                'name': playlist_name,
                'spotify_count': len(spotify_track_ids),
                'local_count': len(local_track_ids),
                'missing_tracks': missing_tracks,
                'extra_tracks': extra_tracks,
                'files_without_trackid': missing_trackids
            })

    print("\n")  # New line after progress indicator

    if mismatched_playlists or missing_playlists or extra_playlists or broken_links:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = playlist_validation_dir / f'playlist_validation_{timestamp}.log'

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("Playlist Validation Report\n")
            f.write("========================\n\n")

            if broken_links:
                f.write("Broken Symlinks:\n")
                f.write("===============\n")
                for playlist_name, file in sorted(broken_links):
                    f.write(f"• {playlist_name}: {file}\n")
                f.write("\n")

            if missing_playlists:
                f.write("Missing Playlist Folders:\n")
                f.write("========================\n")
                for playlist in sorted(missing_playlists):
                    f.write(f"• {playlist}\n")
                f.write("\n")

            if extra_playlists:
                f.write("Extra Playlist Folders:\n")
                f.write("=====================\n")
                for playlist in sorted(extra_playlists):
                    f.write(f"• {playlist}\n")
                f.write("\n")

            if mismatched_playlists:
                f.write("Mismatched Playlists:\n")
                f.write("===================\n")
                for playlist in mismatched_playlists:
                    f.write(f"\n{playlist['name']}:\n")
                    f.write(f"  Spotify tracks: {playlist['spotify_count']}\n")
                    f.write(f"  Local files: {playlist['local_count']}\n")

                    if playlist['missing_tracks']:
                        f.write("\n  Missing tracks (in Spotify but not local):\n")
                        f.write("  ----------------------------------------\n")
                        for track_id in sorted(playlist['missing_tracks']):
                            try:
                                track = get_track_info(spotify_client, track_id)
                                if track:
                                    f.write(f"  • {track['artists'][0]['name']} - {track['name']}\n")
                                    f.write(f"    Track ID: {track_id}\n\n")
                                else:
                                    f.write(f"  • Track ID: {track_id}\n")
                                    f.write(f"    Error: Could not fetch track info\n\n")
                            except Exception as e:
                                f.write(f"  • Track ID: {track_id}\n")
                                f.write(f"    Error fetching track info: {str(e)}\n\n")

                    if playlist['extra_tracks']:
                        f.write("\n  Extra tracks (local but not in Spotify):\n")
                        f.write("  ------------------------------------\n")
                        for track_id, filename in sorted(playlist['extra_tracks'].items()):
                            f.write(f"  • {filename}\n")
                            f.write(f"    Track ID: {track_id}\n\n")

                    if playlist['files_without_trackid']:
                        f.write("\n  Files without TrackId:\n")
                        f.write("  --------------------\n")
                        for file in sorted(playlist['files_without_trackid']):
                            f.write(f"  • {file}\n")
                        f.write("\n")

        print(f"\nValidation complete! Issues found:")
        if broken_links:
            print(f"- Broken symlinks: {len(broken_links)}")
        print(f"- Missing playlists: {len(missing_playlists)}")
        print(f"- Extra playlists: {len(extra_playlists)}")
        print(f"- Mismatched playlists: {len(mismatched_playlists)}")
        print(f"Report saved to: {log_path}")

    else:
        print("\nValidation complete! All playlist folders match their Spotify playlists.")

    return {
        'missing_playlists': len(missing_playlists),
        'extra_playlists': len(extra_playlists),
        'mismatched_playlists': len(mismatched_playlists),
        'broken_links': len(broken_links)
    }


#  Quick validation of playlist symlinks against Spotify playlists.
#  Only checks TrackIds without fetching additional track details.
def validate_playlist_symlinks_quick(playlists_dir):
    # Create logs directory and subdirectory for playlist validation
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    playlist_validation_dir = logs_dir / 'playlist_validation'
    playlist_validation_dir.mkdir(exist_ok=True)

    print("\nQuick validation of playlist symlinks...")

    # Get Spotify client and fetch all playlists
    spotify_client = authenticate_spotify()

    if not MASTER_PLAYLIST_ID:
        db_logger.error("MASTER_PLAYLIST_ID environment variable not set!")
        return

    spotify_playlists = [
        (name, owner, playlist_id)
        for name, owner, playlist_id in fetch_playlists(spotify_client)
        if playlist_id != MASTER_PLAYLIST_ID
    ]

    # Track validation results
    mismatched_playlists = []
    missing_playlists = []
    extra_playlists = []

    # Get local playlist folders
    local_playlist_folders = {
        d.strip() for d in os.listdir(playlists_dir)
        if os.path.isdir(os.path.join(playlists_dir, d)) and d.upper() != "MASTER"
    }

    # Get Spotify playlist names, excluding MASTER
    spotify_playlist_names = {
        name.strip() for name, _, _ in spotify_playlists
        if name.upper() != "MASTER"
    }

    # Find missing and extra playlists
    missing_playlists = spotify_playlist_names - local_playlist_folders
    extra_playlists = local_playlist_folders - spotify_playlist_names

    # Compare each playlist's contents
    for playlist_name, _, playlist_id in spotify_playlists:
        playlist_name = playlist_name.strip()

        # Skip MASTER playlist
        if playlist_name.upper() == "MASTER":
            continue

        if playlist_name not in local_playlist_folders:
            continue

        local_folder = os.path.join(playlists_dir, playlist_name)

        # Get Spotify track IDs for this playlist
        spotify_track_ids = set(get_playlist_track_ids(spotify_client, playlist_id))

        # Get local track IDs and filenames
        local_track_ids = set()
        missing_trackids = []

        for file in os.listdir(local_folder):
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(local_folder, file)

            if not os.path.exists(os.path.realpath(file_path)):
                missing_trackids.append(file)
                continue

            try:
                tags = ID3(os.path.realpath(file_path))
                if 'TXXX:TRACKID' in tags:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    local_track_ids.add(track_id)
                else:
                    missing_trackids.append(file)
            except Exception as e:
                db_logger.error(f"Error reading TrackId from {file}: {e}")
                missing_trackids.append(file)

        # Compare track sets
        if len(spotify_track_ids) != len(local_track_ids) or spotify_track_ids != local_track_ids or missing_trackids:
            mismatched_playlists.append({
                'name': playlist_name,
                'spotify_count': len(spotify_track_ids),
                'local_count': len(local_track_ids),
                'missing_tracks': spotify_track_ids - local_track_ids,
                'extra_tracks': local_track_ids - spotify_track_ids,
                'files_without_trackid': missing_trackids
            })

    # Generate report
    if mismatched_playlists or missing_playlists or extra_playlists:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = playlist_validation_dir / f'playlist_validation_quick_{timestamp}.log'

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("Quick Playlist Validation Report\n")
            f.write("Quick validation of playlist symlinks against Spotify playlists\n")
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
                    f.write(f"  Spotify tracks: {playlist['spotify_count']}\n")
                    f.write(f"  Local files: {playlist['local_count']}\n")

                    if playlist['missing_tracks']:
                        f.write("\n  Missing track IDs (in Spotify but not local):\n")
                        f.write("  -----------------------------------------\n")
                        for track_id in sorted(playlist['missing_tracks']):
                            f.write(f"  • {track_id}\n")

                    if playlist['extra_tracks']:
                        f.write("\n  Extra track IDs (local but not in Spotify):\n")
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
        print("\nValidation complete! All playlist folders match their Spotify playlists.")

    return {
        'missing_playlists': len(missing_playlists),
        'extra_playlists': len(extra_playlists),
        'mismatched_playlists': len(mismatched_playlists)
    }


# Create directory if it doesn't exist
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        db_logger.info(f"Created directory: {directory}")


def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']
