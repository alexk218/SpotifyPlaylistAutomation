import os
from os import PathLike
from typing import Union
from datetime import datetime
from mutagen.id3 import ID3, ID3NoHeaderError
from drivers.spotify_client import authenticate_spotify, fetch_master_tracks_for_validation
from helpers.file_helper import create_symlink
from sql.helpers.db_helper import fetch_playlists_for_track, fetch_all_playlists_db, fetch_all_tracks
from utils.logger import setup_logger

MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_logger', 'sql/db.log')


# Organizes all songs from 'tracks_master' by creating symlinks in playlist folders based on db association
# ! CREATES FOLDERS WITH SYMLINKS
def organize_songs_into_playlists(
        master_tracks_dir: Union[str, PathLike[str]],
        playlists_dir: Union[str, PathLike[str]],
        dry_run: bool = False,
        interactive: bool = False
) -> None:
    print("Organizing songs into playlist folders with symlinks...")
    db_logger.info("Starting to organize songs into playlists.")

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


def get_file_track_id(file_path):
    """Get TrackId from a file if it exists"""
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            return tags['TXXX:TRACKID'].text[0]
    except Exception as e:
        db_logger.error(f"Error reading TrackId from {file_path}: {e}")
    return None


def validate_master_tracks(master_tracks_dir, validation_logs_dir):
    """
    Validate local tracks against Spotify MASTER playlist with enhanced error reporting
    """
    print("\nValidating master tracks directory...")

    ensure_directory_exists(validation_logs_dir)

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
    validation_path = os.path.join(validation_logs_dir, f'track_validation_{timestamp}.txt')

    with open(validation_path, 'w', encoding='utf-8') as f:
        f.write("Track Validation Report\n")
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

    print(f"\nValidation complete! Report saved to: {validation_path}")

    return {
        'total_spotify_tracks': len(spotify_tracks),
        'total_local_files': total_files,
        'files_with_valid_trackid': len(found_track_ids),
        'files_without_trackid': len(files_without_trackid),
        'unmatched_files': len(unmatched_files),
        'missing_downloads': len(missing_downloads)
    }


def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        db_logger.info(f"Created directory: {directory}")


def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']
