import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any

from mutagen.id3 import ID3

from api.constants.file_extensions import SUPPORTED_AUDIO_EXTENSIONS
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

validation_logger = setup_logger('validation_helper', 'sql', 'validation_helper.log')
db_logger = setup_logger('db_logger', 'sql', 'db_validation_helper.log')

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
    with UnitOfWork() as uow:
        master_tracks = uow.track_repository.get_all_as_dict_list()

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
            'missing_downloads': 0,
            'wav_files': 0,
            'aiff_files': 0,
            'mp3_files': 0
        }

    # Extract track IDs from master tracks for quick lookup
    track_ids = {track['id'] for track in master_tracks}
    validation_logger.info(f"Validating against {len(track_ids)} track IDs")

    # Initialize tracking
    found_track_ids = set()
    missing_downloads = []
    unmatched_files = []
    files_without_trackid = []

    # Track files by extension type
    files_by_extension = {ext: 0 for ext in SUPPORTED_AUDIO_EXTENSIONS}

    # Scan local files
    total_files = 0
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            file_ext = os.path.splitext(file.lower())[1]
            if file_ext not in SUPPORTED_AUDIO_EXTENSIONS:
                continue

            total_files += 1
            files_by_extension[file_ext] += 1
            file_path = os.path.join(root, file)

            # For MP3 files, check for embedded TrackId
            if file_ext == '.mp3':
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
            else:
                # For WAV/AIFF files, we can't embed TrackId but we'll count them
                # They're handled separately in the m3u playlist generation
                validation_logger.info(f"Found {file_ext} file: {file}")
                # We don't add them to files_without_trackid since that's for MP3s that should have TrackId

    # Find missing tracks - tracks in database but not locally
    missing_track_ids = track_ids - found_track_ids
    for track in master_tracks:
        if track['id'] in missing_track_ids:
            # Try to find a file that matches the name pattern
            expected_filename = f"{track['artists'].split(',')[0]} - {track['name']}"
            file_exists = False
            actual_track_id = None

            # Look for the file with any of the supported extensions
            for ext in SUPPORTED_AUDIO_EXTENSIONS:
                expected_filename_with_ext = expected_filename + ext
                for root, _, files in os.walk(master_tracks_dir):
                    for file in files:
                        if file.lower() == expected_filename_with_ext.lower():
                            file_exists = True
                            file_path = os.path.join(root, file)
                            # Only try to extract TrackId from MP3 files
                            if ext == '.mp3':
                                actual_track_id = get_file_track_id(file_path)
                            break
                    if file_exists:
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
        for ext, count in files_by_extension.items():
            f.write(f"  {ext} files: {count}\n")
        f.write(f"Files with valid TrackId: {len(found_track_ids)}\n")
        f.write(f"MP3 files without TrackId: {len(files_without_trackid)}\n")
        f.write(f"Files with unmatched TrackId: {len(unmatched_files)}\n")
        f.write(f"Missing downloads: {len(missing_downloads)}\n\n")

        if missing_downloads:
            f.write("\nMissing Downloads (Sorted by Spotify Addition Date):\n")
            f.write("============================================\n")
            for track in missing_downloads:
                # Handle both datetime objects and string dates
                if track['added_at']:
                    if isinstance(track['added_at'], str):
                        try:
                            # Parse the string datetime
                            parsed_date = datetime.fromisoformat(track['added_at'].replace('Z', '+00:00'))
                            date_str = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                        except (ValueError, TypeError):
                            date_str = str(track['added_at'])  # Fallback to string representation
                    else:
                        # It's already a datetime object
                        date_str = track['added_at'].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    date_str = "Unknown date"

                f.write(f"• {track['artist']} - {track['title']}\n")
                f.write(f"  Added to Spotify: {date_str}\n")
                f.write(f"  Expected TrackId: {track['track_id']}\n")

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

    # Print summary of files by extension
    print(f"\nFiles by extension:")
    for ext, count in files_by_extension.items():
        print(f"  {ext} files: {count}")

    return {
        'total_spotify_tracks': len(master_tracks),
        'total_local_files': total_files,
        'files_with_valid_trackid': len(found_track_ids),
        'files_without_trackid': len(files_without_trackid),
        'unmatched_files': len(unmatched_files),
        'missing_downloads': len(missing_downloads),
        'mp3_files': files_by_extension['.mp3'],
        'wav_files': files_by_extension['.wav'],
        'aiff_files': files_by_extension['.aiff']
    }
