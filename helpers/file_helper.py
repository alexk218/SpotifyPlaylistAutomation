import hashlib
import os
import re
import urllib
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import Levenshtein
from mutagen import File
from mutagen.id3 import ID3, TXXX, ID3NoHeaderError

from sql.helpers.db_helper import get_track_added_date
from utils.logger import setup_logger

db_logger = setup_logger('db_logger', 'sql', 'file_helper.log')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


# Sanitize filenames. Replaces invalid characters.
def sanitize_filename(filename):
    return re.sub(r'\s+|[()\\/*?:"<>|]', "", filename)


# Return the normalized filename for a track based on the naming convention.
def get_normalized_filename(track_title, artist):
    sanitized_title = sanitize_filename(track_title)
    sanitized_artist = sanitize_filename(artist)
    return f"{sanitized_artist}{sanitized_title}".lower()


# Check if a track already exists in the download directory using a similarity threshold.
def track_exists(track_title, artist, directory, threshold=0.5):
    normalized_filename = get_normalized_filename(track_title, artist)
    for filename in os.listdir(directory):
        sanitized_filename = sanitize_filename(os.path.splitext(filename)[0]).lower()
        similarity = Levenshtein.ratio(normalized_filename, sanitized_filename)
        if similarity >= threshold:
            return True
    return False


def validate_song_lengths(master_tracks_dir, min_length_minutes=5):
    # Create logs directory and subdirectory
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent

    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    master_validation_dir = logs_dir / 'master_validation'
    master_validation_dir.mkdir(exist_ok=True)

    # Track short songs
    short_songs = []
    total_files = 0
    min_length_seconds = min_length_minutes * 60

    print(f"\nValidating song lengths (minimum {min_length_minutes} minutes)...")

    # Scan all MP3 files
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            file_path = os.path.join(root, file)

            try:
                audio = File(file_path)
                if audio is None:
                    db_logger.error(f"Could not read file: {file}")
                    continue

                length = audio.info.length

                if length < min_length_seconds:
                    track_id = None
                    added_at = None
                    try:
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]
                            added_at = get_track_added_date(track_id)
                    except Exception as e:
                        db_logger.warning(f"Could not read TrackId for {file}: {e}")

                    short_songs.append({
                        'file': file,
                        'length': length,
                        'track_id': track_id,
                        'added_at': added_at
                    })
                    db_logger.info(f"Found short song: {file} ({length:.2f} seconds)")

            except Exception as e:
                db_logger.error(f"Error processing {file}: {e}")

    # Sort short songs by added_at date, putting songs without dates at the end
    short_songs.sort(key=lambda x: (x['added_at'] is None,
                                    datetime.min if x['added_at'] is None else -x['added_at'].timestamp()))

    # Generate report
    if short_songs:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = master_validation_dir / f'song_length_validation_{timestamp}.log'

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("Song Length Validation Report\n")
            f.write("===========================\n\n")

            # Write summary
            f.write("Summary\n")
            f.write("-------\n")
            f.write(f"Minimum length required: {min_length_minutes} minutes\n")
            f.write(f"Total files scanned: {total_files}\n")
            f.write(f"Short songs found: {len(short_songs)}\n")
            songs_with_dates = sum(1 for song in short_songs if song['added_at'])
            f.write(f"Songs with MASTER playlist dates: {songs_with_dates}\n")
            f.write(f"Songs without MASTER playlist dates: {len(short_songs) - songs_with_dates}\n\n")

            f.write("Songs to Replace with Extended Versions:\n")
            f.write("=====================================\n")
            f.write("(Sorted by date added to MASTER playlist)\n\n")

            for song in short_songs:
                minutes = int(song['length'] // 60)
                seconds = int(song['length'] % 60)

                f.write(f"• {song['file']}\n")
                if song['added_at']:
                    f.write(f"  Added to MASTER: {song['added_at'].strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"  Length: {minutes}:{seconds:02d}\n")
                if song['track_id']:
                    f.write(f"  TrackId: {song['track_id']}\n")
                f.write("\n")

        db_logger.info(f"Validation complete. Report saved to: {log_path}")
        print(f"\nValidation complete!")
        print(f"Found {len(short_songs)} songs shorter than {min_length_minutes} minutes")
        print(f"Report saved to: {log_path}")
    else:
        db_logger.info("No short songs found")
        print(f"\nValidation complete! All songs are {min_length_minutes} minutes or longer.")

    return len(short_songs), total_files
