import hashlib
import os
import re
import shutil
import urllib
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import Levenshtein
from mutagen import File
from mutagen.id3 import ID3, TXXX, ID3NoHeaderError

from helpers.track_helper import find_track_id_fuzzy, has_track_id
from sql.helpers.db_helper import fetch_all_tracks_db
from sql.helpers.db_helper import get_track_added_date
from utils.logger import setup_logger

db_logger = setup_logger('db_logger', 'sql/db.log')

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


# Return the expected filename prefix for a track based on the naming convention.
def get_expected_filename_prefix(track_title, artist):
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


# Embed TrackId into MP3 file's metadata - using a TXXX frame
def embed_track_id(file_path, track_id):
    # * file_path (str): Path to the audio file.
    # * track_id (str): The TrackId to embed.
    # * Returns True if successful, False otherwise.
    try:
        try:
            tags = ID3(file_path)
        except ID3NoHeaderError:
            tags = ID3()

        # Remove existing TRACKID if any
        tags.delall('TXXX:TRACKID')

        # Add new TRACKID
        db_logger.info(f"Embedding TrackId '{track_id}' into '{file_path}' (MP3)")
        tags.add(TXXX(encoding=3, desc='TRACKID', text=track_id))
        tags.save(file_path)

        db_logger.info(f"Embedded TrackId '{track_id}' into '{file_path}' (MP3)")
        return True
    except Exception as e:
        db_logger.error(f"Error embedding TrackId into file {file_path}: {e}")
        return False


def embed_track_metadata(master_tracks_dir):
    tracks_db = fetch_all_tracks_db()
    db_logger.debug(f"Fetched all tracks.")

    # Create logs directory and subdirectory
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    master_validation_dir = logs_dir / 'master_validation'
    master_validation_dir.mkdir(exist_ok=True)

    # Initialize tracking lists for logging
    planned_embeds = []
    skipped_already_tagged = []
    skipped_invalid_format = []

    # Files that need processing
    files_to_process = []

    # Initialize counters
    total_files = 0
    already_tagged = 0

    # FIRST PASS: Scan files and collect those that need processing
    print("Scanning files...")
    for root, dirs, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            file_path = os.path.join(root, file)

            # Skip if file already has TrackId
            if has_track_id(file_path):
                already_tagged += 1
                skipped_already_tagged.append(file)
                db_logger.debug(f"Skipping '{file}' (already tagged)")
                continue

            # Add to list of files that need processing
            files_to_process.append((file, file_path))

    print(f"\nFound {total_files} MP3 files")
    print(f"{already_tagged} files already have TrackId")
    print(f"{len(files_to_process)} files need processing")

    if not files_to_process:
        print("No files need processing. All done!")
        return 0, total_files

    # SECOND PASS: Interactive matching and preview
    print("\nStarting interactive matching...")

    for i, (file, file_path) in enumerate(files_to_process, 1):
        print(f"\nProcessing file {i}/{len(files_to_process)}: {file}")

        # Try to find a match
        match_result = find_track_id_fuzzy(file, tracks_db, threshold=0.75)

        if match_result:
            track_id, match_ratio = match_result

            # Get track details for confirmation display
            track_details = None
            for track in tracks_db:
                if track.track_id == track_id:
                    track_details = track
                    break

            if track_details:
                planned_embeds.append({
                    'file': file,
                    'file_path': file_path,
                    'track_id': track_id,
                    'match_ratio': match_ratio,
                    'track_details': track_details
                })

    # THIRD PASS: Show preview and ask for final confirmation
    if planned_embeds:
        print("\n=== PREVIEW OF PLANNED EMBEDDINGS ===")
        print(f"Planning to embed {len(planned_embeds)} files:")

        for i, embed in enumerate(planned_embeds, 1):
            print(f"\n{i}. {embed['file']}")
            print(f"   → {embed['track_details'].artists} - {embed['track_details'].title}")
            print(f"   → {embed['track_details'].album}")
            print(f"   → Track ID: {embed['track_id']}")
            print(f"   → Confidence: {embed['match_ratio']:.2f}")

        # Ask for final confirmation
        confirmation = input("\nProceed with embedding these TrackIds? (y/n): ")
        if confirmation.lower() != 'y':
            print("Embedding cancelled. No changes were made.")
            return 0, total_files
    else:
        print("\nNo matches found for any files.")
        return 0, total_files

    # FOURTH PASS: Actually perform the embedding
    print("\nEmbedding TrackIds...")
    successful_embeds = []
    failed_embeds = []
    successful_count = 0
    failed_count = 0

    for embed in planned_embeds:
        if embed_track_id(embed['file_path'], embed['track_id']):
            successful_count += 1
            successful_embeds.append(embed)
        else:
            failed_count += 1
            failed_embeds.append(embed)

    # Generate detailed log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = master_validation_dir / f'metadata_embedding_{timestamp}.log'

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write("Track Embedding Report\n")
        f.write("=====================\n\n")

        # Write summary statistics
        f.write("Summary Statistics\n")
        f.write("-----------------\n")
        f.write(f"Total MP3 files processed: {total_files}\n")
        f.write(f"Already tagged (skipped): {already_tagged}\n")
        f.write(f"Newly processed: {len(files_to_process)}\n")
        f.write(f"Matches found: {len(planned_embeds)}\n")
        f.write(f"Successfully embedded: {successful_count}\n")
        f.write(f"Failed to embed: {failed_count}\n")

        # Write successful embeddings
        if successful_embeds:
            f.write("\nSuccessful Embeddings:\n")
            f.write("=====================\n")
            for embed in successful_embeds:
                f.write(f"• {embed['file']}\n")
                f.write(f"  Track: {embed['track_details'].artists} - {embed['track_details'].title}\n")
                f.write(f"  Album: {embed['track_details'].album}\n")
                f.write(f"  TrackId: {embed['track_id']}\n")
                f.write(f"  Confidence: {embed['match_ratio']:.2f}\n\n")

        # Write failed embeddings
        if failed_embeds:
            f.write("\nFailed Embeddings:\n")
            f.write("=================\n")
            for embed in failed_embeds:
                f.write(f"• {embed['file']}\n")
                f.write(f"  Track: {embed['track_details'].artists} - {embed['track_details'].title}\n")
                f.write(f"  TrackId: {embed['track_id']}\n")
                f.write(f"  Reason: Failed to write to file\n\n")

        # Write skipped files
        if skipped_already_tagged:
            f.write("\nSkipped (Already Tagged):\n")
            f.write("=======================\n")
            for file in skipped_already_tagged:
                f.write(f"• {file}\n")

    print(f"\nEmbedding complete! {successful_count} successful, {failed_count} failed.")
    print(f"Detailed report saved to: {log_path}")
    return successful_count, total_files


def remove_all_track_ids(master_tracks_dir):
    # Confirmation prompt
    print("\n⚠️  WARNING: This will remove TrackIds from ALL MP3 files in the directory.")
    print("This is a destructive action that cannot be undone!")
    confirmation = input("Are you ABSOLUTELY SURE you want to continue? (type 'YES' to confirm): ")

    if confirmation.strip().upper() != 'YES':
        print("Operation cancelled. No TrackIds were removed.")
        return 0

    removed_count = 0
    total_count = 0

    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, file)
            total_count += 1

            try:
                tags = ID3(file_path)
                # Check if TRACKID exists before trying to remove
                if 'TXXX:TRACKID' in tags:
                    tags.delall('TXXX:TRACKID')
                    tags.save(file_path)
                    removed_count += 1
                    db_logger.info(f"Removed TrackId from: {file}")
            except ID3NoHeaderError:
                db_logger.debug(f"No ID3 tags found in: {file}")
            except Exception as e:
                db_logger.error(f"Error processing {file}: {e}")

    print(f"Processed {total_count} MP3 files")
    print(f"Removed TrackId from {removed_count} files")
    return removed_count


def count_tracks_with_id(master_tracks_dir):
    tracks_with_id = 0
    total_count = 0

    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, file)
            total_count += 1

            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    tracks_with_id += 1
                    track_id = tags['TXXX:TRACKID'].text[0]
                    db_logger.info(f"Found TrackId in {file}: {track_id}")
            except ID3NoHeaderError:
                db_logger.debug(f"No ID3 tags found in: {file}")
            except Exception as e:
                db_logger.error(f"Error processing {file}: {e}")

    print(f"\nResults:")
    print(f"Total MP3 files: {total_count}")
    print(f"Files with TrackId: {tracks_with_id}")
    print(f"Files without TrackId: {total_count - tracks_with_id}")
    print(f"Percentage with TrackId: {(tracks_with_id / total_count * 100):.2f}%")

    return tracks_with_id, total_count


def cleanup_tracks(master_tracks_dir, quarantine_dir):
    # 1. Fetch all tracks from DB and build a set of valid TrackIds
    from sql.helpers.db_helper import get_existing_track_ids
    valid_track_ids = get_existing_track_ids()
    db_logger.info(f"Fetched {len(valid_track_ids)} valid TrackIds from the DB.")

    # 2. Ensure quarantine directory exists
    os.makedirs(quarantine_dir, exist_ok=True)
    db_logger.info(f"Using quarantine directory: {quarantine_dir}")

    # 3. Walk through the master_tracks_dir
    files_examined = 0
    files_moved = 0

    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            # Handle only MP3 for example — adapt if you want other formats
            if not filename.lower().endswith('.mp3'):
                continue

            files_examined += 1
            file_path = os.path.join(root, filename)

            # Extract TrackId from ID3 tags
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' not in tags:
                    # No ID, so not in DB for sure
                    db_logger.warning(f"No TRACKID in {filename}, moving to quarantine.")
                    is_unwanted = True
                else:
                    track_id = tags['TXXX:TRACKID'].text[0]
                    # Check if track_id is in our valid set
                    is_unwanted = (track_id not in valid_track_ids)
                    if is_unwanted:
                        db_logger.info(f"TrackId '{track_id}' in {filename} not found in DB, moving to quarantine.")
            except ID3NoHeaderError:
                db_logger.warning(f"No ID3 header in {filename}, moving to quarantine.")
                is_unwanted = True
            except Exception as e:
                db_logger.error(f"Error reading {filename}: {e}")
                # If we can't read it, treat it as unwanted and move
                is_unwanted = True

            # Move unwanted files to quarantine directory
            if is_unwanted:
                files_moved += 1
                try:
                    # To avoid filename collisions in quarantine, optionally rename files
                    # For simplicity, we'll move them with the same name
                    new_path = os.path.join(quarantine_dir, filename)
                    # If file with same name exists in quarantine, append a number
                    base, extension = os.path.splitext(new_path)
                    counter = 1
                    while os.path.exists(new_path):
                        new_path = f"{base}_{counter}{extension}"
                        counter += 1

                    shutil.move(file_path, new_path)
                    db_logger.info(f"Moved unwanted file to quarantine: {new_path}")
                except Exception as e:
                    db_logger.error(f"Failed to move {file_path} to {quarantine_dir}: {e}")

    db_logger.info(f"\nCleanup Complete! "
                   f"\nFiles Examined: {files_examined} "
                   f"\nFiles Moved to Quarantine: {files_moved}")

    print(f"\nCleanup Complete!")
    print(f"Files Examined: {files_examined}")
    print(f"Files Moved to Quarantine: {files_moved}")


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


# ! LOCAL FILES ON SPOTIFY
def parse_local_file_uri(uri):
    """
    Parse a Spotify local file URI into its components.
    Format: spotify:local:Artist:Album:Track:Duration

    Returns a dictionary with title, artist, etc.
    """
    parts = uri.split(':')
    if len(parts) < 6 or parts[0] != 'spotify' or parts[1] != 'local':
        return None

    return {
        'artist': unquote(parts[2].replace('+', ' ')),
        'album': unquote(parts[3].replace('+', ' ')),
        'title': unquote(parts[4].replace('+', ' ')),
        'duration': parts[5] if len(parts) >= 6 else '0'
    }


def normalize_local_file_uri(uri):
    """
    Parse a Spotify local file URI and extract the components.
    Handles both spotify:local: URIs and https://open.spotify.com/local/ URLs.

    Returns a dictionary with 'artist', 'album', 'title', and 'duration'.
    """
    if not uri:
        return {'artist': '', 'album': '', 'title': '', 'duration': '0'}

    try:
        # Handle Spotify URI format
        if uri.startswith('spotify:local:'):
            parts = uri.split(':')
            # Format: spotify:local:artist:album:title:duration
            if len(parts) >= 6:
                return {
                    'artist': urllib.parse.unquote_plus(parts[2]),
                    'album': urllib.parse.unquote_plus(parts[3]),
                    'title': urllib.parse.unquote_plus(parts[4]),
                    'duration': parts[5]
                }
            else:
                # Handle incomplete URI
                return {
                    'artist': urllib.parse.unquote_plus(parts[2]) if len(parts) > 2 else '',
                    'album': urllib.parse.unquote_plus(parts[3]) if len(parts) > 3 else '',
                    'title': urllib.parse.unquote_plus(parts[4]) if len(parts) > 4 else '',
                    'duration': parts[5] if len(parts) > 5 else '0'
                }

        # Handle web URL format
        elif uri.startswith('https://open.spotify.com/local/'):
            path = uri.replace('https://open.spotify.com/local/', '')
            parts = path.split('/')
            # Format: artist/album/title/duration
            return {
                'artist': urllib.parse.unquote_plus(parts[0]) if len(parts) > 0 else '',
                'album': urllib.parse.unquote_plus(parts[1]) if len(parts) > 1 else '',
                'title': urllib.parse.unquote_plus(parts[2]) if len(parts) > 2 else '',
                'duration': parts[3] if len(parts) > 3 else '0'
            }

        # Handle other formats or return empty
        return {'artist': '', 'album': '', 'title': '', 'duration': '0'}

    except Exception as e:
        db_logger.error(f"Error parsing local file URI '{uri}': {e}")
        return {'artist': '', 'album': '', 'title': '', 'duration': '0'}


def generate_local_track_id(metadata):
    """
    Generate a consistent track ID for local files based on artist and title.

    Args:
        metadata: Dictionary with 'artist' and 'title' or a URI string

    Returns:
        A consistent local track ID
    """
    # Handle both dictionary and URI string inputs
    if isinstance(metadata, str):
        metadata = normalize_local_file_uri(metadata)

    # Extract and normalize title and artist
    title = metadata.get('title', '')
    artist = metadata.get('artist', '')

    # Normalize strings - remove special characters, convert to lowercase
    normalized_title = ''.join(c.lower() for c in title if c.isalnum() or c in ' &-_').strip()
    normalized_artist = ''.join(c.lower() for c in artist if c.isalnum() or c in ' &-_').strip()

    # Create ID string and hash it
    id_string = f"{normalized_artist}_{normalized_title}".lower()
    track_id = f"local_{hashlib.md5(id_string.encode()).hexdigest()[:16]}"

    return track_id
