import logging
import os
import re
import uuid
from datetime import datetime
import Levenshtein
import shutil
from mutagen import File
from mutagen.id3 import ID3, TXXX, ID3NoHeaderError
from helpers.track_helper import find_track_id_fuzzy, has_track_id
from sql.helpers.db_helper import fetch_all_tracks
from utils.logger import setup_logger
from utils.symlink_tracker import tracker

db_logger = setup_logger('db_logger', 'sql/db.log')


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


# Creates symlink pointing to target_path named link_path
def create_symlink(target_path, link_path):
    try:
        if not os.path.exists(link_path):
            os.symlink(target_path, link_path)
            tracker.created_symlinks.append((link_path, target_path))
            db_logger.info(f"Created symlink: {link_path} -> {target_path}")
        else:
            db_logger.info(f"Symlink already exists: {link_path}")
    except OSError as e:
        db_logger.error(f"Failed to create symlink: {link_path} -> {target_path} ({e})")


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


# Embed TrackId into song file metadata. Processes multiple files within master tracks directory
def embed_track_metadata(master_tracks_dir, interactive=False):
    """
    Embed TrackId into song file metadata with improved tracking and statistics.
    Skips files that already have a TrackId embedded.
    """
    tracks_db = fetch_all_tracks()
    db_logger.debug(f"Fetched all tracks.")

    # Initialize counters
    total_files = 0
    successful_embeds = 0
    failed_embeds = 0
    skipped_files = 0
    already_tagged = 0

    # Statistics dictionary
    stats = {
        'exact_matches': 0,
        'fuzzy_matches': 0,
        'failed_matches': 0,
        'invalid_format': 0
    }

    for root, dirs, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            file_path = os.path.join(root, file)

            # Skip if file already has TrackId
            if has_track_id(file_path):
                already_tagged += 1
                print(f"⚡ Skipping '{file}' (already tagged)")
                continue

            try:
                name_part = os.path.splitext(file)[0]
                artist, track_title = name_part.split(' - ', 1)
                db_logger.info(f"Processing: Artist: '{artist}', TrackTitle: '{track_title}'")
            except ValueError:
                db_logger.warning(f"Filename format incorrect: {file_path}")
                stats['invalid_format'] += 1
                skipped_files += 1
                continue

            # Attempt exact matching
            matching_tracks = [
                track for track in tracks_db
                if track.TrackTitle.lower() == track_title.lower() and track.Artists.lower() in artist.lower()
            ]

            if matching_tracks:
                db_logger.info(f"Exact match found for '{file}'")
                track = matching_tracks[0]
                stats['exact_matches'] += 1
                if embed_track_id(file_path, track.TrackId):
                    successful_embeds += 1
                    print(f"✓ Embedded TrackId into '{file}'")
                else:
                    failed_embeds += 1
                    print(f"✗ Failed to embed TrackId into '{file}'")
            else:
                # Attempt fuzzy matching
                db_logger.info(f"No exact match found for '{file}'. Attempting fuzzy matching...")
                track_id = find_track_id_fuzzy(file, tracks_db, threshold=0.6, interactive=interactive)

                if track_id:
                    stats['fuzzy_matches'] += 1
                    if embed_track_id(file_path, track_id):
                        successful_embeds += 1
                        print(f"✓ Embedded TrackId into '{file}' via fuzzy matching")
                    else:
                        failed_embeds += 1
                        print(f"✗ Failed to embed TrackId into '{file}'")
                else:
                    stats['failed_matches'] += 1
                    skipped_files += 1
                    print(f"• Skipped file (no TrackId found): {file}")

    # Print final statistics
    print("\nEmbedding Statistics:")
    print(f"Total MP3 files: {total_files}")
    print(f"Already tagged (skipped): {already_tagged}")
    print(f"Newly processed: {total_files - already_tagged}")
    print(f"Successfully embedded: {successful_embeds}")
    print(f"Failed to embed: {failed_embeds}")
    print(f"Skipped files: {skipped_files}")
    print(f"\nMatching Statistics:")
    print(f"Exact matches: {stats['exact_matches']}")
    print(f"Fuzzy matches: {stats['fuzzy_matches']}")
    print(f"Failed matches: {stats['failed_matches']}")
    print(f"Invalid filename format: {stats['invalid_format']}")

    if total_files == already_tagged:
        print("\nSuccess rate: N/A (all files were already tagged)")
    else:
        print(f"\nSuccess rate: {(successful_embeds / (total_files - already_tagged) * 100):.2f}%")

    # Export unmatched tracks to a file
    unmatched_tracks = []
    for root, dirs, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, file)
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' not in tags:
                    unmatched_tracks.append(file)
            except ID3NoHeaderError:
                unmatched_tracks.append(file)
            except Exception as e:
                db_logger.error(f"Error checking TrackId for {file}: {e}")
                unmatched_tracks.append(file)

    # Export unmatched tracks to a file
    if unmatched_tracks:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = os.path.join(os.path.dirname(master_tracks_dir), f'unmatched_tracks_{timestamp}.txt')
        with open(export_path, 'w', encoding='utf-8') as f:
            f.write("Unmatched tracks:\n")
            for track in unmatched_tracks:
                f.write(f"{track}\n")
        print(f"\nExported list of unmatched tracks to: {export_path}")

    return successful_embeds, total_files


# Remove TrackId from all MP3 files in the directory and its subdirectories.
def remove_all_track_ids(master_tracks_dir):
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


# Count how many MP3 files have a TrackId embedded
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


# Compare files in master_tracks_dir to the 'Tracks' table in the DB.
# Any file whose TrackId is not in the DB is moved (or removed).
def cleanup_tracks(master_tracks_dir, quarantine_dir):
    """
    Compare files in master_tracks_dir to the 'Tracks' table in the DB.
    Any file whose TrackId is not in the DB is moved to the quarantine directory.

    :param master_tracks_dir: Path to tracks_master directory
    :param quarantine_dir: Path to quarantine directory where unwanted files will be moved
    """
    # 1. Fetch all tracks from DB and build a set of valid TrackIds
    db_tracks = fetch_all_tracks()  # Suppose each row has (TrackId, TrackTitle, Artists)
    valid_track_ids = {row.TrackId for row in db_tracks}
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


def validate_song_lengths(master_tracks_dir, validation_logs_dir, min_length_minutes=5):
    """
    Validate song lengths in the master tracks directory.
    Generates a report of songs shorter than the minimum length.

    Args:
        master_tracks_dir (str): Path to master tracks directory
        validation_logs_dir (str): Path to save validation logs
        min_length_minutes (int): Minimum song length in minutes
    """
    # Ensure log directory exists
    if not os.path.exists(validation_logs_dir):
        os.makedirs(validation_logs_dir)

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

                # Get length in seconds
                length = audio.info.length

                if length < min_length_seconds:
                    # Try to get TrackId if available
                    track_id = None
                    try:
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]
                    except:
                        pass

                    short_songs.append({
                        'file': file,
                        'length': length,
                        'track_id': track_id
                    })

            except Exception as e:
                db_logger.error(f"Error processing {file}: {e}")

    # Generate report if short songs found
    if short_songs:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(validation_logs_dir, f'short_songs_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("Short Songs Report\n")
            f.write("================\n\n")

            f.write(f"Minimum length: {min_length_minutes} minutes\n")
            f.write(f"Total files scanned: {total_files}\n")
            f.write(f"Short songs found: {len(short_songs)}\n\n")

            f.write("Songs to Replace with Extended Versions:\n")
            f.write("=====================================\n\n")

            # Sort by length
            short_songs.sort(key=lambda x: x['length'])

            for song in short_songs:
                minutes = int(song['length'] // 60)
                seconds = int(song['length'] % 60)
                f.write(f"• {song['file']}\n")
                f.write(f"  Length: {minutes}:{seconds:02d}\n")
                if song['track_id']:
                    f.write(f"  TrackId: {song['track_id']}\n")
                f.write("\n")

        print(f"\nValidation complete!")
        print(f"Found {len(short_songs)} songs shorter than {min_length_minutes} minutes")
        print(f"Report saved to: {report_path}")

    else:
        print(f"\nValidation complete! All songs are {min_length_minutes} minutes or longer.")

    return len(short_songs), total_files
