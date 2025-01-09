import logging
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Union, Tuple, List
import Levenshtein
import shutil
from mutagen import File
from mutagen.id3 import ID3, TXXX, ID3NoHeaderError
from helpers.track_helper import find_track_id_fuzzy, has_track_id
from sql.helpers.db_helper import fetch_all_tracks_db, get_track_added_date
from utils.logger import setup_logger
from utils.symlink_tracker import tracker

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


# Scans through all playlist folders and removes broken symlinks.
# * Broken symlinks occur when you delete a track from the master tracks playlist
def cleanup_broken_symlinks(playlists_dir: Union[str, os.PathLike[str]], dry_run: bool = False) -> List[Tuple[str, str]]:
    # Returns: List of tuples containing (playlist_name, filename) of removed symlinks
    removed_links = []

    print("\nChecking for broken symlinks...")
    db_logger.info("Starting broken symlink cleanup")

    try:
        # Iterate through all playlist folders
        for playlist_name in os.listdir(playlists_dir):
            playlist_path = os.path.join(playlists_dir, playlist_name)

            if not os.path.isdir(playlist_path) or playlist_name.upper() == "MASTER":
                continue

            # Check each file in the playlist folder
            for filename in os.listdir(playlist_path):
                if not filename.lower().endswith('.mp3'):
                    continue

                file_path = os.path.join(playlist_path, filename)

                # Check if it's a symlink and if it's broken
                if os.path.islink(file_path):
                    target_path = os.path.realpath(file_path)
                    if not os.path.exists(target_path):
                        if dry_run:
                            db_logger.info(f"[DRY RUN] Would remove broken symlink: {file_path}")
                        else:
                            try:
                                os.remove(file_path)
                                db_logger.info(f"Removed broken symlink: {file_path}")
                                removed_links.append((playlist_name, filename))
                                tracker.removed_symlinks.append(file_path)
                            except Exception as e:
                                db_logger.error(f"Error removing broken symlink {file_path}: {e}")

    except Exception as e:
        db_logger.error(f"Error during symlink cleanup: {e}")

    if removed_links:
        print(f"Removed {len(removed_links)} broken symlinks")
    else:
        print("No broken symlinks found")

    return removed_links


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


# Embed TrackId into song file metadata. Processes all files within master tracks directory
# Skips files that already have a TrackId embedded.
def embed_track_metadata(master_tracks_dir, interactive=False):
    tracks_db = fetch_all_tracks_db()
    db_logger.debug(f"Fetched all tracks.")

    # Create logs directory and subdirectory
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    master_validation_dir = logs_dir / 'master_validation'
    master_validation_dir.mkdir(exist_ok=True)

    # Initialize tracking lists for logging
    successful_embeds = []
    failed_embeds = []
    skipped_already_tagged = []
    skipped_invalid_format = []
    fuzzy_matches = []

    # Initialize counters
    total_files = 0
    successful_count = 0
    failed_count = 0
    skipped_count = 0
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
                skipped_already_tagged.append(file)
                db_logger.debug(f"Skipping '{file}' (already tagged)")
                continue

            try:
                name_part = os.path.splitext(file)[0]
                artist, track_title = name_part.split(' - ', 1)
                db_logger.info(f"Processing: Artist: '{artist}', TrackTitle: '{track_title}'")
            except ValueError:
                db_logger.warning(f"Filename format incorrect: {file_path}")
                stats['invalid_format'] += 1
                skipped_count += 1
                skipped_invalid_format.append(file)
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
                    successful_count += 1
                    successful_embeds.append({
                        'file': file,
                        'artist': artist,
                        'title': track_title,
                        'track_id': track.TrackId,
                        'match_type': 'exact'
                    })
                else:
                    failed_count += 1
                    failed_embeds.append({
                        'file': file,
                        'reason': 'Failed to embed TrackId',
                        'track_id': track.TrackId
                    })
            else:
                # Attempt fuzzy matching
                db_logger.info(f"No exact match found for '{file}'. Attempting fuzzy matching...")
                match_result = find_track_id_fuzzy(file, tracks_db, threshold=0.6, interactive=interactive)

                if match_result:
                    track_id, match_ratio = match_result
                    stats['fuzzy_matches'] += 1
                    if embed_track_id(file_path, track_id):
                        successful_count += 1
                        fuzzy_matches.append({
                            'file': file,
                            'artist': artist,
                            'title': track_title,
                            'track_id': track_id,
                            'match_type': 'fuzzy',
                            'ratio': match_ratio
                        })
                    else:
                        failed_count += 1
                        failed_embeds.append({
                            'file': file,
                            'reason': 'Failed to embed TrackId (fuzzy match)',
                            'track_id': track_id
                        })
                else:
                    stats['failed_matches'] += 1
                    skipped_count += 1
                    failed_embeds.append({
                        'file': file,
                        'reason': 'No matching TrackId found'
                    })

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
        f.write(f"Newly processed: {total_files - already_tagged}\n")
        f.write(f"Successfully embedded: {successful_count}\n")
        f.write(f"Failed to embed: {failed_count}\n")
        f.write(f"Skipped files: {skipped_count}\n")
        f.write("\nMatching Statistics\n")
        f.write(f"Exact matches: {stats['exact_matches']}\n")
        f.write(f"Fuzzy matches: {stats['fuzzy_matches']}\n")
        f.write(f"Failed matches: {stats['failed_matches']}\n")
        f.write(f"Invalid filename format: {stats['invalid_format']}\n")

        if total_files != already_tagged:
            f.write(f"\nSuccess rate: {(successful_count / (total_files - already_tagged) * 100):.2f}%\n")

        # Write successful embeddings
        if successful_embeds or fuzzy_matches:
            f.write("\nSuccessful Embeddings:\n")
            f.write("=====================\n")
            for embed in successful_embeds + fuzzy_matches:
                f.write(f"• {embed['file']}\n")
                f.write(f"  Artist: {embed['artist']}\n")
                f.write(f"  Title: {embed['title']}\n")
                f.write(f"  TrackId: {embed['track_id']}\n")
                f.write(f"  Match Type: {embed['match_type']}")
                if embed['match_type'] == 'fuzzy':
                    f.write(f" (ratio: {embed['ratio']:.2f})")
                f.write("\n\n")

        # Write failed embeddings
        if failed_embeds:
            f.write("\nFailed Embeddings:\n")
            f.write("=================\n")
            for fail in failed_embeds:
                f.write(f"• {fail['file']}\n")
                f.write(f"  Reason: {fail['reason']}\n")
                if 'track_id' in fail:
                    f.write(f"  TrackId: {fail['track_id']}\n")
                f.write("\n")

        # Write skipped files
        if skipped_already_tagged:
            f.write("\nSkipped (Already Tagged):\n")
            f.write("=======================\n")
            for file in skipped_already_tagged:
                f.write(f"• {file}\n")

        if skipped_invalid_format:
            f.write("\nSkipped (Invalid Format):\n")
            f.write("=======================\n")
            for file in skipped_invalid_format:
                f.write(f"• {file}\n")

    print(f"\nEmbedding complete! Detailed report saved to: {log_path}")
    return successful_count, total_files


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
# Any file whose TrackId is not in the DB is moved to quarantine directory.
def cleanup_tracks(master_tracks_dir, quarantine_dir):
    # 1. Fetch all tracks from DB and build a set of valid TrackIds
    db_tracks = fetch_all_tracks_db()
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


# Generates report of songs shorter than minimum length
def validate_song_lengths(master_tracks_dir, min_length_minutes=5):
    # Create logs directory and subdirectory
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
