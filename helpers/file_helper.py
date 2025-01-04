import logging
import os
import Levenshtein
import re
import uuid
from mutagen import File
from helpers.track_helper import find_track_id_fuzzy
from sql.helpers.db_helper import fetch_all_tracks
from utils.logger import setup_logger
import logging
import os
import uuid
from mutagen import File
from mutagen.id3 import ID3, TXXX, ID3NoHeaderError
# from mutagen.flac import FLAC, FLACNoHeaderError
from mutagen.easymp4 import EasyMP4
from mutagen.wavpack import WavPack
from utils.logger import setup_logger

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
            logging.info(f"Created symlink: {link_path} -> {target_path}")
        else:
            logging.info(f"Symlink already exists: {link_path}")
    except OSError as e:
        logging.error(f"Failed to create symlink: {link_path} -> {target_path} ({e})")


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


# Embed TrackId into song file metadata. Processes multiple files within specified directory (K:\\tracks_master).
def embed_track_metadata(master_tracks_dir):
    tracks_db = fetch_all_tracks()
    db_logger.debug(f"Fetched all tracks.")

    # 'walks' through all files starting from K:\\tracks_master. Loops through ALL tracks.
    for root, dirs, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue  # Skip non-MP3 files

            file_path = os.path.join(root, file)
            try:
                # Extract Artist and TrackTitle from filename
                # splitext: splits filename into name and extension. "artist - title.mp3" -> ["artist - title", ".mp3"]
                # so first element ([0]) is just "artist - title"
                name_part = os.path.splitext(file)[0]
                artist, track_title = name_part.split(' - ', 1)
                db_logger.info(f"Extracted Artist: '{artist}', TrackTitle: '{track_title}'")
            except ValueError:
                db_logger.warning(f"Filename format incorrect: {file_path}")
                continue  # Skip files that don't follow the naming convention

            # Attempt exact matching. Looks through ALL tracks in db and creates a list of all matching tracks.
            matching_tracks = [
                track for track in tracks_db
                if track.TrackTitle.lower() == track_title.lower() and track.Artists.lower() in artist.lower()
            ]

            if matching_tracks:
                db_logger.info(f"Exact match found for '{file}'")
                track = matching_tracks[0]
                db_logger.info(f"Embedding TrackId '{track.TrackId}' into '{file}'")
                embed_success = embed_track_id(file_path, track.TrackId)
                if embed_success:
                    print(f"Embedded TrackId into '{file}'")
            else:
                # Attempt fuzzy matching
                db_logger.info(f"No exact match found for '{file}'. Attempting fuzzy matching...")
                track_id = find_track_id_fuzzy(file, tracks_db, threshold=0.8, interactive=True)
                if track_id:
                    embed_success = embed_track_id(file_path, track_id)
                    if embed_success:
                        print(f"Embedded TrackId into '{file}' via fuzzy matching")
                else:
                    # Handle tracks without TrackId (external tracks)
                    unique_id = assign_unique_id()
                    embed_success = embed_track_id(file_path, unique_id)
                    if embed_success:
                        print(f"Embedded unique ID into '{file}' as external track")


# For tracks without a TrackId - generate a UUID
def assign_unique_id(source_id=None):
    if source_id:
        return source_id
    else:
        return str(uuid.uuid4())
