import Levenshtein
import logging
import os
from mutagen import File
from utils.logger import setup_logger

db_logger = setup_logger('db_logger', 'sql/db.log')

# Compare with stored data and find new tracks
def find_new_tracks(current_tracks, stored_tracks):
    new_tracks = list(set(current_tracks) - set(stored_tracks))
    db_logger.info(f"New tracks identified: {new_tracks}")
    return new_tracks

# Use fuzzy matching to find the best matching TrackId for a given filename.
def find_track_id_fuzzy(file_name, tracks_db, threshold=0.6, interactive=False):
    # * If interactive is True, prompt the user for low-confidence matches
    # * Change threshold to change sensitivity of matching
    # * Returns str or None: The matched TrackId or None if no suitable match found
    # Extract Artist and TrackTitle from filename
    try:
        name_part = os.path.splitext(file_name)[0]
        artist, track_title = name_part.split(' - ', 1)
    except ValueError:
        db_logger.warning(f"Filename format incorrect: {file_name}")
        return None

    # Iterate through the db to find the best match
    best_match = None
    highest_ratio = 0

    for track in tracks_db:
        db_artist = track.Artists.lower()
        db_title = track.TrackTitle.lower()

        # Compute similarity ratios
        artist_ratio = Levenshtein.ratio(artist.lower(), db_artist)
        title_ratio = Levenshtein.ratio(track_title.lower(), db_title)

        overall_ratio = (artist_ratio + title_ratio) / 2

        if overall_ratio > highest_ratio:
            highest_ratio = overall_ratio
            best_match = track.TrackId

    if highest_ratio >= threshold:
        db_logger.info(f"Fuzzy matched '{file_name}' to TrackId '{best_match}' with ratio {highest_ratio}")
        if highest_ratio < 0.75:
            db_logger.warning(f"Low-confidence match for '{file_name}' with TrackId '{best_match}' "
                              f"(Ratio: {highest_ratio})")
        return best_match
    elif highest_ratio >= (threshold - 0.2) and interactive:
        # Prompt user for confirmation if interactive mode is enabled
        user_input = input(
            f"Low-confidence match for '{file_name}': TrackId '{best_match}' with similarity {highest_ratio:.2f}. "
            f"Accept? (y/n): ")
        if user_input.lower() == 'y':
            db_logger.info(f"User accepted fuzzy match for '{file_name}' with TrackId '{best_match}'")
            return best_match
        else:
            db_logger.warning(f"User rejected fuzzy match for '{file_name}'.")
            return None
    else:
        db_logger.warning(f"No suitable fuzzy match found for '{file_name}' (Highest ratio: {highest_ratio})")
        return None


# Retrieves TrackId and Source from a song file's metadata
def extract_track_id_from_metadata(file_path):
    try:
        audio = File(file_path, easy=True)
        if audio is None:
            logging.warning(f"No metadata found for file: {file_path}")
            return None, None
        track_ids = audio.get('trackid', [])
        if track_ids:
            return track_ids[0]
        else:
            logging.warning(f"No TrackId found in metadata for file: {file_path}")
            return None, None
    except Exception as e:
        logging.error(f"Error reading metadata from file {file_path}: {e}")
        return None, None
