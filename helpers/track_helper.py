import Levenshtein
import logging
import os
import re
from mutagen import File
from mutagen.id3 import ID3, ID3NoHeaderError

from utils.logger import setup_logger

db_logger = setup_logger('db_logger', 'sql/db.log')


# Compare with stored data and find new tracks
def find_new_tracks(current_tracks, stored_tracks):
    new_tracks = list(set(current_tracks) - set(stored_tracks))
    db_logger.info(f"New tracks identified: {new_tracks}")
    return new_tracks


# Use fuzzy matching to find the best matching TrackId for a given filename.
def find_track_id_fuzzy(file_name, tracks_db, threshold=0.6, interactive=False, max_matches=5):
    # * Returns:  Tuple[str, float] or None: (track_id, match_ratio) if match found, None if no suitable match found
    try:
        name_part = os.path.splitext(file_name)[0]
        artist, track_title = name_part.split(' - ', 1)
    except ValueError:
        db_logger.warning(f"Filename format incorrect: {file_name}")
        return None

    # Store all matches above threshold
    matches = []

    # Normalize the input filename for comparison
    normalized_artist = artist.lower().replace('&', 'and')
    normalized_title = track_title.lower()

    # Handle remix information
    remix_info = ""
    if "remix" in normalized_title.lower():
        remix_parts = normalized_title.lower().split("remix")
        normalized_title = remix_parts[0].strip()
        remix_info = "remix" + remix_parts[1] if len(remix_parts) > 1 else "remix"

    for track in tracks_db:
        # Always use the new Track domain model properties
        db_artists = track.artists.lower().replace('&', 'and')
        db_title = track.title.lower()
        track_id = track.track_id

        # Split artists into list and normalize each
        db_artist_list = [a.strip() for a in db_artists.split(',')]

        # Handle "and" in artist names by also splitting those
        expanded_artists = []
        for artist in db_artist_list:
            if ' and ' in artist:
                expanded_artists.extend([a.strip() for a in artist.split(' and ')])
            else:
                expanded_artists.append(artist)

        # Compare with each possible artist
        artist_ratios = [Levenshtein.ratio(normalized_artist, db_artist) for db_artist in expanded_artists]
        artist_ratio = max(artist_ratios) if artist_ratios else 0

        # Add bonus if artist appears anywhere in the list
        if any(normalized_artist in db_artist for db_artist in expanded_artists):
            artist_ratio += 0.1

        # Normalize titles by removing parentheses and dashes
        normalized_title = re.sub(r'[(\[].*?[)\]]', '', normalized_title).strip()  # Remove parenthetical content
        db_title_clean = re.sub(r'[(\[].*?[)\]]', '', db_title).strip()

        # Handle special title cases (remixes, edits, etc.)
        title_variations = []
        # Original title
        title_variations.append(db_title)
        # Clean title without parentheses
        title_variations.append(db_title_clean)
        # Title with standardized format
        standardized_db_title = db_title.replace(' - ', ' ').replace("'s", "s")
        title_variations.append(standardized_db_title)

        # Calculate best title match from variations
        title_ratios = [Levenshtein.ratio(normalized_title, var) for var in title_variations]
        title_ratio = max(title_ratios)

        # Add bonus for remix/edit matching if present
        remix_bonus = 0
        if any(x in track_title.lower() for x in ['remix', 'edit', 'mix']) and \
                any(x in db_title for x in ['remix', 'edit', 'mix']):
            remix_bonus = 0.2

        # Calculate weighted overall ratio
        overall_ratio = (artist_ratio * 0.4 + title_ratio * 0.4 + remix_bonus)

        if overall_ratio >= (threshold - 0.2):  # Lower threshold for collecting potential matches
            matches.append({
                'track_id': track_id,
                'ratio': overall_ratio,
                'artist': track.artists,
                'title': track.title
            })

    # Sort matches by ratio in descending order
    matches.sort(key=lambda x: x['ratio'], reverse=True)

    # Take top matches up to max_matches
    top_matches = matches[:max_matches]

    if not top_matches:
        db_logger.error(f"No matches found for '{file_name}' above minimum threshold")
        return None

    # If we have a high confidence match (above threshold), return it without prompting
    if top_matches[0]['ratio'] >= threshold:
        db_logger.info(f"Fuzzy matched '{file_name}' to TrackId '{top_matches[0]['track_id']}' "
                       f"with ratio {top_matches[0]['ratio']}")
        return top_matches[0]['track_id'], top_matches[0]['ratio']

    # Only prompt user in interactive mode for low confidence matches
    if interactive and top_matches[0]['ratio'] >= (threshold - 0.2):
        print(f"\nPotential matches for: {file_name}")
        print("0. Skip this file")
        for i, match in enumerate(top_matches, 1):
            print(f"{i}. {match['artist']} - {match['title']} (similarity: {match['ratio']:.2f})")

        while True:
            try:
                choice = input("Select the correct match (0-{}): ".format(len(top_matches)))
                choice = int(choice)
                if choice == 0:
                    db_logger.warning(f"User skipped matching for '{file_name}'")
                    return None
                if 1 <= choice <= len(top_matches):
                    selected_match = top_matches[choice - 1]
                    db_logger.info(f"User selected match for '{file_name}': "
                                   f"{selected_match['artist']} - {selected_match['title']}")
                    return selected_match['track_id'], selected_match['ratio']
                print("Invalid choice. Please try again.")
            except ValueError:
                print("Please enter a valid number.")

    # If not interactive and no good matches, return None
    db_logger.error(f"No suitable fuzzy match found for '{file_name}' "
                    f"(Best ratio: {top_matches[0]['ratio'] if top_matches else 0})")
    return None


# Check if file already has a TrackId embedded
def has_track_id(file_path):
    try:
        tags = ID3(file_path)
        return 'TXXX:TRACKID' in tags
    except ID3NoHeaderError:
        return False
    except Exception as e:
        db_logger.error(f"Error checking TrackId for {file_path}: {e}")
        return False


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