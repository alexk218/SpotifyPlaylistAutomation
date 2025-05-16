import Levenshtein
import logging
import os
import re
from mutagen import File
from mutagen.id3 import ID3, ID3NoHeaderError

from utils.logger import setup_logger

db_logger = setup_logger('db_logger', 'sql', 'track_helper.log')


# Compare with stored data and find new tracks
def find_new_tracks(current_tracks, stored_tracks):
    new_tracks = list(set(current_tracks) - set(stored_tracks))
    db_logger.info(f"New tracks identified: {new_tracks}")
    return new_tracks


# Use fuzzy matching to find the best matching TrackId for a given filename.
def find_track_id_fuzzy(file_name, tracks_db, threshold=0.75, max_matches=8):
    """
    Use fuzzy matching to find the best matching TrackId for a given filename.
    ALWAYS prompts the user for confirmation, never auto-accepts matches.

    Args:
        file_name: Filename to match
        tracks_db: List of Track objects
        threshold: Minimum similarity threshold for initial filtering
        max_matches: Maximum number of matches to show

    Returns:
        Tuple[str, float] or None: (track_id, match_ratio) if match found, None otherwise
    """
    # Extract artist and title, allowing flexible formats
    try:
        name_part = os.path.splitext(file_name)[0]

        # Try standard "Artist - Title" format first
        if " - " in name_part:
            artist, track_title = name_part.split(" - ", 1)
        else:
            # Handle files without the separator by showing them to the user anyway
            artist = ""
            track_title = name_part
            db_logger.info(f"File '{file_name}' doesn't use standard 'Artist - Title' format. Treating as title only.")
            print(f"NOTE: '{file_name}' doesn't follow 'Artist - Title' format. Will try to match by title only.")
    except ValueError:
        db_logger.warning(f"Filename format issue: {file_name}")
        artist = ""
        track_title = name_part

    # Store all matches above threshold
    matches = []

    # Normalize the input filename for comparison
    normalized_artist = artist.lower().replace('&', 'and')
    normalized_title = track_title.lower()

    # Original artist and title for display
    original_artist = artist
    original_title = track_title

    # Handle remix information
    remix_info = ""
    if "remix" in normalized_title.lower():
        remix_parts = normalized_title.lower().split("remix")
        normalized_title = remix_parts[0].strip()
        remix_info = "remix" + remix_parts[1] if len(remix_parts) > 1 else "remix"

    for track in tracks_db:
        # Skip local files in the tracks_db list - we don't want to match to other local files
        if track.is_local:
            continue

        # Always use the new Track domain model properties
        db_artists = track.artists.lower().replace('&', 'and')
        db_title = track.title.lower()
        track_id = track.track_id

        # Split artists into list and normalize each
        db_artist_list = [a.strip() for a in db_artists.split(',')]

        # Handle "and" in artist names by also splitting those
        expanded_artists = []
        for db_artist in db_artist_list:
            if ' and ' in db_artist:
                expanded_artists.extend([a.strip() for a in db_artist.split(' and ')])
            else:
                expanded_artists.append(db_artist)

        # Compare with each possible artist
        artist_ratios = []
        if artist:  # Only do artist matching if we have an artist name
            artist_ratios = [Levenshtein.ratio(normalized_artist, db_artist) for db_artist in expanded_artists]
            artist_ratio = max(artist_ratios) if artist_ratios else 0

            # Add bonus if artist matches exactly
            if any(normalized_artist == db_artist for db_artist in expanded_artists):
                artist_ratio = 1.0  # Perfect match
        else:
            artist_ratio = 0.0  # No artist to match

        # Normalize titles by removing parentheses and dashes
        clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
        db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

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
        title_ratios = [Levenshtein.ratio(clean_normalized_title, var) for var in title_variations]
        title_ratio = max(title_ratios)

        # Add perfect match bonus for exact title match
        if clean_normalized_title in [var.lower() for var in title_variations]:
            title_ratio = 1.0

        # Add bonus for remix/edit matching if present
        remix_bonus = 0
        if any(x in track_title.lower() for x in ['remix', 'edit', 'mix', 'version']) and \
                any(x in db_title for x in ['remix', 'edit', 'mix', 'version']):
            remix_bonus = 0.1

            # Add extra bonus if the same artist is doing the remix
            remix_pattern = r'\(([^)]+)(remix|edit|version|mix)\)'
            local_remix_match = re.search(remix_pattern, track_title.lower())
            db_remix_match = re.search(remix_pattern, db_title.lower())

            if local_remix_match and db_remix_match and local_remix_match.group(1) == db_remix_match.group(1):
                remix_bonus += 0.1

        # Calculate weighted overall ratio - weight artist match higher
        # If no artist (single text field), rely more on title match
        if artist:
            overall_ratio = (artist_ratio * 0.6 + title_ratio * 0.3 + remix_bonus)
        else:
            overall_ratio = (title_ratio * 0.9 + remix_bonus)

        # Include all potential matches
        if overall_ratio >= (threshold - 0.3):  # Lower threshold for collecting potential matches
            matches.append({
                'track_id': track_id,
                'ratio': overall_ratio,
                'artist': track.artists,
                'title': track.title,
                'album': track.album
            })

    # Sort matches by ratio in descending order
    matches.sort(key=lambda x: x['ratio'], reverse=True)

    # Take top matches up to max_matches
    top_matches = matches[:max_matches]

    if not top_matches:
        db_logger.error(f"No matches found for '{file_name}' above minimum threshold")
        return None

    # ALWAYS show matches to the user, regardless of confidence
    print(f"\nPotential matches for: {file_name}")
    print(f"Original: {original_artist + ' - ' if original_artist else ''}{original_title}")
    print("0. Skip this file (no match)")
    for i, match in enumerate(top_matches, 1):
        print(f"{i}. {match['artist']} - {match['title']} ({match['album']})")
        print(f"   Confidence: {match['ratio']:.2f}")

    while True:
        try:
            choice = input(f"Select the correct match (0-{len(top_matches)}): ")
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

    # We should never reach here if the user selected a valid option
    db_logger.error(f"No suitable match selected for '{file_name}'")
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
