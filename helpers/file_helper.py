import re
import os
import Levenshtein

import re


def sanitize_filename(filename):
    """
    Sanitize a string to be used as a valid filename.
    """
    return re.sub(r'\s+|[()\\/*?:"<>|]', "", filename)


def get_normalized_filename(track_title, artist):
    """
    Return the normalized filename for a track based on the naming convention.
    """
    sanitized_title = sanitize_filename(track_title)
    sanitized_artist = sanitize_filename(artist)
    return f"{sanitized_artist}{sanitized_title}".lower()

def get_expected_filename_prefix(track_title, artist):
    """
    Return the expected filename prefix for a track based on the naming convention.
    """
    sanitized_title = sanitize_filename(track_title)
    sanitized_artist = sanitize_filename(artist)
    return f"{sanitized_artist}{sanitized_title}".lower()


def track_exists(track_title, artist, directory, threshold=0.5):
    """
    Check if a track already exists in the download directory using a similarity threshold.
    """
    normalized_filename = get_normalized_filename(track_title, artist)
    for filename in os.listdir(directory):
        sanitized_filename = sanitize_filename(os.path.splitext(filename)[0]).lower()
        similarity = Levenshtein.ratio(normalized_filename, sanitized_filename)
        if similarity >= threshold:
            return True
    return False
