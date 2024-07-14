import re
import os

def sanitize_filename(filename):
    """
    Sanitize a string to be used as a valid filename.
    """
    return re.sub(r'[\\/*?:"<>|]', "", filename)


def get_expected_filename(track_title, artist):
    """
    Return the expected filename for a track based on the naming convention.
    """
    sanitized_title = sanitize_filename(track_title)
    sanitized_artist = sanitize_filename(artist)
    return f"{sanitized_title} - {sanitized_artist}.mp3"


def track_exists(track_title, artist, directory):
    """
    Check if a track already exists in the download directory.
    """
    filename = get_expected_filename(track_title, artist)
    file_path = os.path.join(directory, filename)
    return os.path.isfile(file_path)
