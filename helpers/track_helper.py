# Compare with stored data and find new tracks
import logging


def find_new_tracks(current_tracks, stored_tracks):
    new_tracks = list(set(current_tracks) - set(stored_tracks))
    logging.info(f"New tracks identified: {new_tracks}")
    return new_tracks
