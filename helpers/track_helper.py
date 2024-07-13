import logging


def format_track(track):
    track_title = track[0]
    artist = track[1]
    return f"{track_title} {artist}"

# Compare with stored data and find new tracks
def find_new_tracks(current_tracks, stored_tracks):
    new_tracks = list(set(current_tracks) - set(stored_tracks))
    logging.info(f"New tracks identified: {new_tracks}")
    return new_tracks
