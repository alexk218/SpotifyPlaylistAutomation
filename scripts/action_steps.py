import logging
import os
from typing import List, Tuple

# Fetch tracks from a playlist
# def fetch_playlist_tracks(spotify_client, playlist_id):
#     logging.info(f"Fetching tracks for playlist ID {playlist_id}")
#     tracks = spotify_client.playlist_tracks(playlist_id)
#     return [(" - ".join([track['track']['name'], ", ".join([artist['name'] for artist in track['track']['artists']])]))
#             for track in tracks['items']]

def fetch_playlist_tracks(spotify_client, playlist_id: str) -> List[Tuple[str, str, str]]:
    logging.info(f"Fetching tracks for playlist: {playlist_id}")

    tracks = []
    offset = 0
    limit = 100  # Spotify API limit per request

    while True:
        response = spotify_client.playlist_tracks(playlist_id, limit=limit, offset=offset)
        if not response['items']:
            break

        for item in response['items']:
            track = item['track']
            track_name = track['name']
            artist_name = track['artists'][0]['name']
            album_name = track['album']['name']
            tracks.append((track_name, artist_name, album_name))

        offset += limit

    return tracks

def load_stored_tracks(playlist_name):
    logging.info(f"Loading stored tracks for {playlist_name}")
    try:
        with open(f"{playlist_name}_tracks.txt", "r", encoding='utf-8') as file:
            stored_tracks = [tuple(line.strip().split(' - ')) for line in file]
        return stored_tracks
    except FileNotFoundError:
        return []
    except UnicodeDecodeError:
        logging.error(f"Error decoding file for {playlist_name}")
        return []


def store_tracks(playlist_name, tracks):
    logging.info(f"Storing tracks for {playlist_name}")
    with open(f"{playlist_name}_tracks.txt", "w", encoding='utf-8') as file:
        for track in tracks:
            file.write(f"{track[0]} - {track[1]}\n")


def store_playlist_song_count(playlist_name, song_count):
    directory = "txtfiles"
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, f"{playlist_name}_count.txt")

    with open(file_path, "w", encoding="utf-8") as file:
        file.write("Song count: " + str(song_count))


def load_stored_playlist_song_count(playlist_name):
    directory = "txtfiles"
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, f"{playlist_name}_count.txt")

    try:
        with open(file_path, "r", encoding='utf-8') as file:
            return int(file.read().strip())
    except (FileNotFoundError, ValueError):
        return 0
