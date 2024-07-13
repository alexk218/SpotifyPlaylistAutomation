# Setup logging
import logging
import os
import smtplib
import spotipy
from spotipy import SpotifyOAuth

logging.basicConfig(filename='spotify_script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')


# Authentication with Spotify
def authenticate_spotify():
    logging.info("Authenticating with Spotify")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID,
                                                   client_secret=SPOTIFY_CLIENT_SECRET,
                                                   redirect_uri="http://localhost:8888/callback",
                                                   scope="playlist-read-private user-library-read"))
    return sp


# Fetch all private playlists
# Returns a list of tuples containing playlist's name and its unique ID
def fetch_my_playlists(spotify_client):
    logging.info("Fetching all my playlists")
    user_id = spotify_client.current_user()['id']
    playlists = spotify_client.current_user_playlists()

    my_playlists = [
        (playlist['name'], playlist['id'])
        for playlist in playlists['items']
        if playlist['owner']['id'] == user_id
    ]
    return [(playlist['name'], playlist['id']) for playlist in playlists['items']]


# Fetches Liked Songs
def fetch_liked_songs(spotify_client):
    logging.info("Fetching Liked Songs")
    results = spotify_client.current_user_saved_tracks()
    return [(results['track']['name'], item['track']['id']) for item in results['items']]


# Fetch tracks from a playlist
def fetch_playlist_tracks(spotify_client, playlist_id):
    logging.info(f"Fetching tracks for playlist ID {playlist_id}")
    tracks = spotify_client.playlist_tracks(playlist_id)
    return [(" - ".join([track['track']['name'], ", ".join([artist['name'] for artist in track['track']['artists']])]))
            for track in tracks['items']]


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

