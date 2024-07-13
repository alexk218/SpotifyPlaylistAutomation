import logging
import os

import spotipy
from dotenv import load_dotenv
from spotipy import SpotifyOAuth

# Load environment variables
# SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
# SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
# SENDER_EMAIL = os.getenv('SENDER_EMAIL')
# EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')


logging.basicConfig(filename='spotify_script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Authentication with Spotify
def authenticate_spotify():
    SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
    SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

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
    playlists = spotify_client.current_user_playlists(50)

    my_playlists = [
        (playlist['name'], playlist['id'])
        for playlist in playlists['items']
        if playlist['owner']['id'] == user_id and playlist['name'] not in ["Discover Weekly", "Release Radar"]
    ]

    return my_playlists

# Fetch Liked Songs
def fetch_liked_songs(spotify_client):
    logging.info("Fetching Liked Songs")
    results = spotify_client.current_user_saved_tracks()
    return [(results['track']['name'], item['track']['id']) for item in results['items']]