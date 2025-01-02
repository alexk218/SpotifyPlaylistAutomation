import html
import logging
import os
import json
import re
import spotipy
from spotipy import SpotifyOAuth
from pathlib import Path
from typing import List, Tuple

# Get the path to the current file (spotify_client.py)
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
config_path = project_root / 'exclusion_config.json'

with config_path.open('r', encoding='utf-8') as config_file:
    config = json.load(config_file)

forbidden_playlists = config.get('forbidden_playlists', [])
forbidden_words = config.get('forbidden_words', [])
description_keywords = config.get('description_keywords', [])
forbidden_patterns = [
    r'\b' + re.escape(word.lower()) + r'\b' for word in forbidden_words
]

def authenticate_spotify():
    SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
    SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

    logging.info("Authenticating with Spotify")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID,
                                                   client_secret=SPOTIFY_CLIENT_SECRET,
                                                   redirect_uri="http://localhost:8888/callback",
                                                   scope="playlist-read-private user-library-read"))
    return sp

# Fetch all user's private playlists (self-created). Excludes playlists with forbidden words in their name.
# Returns a list of tuples containing playlist's name, its unique ID, and description (file path).
def fetch_my_playlists(spotify_client, total_limit=500) -> List[Tuple[str, str, str]]:
    logging.info("Fetching all my playlists")
    user_id = spotify_client.current_user()['id']

    all_playlists = []
    offset = 0
    limit = 50  # Spotify API limit per request

    while len(all_playlists) < total_limit:
        playlists = spotify_client.current_user_playlists(limit=limit, offset=offset)
        if not playlists['items']:
            break  # No more playlists to fetch

        all_playlists.extend(playlists['items'])
        offset += limit

    logging.info(f"Total playlists fetched: {len(all_playlists)}")

    my_playlists = [
        # creates row in db with these 3 columns
        (playlist['name'], html.unescape(playlist['description'] or ""), playlist['id'])
        for playlist in all_playlists
        if (
            playlist['owner']['id'] == user_id and
            not is_forbidden_playlist(playlist['name'], playlist['description'] or "")
        )
    ]

    logging.info(f"Total playlists after exclusion: {len(my_playlists)}")
    return my_playlists

def is_forbidden_playlist(name: str, description: str) -> bool:
    name_lower = name.lower()
    description_lower = description.lower()

    if any(word.lower() in name_lower for word in forbidden_words):
        logging.info(f"Excluding playlist '{name}' due to forbidden word in name.")
        return True

    if name in forbidden_playlists:
        logging.info(f"Excluding playlist '{name}' as it is in forbidden_playlists.")
        return True

    for keyword in description_keywords:
        # Create a regex pattern to match whole words (case-insensitive)
        pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
        if re.search(pattern, description_lower):
            logging.info(f"Excluding playlist '{name}' because description contains '{keyword}'.")
            return True

    return False

# Fetch user's Liked Songs
def fetch_liked_songs(spotify_client):
    logging.info("Fetching Liked Songs")
    results = spotify_client.current_user_saved_tracks()
    return [(results['track']['name'], item['track']['id']) for item in results['items']]

# Fetch ALL unique tracks from all user's playlists. Gets track title and artist name.
def fetch_master_tracks(spotify_client, my_playlists) -> List[Tuple[str, str, str]]:
    logging.info("Fetching all unique tracks from all my playlists")
    all_tracks = []

    for playlist_name, playlist_description, playlist_id in my_playlists:
        logging.info(f"Fetching tracks for playlist: {playlist_name} (ID: {playlist_id})")
        offset = 0
        limit = 100
        while True:
            try:
                tracks = spotify_client.playlist_tracks(playlist_id, offset=offset, limit=limit)
                if not tracks['items']:
                    break
                all_tracks.extend(
                    (
                        track['track']['name'],
                        ", ".join([artist['name'] for artist in track['track']['artists']]),
                        track['track']['album']['name']
                    )
                    for track in tracks['items']
                )
                offset += limit
                if not tracks['next']:
                    break
            except Exception as e:
                logging.error(f"Error fetching tracks for playlist {playlist_name} (ID: {playlist_id}): {e}")
                break

    # Return unique tracks
    unique_tracks = list(set(all_tracks))
    logging.info(f"Fetched {len(unique_tracks)} unique tracks from all playlists")
    return unique_tracks

# Find which playlists each track belongs to
def find_playlists_for_tracks(spotify_client, tracks: List[Tuple[str, str, str]], my_playlists) -> List[
    Tuple[str, str, str, List[str]]]:
    logging.info("Finding playlists for each track")
    track_to_playlists = {track: [] for track in tracks}

    for playlist_name, playlist_description, playlist_id in my_playlists:
        logging.info(f"Checking tracks for playlist: {playlist_name} (ID: {playlist_id})")
        offset = 0
        limit = 100
        while True:
            try:
                playlist_tracks = spotify_client.playlist_tracks(playlist_id, offset=offset, limit=limit)
                if not playlist_tracks['items']:
                    break
                for track in playlist_tracks['items']:
                    track_name = track['track']['name']
                    track_artists = ", ".join([artist['name'] for artist in track['track']['artists']])
                    track_album = track['track']['album']['name']
                    track_key = (track_name, track_artists, track_album)
                    if track_key in track_to_playlists:
                        track_to_playlists[track_key].append(playlist_name)
                offset += limit
                if not playlist_tracks['next']:
                    break
            except Exception as e:
                logging.error(f"Error checking tracks for playlist {playlist_name} (ID: {playlist_id}): {e}")
                break

    tracks_with_playlists = [(track[0], track[1], track[2], track_to_playlists[track]) for track in tracks]
    return tracks_with_playlists
