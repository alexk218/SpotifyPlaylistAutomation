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
# Returns: List of tuples containing (PlaylistName, PlaylistDescription, PlaylistId)
def fetch_playlists(spotify_client, total_limit=500) -> List[Tuple[str, str, str]]:
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
        (
            playlist['name'],
            html.unescape(playlist['description'] or ""),
            playlist['id']
        )
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


# Fetch all unique tracks from 'MASTER' playlist
# Returns: List of tuples containing (TrackId, TrackTitle, Artists, Album)
def fetch_master_tracks(spotify_client, master_playlist_id: str) -> List[Tuple[str, str, str, str]]:
    logging.info(f"Fetching all unique tracks from 'MASTER' playlist (ID: {master_playlist_id})")
    all_tracks = []

    offset = 0
    limit = 100  # Maximum allowed by Spotify API per request

    while True:
        try:
            tracks = spotify_client.playlist_tracks(master_playlist_id, offset=offset, limit=limit)
            if not tracks['items']:
                break
            all_tracks.extend(
                (
                    track['track']['id'],
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
            logging.error(f"Error fetching tracks for 'MASTER' playlist (ID: {master_playlist_id}): {e}")
            break

    # Remove duplicates based on TrackId
    unique_tracks = list({track[0]: track for track in all_tracks}.values())
    logging.info(f"Fetched {len(unique_tracks)} unique tracks from 'MASTER' playlist")
    return unique_tracks


# Fetch ALL unique tracks from all user's playlists. (NOT NEEDED)
# Returns: List of tuples containing (TrackId, TrackTitle, Artists, Album)
def fetch_all_unique_tracks(spotify_client, my_playlists) -> List[Tuple[str, str, str, str]]:
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
                        track['track']['id'],
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

    # Remove duplicates based on TrackId
    unique_tracks = list({track[0]: track for track in all_tracks}.values())
    logging.info(f"Fetched {len(unique_tracks)} unique tracks from all playlists")
    return unique_tracks


# Find which playlists each track from 'MASTER' belongs to
# Returns: List of tuples containing (TrackId, TrackTitle, Artists, Album, [Playlists])
def find_playlists_for_master_tracks(spotify_client, master_tracks: List[Tuple[str, str, str, str]], master_playlist_id) -> (
        List)[Tuple[str, str, str, str, List[str]]]:
    logging.info("Finding playlists for each track in 'MASTER'")

    # Extract TrackIds from master_tracks for quick lookup
    master_track_ids = set(track[0] for track in master_tracks)

    # Fetch all playlists excluding the 'MASTER' playlist
    all_playlists = fetch_playlists(spotify_client)
    other_playlists = [pl for pl in all_playlists if pl[2] != master_playlist_id]

    logging.info(f"Total other playlists to check: {len(other_playlists)}")

    # Initialize a dictionary to map TrackId to playlists
    track_to_playlists = {track_id: [] for track_id in master_track_ids}

    for playlist_name, playlist_description, playlist_id in other_playlists:
        logging.info(f"Checking tracks for playlist: {playlist_name} (ID: {playlist_id})")
        offset = 0
        limit = 100

        while True:
            try:
                response = spotify_client.playlist_tracks(playlist_id, offset=offset, limit=limit)
                items = response.get('items', [])

                if not items:
                    break  # No more tracks to fetch in this playlist

                for item in items:
                    track = item.get('track')
                    if track:
                        track_id = track.get('id')
                        if track_id in master_track_ids:
                            track_to_playlists[track_id].append(playlist_name)

                if not response.get('next'):
                    break  # No next page
                offset += limit
            except Exception as e:
                logging.error(f"Error fetching tracks for playlist '{playlist_name}' (ID: {playlist_id}): {e}")
                break  # Skip to the next playlist in case of an error

    # Prepare the final list with playlists associated to each track
    tracks_with_playlists = []
    for track in master_tracks:
        track_id, track_title, artist_names, album_name = track
        playlists = track_to_playlists.get(track_id, [])
        tracks_with_playlists.append((track_id, track_title, artist_names, album_name, playlists))

    logging.info("Completed finding playlists for all tracks in the 'MASTER' playlist")
    return tracks_with_playlists


# Fetch PlaylistId of 'MASTER' playlist (NOT USED ANYMORE. MANUALLY USING PLAYLISTID).
def fetch_master_playlist_id(spotify_client) -> str | None:
    logging.info("Fetching 'MASTER' playlist ID")
    playlists = fetch_playlists(spotify_client)
    for playlist_name, playlist_description, playlist_id in playlists:
        if playlist_name.upper() == "MASTER":
            logging.info(f"'MASTER' playlist found with ID: {playlist_id}")
            return playlist_id
    logging.error("'MASTER' playlist not found.")
    return None
