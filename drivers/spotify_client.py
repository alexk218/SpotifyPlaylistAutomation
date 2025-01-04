import html
import logging
import os
import json
import re
from datetime import datetime
import spotipy
from spotipy import SpotifyOAuth
from pathlib import Path
from typing import List, Tuple
from utils.logger import setup_logger

spotify_logger = setup_logger('spotify_logger', 'drivers/spotify.log')

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

    spotify_logger.info("Authenticating with Spotify")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri="http://localhost:8888/callback",
        scope="playlist-read-private user-library-read playlist-modify-public playlist-modify-private"
    ))
    return sp


# Fetch all user's private playlists (self-created). Excludes playlists with forbidden words in their name.
# Returns: List of tuples containing (PlaylistName, PlaylistDescription, PlaylistId)
def fetch_playlists(spotify_client, total_limit=500) -> List[Tuple[str, str, str]]:
    spotify_logger.info("Fetching all my playlists")
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

    spotify_logger.info(f"Total playlists fetched: {len(all_playlists)}")

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

    spotify_logger.info(f"Total playlists after exclusion: {len(my_playlists)}")
    return my_playlists


def is_forbidden_playlist(name: str, description: str) -> bool:
    name_lower = name.lower()
    description_lower = description.lower()

    if any(word.lower() in name_lower for word in forbidden_words):
        spotify_logger.info(f"Excluding playlist '{name}' due to forbidden word in name.")
        return True

    if name in forbidden_playlists:
        spotify_logger.info(f"Excluding playlist '{name}' as it is in forbidden_playlists.")
        return True

    for keyword in description_keywords:
        # Create a regex pattern to match whole words (case-insensitive)
        pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
        if re.search(pattern, description_lower):
            spotify_logger.info(f"Excluding playlist '{name}' because description contains '{keyword}'.")
            return True

    return False


# Fetch user's Liked Songs
def fetch_liked_songs(spotify_client):
    spotify_logger.info("Fetching Liked Songs")
    results = spotify_client.current_user_saved_tracks()
    return [(results['track']['name'], item['track']['id']) for item in results['items']]


# Fetch all unique tracks from 'MASTER' playlist
# Returns: List of tuples containing (TrackId, TrackTitle, Artists, Album)
def fetch_master_tracks(spotify_client, master_playlist_id: str) -> List[Tuple[str, str, str, str]]:
    spotify_logger.info(f"Fetching all unique tracks from 'MASTER' playlist (ID: {master_playlist_id})")
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
            spotify_logger.error(f"Error fetching tracks for 'MASTER' playlist (ID: {master_playlist_id}): {e}")
            break

    # Remove duplicates based on TrackId
    unique_tracks = list({track[0]: track for track in all_tracks}.values())
    spotify_logger.info(f"Fetched {len(unique_tracks)} unique tracks from 'MASTER' playlist")
    return unique_tracks


def fetch_master_tracks_for_validation(spotify_client, master_playlist_id):
    spotify_logger.info(f"Fetching tracks with dates from 'MASTER' playlist for validation")
    all_tracks = []

    offset = 0
    limit = 100

    while True:
        try:
            tracks = spotify_client.playlist_tracks(
                master_playlist_id,
                offset=offset,
                limit=limit,
                fields='items(added_at,track(id,name,artists(name),album(name))),total'
            )

            if not tracks['items']:
                break

            for item in tracks['items']:
                if not item['track']:
                    continue

                track = item['track']
                added_at = datetime.strptime(item['added_at'], '%Y-%m-%dT%H:%M:%SZ')
                all_tracks.append({
                    'id': track['id'],
                    'name': track['name'],
                    'artists': ", ".join(artist['name'] for artist in track['artists']),
                    'album': track['album']['name'],
                    'added_at': added_at
                })

            offset += limit
            # Continue until we've processed all tracks
            if offset >= tracks.get('total', 0):
                break

        except Exception as e:
            spotify_logger.error(f"Error fetching tracks for validation: {e}")
            break

    all_tracks.sort(key=lambda x: x['added_at'], reverse=True)
    spotify_logger.info(f"Fetched {len(all_tracks)} tracks for validation")
    return all_tracks


# ! NOT NEEDED
# Fetch ALL unique tracks from all user's playlists.
# Returns: List of tuples containing (TrackId, TrackTitle, Artists, Album)
def fetch_all_unique_tracks(spotify_client, my_playlists) -> List[Tuple[str, str, str, str]]:
    spotify_logger.info("Fetching all unique tracks from all my playlists")
    all_tracks = []

    for playlist_name, playlist_description, playlist_id in my_playlists:
        spotify_logger.info(f"Fetching tracks for playlist: {playlist_name} (ID: {playlist_id})")
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
                spotify_logger.error(f"Error fetching tracks for playlist {playlist_name} (ID: {playlist_id}): {e}")
                break

    # Remove duplicates based on TrackId
    unique_tracks = list({track[0]: track for track in all_tracks}.values())
    spotify_logger.info(f"Fetched {len(unique_tracks)} unique tracks from all playlists")
    return unique_tracks


# Find which playlists each track from 'MASTER' belongs to
# * Returns: List of tuples containing (TrackId, TrackTitle, Artists, Album, [Playlists])
def find_playlists_for_master_tracks(spotify_client, master_tracks: List[Tuple[str, str, str, str]], master_playlist_id) -> (
        List)[Tuple[str, str, str, str, List[str]]]:
    spotify_logger.info("Finding playlists for each track in 'MASTER'")

    # Extract TrackIds from master_tracks for quick lookup
    master_track_ids = set(track[0] for track in master_tracks)

    # Fetch all playlists excluding the 'MASTER' playlist
    all_playlists = fetch_playlists(spotify_client)
    other_playlists = [pl for pl in all_playlists if pl[2] != master_playlist_id]

    spotify_logger.info(f"Total other playlists to check: {len(other_playlists)}")

    # Initialize a dictionary to map TrackId to playlists
    track_to_playlists = {track_id: [] for track_id in master_track_ids}

    for playlist_name, playlist_description, playlist_id in other_playlists:
        spotify_logger.info(f"Checking tracks for playlist: {playlist_name} (ID: {playlist_id})")
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
                spotify_logger.error(f"Error fetching tracks for playlist '{playlist_name}' (ID: {playlist_id}): {e}")
                break  # Skip to the next playlist in case of an error

    # Prepare the final list with playlists associated to each track
    tracks_with_playlists = []
    for track in master_tracks:
        track_id, track_title, artist_names, album_name = track
        playlists = track_to_playlists.get(track_id, [])
        tracks_with_playlists.append((track_id, track_title, artist_names, album_name, playlists))

    spotify_logger.info("Completed finding playlists for all tracks in the 'MASTER' playlist")
    return tracks_with_playlists


# Fetch PlaylistId of 'MASTER' playlist (NOT USED ANYMORE. MANUALLY USING PLAYLISTID).
def fetch_master_playlist_id(spotify_client) -> str | None:
    spotify_logger.info("Fetching 'MASTER' playlist ID")
    playlists = fetch_playlists(spotify_client)
    for playlist_name, playlist_description, playlist_id in playlists:
        if playlist_name.upper() == "MASTER":
            spotify_logger.info(f"'MASTER' playlist found with ID: {playlist_id}")
            return playlist_id
    spotify_logger.error("'MASTER' playlist not found.")
    return None


# Gets all track IDs from a playlist.
def get_playlist_track_ids(spotify_client: spotipy.Spotify, playlist_id: str) -> List[str]:
    tracks = []
    offset = 0
    limit = 100  # Spotify API limit

    while True:
        response = spotify_client.playlist_items(
            playlist_id,
            offset=offset,
            limit=limit,
            fields='items.track.id,total'
        )

        if not response['items']:
            break

        tracks.extend(
            item['track']['id'] for item in response['items']
            if item['track'] and item['track']['id']
        )

        offset += limit
        if offset >= response['total']:
            break

    return tracks

# Get tracks that would be added to MASTER, organized by source playlist.
# * Returns a dictionary of playlist names to lists of track details.
def get_tracks_to_sync(spotify_client: spotipy.Spotify, master_playlist_id: str) -> dict:
    spotify_logger.info("Analyzing tracks to sync to MASTER playlist")

    # Get all tracks currently in MASTER playlist
    master_tracks = get_playlist_track_ids(spotify_client, master_playlist_id)
    spotify_logger.info(f"Found {len(master_tracks)} tracks in MASTER playlist")

    # Get all user's playlists except MASTER and forbidden ones
    user_playlists = fetch_playlists(spotify_client)
    other_playlists = [pl for pl in user_playlists if pl[2] != master_playlist_id]

    # Track which songs come from which playlists
    new_tracks_by_playlist = {}

    # Process each playlist with progress indicator
    print("\nAnalyzing playlists...")
    for i, (playlist_name, _, playlist_id) in enumerate(other_playlists, 1):
        print(f"Checking playlist {i}/{len(other_playlists)}: {playlist_name}")
        spotify_logger.info(f"Checking tracks in playlist: {playlist_name}")

        # Get detailed track information
        offset = 0
        limit = 100
        playlist_tracks = []

        while True:
            results = spotify_client.playlist_tracks(
                playlist_id,
                offset=offset,
                limit=limit,
                fields='items(track(id,name,artists(name))),total'
            )

            if not results['items']:
                break

            for item in results['items']:
                track = item['track']
                if track and track['id'] and track['id'] not in master_tracks:
                    track_info = {
                        'id': track['id'],
                        'name': track['name'],
                        'artists': ', '.join(artist['name'] for artist in track['artists'])
                    }
                    playlist_tracks.append(track_info)

            offset += limit
            if offset >= results['total']:
                break

        if playlist_tracks:
            new_tracks_by_playlist[playlist_name] = playlist_tracks

    return new_tracks_by_playlist


# Syncs all tracks from all playlists to the MASTER playlist (except for forbidden playlists)
# * Waits for user confirmation before syncing. To make sure the right tracks are being added.
def sync_to_master_playlist(spotify_client: spotipy.Spotify, master_playlist_id: str) -> None:
    spotify_logger.info("Starting sync to MASTER playlist")
    print("\nStarting sync analysis...")

    # Get tracks that would be added, organized by playlist
    tracks_by_playlist = get_tracks_to_sync(spotify_client, master_playlist_id)

    if not tracks_by_playlist:
        spotify_logger.info("No new tracks to add to MASTER playlist")
        print("\nNo new tracks found to add to MASTER playlist.")
        return

    # Display summary of changes
    total_tracks = sum(len(tracks) for tracks in tracks_by_playlist.values())
    print(f"\nFound {total_tracks} tracks to add to MASTER playlist from {len(tracks_by_playlist)} playlists:")
    print("\nChanges to be made:")

    # Sort playlists by name for easier reading
    for playlist_name, tracks in sorted(tracks_by_playlist.items()):
        print(f"\n{playlist_name} ({len(tracks)} tracks):")
        # Sort tracks by artist name, then track name
        sorted_tracks = sorted(tracks, key=lambda x: (x['artists'], x['name']))
        for track in sorted_tracks:
            print(f"  • {track['artists']} - {track['name']}")

    # Ask for confirmation
    confirmation = input("\nWould you like to proceed with adding these tracks to MASTER? (y/n): ")

    if confirmation.lower() != 'y':
        spotify_logger.info("Sync cancelled by user")
        print("Sync cancelled.")
        return

    # Proceed with sync
    spotify_logger.info(f"Starting to add {total_tracks} tracks to MASTER playlist")
    print("\nStarting sync process...")

    # Collect all track IDs to add, ensuring uniqueness
    all_track_ids = list({
        track['id']
        for playlist_tracks in tracks_by_playlist.values()
        for track in playlist_tracks
    })

    # Add tracks in batches with progress tracking
    tracks_added = 0
    for i in range(0, len(all_track_ids), 100):
        batch = all_track_ids[i:i + 100]
        try:
            spotify_client.playlist_add_items(master_playlist_id, batch)
            tracks_added += len(batch)
            spotify_logger.info(f"Added batch of {len(batch)} tracks to MASTER playlist")
            print(f"Progress: {tracks_added}/{total_tracks} tracks added to MASTER playlist")
        except Exception as e:
            spotify_logger.error(f"Error adding tracks to MASTER playlist: {e}")
            print(f"Error adding tracks to MASTER playlist: {e}")

    spotify_logger.info("Sync completed successfully")
    print(f"\nSync completed successfully! Added {tracks_added} tracks to MASTER playlist.")
