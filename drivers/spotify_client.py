import html
import os
import json
import re
import time
from datetime import datetime
import spotipy
from spotipy import SpotifyOAuth
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Any, Set

from cache_manager import spotify_cache
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

spotify_logger = setup_logger('spotify_client', 'drivers/spotify.log')

# Default since date - September 12, 2021. Will not fetch Liked Songs before this date.
DEFAULT_SINCE_DATE = datetime(2021, 9, 12)

# Get the path to the current file
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


def fetch_playlists(spotify_client, total_limit=500, force_refresh=False) -> List[Tuple[str, str, str]]:
    """
    Fetch all user's private playlists (self-created), excluding forbidden playlists.
    Uses cache if available and not forcing refresh.

    Args:
        spotify_client: Authenticated Spotify client
        total_limit: Maximum number of playlists to fetch
        force_refresh: Whether to force a refresh from the API

    Returns:
        List of tuples containing (PlaylistName, PlaylistDescription, PlaylistId)
    """
    # Try to get playlists from cache first
    if not force_refresh:
        cached_playlists = spotify_cache.get_playlists()
        if cached_playlists:
            spotify_logger.info(f"Using cached playlists data ({len(cached_playlists)} playlists)")
            return cached_playlists

    # If we get here, we need to fetch from Spotify API
    spotify_logger.info("Fetching all my playlists from Spotify API")
    user_id = spotify_client.current_user()['id']
    spotify_logger.info(f"User id: {user_id}")

    all_playlists = []
    offset = 0
    limit = 50  # Spotify API limit per request

    while len(all_playlists) < total_limit:
        spotify_logger.info(f"Fetching playlists (offset: {offset}, limit: {limit})")
        playlists = spotify_client.current_user_playlists(limit=limit, offset=offset)
        spotify_logger.info(f"Fetched {len(playlists['items'])} playlists in this batch")
        if not playlists['items']:
            break  # No more playlists to fetch

        all_playlists.extend(playlists['items'])
        offset += limit

    spotify_logger.info(f"Total playlists fetched from API: {len(all_playlists)}")

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

    # Cache the result
    spotify_cache.cache_playlists(my_playlists)

    return my_playlists


def fetch_master_tracks(spotify_client, master_playlist_id: str, force_refresh=False) -> List[
    Tuple[str, str, str, str, datetime]]:
    """
    Fetch all unique tracks from 'MASTER' playlist.
    Uses cache if available and not forcing refresh.

    Args:
        spotify_client: Authenticated Spotify client
        master_playlist_id: ID of the master playlist
        force_refresh: Whether to force a refresh from the API

    Returns:
        List of tuples containing (TrackId, TrackTitle, Artists, Album, AddedAt)
    """
    # Try to get master tracks from cache first
    if not force_refresh:
        cached_tracks = spotify_cache.get_master_tracks(master_playlist_id)
        if cached_tracks:
            spotify_logger.info(f"Using cached master tracks data ({len(cached_tracks)} tracks)")
            return cached_tracks

    # If we get here, we need to fetch from Spotify API
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
                    track['track'].get('id'),
                    track['track']['name'],
                    ", ".join([artist['name'] for artist in track['track']['artists']]),
                    track['track'].get('album', {}).get('name', 'Local File'),
                    datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ')
                )
                for track in tracks['items']
                if track['track'] is not None
            )
            offset += limit
            if not tracks['next']:
                break
        except Exception as e:
            spotify_logger.error(f"Error fetching tracks for 'MASTER' playlist (ID: {master_playlist_id}): {e}")
            break

    # Remove duplicates based on TrackId, keeping the earliest added_at date
    unique_tracks = {}
    for track in all_tracks:
        track_id = track[0]
        if track_id not in unique_tracks or track[4] < unique_tracks[track_id][4]:
            unique_tracks[track_id] = track

    unique_tracks_list = list(unique_tracks.values())
    spotify_logger.info(f"Fetched {len(unique_tracks_list)} unique tracks from 'MASTER' playlist")

    # Cache the result
    spotify_cache.cache_master_tracks(master_playlist_id, unique_tracks_list)

    return unique_tracks_list


def get_playlist_track_ids(spotify_client: spotipy.Spotify, playlist_id: str, force_refresh=False) -> List[str]:
    """
    Get all track IDs from a playlist.
    Uses database if available, then cache, then API as a fallback.

    Args:
        spotify_client: Authenticated Spotify client
        playlist_id: The playlist ID to fetch tracks for
        force_refresh: Whether to force a refresh from the API

    Returns:
        List of track IDs
    """
    # First, try to get track IDs from database (if not forcing refresh)
    if not force_refresh:
        with UnitOfWork() as uow:
            track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id)
            if track_ids:
                spotify_logger.info(f"Retrieved {len(track_ids)} track IDs for playlist {playlist_id} from database")
                return track_ids

    # If not in database or forcing refresh, try cache
    if not force_refresh:
        cached_tracks = spotify_cache.get_playlist_tracks(playlist_id)
        if cached_tracks:
            spotify_logger.info(f"Using {len(cached_tracks)} cached track IDs for playlist {playlist_id}")
            return cached_tracks

    # If we get here, fetch from Spotify API
    spotify_logger.info(f"Fetching tracks for playlist {playlist_id} from Spotify API")

    tracks = []
    offset = 0
    limit = 100  # Spotify API limit

    try:
        # Get initial response to get total tracks
        initial_response = spotify_client.playlist_items(
            playlist_id,
            offset=0,
            limit=1,
            fields='total'
        )
        total_tracks = initial_response['total']
        spotify_logger.info(f"Total tracks to fetch: {total_tracks}")

        while True:
            try:
                response = spotify_client.playlist_items(
                    playlist_id,
                    offset=offset,
                    limit=limit,
                    fields='items.track.id,total'
                )

                if not response['items']:
                    break

                batch_tracks = [
                    item['track']['id'] for item in response['items']
                    if item['track'] and item['track']['id']
                ]

                tracks.extend(batch_tracks)
                spotify_logger.debug(f"Fetched {len(batch_tracks)} tracks (offset: {offset})")

                offset += limit
                if offset >= response['total']:
                    break

            except Exception as e:
                spotify_logger.error(f"Error fetching tracks at offset {offset}: {str(e)}")
                # Wait a bit before retrying
                time.sleep(1)
                continue

        spotify_logger.info(f"Successfully fetched {len(tracks)} tracks from playlist {playlist_id}")

        # Cache the result
        spotify_cache.cache_playlist_tracks(playlist_id, tracks)

        return tracks

    except Exception as e:
        spotify_logger.error(f"Failed to fetch tracks for playlist {playlist_id}: {str(e)}")
        return []


def get_playlist_tracks_from_db(playlist_id: str) -> List[str]:
    """
    Get all track IDs for a playlist from the database.

    Args:
        playlist_id: Playlist ID

    Returns:
        List of track IDs
    """
    with UnitOfWork() as uow:
        track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id)
        spotify_logger.info(f"Retrieved {len(track_ids)} track IDs for playlist {playlist_id} from database")
        return track_ids


def fetch_master_tracks_from_db() -> List[Dict[str, Any]]:
    """
    Get track details from the database for all tracks in the MASTER playlist.

    Returns:
        List of track data dictionaries
    """
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()

        track_data = []
        for track in tracks:
            track_data.append({
                'id': track.track_id,
                'name': track.title,
                'artists': track.artists,
                'album': track.album,
                'added_at': track.added_to_master
            })

        spotify_logger.info(f"Retrieved {len(track_data)} tracks from database")
        return track_data


def find_playlists_for_master_tracks(spotify_client, master_tracks: List[Tuple[str, str, str, str, datetime]],
                                     master_playlist_id, use_db_first=True, force_refresh=False) -> list[
    tuple[str, Any, list[Any]]]:
    """
    Find which playlists each track from 'MASTER' belongs to.
    Uses database if available, but always checks Spotify for updates.

    Args:
        spotify_client: Authenticated Spotify client
        master_tracks: List of master track tuples
        master_playlist_id: ID of the master playlist
        use_db_first: Whether to try using the database first
        force_refresh: Whether to force a refresh from API and ignore cache

    Returns:
        List of tuples containing (TrackId, TrackTitle, Artists, Album, AddedAt, [Playlists])
    """
    spotify_logger.info("Finding playlists for each track in 'MASTER'")

    # Extract TrackIds from master_tracks for quick lookup
    master_track_ids = set(track[0] for track in master_tracks)
    spotify_logger.info(f"Total master tracks to check: {len(master_track_ids)}")

    # Create mapping of TrackId to track details for faster lookups
    track_details = {track[0]: track[1:5] for track in master_tracks}

    # Initialize a dictionary to map TrackId to playlists
    track_to_playlists = {track_id: [] for track_id in master_track_ids}

    # If using database first and not forcing refresh, start with getting associations from there
    if use_db_first and not force_refresh:
        spotify_logger.info("Getting initial track-playlist associations from database")
        with UnitOfWork() as uow:
            # For each track, get the associated playlists
            for track_id in master_track_ids:
                playlists = uow.playlist_repository.get_playlists_for_track(track_id)
                playlist_names = [playlist.name for playlist in playlists]
                if playlist_names:
                    track_to_playlists[track_id] = playlist_names

        # Count how many tracks we found associations for
        tracks_with_playlists = sum(1 for playlists in track_to_playlists.values() if playlists)
        spotify_logger.info(f"Found playlist associations for {tracks_with_playlists} tracks in database")

    # Now always check Spotify for updated associations
    spotify_logger.info("Checking Spotify for updated playlist associations")

    # Fetch all playlists excluding the 'MASTER' playlist with force_refresh parameter
    all_playlists = fetch_playlists(spotify_client, force_refresh=force_refresh)
    other_playlists = [pl for pl in all_playlists if pl[2] != master_playlist_id]

    spotify_logger.info(f"Total other playlists to check: {len(other_playlists)}")

    # For each playlist, check which master tracks it contains
    # This is more efficient than checking each track against all playlists
    for playlist_name, playlist_description, playlist_id in other_playlists:
        spotify_logger.info(f"Checking tracks for playlist: {playlist_name} (ID: {playlist_id})")

        # Get track IDs for this playlist - pass force_refresh to ensure fresh data
        playlist_track_ids = set(get_playlist_track_ids(spotify_client, playlist_id, force_refresh=force_refresh))

        # Find intersection with master tracks
        common_tracks = master_track_ids.intersection(playlist_track_ids)

        # Update track_to_playlists
        for track_id in common_tracks:
            if playlist_name not in track_to_playlists[track_id]:
                track_to_playlists[track_id].append(playlist_name)
                spotify_logger.info(f"Found playlist association: Track {track_id} in playlist '{playlist_name}'")

        spotify_logger.info(f"Found {len(common_tracks)} master tracks in playlist '{playlist_name}'")

    # Prepare the final list with playlists associated to each track
    tracks_with_playlists = [
        (track_id, *track_details[track_id], track_to_playlists[track_id])
        for track_id in master_track_ids
    ]

    spotify_logger.info("Completed finding playlists for all tracks in the 'MASTER' playlist")
    return tracks_with_playlists


def get_liked_songs_with_dates(spotify_client, since_date=DEFAULT_SINCE_DATE):
    """
    Fetch user's Liked Songs with their added dates.

    Args:
        spotify_client: Authenticated Spotify client
        since_date: Only fetch songs added after this date

    Returns:
        List of dicts with track info and added_at date
    """
    spotify_logger.info(f"Fetching Liked Songs with dates since {since_date.strftime('%Y-%m-%d')}")
    liked_songs = []
    offset = 0
    limit = 50  # Spotify's maximum limit per request

    while True:
        results = spotify_client.current_user_saved_tracks(limit=limit, offset=offset)
        if not results['items']:
            break

        should_break = False
        for item in results['items']:
            added_at = datetime.strptime(item['added_at'], '%Y-%m-%dT%H:%M:%SZ')

            # If we've hit songs older than our target date, we can stop
            if added_at < since_date:
                should_break = True
                break

            track = item['track']
            liked_songs.append({
                'id': track['id'],
                'name': track['name'],
                'artists': ', '.join(artist['name'] for artist in track['artists']),
                'added_at': added_at
            })

        if should_break:
            break

        offset += limit
        if offset >= results['total']:
            break

    return sorted(liked_songs, key=lambda x: x['added_at'], reverse=True)  # Most recent first


def sync_to_master_playlist(spotify_client: spotipy.Spotify, master_playlist_id: str) -> None:
    """
    Syncs all tracks from all playlists to the MASTER playlist (except for forbidden playlists).
    Makes necessary API calls to ensure accuracy, but doesn't use optimizations.

    Args:
        spotify_client: Authenticated Spotify client
        master_playlist_id: ID of the MASTER playlist
    """
    spotify_logger.info("Starting sync to MASTER playlist")
    print("\nStarting sync analysis...")

    # Get current tracks in MASTER playlist directly from Spotify for accuracy
    master_track_ids = set(get_playlist_track_ids(spotify_client, master_playlist_id, force_refresh=True))
    spotify_logger.info(f"Found {len(master_track_ids)} tracks in MASTER playlist")

    # Fetch all playlists (excluding forbidden ones)
    user_playlists = fetch_playlists(spotify_client, force_refresh=True)
    other_playlists = [pl for pl in user_playlists if pl[2] != master_playlist_id]

    # Track which songs come from which playlists
    new_tracks_by_playlist = {}

    # Process each playlist
    print("\nAnalyzing playlists...")
    for i, (playlist_name, _, playlist_id) in enumerate(other_playlists, 1):
        print(f"Checking playlist {i}/{len(other_playlists)}: {playlist_name}")
        spotify_logger.info(f"Checking tracks in playlist: {playlist_name}")

        # Get tracks for this playlist directly from Spotify
        playlist_track_ids = get_playlist_track_ids(spotify_client, playlist_id, force_refresh=True)

        # Find tracks not in master playlist
        new_track_ids = [id for id in playlist_track_ids if id not in master_track_ids]

        if not new_track_ids:
            continue

        # For new tracks, get detailed information
        playlist_tracks = []

        # Process in batches of 50 for efficiency
        for j in range(0, len(new_track_ids), 50):
            batch = new_track_ids[j:j + 50]
            try:
                tracks_info = spotify_client.tracks(batch)
                for track in tracks_info['tracks']:
                    if track:
                        track_info = {
                            'id': track['id'],
                            'name': track['name'],
                            'artists': ', '.join(artist['name'] for artist in track['artists'])
                        }
                        playlist_tracks.append(track_info)
            except Exception as e:
                spotify_logger.error(f"Error fetching details for track batch: {e}")
                continue

        if playlist_tracks:
            new_tracks_by_playlist[playlist_name] = playlist_tracks

    if not new_tracks_by_playlist:
        spotify_logger.info("No new tracks to add to MASTER playlist")
        print("\nNo new tracks found to add to MASTER playlist.")
        return

    # Display summary of changes
    total_tracks = sum(len(tracks) for tracks in new_tracks_by_playlist.values())
    print(f"\nFound {total_tracks} tracks to add to MASTER playlist from {len(new_tracks_by_playlist)} playlists:")
    print("\nChanges to be made:")

    # Sort playlists by name for easier reading
    for playlist_name, tracks in sorted(new_tracks_by_playlist.items()):
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
        for playlist_tracks in new_tracks_by_playlist.values()
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

    # Invalidate relevant caches
    try:
        from cache_manager import spotify_cache
        spotify_cache.invalidate_tracks_cache(master_playlist_id)
        spotify_logger.info("Invalidated master tracks cache")
    except Exception as e:
        spotify_logger.warning(f"Could not invalidate cache: {e}")


def sync_unplaylisted_to_unsorted(spotify_client, unsorted_playlist_id: str):
    """
    Add Liked Songs that aren't in any other playlist to 'UNSORTED' playlist.
    Makes necessary API calls to ensure accuracy, but optimizes where possible.

    Args:
        spotify_client: Authenticated Spotify client
        unsorted_playlist_id: ID of the UNSORTED playlist

    Returns:
        List of unplaylisted songs that were added
    """
    if not unsorted_playlist_id:
        spotify_logger.error("UNSORTED playlist ID not provided!")
        return []

    spotify_logger.info("Starting sync of unplaylisted Liked Songs to UNSORTED playlist...")

    # Create logs/unplaylisted directories if doesn't exist
    logs_dir = project_root / 'logs'
    logs_dir.mkdir(exist_ok=True)
    unplaylisted_logs_dir = logs_dir / 'unplaylisted'
    unplaylisted_logs_dir.mkdir(exist_ok=True)

    # Get all Liked Songs with dates - this requires API call
    liked_songs_with_dates = get_liked_songs_with_dates(spotify_client)
    liked_song_ids = {song['id'] for song in liked_songs_with_dates}
    spotify_logger.info(f"Found {len(liked_song_ids)} Liked Songs")

    # Get all user's playlists
    all_playlists = fetch_playlists(spotify_client)
    spotify_logger.info(f"Found {len(all_playlists)} playlists")

    # Get all tracks from all playlists (excluding forbidden playlists)
    tracks_in_playlists = set()
    for _, _, playlist_id in all_playlists:
        if playlist_id != unsorted_playlist_id:  # Skip UNSORTED playlist
            playlist_tracks = get_playlist_track_ids(spotify_client, playlist_id)
            tracks_in_playlists.update(playlist_tracks)
            spotify_logger.info(f"Added {len(playlist_tracks)} tracks from a playlist")

    # Get tracks from UNSORTED playlist separately
    unsorted_tracks = get_playlist_track_ids(spotify_client, unsorted_playlist_id)
    spotify_logger.info(f"Found {len(unsorted_tracks)} tracks in UNSORTED playlist")

    # Find tracks that should be removed from UNSORTED (they're now in other playlists)
    tracks_to_remove = [
        track_id for track_id in unsorted_tracks
        if track_id in tracks_in_playlists
    ]

    # Find liked songs that aren't in any playlist
    tracks_in_playlists.update(unsorted_tracks)
    unplaylisted = liked_song_ids - tracks_in_playlists

    # Filter and sort the unplaylisted songs with their dates
    unplaylisted_songs = [
        song for song in liked_songs_with_dates
        if song['id'] in unplaylisted
    ]

    # Display summary of changes
    print("\nUNPLAYLISTED TRACKS SYNC ANALYSIS COMPLETE")
    print("========================================")

    if tracks_to_remove:
        print(f"\nTracks to remove from UNSORTED: {len(tracks_to_remove)}")
        # Get track details for display
        removed_tracks_info = []
        for i in range(0, len(tracks_to_remove), 50):
            batch = tracks_to_remove[i:i + 50]
            try:
                tracks_info = spotify_client.tracks(batch)
                for track in tracks_info['tracks']:
                    if track:
                        artists = ", ".join([artist['name'] for artist in track['artists']])
                        removed_tracks_info.append({
                            'name': track['name'],
                            'artists': artists,
                            'id': track['id']
                        })
            except Exception as e:
                spotify_logger.error(f"Error fetching track details: {e}")

        # Display sample of tracks to remove
        print("\nSAMPLE TRACKS TO REMOVE FROM UNSORTED:")
        print("=====================================")
        # Sort tracks by artist name, then track name for better readability
        sorted_tracks = sorted(removed_tracks_info[:10], key=lambda x: (x['artists'], x['name']))
        for track in sorted_tracks:
            print(f"• {track['artists']} - {track['name']}")
        if len(removed_tracks_info) > 10:
            print(f"...and {len(removed_tracks_info) - 10} more")
    else:
        print("\nNo tracks to remove from UNSORTED playlist")

    if unplaylisted_songs:
        print(f"\nLiked Songs to add to UNSORTED: {len(unplaylisted_songs)}")

        # Display sample of tracks to add
        print("\nSAMPLE TRACKS TO ADD TO UNSORTED:")
        print("================================")
        # Sort by date added, newest first, and take first 10
        sorted_songs = sorted(unplaylisted_songs[:10], key=lambda x: x['added_at'], reverse=True)
        for song in sorted_songs:
            added_date = song['added_at'].strftime('%Y-%m-%d')
            print(f"• {song['artists']} - {song['name']} (Added: {added_date})")
        if len(unplaylisted_songs) > 10:
            print(f"...and {len(unplaylisted_songs) - 10} more")
    else:
        print("\nNo unplaylisted Liked Songs to add")

    # Ask for confirmation
    if tracks_to_remove or unplaylisted_songs:
        confirmation = input("\nWould you like to proceed with syncing these changes to Spotify? (y/n): ")
        if confirmation.lower() != 'y':
            spotify_logger.info("Unplaylisted sync cancelled by user")
            print("Sync cancelled.")
            return []
    else:
        print("\nNo changes needed. All Liked Songs are already in playlists, and no tracks need to be removed.")
        return []

    removed_tracks_info = []
    # Remove tracks that are now in other playlists
    spotify_logger.info("Fetching details for removed tracks for logging purposes...")
    if tracks_to_remove:
        spotify_logger.info(f"Found {len(tracks_to_remove)} tracks to remove from UNSORTED (now in other playlists)")
        print(f"\nRemoving {len(tracks_to_remove)} tracks from UNSORTED playlist...")

        # Remove tracks in batches
        for i in range(0, len(tracks_to_remove), 100):
            batch = tracks_to_remove[i:i + 100]
            try:
                spotify_client.playlist_remove_all_occurrences_of_items(unsorted_playlist_id, batch)
                spotify_logger.info(f"Removed batch of {len(batch)} tracks from UNSORTED playlist")
                print(f"Removed batch of {len(batch)} tracks from UNSORTED playlist")
            except Exception as e:
                spotify_logger.error(f"Error removing tracks from UNSORTED playlist: {e}")
                print(f"Error removing tracks: {e}")
                continue

        # Get track details after successful removal
        for i in range(0, len(tracks_to_remove), 50):
            batch = tracks_to_remove[i:i + 50]
            try:
                tracks_info = spotify_client.tracks(batch)
                for track in tracks_info['tracks']:
                    if track:
                        artists = ", ".join([artist['name'] for artist in track['artists']])
                        removed_tracks_info.append({
                            'name': track['name'],
                            'artists': artists,
                            'id': track['id']
                        })
            except Exception as e:
                spotify_logger.error(f"Error fetching track details: {e}")

    # Generate log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = unplaylisted_logs_dir / f'playlist_sync_{timestamp}.log'

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(f"Playlist Sync Report - {datetime.now()}\n\n")

        # Report removed tracks
        f.write("=== Tracks Removed from UNSORTED ===\n")
        if removed_tracks_info:
            f.write(f"Total tracks removed: {len(removed_tracks_info)}\n\n")
            for track in removed_tracks_info:
                log_line = (f"Track: {track['artists']} - {track['name']}\n"
                            f"ID: {track['id']}\n\n")
                f.write(log_line)
        else:
            f.write("No tracks were removed from UNSORTED playlist\n\n")

        # Report unplaylisted songs
        f.write("=== Unplaylisted Songs Added to UNSORTED ===\n")
        if unplaylisted_songs:
            f.write(f"Total songs to add: {len(unplaylisted_songs)}\n\n")
            for song in unplaylisted_songs:
                log_line = (f"Added on: {song['added_at']}\n"
                            f"Track: {song['artists']} - {song['name']}\n"
                            f"ID: {song['id']}\n\n")
                f.write(log_line)
                spotify_logger.info(f"Found unplaylisted song: {song['artists']} - {song['name']}")
        else:
            f.write("No unplaylisted songs found - all Liked Songs are in at least one playlist\n")

    # Add unplaylisted tracks to UNSORTED playlist if any exist
    if unplaylisted_songs:
        spotify_logger.info("Adding unplaylisted songs to UNSORTED playlist...")
        print(f"\nAdding {len(unplaylisted_songs)} tracks to UNSORTED playlist...")

        # Add tracks in batches of 100 (Spotify API limit)
        track_ids = [song['id'] for song in unplaylisted_songs]
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i + 100]
            try:
                spotify_client.playlist_add_items(unsorted_playlist_id, batch, position=0)
                spotify_logger.info(f"Added batch of {len(batch)} tracks to UNSORTED playlist")
                print(f"Added batch of {len(batch)} tracks to UNSORTED playlist")
            except Exception as e:
                spotify_logger.error(f"Error adding tracks to UNSORTED playlist: {e}")
                print(f"Error adding tracks: {e}")

    spotify_logger.info(f"Sync complete. Check log file at {log_path}")
    print(f"\nSync complete! A detailed log file is available at: {log_path}")
    return unplaylisted_songs
