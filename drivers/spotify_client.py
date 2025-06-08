import hashlib
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Any
from dotenv import load_dotenv

from helpers.playlist_helper import is_forbidden_playlist, load_exclusion_config
from sql.dto.playlist_info import PlaylistInfo

load_dotenv()

import spotipy
from spotipy import SpotifyOAuth

from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

spotify_logger = setup_logger('spotify_client', 'drivers', 'spotify.log')

# Default since date - September 12, 2021. Will not fetch Liked Songs before this date.
DEFAULT_SINCE_DATE = datetime(2021, 9, 12)

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
config_path = project_root / 'exclusion_config.json'

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')


def authenticate_spotify():
    spotify_logger.info("Authenticating with Spotify")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri="http://localhost:8888/callback",
        scope="playlist-read-private user-library-read playlist-modify-public playlist-modify-private"
    ))
    return sp


def fetch_playlists(spotify_client, exclusion_config=None) -> List[PlaylistInfo]:
    """
    Fetch all user's private playlists (self-created), excluding forbidden playlists.
    Uses cache if available and not forcing refresh.

    Args:
        spotify_client: Authenticated Spotify client
        exclusion_config: Optional dictionary with exclusion configuration

    Returns:
        List of PlaylistInfo [playlist_name, playlist_id, snapshot_id]
    """
    config = load_exclusion_config(exclusion_config)

    spotify_logger.info("Fetching all my playlists from Spotify API")
    user_id = spotify_client.current_user()['id']
    spotify_logger.info(f"User id: {user_id}")

    all_playlists = []
    offset = 0
    limit = 50  # Spotify API limit per request

    while True:
        spotify_logger.info(f"Fetching playlists (offset: {offset}, limit: {limit})")
        playlists = spotify_client.current_user_playlists(limit=limit, offset=offset)
        spotify_logger.info(f"Fetched {len(playlists['items'])} playlists in this batch")
        if not playlists['items']:
            break  # No more playlists to fetch

        all_playlists.extend(playlists['items'])
        offset += limit
        if offset >= playlists['total']:
            break

    spotify_logger.info(f"Total playlists fetched from API: {len(all_playlists)}")

    my_playlists = [
        PlaylistInfo(
            playlist['name'],
            playlist['id'],
            playlist['snapshot_id']
        )
        for playlist in all_playlists
        if (
                playlist['owner']['id'] == user_id and
                not is_forbidden_playlist(
                    playlist['name'],
                    playlist['description'] or "",
                    playlist['id'],
                    config.get('forbidden_playlists', []),
                    config.get('forbidden_words', []),
                    config.get('description_keywords', []),
                    config.get('forbidden_playlist_ids', [])
                )
        )
    ]

    spotify_logger.info(f"Total playlists after exclusion: {len(my_playlists)}")

    return my_playlists


def fetch_master_tracks(spotify_client, master_playlist_id: str) -> List[
    Tuple[str, str, str, str, str, datetime]]:
    """
    Fetch all unique tracks from 'MASTER' playlist.

    Args:
        spotify_client: Authenticated Spotify client
        master_playlist_id: ID of the master playlist

    Returns:
        List of tuples containing (Uri, TrackId, TrackTitle, Artists, Album, AddedAt)
    """
    spotify_logger.info(f"Fetching all unique tracks from 'MASTER' playlist (ID: {master_playlist_id})")
    all_tracks = []

    offset = 0
    limit = 100  # Maximum allowed by Spotify API per request

    while True:
        try:
            tracks = spotify_client.playlist_tracks(
                master_playlist_id,
                offset=offset,
                limit=limit,
                fields='items(added_at,track(id,name,artists(name),album(name),uri,is_local)),next'  # Added 'uri' field
            )

            if not tracks['items']:
                break

            for track_item in tracks['items']:
                try:
                    if track_item['track'] is None:
                        continue

                    track = track_item['track']
                    added_at = datetime.strptime(track_item['added_at'], '%Y-%m-%dT%H:%M:%SZ')

                    spotify_uri = track.get('uri', '')

                    is_local = track.get('is_local', False)
                    if is_local:
                        # This is a local file - URI contains the metadata
                        track_id = None  # No track ID for local files
                        track_name = track.get('name', '')
                        artist_names = ", ".join([artist['name'] for artist in track.get('artists', [])])
                        album_name = track.get('album', {}).get('name', 'Local File')

                        spotify_logger.info(
                            f"Found local file: '{track_name}' by '{artist_names}' (URI: {spotify_uri})")
                    else:
                        # Regular Spotify track
                        track_id = track.get('id')
                        track_name = track.get('name', '')
                        artist_names = ", ".join([artist['name'] for artist in track.get('artists', [])])
                        album_name = track.get('album', {}).get('name', '')

                        spotify_logger.debug(
                            f"Found regular track: '{track_name}' by '{artist_names}' (URI: {spotify_uri})")

                    # Add to our list of tracks - URI is now the first element
                    all_tracks.append((spotify_uri, track_id, track_name, artist_names, album_name, added_at))

                except Exception as e:
                    spotify_logger.error(f"Error processing track: {e}")
                    continue

            # Move to the next page
            offset += limit
            if not tracks.get('next'):
                break

        except Exception as e:
            spotify_logger.error(f"Error fetching tracks for 'MASTER' playlist (ID: {master_playlist_id}): {e}")
            break

    # Count local files for reporting
    local_files_count = sum(1 for track in all_tracks if track[1] is None)  # track_id is None for local files
    spotify_logger.info(f"Found {local_files_count} local files in MASTER playlist")
    print(f"Found {local_files_count} local files in MASTER playlist")

    # For regular tracks, remove duplicates based on TrackId, keeping the earliest added_at date
    # For local files, remove duplicates based on URI since they don't have track IDs
    unique_tracks = {}
    processed_uris = set()

    for track in all_tracks:
        spotify_uri, track_id, track_name, artist_names, album_name, added_at = track

        if track_id is None:
            # This is a local file - use URI for deduplication
            if spotify_uri not in processed_uris:
                unique_tracks[spotify_uri] = track
                processed_uris.add(spotify_uri)
        else:
            # This is a regular track - deduplicate by track_id
            if track_id not in unique_tracks or added_at < unique_tracks[track_id][5]:
                unique_tracks[track_id] = track

    unique_tracks_list = list(unique_tracks.values())

    spotify_logger.info(
        f"Fetched {len(unique_tracks_list)} unique tracks from 'MASTER' playlist (including {local_files_count} local files)")

    return unique_tracks_list


def get_track_uris_for_playlist(spotify_client, playlist_id: str, force_refresh=False) -> List[str]:
    """
    Get all track URIs from a playlist (updated version of get_track_ids_for_playlist).

    Args:
        spotify_client: Authenticated Spotify client
        playlist_id: The playlist ID to fetch tracks for
        force_refresh: Whether to force a refresh from the API

    Returns:
        List of Spotify URIs (both regular tracks and local files)
    """
    # First, try to get URIs from database (if not forcing refresh)
    if not force_refresh:
        with UnitOfWork() as uow:
            track_uris = uow.track_playlist_repository.get_uris_for_playlist(playlist_id)
            if track_uris:
                spotify_logger.info(f"Retrieved {len(track_uris)} track URIs for playlist {playlist_id} from database")
                return track_uris

    # If we get here, fetch from Spotify API
    spotify_logger.info(f"Fetching tracks for playlist {playlist_id} from Spotify API")

    track_uris = []
    offset = 0
    limit = 100

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

        # Loop through all tracks in the playlist
        while True:
            try:
                response = spotify_client.playlist_items(
                    playlist_id,
                    offset=offset,
                    limit=limit,
                    fields='items(track(id,uri,name,artists(name),album(name),is_local)),total'
                )

                if not response['items']:
                    break

                for item in response['items']:
                    track = item['track']

                    # Skip if track is None
                    if not track:
                        continue

                    # Get the Spotify URI directly
                    spotify_uri = track.get('uri')
                    if spotify_uri:
                        track_uris.append(spotify_uri)

                        # Log local files for debugging
                        if track.get('is_local', False):
                            track_name = track.get('name', '')
                            artist_name = track.get('artists', [{}])[0].get('name', '') if track.get('artists') else ''
                            spotify_logger.debug(
                                f"Found local file: '{track_name}' by '{artist_name}' (URI: {spotify_uri})")

                # Check if we've processed all tracks
                if len(response['items']) < limit:
                    break

                offset += limit
                if offset >= response['total']:
                    break

            except Exception as e:
                spotify_logger.error(f"Error fetching tracks at offset {offset}: {str(e)}")
                time.sleep(1)
                continue

        spotify_logger.info(f"Successfully fetched {len(track_uris)} track URIs from playlist {playlist_id}")
        return track_uris

    except Exception as e:
        spotify_logger.error(f"Failed to fetch tracks for playlist {playlist_id}: {str(e)}")
        return []


def get_track_ids_for_playlist(spotify_client: spotipy.Spotify, playlist_id: str, force_refresh=False) -> List[str]:
    """
    Get all track IDs from a playlist.
    Uses database if available, then cache, then API as a fallback.

    Args:
        spotify_client: Authenticated Spotify client
        playlist_id: The playlist ID to fetch tracks for
        force_refresh: Whether to force a refresh from the API or use DB for track IDs for each playlist

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

    # If we get here, fetch from Spotify API
    spotify_logger.info(f"Fetching tracks for playlist {playlist_id} from Spotify API")

    track_ids = []  # Initialize track_ids list correctly
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

        # Loop through all tracks in the playlist
        while True:
            try:
                response = spotify_client.playlist_items(
                    playlist_id,
                    offset=offset,
                    limit=limit,
                    fields='items(track(id,uri,name,artists(name),album(name),is_local)),total'
                )

                if not response['items']:
                    break

                for item in response['items']:
                    track = item['track']

                    # Skip if track is None
                    if not track:
                        continue

                    # Handle regular Spotify tracks
                    if not track.get('is_local', False) and track.get('id'):
                        track_ids.append(track['id'])

                    # Special handling for local files
                    elif track.get('is_local', False):
                        # Get metadata
                        track_name = track.get('name', '')
                        artist_name = track.get('artists', [{}])[0].get('name', '') if track.get('artists') else ''
                        album_name = track.get('album', {}).get('name', '') if track.get('album') else ''

                        # Log the local file
                        spotify_logger.debug(f"Found local file in playlist: '{track_name}' by '{artist_name}'")
                        spotify_logger.info(
                            f"Found local file: '{track_name}' by '{artist_name}'")

                        # Generate a consistent ID for the local file
                        normalized_name = ''.join(c.lower() for c in track_name if c.isalnum() or c in ' &-_')
                        normalized_artist = ''.join(c.lower() for c in artist_name if c.isalnum() or c in ' &-_')

                        # Create a consistent string to hash
                        metadata_string = f"{normalized_artist}_{normalized_name}".strip().lower()

                        # Generate hash for ID
                        local_id = f"local_{hashlib.md5(metadata_string.encode()).hexdigest()[:16]}"
                        track_ids.append(local_id)

                        # Print debug info
                        spotify_logger.debug(
                            f"Generated local file ID: {local_id} for '{track_name}' by '{artist_name}'")

                # Check if we've processed all tracks
                if len(response['items']) < limit:
                    break

                offset += limit
                if offset >= response['total']:
                    break

            except Exception as e:
                spotify_logger.error(f"Error fetching tracks at offset {offset}: {str(e)}")
                # Wait a bit before retrying
                time.sleep(1)
                continue

        spotify_logger.info(f"Successfully fetched {len(track_ids)} tracks from playlist {playlist_id}")

        return track_ids

    except Exception as e:
        spotify_logger.error(f"Failed to fetch tracks for playlist {playlist_id}: {str(e)}")
        return []


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


def sync_to_master_playlist(spotify_client, master_playlist_id, changed_playlists_only):
    """
    Processes playlists that have changed.

    Args:
        spotify_client: Authenticated Spotify client
        master_playlist_id: ID of the master playlist
        changed_playlists_only: List of PlaylistInfo objects that have changed
    """
    print(f"Starting MASTER sync for {len(changed_playlists_only)} changed playlists...")

    # Get current tracks in master playlist
    master_track_ids = get_track_ids_for_playlist(spotify_client, master_playlist_id, force_refresh=True)
    master_track_ids_set = set(master_track_ids)

    # Collect all track IDs from changed playlists only
    tracks_to_add = set()

    for playlist_info in changed_playlists_only:
        print(f"Processing changed playlist: {playlist_info.name}")
        playlist_track_ids = get_track_ids_for_playlist(spotify_client, playlist_info.playlist_id, force_refresh=True)

        # Add tracks that aren't already in master
        new_tracks = set(playlist_track_ids) - master_track_ids_set
        tracks_to_add.update(new_tracks)

        print(f"  Found {len(new_tracks)} new tracks to add from this playlist")

    if tracks_to_add:
        print(f"Adding {len(tracks_to_add)} total new tracks to master playlist...")

        # Add tracks in batches
        track_list = list(tracks_to_add)
        batch_size = 100

        for i in range(0, len(track_list), batch_size):
            batch = track_list[i:i + batch_size]
            spotify_client.playlist_add_items(master_playlist_id, batch)
            print(f"Added batch {i // batch_size + 1}/{(len(track_list) + batch_size - 1) // batch_size}")
            time.sleep(1)  # Rate limiting

    # Update MasterSyncSnapshotId for all processed playlists
    with UnitOfWork() as uow:
        for playlist_info in changed_playlists_only:
            db_playlist = uow.playlist_repository.get_by_id(playlist_info.playlist_id)
            if db_playlist:
                old_snapshot = db_playlist.master_sync_snapshot_id
                db_playlist.master_sync_snapshot_id = playlist_info.snapshot_id
                uow.playlist_repository.update(db_playlist)
                print(
                    f"Updated master sync snapshot for '{playlist_info.name}': {old_snapshot} -> {playlist_info.snapshot_id}")

    print(
        f"MASTER sync complete! Added {len(tracks_to_add)} tracks, updated {len(changed_playlists_only)} playlist snapshots.")


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
            playlist_tracks = get_track_ids_for_playlist(spotify_client, playlist_id)
            tracks_in_playlists.update(playlist_tracks)
            spotify_logger.info(f"Added {len(playlist_tracks)} tracks from a playlist")

    # Get tracks from UNSORTED playlist separately
    unsorted_tracks = get_track_ids_for_playlist(spotify_client, unsorted_playlist_id)
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
