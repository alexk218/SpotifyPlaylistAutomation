"""
Incremental sync helpers to minimize API calls when updating database with Spotify data.

This module implements functions for smart incremental sync between Spotify and the local database.
"""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional, Any

from drivers.spotify_client import (
    authenticate_spotify,
    fetch_playlists,
    fetch_master_tracks,
    get_playlist_track_ids
)
from sql.core.unit_of_work import UnitOfWork
from sql.models.playlist import Playlist
from sql.models.track import Track
from utils.logger import setup_logger

sync_logger = setup_logger('sync_helper', 'logs/sync.log')


def get_db_playlists() -> Dict[str, Playlist]:
    """
    Get all playlists from the database.

    Returns:
        Dictionary of playlist_id to Playlist objects
    """
    with UnitOfWork() as uow:
        playlists = uow.playlist_repository.get_all()
        return {playlist.playlist_id: playlist for playlist in playlists}


def get_db_tracks() -> Dict[str, Track]:
    """
    Get all tracks from the database.

    Returns:
        Dictionary of track_id to Track objects
    """
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()
        return {track.track_id: track for track in tracks}


def sync_playlists_incremental(force_full_refresh: bool = False) -> Tuple[int, int, int]:
    """
    Incrementally sync playlists from Spotify to the database.
    Only fetches and updates playlists that have changed.

    Args:
        force_full_refresh: Whether to force a full refresh, ignoring existing data

    Returns:
        Tuple of (added, updated, unchanged) counts
    """
    sync_logger.info("Starting incremental playlist sync")
    print("Starting incremental playlist sync...")

    # Get existing playlists from database
    existing_playlists = get_db_playlists() if not force_full_refresh else {}
    sync_logger.info(f"Found {len(existing_playlists)} existing playlists in database")

    # Fetch all playlists from Spotify
    spotify_client = authenticate_spotify()
    spotify_playlists = fetch_playlists(spotify_client, force_refresh=force_full_refresh)
    sync_logger.info(f"Fetched {len(spotify_playlists)} playlists from Spotify")

    # Track counts
    added_count = 0
    updated_count = 0
    unchanged_count = 0

    with UnitOfWork() as uow:
        for playlist_name, playlist_description, playlist_id in spotify_playlists:
            # Check if playlist exists in database
            if playlist_id in existing_playlists:
                existing_playlist = existing_playlists[playlist_id]

                # Check if playlist details have changed
                if (existing_playlist.name != playlist_name.strip() or
                        existing_playlist.description != playlist_description):

                    # Update the playlist
                    existing_playlist.name = playlist_name.strip()
                    existing_playlist.description = playlist_description
                    uow.playlist_repository.update(existing_playlist)
                    updated_count += 1
                    sync_logger.info(f"Updated playlist: {playlist_name} (ID: {playlist_id})")
                else:
                    unchanged_count += 1
                    sync_logger.debug(f"Playlist unchanged: {playlist_name} (ID: {playlist_id})")
            else:
                # Create new playlist
                new_playlist = Playlist(
                    playlist_id=playlist_id,
                    name=playlist_name.strip(),
                    description=playlist_description
                )
                uow.playlist_repository.insert(new_playlist)
                added_count += 1
                sync_logger.info(f"Added new playlist: {playlist_name} (ID: {playlist_id})")

    print(f"Playlist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged")
    sync_logger.info(
        f"Playlist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged")

    return added_count, updated_count, unchanged_count


def sync_master_tracks_incremental(master_playlist_id: str, force_full_refresh: bool = False) -> Tuple[int, int, int]:
    """
    Incrementally sync tracks from the MASTER playlist to the database.
    Only fetches and updates tracks that have changed.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a full refresh, ignoring existing data

    Returns:
        Tuple of (added, updated, unchanged) counts
    """
    sync_logger.info(f"Starting incremental master tracks sync for playlist {master_playlist_id}")
    print("Starting incremental master tracks sync analysis...")

    # Get existing tracks from database
    existing_tracks = get_db_tracks() if not force_full_refresh else {}
    sync_logger.info(f"Found {len(existing_tracks)} existing tracks in database")

    # Fetch all tracks from the MASTER playlist
    spotify_client = authenticate_spotify()
    master_tracks = fetch_master_tracks(spotify_client, master_playlist_id, force_refresh=force_full_refresh)
    sync_logger.info(f"Fetched {len(master_tracks)} tracks from MASTER playlist")

    # Find which playlists each track belongs to
    tracks_with_playlists = []

    # Process in batches to avoid making too many API calls at once
    batch_size = 50
    for i in range(0, len(master_tracks), batch_size):
        batch = master_tracks[i:i + batch_size]
        batch_with_playlists = find_playlists_for_tracks_from_db(spotify_client, batch, master_playlist_id)
        tracks_with_playlists.extend(batch_with_playlists)
        sync_logger.info(f"Processed batch {i // batch_size + 1}/{(len(master_tracks) + batch_size - 1) // batch_size}")

        # Slight delay to avoid hitting rate limits
        if i + batch_size < len(master_tracks):
            time.sleep(0.5)

    # Analyze changes (without applying them yet)
    tracks_to_add = []
    tracks_to_update = []
    unchanged_tracks = []

    for track_data in tracks_with_playlists:
        track_id, track_title, artist_names, album_name, added_at, playlist_names = track_data

        # Check if track exists in database
        if track_id in existing_tracks:
            existing_track = existing_tracks[track_id]

            # Check if track details have changed
            if (existing_track.title != track_title or
                    existing_track.artists != artist_names or
                    existing_track.album != album_name):

                # Mark for update
                tracks_to_update.append({
                    'id': track_id,
                    'title': track_title,
                    'artists': artist_names,
                    'album': album_name,
                    'old_title': existing_track.title,
                    'old_artists': existing_track.artists,
                    'old_album': existing_track.album,
                    'playlists': playlist_names
                })
            else:
                unchanged_tracks.append(track_id)
        else:
            # Mark for addition
            tracks_to_add.append({
                'id': track_id,
                'title': track_title,
                'artists': artist_names,
                'album': album_name,
                'playlists': playlist_names
            })

    # Display summary of changes
    print("\nSYNC ANALYSIS COMPLETE")
    print("=====================")
    print(f"\nTracks to add: {len(tracks_to_add)}")
    print(f"Tracks to update: {len(tracks_to_update)}")
    print(f"Unchanged tracks: {len(unchanged_tracks)}")

    # Display detailed changes
    if tracks_to_add:
        print("\nNEW TRACKS TO ADD:")
        print("=================")
        # Sort by artist for better readability
        sorted_tracks = sorted(tracks_to_add, key=lambda x: x['artists'] + x['title'])
        for track in sorted_tracks:
            print(f"• {track['artists']} - {track['title']} ({track['album']})")
            if track['playlists']:
                print(f"  In playlists: {', '.join(track['playlists'])}")
            else:
                print("  Not in any playlists")

    if tracks_to_update:
        print("\nTRACKS TO UPDATE:")
        print("================")
        sorted_updates = sorted(tracks_to_update, key=lambda x: x['artists'] + x['title'])
        for track in sorted_updates:
            print(f"• {track['id']}: {track['old_artists']} - {track['old_title']}")
            print(f"  → {track['artists']} - {track['title']}")
            if track['old_album'] != track['album']:
                print(f"  Album: {track['old_album']} → {track['album']}")

    # Ask for confirmation
    if tracks_to_add or tracks_to_update:
        confirmation = input("\nWould you like to proceed with these changes to the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Sync cancelled by user")
            print("Sync cancelled.")
            return 0, 0, len(unchanged_tracks)
    else:
        print("\nNo changes needed. Database is up to date.")
        return 0, 0, len(unchanged_tracks)

    # If confirmed, apply the changes
    added_count = 0
    updated_count = 0

    print("\nApplying changes to database...")
    with UnitOfWork() as uow:
        # Add new tracks
        for track_data in tracks_to_add:
            track_id = track_data['id']
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            playlist_names = track_data['playlists']

            # Get the added_at date from the original master_tracks data
            added_at = next((t[4] for t in master_tracks if t[0] == track_id), None)

            # Create new track
            new_track = Track(
                track_id=track_id,
                title=track_title,
                artists=artist_names,
                album=album_name,
                added_to_master=added_at
            )
            uow.track_repository.insert(new_track)
            added_count += 1
            sync_logger.info(f"Added new track: {track_title} (ID: {track_id})")

            # Update playlist associations
            sync_track_playlist_associations(uow, track_id, playlist_names)

        # Update existing tracks
        for track_data in tracks_to_update:
            track_id = track_data['id']
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            playlist_names = track_data['playlists']

            # Get the existing track
            existing_track = existing_tracks[track_id]

            # Update the track
            existing_track.title = track_title
            existing_track.artists = artist_names
            existing_track.album = album_name
            uow.track_repository.update(existing_track)
            updated_count += 1
            sync_logger.info(f"Updated track: {track_title} (ID: {track_id})")

            # Update playlist associations
            sync_track_playlist_associations(uow, track_id, playlist_names)

    print(f"\nSync complete: {added_count} added, {updated_count} updated, {len(unchanged_tracks)} unchanged")
    sync_logger.info(
        f"Master tracks sync complete: {added_count} added, {updated_count} updated, {len(unchanged_tracks)} unchanged")

    return added_count, updated_count, len(unchanged_tracks)


def find_playlists_for_tracks_from_db(spotify_client, track_batch, master_playlist_id):
    """
    Find which playlists each track belongs to, using database data when possible.

    Args:
        spotify_client: Authenticated Spotify client
        track_batch: Batch of tracks to process
        master_playlist_id: ID of the master playlist

    Returns:
        List of track data with playlists
    """
    from drivers.spotify_client import find_playlists_for_master_tracks

    # Use the optimized function that checks database first
    return find_playlists_for_master_tracks(
        spotify_client,
        track_batch,
        master_playlist_id,
        use_db_first=True
    )


def sync_track_playlist_associations(uow, track_id, playlist_names):
    """
    Sync track-playlist associations for a single track.

    Args:
        uow: Active unit of work
        track_id: ID of the track
        playlist_names: Names of playlists the track should be in
    """
    # Get all playlists by name
    playlist_ids = []
    for playlist_name in playlist_names:
        playlist = uow.playlist_repository.get_by_name(playlist_name)
        if playlist:
            playlist_ids.append(playlist.playlist_id)

    # Get current associations
    current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_track(track_id))

    # Calculate what needs to be added and removed
    playlist_ids_to_add = set(playlist_ids) - current_playlist_ids
    playlist_ids_to_remove = current_playlist_ids - set(playlist_ids)

    # Add new associations
    for playlist_id in playlist_ids_to_add:
        uow.track_playlist_repository.insert(track_id, playlist_id)
        sync_logger.debug(f"Added association: Track {track_id} to Playlist {playlist_id}")

    # Remove old associations
    for playlist_id in playlist_ids_to_remove:
        uow.track_playlist_repository.delete(track_id, playlist_id)
        sync_logger.debug(f"Removed association: Track {track_id} from Playlist {playlist_id}")
