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


def sync_playlists_incremental(force_full_refresh=False, auto_confirm=False):
    """
    Incrementally sync playlists from Spotify to the database.
    Only fetches and updates playlists that have changed.

    Args:
        force_full_refresh: Whether to force a full refresh, ignoring existing data
        auto_confirm: Whether to skip the confirmation prompt

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

    # Track changes for analysis
    playlists_to_add = []
    playlists_to_update = []
    unchanged_count = 0

    # Analyze changes (without applying them yet)
    for playlist_name, playlist_description, playlist_id in spotify_playlists:
        # Check if playlist exists in database
        if playlist_id in existing_playlists:
            existing_playlist = existing_playlists[playlist_id]

            # Check if playlist details have changed
            if (existing_playlist.name != playlist_name.strip() or
                    existing_playlist.description != playlist_description):

                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'description': playlist_description,
                    'old_name': existing_playlist.name,
                    'old_description': existing_playlist.description
                })
            else:
                unchanged_count += 1
        else:
            # Mark for addition
            playlists_to_add.append({
                'id': playlist_id,
                'name': playlist_name.strip(),
                'description': playlist_description
            })

    # Display summary of changes
    print("\nPLAYLIST SYNC ANALYSIS COMPLETE")
    print("==============================")
    print(f"\nPlaylists to add: {len(playlists_to_add)}")
    print(f"Playlists to update: {len(playlists_to_update)}")
    print(f"Unchanged playlists: {unchanged_count}")

    # Display detailed changes
    if playlists_to_add:
        print("\nNEW PLAYLISTS TO ADD:")
        print("====================")
        # Sort by name for better readability
        sorted_playlists = sorted(playlists_to_add, key=lambda x: x['name'])
        for playlist in sorted_playlists:
            print(f"• {playlist['name']}")
            if playlist['description']:
                print(f"  Description: {playlist['description']}")

    if playlists_to_update:
        print("\nPLAYLISTS TO UPDATE:")
        print("===================")
        sorted_updates = sorted(playlists_to_update, key=lambda x: x['name'])
        for playlist in sorted_updates:
            print(f"• {playlist['old_name']} → {playlist['name']}")
            if playlist['old_description'] != playlist['description']:
                if playlist['old_description'] and playlist['description']:
                    print(f"  Description changed")
                elif playlist['description']:
                    print(f"  Description added")
                else:
                    print(f"  Description removed")

    # Ask for confirmation if there are changes (unless auto_confirm is True)
    if (playlists_to_add or playlists_to_update) and not auto_confirm:
        confirmation = input("\nWould you like to proceed with these changes to the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Playlist sync cancelled by user")
            print("Sync cancelled.")
            return 0, 0, unchanged_count
    else:
        print("\nNo changes needed. Database is up to date.")
        return 0, 0, unchanged_count

    # If confirmed, apply the changes
    added_count = 0
    updated_count = 0

    print("\nApplying changes to database...")
    with UnitOfWork() as uow:
        # Add new playlists
        for playlist_data in playlists_to_add:
            # Create new playlist
            new_playlist = Playlist(
                playlist_id=playlist_data['id'],
                name=playlist_data['name'],
                description=playlist_data['description']
            )
            uow.playlist_repository.insert(new_playlist)
            added_count += 1
            sync_logger.info(f"Added new playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

        # Update existing playlists
        for playlist_data in playlists_to_update:
            # Get the existing playlist
            existing_playlist = existing_playlists[playlist_data['id']]

            # Update the playlist
            existing_playlist.name = playlist_data['name']
            existing_playlist.description = playlist_data['description']
            uow.playlist_repository.update(existing_playlist)
            updated_count += 1
            sync_logger.info(f"Updated playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

    print(f"\nPlaylist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged")
    sync_logger.info(
        f"Playlist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged")

    return added_count, updated_count, unchanged_count


def sync_master_tracks_incremental(master_playlist_id: str, force_full_refresh: bool = False) -> Tuple[int, int, int]:
    """
    Incrementally sync tracks from the MASTER playlist to the database.
    Only fetches and updates tracks that have changed.
    Now shows track-playlist association changes and asks for confirmation.

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
        # Pass force_full_refresh to ensure cache is refreshed
        batch_with_playlists = find_playlists_for_tracks_from_db(
            spotify_client,
            batch,
            master_playlist_id,
            force_refresh=force_full_refresh
        )
        tracks_with_playlists.extend(batch_with_playlists)
        sync_logger.info(f"Processed batch {i // batch_size + 1}/{(len(master_tracks) + batch_size - 1) // batch_size}")

        # Slight delay to avoid hitting rate limits
        if i + batch_size < len(master_tracks):
            time.sleep(0.5)

    # Analyze changes (without applying them yet)
    tracks_to_add = []
    tracks_to_update = []
    unchanged_tracks = []

    # Track playlist association changes
    association_changes = {}

    for track_data in tracks_with_playlists:
        track_id, track_title, artist_names, album_name, added_at, playlist_names = track_data

        # Prepare for association tracking
        association_changes[track_id] = {
            'title': track_title,
            'artists': artist_names,
            'new_playlists': set(playlist_names),
            'old_playlists': set(),
            'to_add': set(),
            'to_remove': set()
        }

        # Check if track exists in database
        if track_id in existing_tracks:
            existing_track = existing_tracks[track_id]

            # Get current playlist associations from database
            with UnitOfWork() as uow:
                current_playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track_id)
                # Get playlist names for these IDs
                for pid in current_playlist_ids:
                    playlist = uow.playlist_repository.get_by_id(pid)
                    if playlist:
                        association_changes[track_id]['old_playlists'].add(playlist.name)

            # Calculate playlist changes
            association_changes[track_id]['to_add'] = association_changes[track_id]['new_playlists'] - \
                                                      association_changes[track_id]['old_playlists']
            association_changes[track_id]['to_remove'] = association_changes[track_id]['old_playlists'] - \
                                                         association_changes[track_id]['new_playlists']

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
            # For new tracks, all playlists are additions
            association_changes[track_id]['to_add'] = association_changes[track_id]['new_playlists']

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

    # Count playlist association changes
    total_associations_to_add = sum(len(changes['to_add']) for changes in association_changes.values())
    total_associations_to_remove = sum(len(changes['to_remove']) for changes in association_changes.values())

    print(f"\nPlaylist associations to add: {total_associations_to_add}")
    print(f"Playlist associations to remove: {total_associations_to_remove}")

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

    # Show playlist association changes if there are any
    if total_associations_to_add > 0 or total_associations_to_remove > 0:
        print("\nPLAYLIST ASSOCIATION CHANGES:")
        print("===========================")

        # Get tracks with association changes
        tracks_with_association_changes = {
            tid: changes for tid, changes in association_changes.items()
            if changes['to_add'] or changes['to_remove']
        }

        # Sort by artist and title
        sorted_track_ids = sorted(
            tracks_with_association_changes.keys(),
            key=lambda tid: f"{association_changes[tid]['artists']} - {association_changes[tid]['title']}"
        )

        # Display changes for up to 10 tracks (to avoid overwhelming output)
        max_tracks_to_show = min(10, len(sorted_track_ids))
        for i, track_id in enumerate(sorted_track_ids[:max_tracks_to_show]):
            changes = association_changes[track_id]
            track_name = f"{changes['artists']} - {changes['title']}"

            print(f"\n{i + 1}. {track_name}")

            if changes['to_add']:
                print(f"   Adding to playlists: {', '.join(sorted(changes['to_add']))}")

            if changes['to_remove']:
                print(f"   Removing from playlists: {', '.join(sorted(changes['to_remove']))}")

        if len(sorted_track_ids) > max_tracks_to_show:
            remaining = len(sorted_track_ids) - max_tracks_to_show
            print(f"\n...and {remaining} more tracks with playlist changes")

        # Display summary of all changes
        print(f"\nTotal tracks with playlist changes: {len(tracks_with_association_changes)}")
        print(f"Total playlists associations to add: {total_associations_to_add}")
        print(f"Total playlists associations to remove: {total_associations_to_remove}")

    # Ask for confirmation
    if tracks_to_add or tracks_to_update or total_associations_to_add > 0 or total_associations_to_remove > 0:
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

        # For unchanged tracks, update playlist associations only if needed
        for track_id in unchanged_tracks:
            # Get the current playlists for this track from our analysis
            current_playlist_names = next((t[5] for t in tracks_with_playlists if t[0] == track_id), [])

            # Only update if there are playlist association changes
            if track_id in association_changes and (
                    association_changes[track_id]['to_add'] or association_changes[track_id]['to_remove']):
                sync_track_playlist_associations(uow, track_id, current_playlist_names)
                sync_logger.info(f"Updated playlist associations for unchanged track: {track_id}")

    print(f"\nSync complete: {added_count} added, {updated_count} updated, {len(unchanged_tracks)} unchanged")
    print(f"Playlist associations: {total_associations_to_add} added, {total_associations_to_remove} removed")

    sync_logger.info(
        f"Master tracks sync complete: {added_count} added, {updated_count} updated, {len(unchanged_tracks)} unchanged. "
        f"Playlist associations: {total_associations_to_add} added, {total_associations_to_remove} removed."
    )

    return added_count, updated_count, len(unchanged_tracks)


def find_playlists_for_tracks_from_db(spotify_client, track_batch, master_playlist_id, force_refresh=False):
    """
    Find which playlists each track belongs to, using database data when possible.

    Args:
        spotify_client: Authenticated Spotify client
        track_batch: Batch of tracks to process
        master_playlist_id: ID of the master playlist
        force_refresh: Whether to force a refresh from the API

    Returns:
        List of track data with playlists
    """
    from drivers.spotify_client import find_playlists_for_master_tracks

    # Use the optimized function that checks database first
    return find_playlists_for_master_tracks(
        spotify_client,
        track_batch,
        master_playlist_id,
        use_db_first=True,
        force_refresh=force_refresh
    )


def sync_track_playlist_associations(uow, track_id, playlist_names):
    """
    Sync track-playlist associations for a single track.
    Handles whitespace and is more verbose about failed lookups.

    Args:
        uow: Active unit of work
        track_id: ID of the track
        playlist_names: Names of playlists the track should be in

    Returns:
        Dictionary with counts of added, removed, and unchanged associations
    """
    # Get all playlists by name
    playlist_ids = []
    missing_playlists = []
    found_playlists = []

    # For logging and troubleshooting
    sync_logger = setup_logger('sync_helper', 'logs/sync.log')

    # Normalize playlist names by stripping whitespace
    normalized_playlist_names = [name.strip() if name else name for name in playlist_names]

    # Try to get more information for debugging
    track_info = None
    try:
        track = uow.track_repository.get_by_id(track_id)
        if track:
            track_info = f"{track.artists} - {track.title}"
        else:
            track_info = f"ID:{track_id}"
    except:
        track_info = f"ID:{track_id}"

    sync_logger.info(f"Syncing associations for track: {track_info}")
    sync_logger.info(f"Requested playlists: {normalized_playlist_names}")

    # First, try the original logic with exact names
    for playlist_name in normalized_playlist_names:
        playlist = uow.playlist_repository.get_by_name(playlist_name)
        if playlist:
            playlist_ids.append(playlist.playlist_id)
            found_playlists.append(playlist_name)
        else:
            missing_playlists.append(playlist_name)

    # If we couldn't find some playlists, log detailed information
    if missing_playlists:
        sync_logger.warning(f"Could not find playlists by exact name: {missing_playlists}")

        # Try to debug the issue - get all playlists for comparison
        all_playlists = uow.playlist_repository.get_all()
        all_names = [p.name for p in all_playlists]

        sync_logger.info(f"All available playlists in DB: {all_names}")

        # Try to find close matches
        for missing_name in missing_playlists:
            # Try a more flexible search
            similar_playlists = uow.playlist_repository.find_by_name(missing_name)
            if similar_playlists:
                names = [p.name for p in similar_playlists]
                sync_logger.info(f"Found similar playlists for '{missing_name}': {names}")

                # If there's only one similar playlist, use it
                if len(similar_playlists) == 1:
                    playlist = similar_playlists[0]
                    playlist_ids.append(playlist.playlist_id)
                    found_playlists.append(playlist.name)
                    sync_logger.info(f"Using similar playlist: '{playlist.name}' for requested '{missing_name}'")

    # Get current associations
    current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_track(track_id))

    # For better debugging, get names of current playlists
    current_playlist_names = []
    for pid in current_playlist_ids:
        playlist = uow.playlist_repository.get_by_id(pid)
        if playlist:
            current_playlist_names.append(playlist.name)
        else:
            current_playlist_names.append(f"Unknown({pid})")

    sync_logger.info(f"Current playlists in DB: {current_playlist_names}")

    # Calculate what needs to be added and removed
    playlist_ids_to_add = set(playlist_ids) - current_playlist_ids
    playlist_ids_to_remove = current_playlist_ids - set(playlist_ids)

    # Add new associations
    for playlist_id in playlist_ids_to_add:
        uow.track_playlist_repository.insert(track_id, playlist_id)
        # Try to get playlist name for better logging
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        playlist_name = playlist.name if playlist else f"ID:{playlist_id}"
        sync_logger.info(f"Added association: Track {track_info} to Playlist '{playlist_name}'")

    # Remove old associations
    for playlist_id in playlist_ids_to_remove:
        uow.track_playlist_repository.delete(track_id, playlist_id)
        # Try to get playlist name
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        playlist_name = playlist.name if playlist else f"ID:{playlist_id}"
        sync_logger.info(f"Removed association: Track {track_info} from Playlist '{playlist_name}'")

    # Return summary of changes
    return {
        "added": len(playlist_ids_to_add),
        "removed": len(playlist_ids_to_remove),
        "unchanged": len(current_playlist_ids) - len(playlist_ids_to_remove),
        "missing_playlists": len(missing_playlists) - len(found_playlists)
    }


def analyze_playlists_changes(force_full_refresh=False):
    """
    Analyze what changes would be made to playlists without executing them.

    Args:
        force_full_refresh: Whether to force a full refresh, ignoring existing data

    Returns:
        Tuple of (added_count, updated_count, unchanged_count, changes_details)
    """
    # Get existing playlists from database
    existing_playlists = get_db_playlists() if not force_full_refresh else {}

    # Fetch all playlists from Spotify
    spotify_client = authenticate_spotify()
    spotify_playlists = fetch_playlists(spotify_client, force_refresh=force_full_refresh)

    # Track changes for analysis
    playlists_to_add = []
    playlists_to_update = []
    unchanged_count = 0

    # Analyze changes
    for playlist_name, playlist_description, playlist_id in spotify_playlists:
        # Check if playlist exists in database
        if playlist_id in existing_playlists:
            existing_playlist = existing_playlists[playlist_id]

            # Check if playlist details have changed
            if (existing_playlist.name != playlist_name.strip() or
                    existing_playlist.description != playlist_description):

                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'description': playlist_description,
                    'old_name': existing_playlist.name,
                    'old_description': existing_playlist.description
                })
            else:
                unchanged_count += 1
        else:
            # Mark for addition
            playlists_to_add.append({
                'id': playlist_id,
                'name': playlist_name.strip(),
                'description': playlist_description
            })

    return len(playlists_to_add), len(playlists_to_update), unchanged_count, {
        'to_add': playlists_to_add,
        'to_update': playlists_to_update
    }


def analyze_tracks_changes(master_playlist_id: str, force_full_refresh: bool = False):
    """
    Analyze what changes would be made to tracks without executing them.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a full refresh, ignoring existing data

    Returns:
        Tuple of (tracks_to_add, tracks_to_update, unchanged_tracks)
    """
    # Get existing tracks from database
    existing_tracks = get_db_tracks() if not force_full_refresh else {}

    # Fetch all tracks from the MASTER playlist
    spotify_client = authenticate_spotify()
    master_tracks = fetch_master_tracks(spotify_client, master_playlist_id, force_refresh=force_full_refresh)

    # Find which playlists each track belongs to
    tracks_with_playlists = []

    # Process in smaller batches to avoid API overload
    batch_size = 50
    for i in range(0, len(master_tracks), batch_size):
        batch = master_tracks[i:i + batch_size]
        batch_with_playlists = find_playlists_for_tracks_from_db(spotify_client, batch, master_playlist_id)
        tracks_with_playlists.extend(batch_with_playlists)

        # Slight delay to avoid hitting rate limits
        if i + batch_size < len(master_tracks):
            time.sleep(0.5)

    # Analyze changes without applying them
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

    return tracks_to_add, tracks_to_update, unchanged_tracks
