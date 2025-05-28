import hashlib
import time
from datetime import datetime
from typing import Dict, Tuple, List

from drivers.spotify_client import (
    authenticate_spotify,
    fetch_playlists,
    fetch_master_tracks,
    get_playlist_track_ids
)
from helpers.file_helper import parse_local_file_uri, generate_local_track_id
from sql.core.unit_of_work import UnitOfWork
from sql.dto.playlist_info import PlaylistInfo
from sql.models.playlist import Playlist
from sql.models.track import Track
from utils.logger import setup_logger

sync_logger = setup_logger('sync_helper', 'sql', 'sync.log')


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


def sync_playlists_to_db(force_full_refresh=False, auto_confirm=False, precomputed_changes: Dict = None,
                         exclusion_config=None):
    """
    Sync playlists from Spotify to the database.
    Only fetches and updates playlists that have changed based on snapshot_id.

    Returns:
        Tuple of (added, updated, unchanged, deleted) counts
    """
    sync_logger.info("Starting incremental playlist sync")
    print("Starting incremental playlist sync...")

    playlists_to_add = []
    playlists_to_update = []
    playlists_to_delete = []
    unchanged_count = 0

    existing_playlists = get_db_playlists()
    sync_logger.info(f"Found {len(existing_playlists)} existing playlists in database")

    if precomputed_changes:
        playlists_to_add = precomputed_changes.get('to_add', playlists_to_add)
        playlists_to_update = precomputed_changes.get('to_update', playlists_to_update)
        playlists_to_delete = precomputed_changes.get('to_delete', playlists_to_delete)
        unchanged_count = precomputed_changes.get('unchanged', unchanged_count)

        # Ensure all playlist entries have a snapshot_id
        for playlist in playlists_to_add:
            if 'snapshot_id' not in playlist:
                playlist['snapshot_id'] = ''

        for playlist in playlists_to_update:
            if 'snapshot_id' not in playlist:
                playlist['snapshot_id'] = playlist.get('old_snapshot_id', '')

        sync_logger.info(f"Using precomputed changes: {len(playlists_to_add)} to add, "
                         f"{len(playlists_to_update)} to update, {len(playlists_to_delete)} to delete")
    else:
        # Fetch all playlists from Spotify
        spotify_client = authenticate_spotify()
        spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, force_refresh=force_full_refresh,
                                                                exclusion_config=exclusion_config)
        sync_logger.info(f"Fetched {len(spotify_playlists)} playlists from Spotify")

        # Track changes for analysis
        playlists_to_add = []
        playlists_to_update = []
        playlists_to_delete = []
        unchanged_count = 0

        # Create a set of playlist IDs returned from Spotify
        spotify_playlist_ids = set(playlist.playlist_id for playlist in spotify_playlists)

        # Analyze changes (without applying them yet)
        for playlist in spotify_playlists:
            playlist_name = playlist.name
            playlist_id = playlist.playlist_id
            snapshot_id = playlist.snapshot_id

            # Check if playlist exists in database
            if playlist_id in existing_playlists:
                existing_playlist = existing_playlists[playlist_id]

                # Check if playlist details have changed
                if (existing_playlist.name != playlist_name.strip() or
                        existing_playlist.snapshot_id != snapshot_id):

                    # Mark for update
                    playlists_to_update.append({
                        'id': playlist_id,
                        'name': playlist_name.strip(),
                        'old_name': existing_playlist.name,
                        'snapshot_id': snapshot_id,
                        'old_snapshot_id': existing_playlist.snapshot_id,
                    })
                else:
                    unchanged_count += 1
            else:
                # Mark for addition
                playlists_to_add.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'snapshot_id': snapshot_id
                })

        # Find playlists to delete (in database but not in Spotify results)
        for playlist_id, playlist in existing_playlists.items():
            if playlist_id not in spotify_playlist_ids:
                playlists_to_delete.append({
                    'id': playlist_id,
                    'name': playlist.name
                })

    # Display summary of changes
    print("\nPLAYLIST SYNC ANALYSIS COMPLETE")
    print("==============================")
    print(f"\nPlaylists to add: {len(playlists_to_add)}")
    print(f"Playlists to update: {len(playlists_to_update)}")
    print(f"Playlists to delete: {len(playlists_to_delete)}")
    print(f"Unchanged playlists: {unchanged_count}")

    # Display detailed changes
    if playlists_to_add:
        print("\nNEW PLAYLISTS TO ADD:")
        print("====================")
        # Sort by name for better readability
        sorted_playlists = sorted(playlists_to_add, key=lambda x: x['name'])
        for playlist in sorted_playlists:
            print(f"• {playlist['name']}")

    if playlists_to_update:
        print("\nPLAYLISTS TO UPDATE:")
        print("===================")
        sorted_updates = sorted(playlists_to_update, key=lambda x: x['name'])
        for playlist in sorted_updates:
            print(f"• {playlist['old_name']} → {playlist['name']}")

    if playlists_to_delete:
        print("\nPLAYLISTS TO DELETE:")
        print("===================")
        sorted_deletes = sorted(playlists_to_delete, key=lambda x: x['name'])
        for playlist in sorted_deletes:
            print(f"• {playlist['name']}")

    # Ask for confirmation if there are changes (unless auto_confirm is True)
    if (playlists_to_add or playlists_to_update or playlists_to_delete) and not auto_confirm:
        confirmation = input("\nWould you like to proceed with these changes to the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Playlist sync cancelled by user")
            print("Sync cancelled.")
            return 0, 0, unchanged_count, 0
    elif not playlists_to_add and not playlists_to_update and not playlists_to_delete:
        print("\nNo changes needed. Database is up to date.")
        return 0, 0, unchanged_count, 0

    # If confirmed, apply the changes
    added_count = 0
    updated_count = 0
    deleted_count = 0

    print("\nApplying changes to database...")
    with UnitOfWork() as uow:
        # Add new playlists
        for playlist_data in playlists_to_add:
            # Create new playlist (now with snapshot_id)
            new_playlist = Playlist(
                playlist_id=playlist_data['id'],
                name=playlist_data['name'],
                snapshot_id=playlist_data['snapshot_id']
            )
            uow.playlist_repository.insert(new_playlist)
            added_count += 1
            sync_logger.info(f"Added new playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

        # Update existing playlists
        for playlist_data in playlists_to_update:
            # Get the existing playlist
            existing_playlist = existing_playlists[playlist_data['id']]

            # Update the playlist with new data
            existing_playlist.name = playlist_data['name']
            existing_playlist.snapshot_id = playlist_data['snapshot_id']
            uow.playlist_repository.update(existing_playlist)
            updated_count += 1
            sync_logger.info(f"Updated playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

        # Delete playlists that are no longer present in Spotify or match exclusion criteria
        for playlist_data in playlists_to_delete:
            # First delete all track associations for this playlist
            uow.track_playlist_repository.delete_by_playlist_id(playlist_data['id'])

            # Then delete the playlist itself
            uow.playlist_repository.delete(playlist_data['id'])
            deleted_count += 1
            sync_logger.info(f"Deleted playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

    print(
        f"\nPlaylist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged, {deleted_count} deleted")
    sync_logger.info(
        f"Playlist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged, {deleted_count} deleted"
    )

    return added_count, updated_count, unchanged_count, deleted_count


def analyze_track_playlist_associations(master_playlist_id: str, force_full_refresh: bool = False,
                                        exclusion_config=None) -> dict:
    """
    Analyze what changes would be made to track-playlist associations without actually making them.
    Only analyzes playlists that have changed since the last sync based on snapshot_id.
    """
    sync_logger.info("Analyzing track-playlist association changes")

    # Get all tracks from database
    with UnitOfWork() as uow:
        all_tracks_in_db = uow.track_repository.get_all()
        # Create a lookup dictionary for tracks by ID for quick access
        tracks_by_id = {track.track_id: track for track in all_tracks_in_db}
        total_tracks = len(all_tracks_in_db)
        sync_logger.info(f"Found {total_tracks} tracks in database")

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists_in_db = uow.playlist_repository.get_all()
        total_playlists = len(all_playlists_in_db)
        sync_logger.info(f"Found {total_playlists} playlists in database")

    # Fetch all playlists directly from Spotify to ensure associations are fresh
    spotify_client = authenticate_spotify()
    spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, force_refresh=force_full_refresh,
                                                            exclusion_config=exclusion_config)

    # Filter out the master playlist for association lookups
    all_playlists_except_master_api = [pl for pl in spotify_playlists if pl.playlist_id != master_playlist_id]

    # Filter to only process playlists that have changed based on snapshot_id
    changed_playlists = []
    unchanged_playlists = []
    changed_playlist_names = []

    for playlist in all_playlists_except_master_api:
        # Find the corresponding playlist in the database
        db_playlist = None
        for pl in all_playlists_in_db:
            if pl.playlist_id == playlist.playlist_id:
                db_playlist = pl
                break

        # If playlist exists in database, check if snapshot_id has changed
        if db_playlist:
            if force_full_refresh or db_playlist.snapshot_id != playlist.snapshot_id:
                changed_playlists.append(playlist)
                changed_playlist_names.append(playlist.name)
            else:
                unchanged_playlists.append(playlist)
        else:
            # New playlist not in database, always include it
            changed_playlists.append(playlist)
            changed_playlist_names.append(playlist.name)

    print(f"Found {len(changed_playlists)} playlists that have changed since last sync")
    print(f"Skipping {len(unchanged_playlists)} unchanged playlists")

    if len(unchanged_playlists) > 0:
        print(
            f"Efficiency gain: Only processing {len(changed_playlists)}/{len(all_playlists_except_master_api)} playlists ({(len(changed_playlists) / len(all_playlists_except_master_api) * 100):.1f}%)")

    if not changed_playlists and not force_full_refresh:
        return {
            "tracks_with_changes": [],
            "associations_to_add": 0,
            "associations_to_remove": 0,
            "samples": [],
            "all_changes": [],
            "stats": {
                "tracks_with_playlists": 0,
                "tracks_without_playlists": 0,
                "total_associations": 0,
                "unchanged_playlists": len(unchanged_playlists),
                "changed_playlists": 0
            },
            "changed_playlist_names": changed_playlist_names
        }

    # We'll track all associations to completely refresh the database
    track_playlist_map = {}

    print("Fetching track-playlist associations from Spotify...")

    # First, build a list of all track IDs for reference
    all_track_ids = {track.track_id for track in all_tracks_in_db}

    # Only process the changed playlists
    for i, playlist_info in enumerate(changed_playlists, 1):
        playlist_name = playlist_info[0] if isinstance(playlist_info, tuple) else playlist_info.name
        playlist_id = playlist_info[1] if isinstance(playlist_info, tuple) else playlist_info.playlist_id

        print(f"Processing playlist {i}/{len(changed_playlists)}: {playlist_name}")

        # Always force a fresh API call to get the most up-to-date associations
        playlist_track_ids = get_playlist_track_ids(spotify_client, playlist_id, force_refresh=True)

        # Log number of local files
        local_files = [tid for tid in playlist_track_ids if tid.startswith('local_')]
        sync_logger.info(f"Found {len(local_files)} local files in playlist '{playlist_name}'")

        # Add special handling for local files
        for track_id in playlist_track_ids:
            # Check for different ID formats for local files
            is_local_file = track_id.startswith('local_') or track_id.startswith('spotify:local:')

            # Normalize local file IDs
            if is_local_file and track_id.startswith('spotify:local:'):
                # Extract data and create consistent ID
                parsed_local = parse_local_file_uri(track_id)
                metadata_string = f"{parsed_local.title}_{parsed_local.artist}"
                normalized_id = f"local_{hashlib.md5(metadata_string.encode()).hexdigest()[:16]}"
                track_id = normalized_id

        # Log how many tracks were found for this playlist
        valid_tracks = [tid for tid in playlist_track_ids if tid in all_track_ids]
        sync_logger.info(f"Found {len(valid_tracks)} valid tracks in playlist '{playlist_name}'")

        # For each track in this playlist, update its associations
        for track_id in valid_tracks:
            if track_id not in track_playlist_map:
                track_playlist_map[track_id] = set()

            track_playlist_map[track_id].add(playlist_name)

        # Short delay to avoid rate limiting
        time.sleep(0.5)

    # Count some statistics for reporting
    tracks_with_playlists = len(track_playlist_map)
    total_associations = sum(len(playlists) for playlists in track_playlist_map.values())

    print(f"\nAssociation analysis complete: {tracks_with_playlists}/{total_tracks} tracks have playlist associations")
    print(f"Total associations to sync: {total_associations}")

    # Compare with existing associations to see what will actually change
    actual_changes = {}
    tracks_with_changes = []
    associations_to_add = 0
    associations_to_remove = 0
    samples = []

    print("\nAnalyzing changes in associations...")

    with UnitOfWork() as uow:
        # Check each track to see what will change
        for track_id, new_playlist_names in track_playlist_map.items():
            # Get current associations for this track
            current_playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track_id)
            current_playlist_names = []

            # Convert playlist IDs to names for comparison
            for pid in current_playlist_ids:
                playlist = uow.playlist_repository.get_by_id(pid)
                if playlist:
                    current_playlist_names.append(playlist.name)

            # Identify changes
            new_playlist_names_set = set(new_playlist_names)
            current_playlist_names_set = set(current_playlist_names)

            to_add = new_playlist_names_set - current_playlist_names_set
            to_remove = current_playlist_names_set - new_playlist_names_set

            # Only record if there are changes
            if to_add or to_remove:
                track = uow.track_repository.get_by_id(track_id)
                if track:
                    track_info = f"{track.artists} - {track.title}"
                    actual_changes[track_id] = {
                        'track_info': track_info,
                        'to_add': to_add,
                        'to_remove': to_remove
                    }
                    tracks_with_changes.append({
                        'track_id': track_id,
                        'track_info': track_info,
                        'title': track.title,
                        'artists': track.artists,
                        'add_to': list(to_add),
                        'remove_from': list(to_remove)
                    })
                    associations_to_add += len(to_add)
                    associations_to_remove += len(to_remove)

                    # Add to samples for UI
                    samples.append({
                        'track': track_info,
                        'track_info': track_info,
                        'title': track.title,
                        'artists': track.artists,
                        'add_to': list(to_add),
                        'remove_from': list(to_remove)
                    })

    # Return the analysis results with information about changed playlists
    return {
        "tracks_with_changes": tracks_with_changes,
        "associations_to_add": associations_to_add,
        "associations_to_remove": associations_to_remove,
        "samples": samples,
        "all_changes": samples,
        "stats": {
            "tracks_with_playlists": tracks_with_playlists,
            "tracks_without_playlists": len(all_track_ids) - tracks_with_playlists,
            "total_associations": total_associations,
            "unchanged_playlists": len(unchanged_playlists),
            "changed_playlists": len(changed_playlists)
        },
        "changed_playlist_names": changed_playlist_names
    }


def sync_tracks_to_db(master_playlist_id: str, force_full_refresh: bool = False,
                      auto_confirm: bool = False, precomputed_changes: dict = None) -> Tuple[
    int, int, int, int]:
    """
    Incrementally sync tracks from the MASTER playlist to the database.
    Only fetches and updates tracks that have changed. Does NOT update playlist associations.

    Returns:
        Tuple of (added, updated, unchanged, deleted) counts
    """
    sync_logger.info(f"Starting incremental master tracks sync for playlist {master_playlist_id}")
    print("Starting incremental master tracks sync analysis...")

    tracks_to_add = []
    tracks_to_update = []
    tracks_to_delete = []
    unchanged_tracks = []

    existing_tracks = get_db_tracks()
    sync_logger.info(f"Found {len(existing_tracks)} existing tracks in database")

    if precomputed_changes:
        sync_logger.info("Using precomputed changes to avoid redundant analysis")
        tracks_to_add = precomputed_changes.get('tracks_to_add', tracks_to_add)
        tracks_to_update = precomputed_changes.get('tracks_to_update', tracks_to_update)
        tracks_to_delete = precomputed_changes.get('tracks_to_delete', tracks_to_delete)
        unchanged_tracks = precomputed_changes.get('unchanged_tracks', unchanged_tracks)

        sync_logger.info(f"Precomputed changes: {len(tracks_to_add)} to add, "
                         f"{len(tracks_to_update)} to update, "
                         f"{len(unchanged_tracks) if isinstance(unchanged_tracks, list) else unchanged_tracks} unchanged")
    else:
        # Get existing tracks from database
        existing_tracks = get_db_tracks()
        sync_logger.info(f"Found {len(existing_tracks)} existing tracks in database")

        # Fetch all tracks from the MASTER playlist
        spotify_client = authenticate_spotify()
        master_tracks = fetch_master_tracks(spotify_client, master_playlist_id, force_refresh=force_full_refresh)
        sync_logger.info(f"Fetched {len(master_tracks)} tracks from MASTER playlist")

        # Analyze changes (without applying them yet)
        tracks_to_add = []
        tracks_to_update = []
        unchanged_tracks = []

        for track_data in master_tracks:
            track_id, track_title, artist_names, album_name, added_at = track_data

            # Check if this is a local file (track_id is None)
            is_local = track_id is None

            if is_local:
                # Clean the strings
                normalized_title = ''.join(c for c in track_title if c.isalnum() or c in ' &-_')
                normalized_artist = ''.join(c for c in artist_names if c.isalnum() or c in ' &-_')

                # Generate ID
                metadata = {'title': normalized_title, 'artist': normalized_artist}
                track_id = generate_local_track_id(metadata)

                # Debug output
                sync_logger.info(f"Processing local file: '{track_title}' by '{artist_names}'")
                sync_logger.info(f"Generated ID: {track_id}")
                print(f"Processing local file: '{track_title}' by '{artist_names}'")
                print(f"Generated ID: {track_id}")

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
                        'is_local': is_local,
                        'old_title': existing_track.title,
                        'old_artists': existing_track.artists,
                        'old_album': existing_track.album
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
                    'added_at': added_at,
                    'is_local': is_local
                })

        # Find tracks that are in the database but not in the master playlist
        master_track_ids = set(track_data[0] for track_data in master_tracks if track_data[0] is not None)
        # For local files, generate IDs
        for track_data in master_tracks:
            track_id, track_title, artist_names, album_name, added_at = track_data
            if track_id is None:  # Local file
                normalized_title = ''.join(c for c in track_title if c.isalnum() or c in ' &-_')
                normalized_artist = ''.join(c for c in artist_names if c.isalnum() or c in ' &-_')
                metadata = {'title': normalized_title, 'artist': normalized_artist}
                track_id = generate_local_track_id(metadata)
                master_track_ids.add(track_id)

        # Identify tracks to delete (in database but not in master playlist)
        tracks_to_delete = []
        for track_id, track in existing_tracks.items():
            if track_id not in master_track_ids:
                tracks_to_delete.append({
                    'id': track_id,
                    'title': track.title,
                    'artists': track.artists,
                    'album': track.album,
                    'is_local': track.is_local if hasattr(track, 'is_local') else False
                })

    # Display summary of changes
    print("\nMaster Tracks SYNC ANALYSIS COMPLETE")
    print("=================================")
    print(f"\nTracks to add: {len(tracks_to_add)}")
    print(f"Tracks to update: {len(tracks_to_update)}")
    print(f"Tracks to delete: {len(tracks_to_delete)}")
    if isinstance(unchanged_tracks, (list, set, tuple)):
        print(f"Unchanged tracks: {len(unchanged_tracks)}")
    else:
        print(f"Unchanged tracks: {unchanged_tracks}")

        # Display detailed changes
    if tracks_to_add:
        print("\nNEW TRACKS TO ADD:")
        print("=================")
        # Sort by artist for better readability
        sorted_tracks = sorted(tracks_to_add, key=lambda x: x['artists'] + x['title'])
        for i, track in enumerate(sorted_tracks[:10], 1):  # Show first 10
            local_indicator = " (LOCAL)" if track.get('is_local', False) else ""
            # Use .get() with defaults for fields that might be missing
            album = track.get('album', 'Unknown Album')
            print(
                f"{i}. {track.get('artists', 'Unknown Artist')} - {track.get('title', 'Untitled')} ({album}){local_indicator}")
        if len(sorted_tracks) > 10:
            print(f"...and {len(sorted_tracks) - 10} more tracks")

    if tracks_to_update:
        print("\nTRACKS TO UPDATE:")
        print("================")
        sorted_updates = sorted(tracks_to_update, key=lambda x: x['artists'] + x['title'])
        for i, track in enumerate(sorted_updates[:10], 1):  # Show first 10
            local_indicator = " (LOCAL)" if track.get('is_local', False) else ""
            print(
                f"{i}. {track.get('id', 'Unknown ID')}: {track.get('old_artists', 'Unknown Artist')} - {track.get('old_title', 'Untitled')}")
            print(f"   → {track.get('artists', 'Unknown Artist')} - {track.get('title', 'Untitled')}{local_indicator}")
            if track.get('old_album') != track.get('album'):
                old_album = track.get('old_album', 'Unknown Album')
                new_album = track.get('album', 'Unknown Album')
                print(f"     Album: {old_album} → {new_album}")
        if len(sorted_updates) > 10:
            print(f"...and {len(sorted_updates) - 10} more tracks")

    if tracks_to_delete:
        print("\nTRACKS TO DELETE:")
        print("================")
        sorted_deletes = sorted(tracks_to_delete, key=lambda x: x['artists'] + x['title'])
        for i, track in enumerate(sorted_deletes[:10], 1):  # Show first 10
            local_indicator = " (LOCAL)" if track.get('is_local', False) else ""
            album = track.get('album', 'Unknown Album')
            print(
                f"{i}. {track.get('artists', 'Unknown Artist')} - {track.get('title', 'Untitled')} ({album}){local_indicator}")
        if len(sorted_deletes) > 10:
            print(f"...and {len(sorted_deletes) - 10} more tracks")

    # Ask for confirmation
    if tracks_to_add or tracks_to_update or tracks_to_delete:
        if not auto_confirm:  # Add this condition
            confirmation = input(f"\nWould you like to proceed with these changes to the database?\n"
                                 f"Add: {len(tracks_to_add)}, Update: {len(tracks_to_update)}, "
                                 f"Delete: {len(tracks_to_delete)} (y/n): ")
            if confirmation.lower() != 'y':
                sync_logger.info("Sync cancelled by user")
                print("Sync cancelled.")
                # Handle both list and integer types
                unchanged_count = unchanged_tracks if isinstance(unchanged_tracks, int) else len(unchanged_tracks)
                return 0, 0, unchanged_count, 0
    else:
        print("\nNo track changes needed. Database is up to date.")
        # Handle both list and integer types
        unchanged_count = unchanged_tracks if isinstance(unchanged_tracks, int) else len(unchanged_tracks)
        return 0, 0, unchanged_count, 0

    # If confirmed, apply the changes
    added_count = 0
    updated_count = 0
    deleted_count = 0

    print("\nApplying track changes to database...")
    with UnitOfWork() as uow:
        # Add new tracks
        for track_data in tracks_to_add:
            track_id = track_data['id']
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            is_local = track_data['is_local']
            added_at = None
            if 'added_at' in track_data and track_data['added_at']:
                # If it's already a datetime object, use it directly
                if isinstance(track_data['added_at'], datetime):
                    added_at = track_data['added_at']
                # If it's a string, try to parse it
                elif isinstance(track_data['added_at'], str):
                    try:
                        # Try ISO format first
                        added_at = datetime.fromisoformat(track_data['added_at'].replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            # Try RFC 822 format (e.g., 'Sun, 18 May 2025 04:14:05 GMT')
                            from email.utils import parsedate_to_datetime
                            added_at = parsedate_to_datetime(track_data['added_at'])
                        except Exception:
                            try:
                                # Try RFC 822 format with strptime as fallback
                                added_at = datetime.strptime(track_data['added_at'], '%a, %d %b %Y %H:%M:%S GMT')
                            except ValueError:
                                try:
                                    # Try other common formats
                                    added_at = datetime.strptime(track_data['added_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
                                except ValueError:
                                    try:
                                        added_at = datetime.strptime(track_data['added_at'], '%Y-%m-%d %H:%M:%S')
                                    except ValueError:
                                        # If all parsing fails, log and use None
                                        sync_logger.warning(
                                            f"Could not parse date: {track_data['added_at']} for track {track_id}")
                                        added_at = None

                # If we successfully parsed a date, ensure it's in the correct format for SQL Server
                if added_at is not None:
                    # Format the datetime to match your database format
                    # This doesn't change the datetime object itself, but ensures compatibility with SQL Server
                    formatted_date_str = added_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    # For debugging
                    sync_logger.debug(f"Formatted AddedToMaster date: {formatted_date_str} for track {track_id}")

            # For newly added tracks with no added_at, you might want to use current time
            if added_at is None:
                added_at = datetime.now()
                sync_logger.debug(f"Using current time for AddedToMaster: {added_at} for track {track_id}")

            # Determine local path if it's a local file (can be enhanced later)
            if is_local:
                # This would be where you could search for the local file path
                # For now, we'll just store that it's a local file
                pass

            # Create new track
            new_track = Track(
                track_id=track_id,
                title=track_title,
                artists=artist_names,
                album=album_name,
                added_to_master=added_at,
                is_local=is_local,
            )
            uow.track_repository.insert(new_track)
            added_count += 1
            sync_logger.info(f"Added new track: {track_title} (ID: {track_id})")

        # Update existing tracks
        for track_data in tracks_to_update:
            track_id = track_data['id']
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            is_local = track_data['is_local']

            # Get the existing track
            existing_track = existing_tracks[track_id]

            # Update the track
            existing_track.title = track_title
            existing_track.artists = artist_names
            existing_track.album = album_name
            existing_track.is_local = is_local

            uow.track_repository.update(existing_track)
            updated_count += 1
            sync_logger.info(f"Updated track: {track_title} (ID: {track_id})")

        # Delete tracks that are no longer in the master playlist
        for track_data in tracks_to_delete:
            track_id = track_data['id']

            # First remove all playlist associations for this track
            try:
                uow.track_playlist_repository.delete_by_track_id(track_id)
                sync_logger.info(f"Removed playlist associations for track: {track_data['title']} (ID: {track_id})")
            except Exception as e:
                sync_logger.error(f"Error removing playlist associations for track {track_id}: {e}")
                print(f"Warning: Error removing playlist associations for track {track_id}")

            # Then delete the track itself
            try:
                uow.track_repository.delete(track_id)
                deleted_count += 1
                sync_logger.info(f"Deleted track: {track_data['title']} (ID: {track_id})")
            except Exception as e:
                sync_logger.error(f"Error deleting track {track_id}: {e}")
                print(f"Warning: Error deleting track {track_id}")

    # Log final results with deletion counts
    print(f"\nTrack sync complete: {added_count} added, {updated_count} updated, "
          f"{unchanged_tracks if isinstance(unchanged_tracks, int) else len(unchanged_tracks)} unchanged, {deleted_count} deleted")
    sync_logger.info(f"\nTrack sync complete: {added_count} added, {updated_count} updated, "
                     f"{unchanged_tracks if isinstance(unchanged_tracks, int) else len(unchanged_tracks)} unchanged, {deleted_count} deleted")

    unchanged_count = unchanged_tracks if isinstance(unchanged_tracks, int) else len(unchanged_tracks)
    return added_count, updated_count, unchanged_count, deleted_count


def sync_track_playlist_associations_to_db(master_playlist_id: str, force_full_refresh: bool = False,
                                           auto_confirm: bool = False, precomputed_changes: dict = None,
                                           exclusion_config=None) -> Dict[str, int]:
    sync_logger.info("Starting track-playlist association sync")
    print("Starting track-playlist association sync...")

    # If we have precomputed changes, use them instead of rescanning
    if precomputed_changes and 'tracks_with_changes' in precomputed_changes:
        print(f"Using precomputed changes for {len(precomputed_changes['tracks_with_changes'])} tracks")
        sync_logger.info(f"Using precomputed changes for {len(precomputed_changes['tracks_with_changes'])} tracks")

        associations_added = 0
        associations_removed = 0

        # Process each track with identified changes
        total_changes = len(precomputed_changes['tracks_with_changes'])
        for progress_index, change in enumerate(precomputed_changes['tracks_with_changes']):
            track_id = change['track_id']
            track_info = change.get('track_info', f"ID:{track_id}")
            playlists_to_add = set(change.get('add_to', []))
            playlists_to_remove = set(change.get('remove_from', []))

            with UnitOfWork() as uow:
                # Get current playlist associations
                current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_track(track_id))
                current_playlist_names = []

                # Map playlist IDs to names
                for pid in current_playlist_ids:
                    playlist = uow.playlist_repository.get_by_id(pid)
                    if playlist:
                        current_playlist_names.append(playlist.name)

                # Get current playlist name set
                current_names_set = set(current_playlist_names)

                # Calculate the new set of playlist names
                updated_names_set = (current_names_set - playlists_to_remove) | playlists_to_add

                # Update with the new set of playlists
                result = sync_track_playlist_associations_for_single_track(uow, track_id, updated_names_set)

                # Update stats
                associations_added += result["added"]
                associations_removed += result["removed"]

                # Log changes
                if result["added"] > 0 or result["removed"] > 0:
                    sync_logger.info(f"Updated associations for '{track_info}': "
                                     f"added {result['added']}, removed {result['removed']}")

                # Show progress for large operations
                if total_changes > 20 and progress_index % 10 == 0:
                    print(f"Progress: {progress_index + 1}/{total_changes} tracks processed")

        # Final stats
        stats = {
            "tracks_with_playlists": precomputed_changes.get('tracks_with_playlists', 0),
            "tracks_without_playlists": precomputed_changes.get('tracks_without_playlists', 0),
            "total_associations": precomputed_changes.get('total_associations', 0),
            "associations_added": associations_added,
            "associations_removed": associations_removed,
            "tracks_with_changes": len(precomputed_changes['tracks_with_changes'])
        }

        print(f"\nTrack-playlist association sync complete:")
        print(f"  - {associations_added} associations added")
        print(f"  - {associations_removed} associations removed")
        print(f"  - {len(precomputed_changes['tracks_with_changes'])} tracks had association changes")

        return stats

    # Get all tracks from database
    with UnitOfWork() as uow:
        all_tracks_db = uow.track_repository.get_all()
        total_tracks = len(all_tracks_db)
        sync_logger.info(f"Found {total_tracks} tracks in database")

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists_db = uow.playlist_repository.get_all()
        total_playlists = len(all_playlists_db)
        sync_logger.info(f"Found {total_playlists} playlists in database")

    # Fetch all playlists directly from Spotify to ensure associations are fresh
    spotify_client = authenticate_spotify()
    spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, force_refresh=force_full_refresh,
                                                            exclusion_config=exclusion_config)

    # Filter out the master playlist for association lookups
    all_playlists_except_master_api = [pl for pl in spotify_playlists if pl.playlist_id != master_playlist_id]

    print(
        f"Fetched {len(spotify_playlists)} playlists from Spotify ({len(all_playlists_except_master_api)} excluding MASTER)")

    # Filter to only process playlists that have changed based on snapshot_id
    changed_playlists = []
    unchanged_playlists = []

    for playlist in all_playlists_except_master_api:
        # Find the corresponding playlist in the database
        db_playlist = None
        for pl in all_playlists_db:
            if pl.playlist_id == playlist.playlist_id:
                db_playlist = pl
                break

        # If playlist exists in database, check if snapshot_id has changed
        if db_playlist:
            if force_full_refresh or db_playlist.snapshot_id != playlist.snapshot_id:
                changed_playlists.append(playlist)
                sync_logger.info(
                    f"Playlist '{playlist.name}' snapshot_id changed: {db_playlist.snapshot_id} -> {playlist.snapshot_id}")
            else:
                unchanged_playlists.append(playlist)
                sync_logger.debug(f"Playlist '{playlist.name}' unchanged (snapshot_id: {playlist.snapshot_id})")
        else:
            # New playlist not in database, always include it
            changed_playlists.append(playlist)
            sync_logger.info(f"New playlist '{playlist.name}' found, will process associations")

    print(f"Found {len(changed_playlists)} playlists that have changed since last sync")
    print(f"Skipping {len(unchanged_playlists)} unchanged playlists")

    if not changed_playlists and not force_full_refresh:
        print("No playlists have changed. Skipping association sync.")
        return {
            "tracks_with_playlists": 0,
            "tracks_without_playlists": 0,
            "total_associations": 0,
            "associations_added": 0,
            "associations_removed": 0,
            "tracks_with_changes": 0,
            "playlists_processed": 0,
            "playlists_skipped": len(unchanged_playlists),
            "no_changes": True
        }

    # We'll track all associations to completely refresh the database
    track_playlist_map = {}

    print("Fetching track-playlist associations from Spotify...")

    # First, build a list of all track IDs for reference
    all_track_ids = {track.track_id for track in all_tracks_db}

    # Only process the changed playlists
    for i, playlist in enumerate(changed_playlists, 1):
        playlist_name = playlist.name
        playlist_id = playlist.playlist_id
        snapshot_id = playlist.snapshot_id
        print(f"Processing playlist {i}/{len(changed_playlists)}: {playlist_name}")

        # Always force a fresh API call to get the most up-to-date associations
        playlist_track_ids = get_playlist_track_ids(spotify_client, playlist_id, force_refresh=True)

        # Log number of local files
        local_files = [tid for tid in playlist_track_ids if tid.startswith('local_')]
        sync_logger.info(f"Found {len(local_files)} local files in playlist '{playlist_name}'")
        print(f"Found {len(local_files)} local files in playlist '{playlist_name}'")  # Add print for debugging

        # Update the playlist's snapshot_id in the database
        with UnitOfWork() as uow:
            playlist = uow.playlist_repository.get_by_id(playlist_id)
            if playlist:
                playlist.snapshot_id = snapshot_id
                uow.playlist_repository.update(playlist)
                sync_logger.info(f"Updated snapshot_id for playlist '{playlist_name}'")
            else:
                # New playlist, create it
                new_playlist = Playlist(
                    playlist_id=playlist_id,
                    name=playlist_name,
                    snapshot_id=snapshot_id
                )
                uow.playlist_repository.insert(new_playlist)
                sync_logger.info(f"Added new playlist '{playlist_name}' with snapshot_id")

        # Add special handling for local files
        for track_id in playlist_track_ids:
            # Check for different ID formats for local files
            is_local_file = track_id.startswith('local_') or track_id.startswith('spotify:local:')

            # Normalize local file IDs
            if is_local_file and track_id.startswith('spotify:local:'):
                # Extract data and create consistent ID
                parsed_local = parse_local_file_uri(track_id)
                metadata_string = f"{parsed_local.title}_{parsed_local.artist}"
                normalized_id = f"local_{hashlib.md5(metadata_string.encode()).hexdigest()[:16]}"
                track_id = normalized_id

        # Log how many tracks were found for this playlist
        valid_tracks = [tid for tid in playlist_track_ids if tid in all_track_ids]
        sync_logger.info(f"Found {len(valid_tracks)} valid tracks in playlist '{playlist_name}'")

        # For each track in this playlist, update its associations
        for track_id in valid_tracks:
            if track_id not in track_playlist_map:
                track_playlist_map[track_id] = set()

            track_playlist_map[track_id].add(playlist_name)

        # Short delay to avoid rate limiting
        time.sleep(0.5)

    # Count some statistics for reporting
    tracks_with_playlists = len(track_playlist_map)
    total_associations = sum(len(playlists) for playlists in track_playlist_map.values())

    print(f"\nAssociation analysis complete: {tracks_with_playlists}/{total_tracks} tracks have playlist associations")
    print(f"Total associations to sync: {total_associations}")

    # Compare with existing associations to see what will actually change
    actual_changes = {}
    tracks_with_changes = 0
    associations_to_add = 0
    associations_to_remove = 0

    print("\nAnalyzing changes in associations...")

    with UnitOfWork() as uow:
        # Check each track to see what will change
        for track_id, new_playlist_names in track_playlist_map.items():
            # Get current associations for this track
            current_playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track_id)
            current_playlist_names = []

            # Convert playlist IDs to names for comparison
            for pid in current_playlist_ids:
                playlist = uow.playlist_repository.get_by_id(pid)
                if playlist:
                    current_playlist_names.append(playlist.name)

            # Identify changes
            new_playlist_names_set = set(new_playlist_names)
            current_playlist_names_set = set(current_playlist_names)

            to_add = new_playlist_names_set - current_playlist_names_set
            to_remove = current_playlist_names_set - new_playlist_names_set

            # Only record if there are changes
            if to_add or to_remove:
                track = uow.track_repository.get_by_id(track_id)
                if track:
                    actual_changes[track_id] = {
                        'track_info': f"{track.artists} - {track.title}",
                        'to_add': to_add,
                        'to_remove': to_remove
                    }
                    tracks_with_changes += 1
                    associations_to_add += len(to_add)
                    associations_to_remove += len(to_remove)

    # Now display the actual changes that will be made
    if actual_changes:
        print(f"\nACTUAL ASSOCIATION CHANGES TO MAKE ({tracks_with_changes} tracks):")
        print("============================================")
        print(f"Total associations to add: {associations_to_add}")
        print(f"Total associations to remove: {associations_to_remove}")

        # Sort tracks by artist/title for better readability
        sorted_tracks = sorted(actual_changes.items(),
                               key=lambda x: x[1]['track_info'])

        # Ask if user wants to see all changes or just a summary
        if tracks_with_changes > 10:
            show_all = input(f"\nShow all {tracks_with_changes} tracks with changes? (y/n, default=n): ").lower() == 'y'
        else:
            show_all = True

        # Display the changes
        tracks_to_show = sorted_tracks if show_all else sorted_tracks[:10]

        for track_id, changes in tracks_to_show:
            print(f"\n• {changes['track_info']}")
            if changes['to_add']:
                print(f"  + Adding to: {', '.join(sorted(changes['to_add']))}")
            if changes['to_remove']:
                print(f"  - Removing from: {', '.join(sorted(changes['to_remove']))}")

        if not show_all and tracks_with_changes > 10:
            print(f"\n...and {tracks_with_changes - 10} more tracks with changes")
    else:
        print("\nNo changes to track-playlist associations needed. Database is up to date.")

    # Ask for confirmation only if there are changes
    if not actual_changes:
        return {
            "tracks_with_playlists": tracks_with_playlists,
            "tracks_without_playlists": len(all_track_ids) - tracks_with_playlists,
            "total_associations": total_associations,
            "associations_added": 0,
            "associations_removed": 0,
            "playlists_processed": len(changed_playlists),
            "playlists_skipped": len(unchanged_playlists),
            "no_changes": True
        }

    if not auto_confirm:
        confirmation = input("\nWould you like to update track-playlist associations in the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Association sync cancelled by user")
            print("Sync cancelled.")
            return {
                "tracks_with_playlists": tracks_with_playlists,
                "tracks_without_playlists": len(all_track_ids) - tracks_with_playlists,
                "total_associations": total_associations,
                "associations_added": 0,
                "associations_removed": 0,
                "playlists_processed": len(changed_playlists),
                "playlists_skipped": len(unchanged_playlists)
            }

    # Track statistics for newly synced associations
    associations_added = 0
    associations_removed = 0

    # Now update only the associations that need to change
    print("\nUpdating track-playlist associations in database...")

    # Process tracks with identified changes
    for track_id, changes in actual_changes.items():
        with UnitOfWork() as uow:
            # Get current associations
            current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_track(track_id))

            # Get all current playlist names
            current_playlist_names = []
            for pid in current_playlist_ids:
                playlist = uow.playlist_repository.get_by_id(pid)
                if playlist:
                    current_playlist_names.append(playlist.name)

            # Current playlists plus additions minus removals
            updated_playlist_names = set(current_playlist_names)
            updated_playlist_names |= changes['to_add']
            updated_playlist_names -= changes['to_remove']

            # Update with the new set of playlists
            result = sync_track_playlist_associations_for_single_track(uow, track_id, updated_playlist_names)

            # Log the exact changes made
            if result["added"] > 0 or result["removed"] > 0:
                sync_logger.info(f"Updated associations for '{changes['track_info']}': "
                                 f"added {result['added']}, removed {result['removed']}")

            associations_added += result["added"]
            associations_removed += result["removed"]

            # Print progress for large operations
            if len(actual_changes) > 20 and (list(actual_changes.keys()).index(track_id) + 1) % 20 == 0:
                print(
                    f"Progress: {list(actual_changes.keys()).index(track_id) + 1}/{len(actual_changes)} tracks processed")

    # Update associations for tracks that should have no playlists but currently have some
    tracks_without_playlists = all_track_ids - set(track_playlist_map.keys())
    if tracks_without_playlists:
        # Check which ones actually have associations that need to be removed
        tracks_to_clear = []

        with UnitOfWork() as uow:
            for track_id in tracks_without_playlists:
                current_playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track_id)
                if current_playlist_ids:  # Only process if it has associations
                    track = uow.track_repository.get_by_id(track_id)
                    track_info = f"{track.artists} - {track.title}" if track else f"ID:{track_id}"
                    tracks_to_clear.append((track_id, track_info))

        if tracks_to_clear:
            print(f"\nRemoving all associations from {len(tracks_to_clear)} tracks that should have no playlists...")

            for i, (track_id, track_info) in enumerate(tracks_to_clear):
                with UnitOfWork() as uow:
                    result = sync_track_playlist_associations_for_single_track(uow, track_id, set())
                    associations_removed += result["removed"]

                    if result["removed"] > 0:
                        sync_logger.info(f"Removed all associations for '{track_info}': removed {result['removed']}")

                # Print progress for large operations
                if len(tracks_to_clear) > 20 and (i + 1) % 20 == 0:
                    print(f"Progress: {i + 1}/{len(tracks_to_clear)} tracks processed")

    # Final statistics
    stats = {
        "tracks_with_playlists": tracks_with_playlists,
        "tracks_without_playlists": len(tracks_without_playlists),
        "total_associations": total_associations,
        "associations_added": associations_added,
        "associations_removed": associations_removed,
        "tracks_with_changes": len(actual_changes),
        "playlists_processed": len(changed_playlists),
        "playlists_skipped": len(unchanged_playlists)
    }

    print(f"\nTrack-playlist association sync complete:")
    print(f"  - {associations_added} associations added")
    print(f"  - {associations_removed} associations removed")
    print(f"  - {len(actual_changes)} tracks had association changes")
    print(f"  - {len(changed_playlists)} playlists had changes")
    print(f"  - {len(unchanged_playlists)} playlists are unchanged")
    print(f"  - {tracks_with_playlists} tracks have playlist associations")
    print(f"  - {len(tracks_without_playlists)} tracks have no playlist associations")

    sync_logger.info(
        f"Association sync complete: {associations_added} added, {associations_removed} removed for {len(actual_changes)} tracks")
    return stats


def sync_track_playlist_associations_for_single_track(uow, track_id, playlist_names):
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

    sync_logger.debug(f"Syncing associations for track: {track_info}")
    sync_logger.debug(f"Requested playlists: {normalized_playlist_names}")

    # Find playlist IDs for all the names
    playlist_ids = []
    found_playlists = []
    missing_playlists = []

    # First, try with exact names
    for playlist_name in normalized_playlist_names:
        playlist = uow.playlist_repository.get_by_name(playlist_name)
        if playlist:
            playlist_ids.append(playlist.playlist_id)
            found_playlists.append(playlist_name)
        else:
            missing_playlists.append(playlist_name)

    # If we couldn't find some playlists, try similarity matching
    if missing_playlists:
        sync_logger.warning(f"Could not find playlists by exact name for {track_info}: {missing_playlists}")

        # Try a more flexible search for each missing playlist
        for missing_name in missing_playlists:
            similar_playlists = uow.playlist_repository.find_by_name(missing_name)
            if similar_playlists:
                names = [p.name for p in similar_playlists]
                sync_logger.debug(f"Found similar playlists for '{missing_name}': {names}")

                # If there's only one similar playlist, use it
                if len(similar_playlists) == 1:
                    playlist = similar_playlists[0]
                    playlist_ids.append(playlist.playlist_id)
                    found_playlists.append(playlist.name)
                    sync_logger.info(f"Using similar playlist: '{playlist.name}' for requested '{missing_name}'")

    # Get current associations
    current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_track(track_id))

    # Calculate what needs to be added and removed
    playlist_ids_to_add = set(playlist_ids) - current_playlist_ids
    playlist_ids_to_remove = current_playlist_ids - set(playlist_ids)

    # Add new associations
    for playlist_id in playlist_ids_to_add:
        uow.track_playlist_repository.insert(track_id, playlist_id)
        # Try to get playlist name for better logging
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        playlist_name = playlist.name if playlist else f"ID:{playlist_id}"
        sync_logger.debug(f"Added association: Track {track_info} to Playlist '{playlist_name}'")

    # Remove old associations
    for playlist_id in playlist_ids_to_remove:
        uow.track_playlist_repository.delete(track_id, playlist_id)
        # Try to get playlist name
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        playlist_name = playlist.name if playlist else f"ID:{playlist_id}"
        sync_logger.debug(f"Removed association: Track {track_info} from Playlist '{playlist_name}'")

    # Return summary of changes
    return {
        "added": len(playlist_ids_to_add),
        "removed": len(playlist_ids_to_remove),
        "unchanged": len(current_playlist_ids) - len(playlist_ids_to_remove),
        "missing_playlists": len(missing_playlists) - len(found_playlists)
    }


def analyze_playlists_changes(force_full_refresh=False, exclusion_config=None):
    """
    Analyze what changes would be made to playlists without executing them.

    Args:
        force_full_refresh: Whether to force a full refresh
        exclusion_config: Configuration for excluding playlists

    Returns:
        Tuple of (added_count, updated_count, unchanged_count, deleted_count, changes_details (dict))
    """
    # Get existing playlists from database
    existing_playlists = get_db_playlists()

    # Fetch all playlists from Spotify (playlist_name, playlist_id, snapshot_id)
    spotify_client = authenticate_spotify()
    spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, force_refresh=force_full_refresh,
                                                            exclusion_config=exclusion_config)

    # Create a set of playlist IDs returned from Spotify
    spotify_playlist_ids = set()
    for playlist in spotify_playlists:
        spotify_playlist_ids.add(playlist.playlist_id)

    # Track changes for analysis
    playlists_to_add = []
    playlists_to_update = []
    playlists_to_delete = []
    unchanged_count = 0

    # Analyze changes
    for playlist in spotify_playlists:
        playlist_name = playlist.name.strip()
        playlist_id = playlist.playlist_id
        snapshot_id = playlist.snapshot_id

        # Check if playlist exists in database
        if playlist_id in existing_playlists:
            existing_playlist = existing_playlists[playlist_id]

            name_changed = existing_playlist.name != playlist_name
            snapshot_id_changed = existing_playlist.snapshot_id != snapshot_id

            if name_changed or snapshot_id_changed:
                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name,
                    'old_name': existing_playlist.name,
                    'snapshot_id': snapshot_id,
                    'old_snapshot_id': existing_playlist.snapshot_id,
                })
            else:
                unchanged_count += 1
        else:
            # Mark for addition
            playlists_to_add.append({
                'id': playlist_id,
                'name': playlist_name.strip(),
                'snapshot_id': snapshot_id
            })

    # Find playlists to delete
    for playlist_id, playlist in existing_playlists.items():
        if playlist_id not in spotify_playlist_ids:
            playlists_to_delete.append({
                'id': playlist_id,
                'name': playlist.name
            })

    return (
        len(playlists_to_add),
        len(playlists_to_update),
        unchanged_count,
        len(playlists_to_delete),
        {
            'to_add': playlists_to_add,
            'to_update': playlists_to_update,
            'to_delete': playlists_to_delete
        }
    )


def analyze_tracks_changes(master_playlist_id: str, force_full_refresh: bool = False):
    """
    Analyze what changes would be made to tracks without executing them.
    Fixed to correctly identify local tracks that are already in the database.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a full refresh, ignoring existing data

    Returns:
        Tuple of (tracks_to_add, tracks_to_update, unchanged_tracks)
    """
    # Get existing tracks from database
    existing_tracks = get_db_tracks()

    # Create a lookup dictionary for local tracks in the database
    # This will help us match local files more effectively
    local_tracks_lookup = {}
    for track_id, track in existing_tracks.items():
        if track.is_local or track_id.startswith('local_'):
            # Create alternative keys for matching
            normalized_key = f"{track.artists}_{track.title}".lower().replace(' ', '')
            local_tracks_lookup[normalized_key] = track_id

    # Fetch all tracks from the MASTER playlist
    spotify_client = authenticate_spotify()
    master_tracks = fetch_master_tracks(spotify_client, master_playlist_id, force_refresh=force_full_refresh)

    # Analyze changes without applying them
    tracks_to_add = []
    tracks_to_update = []
    unchanged_tracks = []

    for track_data in master_tracks:
        track_id, track_title, artist_names, album_name, added_at = track_data

        # Handle local files
        is_local = track_id is None

        if is_local:
            # Clean the strings
            normalized_title = ''.join(c for c in track_title if c.isalnum() or c in ' &-_')
            normalized_artist = ''.join(c for c in artist_names if c.isalnum() or c in ' &-_')

            # Generate a lookup key for local track matching
            normalized_key = f"{normalized_artist}_{normalized_title}".lower().replace(' ', '')

            # First check if we can find this local track by our normalized key
            if normalized_key in local_tracks_lookup:
                # Found the track in our local tracks
                track_id = local_tracks_lookup[normalized_key]
                sync_logger.info(f"Matched local file: '{track_title}' with existing track ID: {track_id}")
            else:
                # Generate a new ID for this local track
                metadata = {'title': normalized_title, 'artist': normalized_artist}
                track_id = generate_local_track_id(metadata)
                sync_logger.info(f"Generated new ID for local file: '{track_title}' -> {track_id}")

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
                    'is_local': is_local,
                    'old_title': existing_track.title,
                    'old_artists': existing_track.artists,
                    'old_album': existing_track.album
                })
            else:
                unchanged_tracks.append(track_id)
        else:
            # Try an alternative lookup for local tracks by artist and title
            if is_local:
                # Create a normalized key for checking
                normalized_key = f"{normalized_artist}_{normalized_title}".lower().replace(' ', '')

                # Check if we can find a match by normalized artist+title
                found_match = False
                for existing_id, existing_track in existing_tracks.items():
                    if existing_track.is_local:
                        existing_normalized_key = f"{existing_track.artists}_{existing_track.title}".lower().replace(
                            ' ', '')
                        if existing_normalized_key == normalized_key:
                            # Found a match - consider it unchanged
                            unchanged_tracks.append(existing_id)
                            found_match = True
                            sync_logger.info(
                                f"Found alternative match for local file: '{track_title}' -> {existing_id}")
                            break

                if found_match:
                    continue

            # Mark for addition
            tracks_to_add.append({
                'id': track_id,
                'title': track_title,
                'artists': artist_names,
                'album': album_name,
                'added_at': added_at,
                'is_local': is_local
            })

    return tracks_to_add, tracks_to_update, unchanged_tracks
