import time
from datetime import datetime
from typing import Dict, Tuple, List

from drivers.spotify_client import (
    authenticate_spotify,
    fetch_playlists,
    fetch_master_tracks,
    get_track_uris_for_playlist
)
from sql.core.unit_of_work import UnitOfWork
from sql.dto.playlist_info import PlaylistInfo
from sql.helpers.db_helper import get_db_playlists, get_db_tracks_by_uri
from sql.models.playlist import Playlist
from sql.models.track import Track
from utils.logger import setup_logger

sync_logger = setup_logger('sync_helper', 'sql', 'sync.log')


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
    existing_playlists_db = get_db_playlists()

    # Fetch all playlists from Spotify (playlist_name, playlist_id, snapshot_id)
    spotify_client = authenticate_spotify()
    spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, exclusion_config=exclusion_config)

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
        if playlist_id in existing_playlists_db:
            existing_playlist = existing_playlists_db[playlist_id]

            name_changed = existing_playlist.name != playlist_name
            snapshot_id_changed = existing_playlist.master_sync_snapshot_id != snapshot_id

            if name_changed or snapshot_id_changed:
                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name,
                    'old_name': existing_playlist.name,
                    'snapshot_id': snapshot_id,
                    'old_snapshot_id': existing_playlist.master_sync_snapshot_id,
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
    for playlist_id, playlist in existing_playlists_db.items():
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


def sync_playlists_to_db(force_full_refresh=False, skip_confirmation=False, precomputed_changes: Dict = None,
                         exclusion_config=None):
    """
    Sync playlists from Spotify to the database.
    Only fetches and updates playlists that have changed based on snapshot_id.

    Returns:
        Tuple of (added, updated, unchanged, deleted) counts
    """
    sync_logger.info("Starting incremental playlist sync")
    print("Starting incremental playlist sync...")

    # track changes for analysis
    playlists_to_add = []
    playlists_to_update = []
    playlists_to_delete = []
    unchanged_playlists_count = 0

    existing_playlists_db = get_db_playlists()
    sync_logger.info(f"Found {len(existing_playlists_db)} existing playlists in database")

    if precomputed_changes:
        playlists_to_add = precomputed_changes.get('items_to_add', playlists_to_add)
        playlists_to_update = precomputed_changes.get('items_to_update', playlists_to_update)
        playlists_to_delete = precomputed_changes.get('items_to_delete', playlists_to_delete)
        unchanged_playlists_count = precomputed_changes.get('unchanged', unchanged_playlists_count)

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
        spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, exclusion_config=exclusion_config)
        sync_logger.info(f"Fetched {len(spotify_playlists)} playlists from Spotify")

        # Create a set of playlist IDs returned from Spotify
        spotify_playlist_ids = set(playlist.playlist_id for playlist in spotify_playlists)

        # Analyze changes (without applying them yet)
        for playlist in spotify_playlists:
            playlist_name = playlist.name
            playlist_id = playlist.playlist_id
            snapshot_id = playlist.snapshot_id

            # Check if playlist exists in database
            if playlist_id in existing_playlists_db:
                existing_playlist = existing_playlists_db[playlist_id]

                # Check if playlist details have changed
                if (existing_playlist.name != playlist_name.strip() or
                        existing_playlist.associations_snapshot_id != snapshot_id):

                    # Mark for update
                    playlists_to_update.append({
                        'id': playlist_id,
                        'name': playlist_name.strip(),
                        'old_name': existing_playlist.name,
                        'snapshot_id': snapshot_id,
                        'old_snapshot_id': existing_playlist.associations_snapshot_id,
                    })
                else:
                    unchanged_playlists_count += 1
            else:
                # Mark for addition
                playlists_to_add.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'snapshot_id': snapshot_id
                })

        # Find playlists to delete (in database but not in Spotify results)
        for playlist_id, playlist in existing_playlists_db.items():
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
    print(f"Unchanged playlists: {unchanged_playlists_count}")

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
    if (playlists_to_add or playlists_to_update or playlists_to_delete) and not skip_confirmation:
        confirmation = input("\nWould you like to proceed with these changes to the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Playlist sync cancelled by user")
            print("Sync cancelled.")
            return 0, 0, unchanged_playlists_count, 0
    elif not playlists_to_add and not playlists_to_update and not playlists_to_delete:
        print("\nNo changes needed. Database is up to date.")
        return 0, 0, unchanged_playlists_count, 0

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
                master_sync_snapshot_id=playlist_data['snapshot_id']
            )
            uow.playlist_repository.insert(new_playlist)
            added_count += 1
            sync_logger.info(f"Added new playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

        # Update existing playlists
        for playlist_data in playlists_to_update:
            # Get the existing playlist
            existing_playlist = existing_playlists_db[playlist_data['id']]

            # Update the playlist with new data
            existing_playlist.name = playlist_data['name']
            existing_playlist.master_sync_snapshot_id = playlist_data['snapshot_id']
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
        f"\nPlaylist sync complete: {added_count} added, {updated_count} updated, {unchanged_playlists_count} unchanged, {deleted_count} deleted")
    sync_logger.info(
        f"Playlist sync complete: {added_count} added, {updated_count} updated, {unchanged_playlists_count} unchanged, {deleted_count} deleted"
    )

    return added_count, updated_count, unchanged_playlists_count, deleted_count


def analyze_tracks_changes(master_playlist_id: str):
    """
    Analyze what changes would be made to tracks without executing them.
    Updated to use Spotify URIs as primary identifiers.

    Args:
        master_playlist_id: ID of the master playlist

    Returns:
        Tuple of (tracks_to_add, tracks_to_update, unchanged_tracks, tracks_to_delete)
    """
    # Get existing tracks from database INDEXED BY URI (not track_id)
    master_tracks_db = get_db_tracks_by_uri()  # This returns {uri: track} dictionary

    sync_logger.info(f"Found {len(master_tracks_db)} existing tracks in database (indexed by URI)")

    # Fetch all tracks from the MASTER playlist
    spotify_client = authenticate_spotify()
    master_tracks_api = fetch_master_tracks(spotify_client, master_playlist_id)

    # Analyze changes without applying them
    tracks_to_add = []
    tracks_to_update = []
    unchanged_tracks = []
    master_track_uris = set()  # Changed from master_track_ids to master_track_uris

    for track_data in master_tracks_api:
        uri, track_id, track_title, artist_names, album_name, added_at = track_data

        # URI is now the primary identifier - no need to generate it
        spotify_uri = uri
        is_local = track_id is None  # Local files have no track_id

        if is_local:
            sync_logger.info(f"Processing local file: '{track_title}' by '{artist_names}' (URI: {spotify_uri})")
        else:
            sync_logger.debug(f"Processing regular track: '{track_title}' by '{artist_names}' (URI: {spotify_uri})")

        master_track_uris.add(spotify_uri)

        # Check if track exists in database BY URI
        if spotify_uri in master_tracks_db:
            existing_track = master_tracks_db[spotify_uri]
            existing_album = existing_track.album or "Unknown Album"
            api_album = album_name or "Unknown Album"

            changes = []
            if existing_track.title != track_title:
                changes.append(f"Title: '{existing_track.title}' → '{track_title}'")
            if existing_track.artists != artist_names:
                changes.append(f"Artists: '{existing_track.artists}' → '{artist_names}'")
            if existing_album != api_album:
                changes.append(f"Album: '{existing_album}' → '{api_album}'")

            # Check if track details have changed
            if changes:
                tracks_to_update.append({
                    'uri': spotify_uri,
                    'track_id': track_id,  # Keep for backward compatibility (may be None for local files)
                    'title': track_title,
                    'artists': artist_names,
                    'album': album_name,
                    'is_local': is_local,
                    'old_title': existing_track.title,
                    'old_artists': existing_track.artists,
                    'old_album': existing_track.album,
                    'changes': changes
                })
            else:
                unchanged_tracks.append(spotify_uri)
        else:
            # Mark for addition
            tracks_to_add.append({
                'uri': spotify_uri,
                'track_id': track_id,
                'title': track_title,
                'artists': artist_names,
                'album': album_name,
                'added_at': added_at,
                'is_local': is_local
            })

    # Find tracks that are in the database but not in the master playlist
    tracks_to_delete = []
    for existing_uri, track in master_tracks_db.items():
        if existing_uri not in master_track_uris:
            tracks_to_delete.append({
                'uri': existing_uri,
                'track_id': track.track_id,
                'title': track.title,
                'artists': track.artists,
                'album': track.album,
                'is_local': track.is_local_file() if hasattr(track, 'is_local_file') else getattr(track, 'is_local',
                                                                                                  False)
            })

    sync_logger.info(f"Analysis complete: {len(tracks_to_add)} to add, {len(tracks_to_update)} to update, "
                     f"{len(unchanged_tracks)} unchanged, {len(tracks_to_delete)} to delete")

    return tracks_to_add, tracks_to_update, unchanged_tracks, tracks_to_delete


def sync_tracks_to_db(master_playlist_id: str, force_full_refresh: bool = False,
                      auto_confirm: bool = False, precomputed_changes: dict = None) -> Tuple[
    int, int, int, int]:
    """
    Incrementally sync tracks from the MASTER playlist to the database using Spotify URIs.
    Only fetches and updates tracks that have changed. Does NOT update playlist associations.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a full refresh
        auto_confirm: Whether to auto-confirm changes
        precomputed_changes: Dict with 'items_to_add', 'items_to_update', 'items_to_delete', 'unchanged_count'

    Returns:
        Tuple of (added, updated, unchanged, deleted) counts
    """
    sync_logger.info(f"Starting incremental master tracks sync for playlist {master_playlist_id}")
    print("Starting incremental master tracks sync analysis...")

    # Get existing tracks indexed by URI instead of track_id
    master_tracks_db = get_db_tracks_by_uri()  # New function needed
    sync_logger.info(f"Found {len(master_tracks_db)} existing tracks in database")

    if precomputed_changes:
        sync_logger.info("Using precomputed changes to avoid redundant analysis")
        tracks_to_add = precomputed_changes.get('items_to_add', [])
        tracks_to_update = precomputed_changes.get('items_to_update', [])
        tracks_to_delete = precomputed_changes.get('items_to_delete', [])
        unchanged_count = precomputed_changes.get('unchanged_count', 0)

        sync_logger.info(f"Precomputed changes: {len(tracks_to_add)} to add, "
                         f"{len(tracks_to_update)} to update, "
                         f"{len(tracks_to_delete)} to delete, "
                         f"{unchanged_count} unchanged")
    else:
        # Fetch all tracks from the MASTER playlist
        spotify_client = authenticate_spotify()
        master_tracks_api = fetch_master_tracks(spotify_client, master_playlist_id)
        sync_logger.info(f"Fetched {len(master_tracks_api)} tracks from MASTER playlist")

        # Analyze changes (without applying them yet)
        tracks_to_add = []
        tracks_to_update = []
        unchanged_tracks = []

        # Build set of master track URIs for deletion detection
        master_track_uris = set()

        for track_data in master_tracks_api:
            uri, track_id, track_title, artist_names, album_name, added_at = track_data

            is_local = track_id is None

            master_track_uris.add(uri)

            # Check if track exists in database
            if uri in master_tracks_db:
                existing_track = master_tracks_db[uri]

                # Check if track details have changed
                if (existing_track.title != track_title or
                        existing_track.artists != artist_names or
                        existing_track.album != album_name):

                    # Mark for update
                    tracks_to_update.append({
                        'uri': uri,
                        'track_id': track_id,  # Keep for backward compatibility
                        'title': track_title,
                        'artists': artist_names,
                        'album': album_name,
                        'is_local': is_local,
                        'old_title': existing_track.title,
                        'old_artists': existing_track.artists,
                        'old_album': existing_track.album
                    })
                else:
                    unchanged_tracks.append(uri)
            else:
                # Mark for addition
                tracks_to_add.append({
                    'uri': uri,
                    'track_id': track_id,  # Keep for backward compatibility
                    'title': track_title,
                    'artists': artist_names,
                    'album': album_name,
                    'added_at': added_at,
                    'is_local': is_local
                })

        # Find tracks that are in the database but not in the master playlist
        tracks_to_delete = []
        for uri, track in master_tracks_db.items():
            if uri not in master_track_uris:
                tracks_to_delete.append({
                    'uri': uri,
                    'track_id': track.track_id,  # May be None for local files
                    'title': track.title,
                    'artists': track.artists,
                    'album': track.album,
                    'is_local': track.is_local if hasattr(track, 'is_local') else False
                })

        unchanged_count = len(unchanged_tracks)

    # Display summary of changes
    print("\nMaster Tracks SYNC ANALYSIS COMPLETE")
    print("=================================")
    print(f"\nTracks to add: {len(tracks_to_add)}")
    print(f"Tracks to update: {len(tracks_to_update)}")
    print(f"Tracks to delete: {len(tracks_to_delete)}")
    print(f"Unchanged tracks: {unchanged_count}")

    # Display detailed changes (existing code for add/update, plus deletion display)
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
        if not auto_confirm:
            confirmation = input(f"\nWould you like to proceed with these changes to the database?\n"
                                 f"Add: {len(tracks_to_add)}, Update: {len(tracks_to_update)}, "
                                 f"Delete: {len(tracks_to_delete)} (y/n): ")
            if confirmation.lower() != 'y':
                sync_logger.info("Sync cancelled by user")
                print("Sync cancelled.")
                return 0, 0, unchanged_count, 0
    else:
        print("\nNo track changes needed. Database is up to date.")
        return 0, 0, unchanged_count, 0

    # If confirmed, apply the changes
    added_count = 0
    updated_count = 0
    deleted_count = 0

    print("\nApplying track changes to database...")
    with UnitOfWork() as uow:
        # Delete tracks first
        for track_data in tracks_to_delete:
            uri = track_data['uri']

            # First remove all playlist associations for this track
            try:
                playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_uri(uri)
                for playlist_id in playlist_ids:
                    uow.track_playlist_repository.delete_by_uri(uri, playlist_id)
                sync_logger.info(f"Removed playlist associations for track: {track_data['title']} (URI: {uri})")
            except Exception as e:
                sync_logger.error(f"Error removing playlist associations for track {uri}: {e}")
                print(f"Warning: Error removing playlist associations for track {uri}")

            # Then delete the track itself
            try:
                uow.track_repository.delete_by_uri(uri)
                deleted_count += 1
                sync_logger.info(f"Deleted track: {track_data['title']} (URI: {uri})")
            except Exception as e:
                sync_logger.error(f"Error deleting track {uri}: {e}")
                print(f"Warning: Error deleting track {uri}")

        # Add new tracks
        for track_data in tracks_to_add:
            uri = track_data['uri']
            track_id = track_data['id']  # May be None for local files
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            is_local = track_data['is_local']
            added_at = None

            if 'added_at' in track_data and track_data['added_at']:
                # Handle datetime parsing (existing logic)
                if isinstance(track_data['added_at'], datetime):
                    added_at = track_data['added_at']
                elif isinstance(track_data['added_at'], str):
                    try:
                        added_at = datetime.fromisoformat(track_data['added_at'].replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            from email.utils import parsedate_to_datetime
                            added_at = parsedate_to_datetime(track_data['added_at'])
                        except Exception:
                            try:
                                added_at = datetime.strptime(track_data['added_at'], '%a, %d %b %Y %H:%M:%S GMT')
                            except ValueError:
                                try:
                                    added_at = datetime.strptime(track_data['added_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
                                except ValueError:
                                    try:
                                        added_at = datetime.strptime(track_data['added_at'], '%Y-%m-%d %H:%M:%S')
                                    except ValueError:
                                        sync_logger.warning(
                                            f"Could not parse date: {track_data['added_at']} for track {uri}")
                                        added_at = None

            # For newly added tracks with no added_at, use current time
            if added_at is None:
                added_at = datetime.now()
                sync_logger.debug(f"Using current time for AddedToMaster: {added_at} for track {uri}")

            # Create new track with URI as primary identifier
            new_track = Track(
                uri=uri,  # Primary identifier
                track_id=track_id,  # May be None for local files, keep for compatibility
                title=track_title,
                artists=artist_names,
                album=album_name,
                added_to_master=added_at,
                is_local=is_local,
            )
            uow.track_repository.insert(new_track)
            added_count += 1
            sync_logger.info(f"Added new track: {track_title} (URI: {uri})")

        # Update existing tracks
        for track_data in tracks_to_update:
            uri = track_data['uri']
            track_id = track_data['id']
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            is_local = track_data['is_local']

            # Get the existing track by URI
            existing_track = master_tracks_db[uri]

            # Update the track
            existing_track.title = track_title
            existing_track.artists = artist_names
            existing_track.album = album_name
            existing_track.is_local = is_local
            # Note: URI doesn't change, track_id might be updated for consistency

            uow.track_repository.update(existing_track)
            updated_count += 1
            sync_logger.info(f"Updated track: {track_title} (URI: {uri})")

    # Log final results
    print(f"\nTrack sync complete: {added_count} added, {updated_count} updated, "
          f"{unchanged_count} unchanged, {deleted_count} deleted")
    sync_logger.info(f"Track sync complete: {added_count} added, {updated_count} updated, "
                     f"{unchanged_count} unchanged, {deleted_count} deleted")

    return added_count, updated_count, unchanged_count, deleted_count


def analyze_track_playlist_associations(master_playlist_id: str, force_full_refresh: bool = False,
                                        exclusion_config=None) -> dict:
    """
    Analyze what changes would be made to track-playlist associations without actually making them.
    Updated to use Spotify URIs as primary identifiers.
    """
    sync_logger.info("Analyzing track-playlist association changes")

    # Get all tracks from database INDEXED BY URI
    with UnitOfWork() as uow:
        all_tracks_in_db = uow.track_repository.get_all()
        # Create a lookup dictionary for tracks by URI for quick access
        tracks_by_uri = {track.uri: track for track in all_tracks_in_db if track.uri}
        total_tracks = len(all_tracks_in_db)
        sync_logger.info(f"Found {total_tracks} tracks in database")

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists_in_db = uow.playlist_repository.get_all()
        total_playlists = len(all_playlists_in_db)
        sync_logger.info(f"Found {total_playlists} playlists in database")

    # Fetch all playlists directly from Spotify to ensure associations are fresh
    spotify_client = authenticate_spotify()
    spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, exclusion_config=exclusion_config)

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
            if force_full_refresh or db_playlist.associations_snapshot_id != playlist.snapshot_id:
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
            "changed_playlist_names": changed_playlist_names,
            "changed_playlists": []
        }

    # We'll track all associations for changed playlists only
    track_playlist_map = {}  # URI -> set of playlist names

    print("Fetching track-playlist associations from Spotify...")

    # Build a list of all track URIs for reference
    all_track_uris = {track.uri for track in all_tracks_in_db if track.uri}

    # Only process the changed playlists
    for i, playlist in enumerate(changed_playlists, 1):
        playlist_name = playlist.name
        playlist_id = playlist.playlist_id

        print(f"Processing playlist {i}/{len(changed_playlists)}: {playlist_name}")

        # Get track URIs for this playlist (updated to return URIs)
        playlist_track_uris = get_track_uris_for_playlist(spotify_client, playlist_id, force_refresh=True)

        # Log number of local files
        local_files = [uri for uri in playlist_track_uris if uri.startswith('spotify:local:')]
        sync_logger.info(f"Found {len(local_files)} local files in playlist '{playlist_name}'")

        # Log how many tracks were found for this playlist
        valid_tracks = [uri for uri in playlist_track_uris if uri in all_track_uris]
        sync_logger.info(f"Found {len(valid_tracks)} valid tracks in playlist '{playlist_name}'")

        # For each track in this playlist, update its associations
        for track_uri in valid_tracks:
            if track_uri not in track_playlist_map:
                track_playlist_map[track_uri] = set()

            track_playlist_map[track_uri].add(playlist_name)

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

    # Get the set of changed playlist names for filtering
    changed_playlist_names_set = set(playlist.name for playlist in changed_playlists)

    with UnitOfWork() as uow:
        # Check each track to see what will change
        for track_uri, new_playlist_names in track_playlist_map.items():
            # Get current associations for this track BY URI
            current_playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_uri(track_uri)
            current_playlist_names = []

            # Convert playlist IDs to names for comparison
            for pid in current_playlist_ids:
                playlist = uow.playlist_repository.get_by_id(pid)
                if playlist:
                    current_playlist_names.append(playlist.name)

            # IMPORTANT: Only consider associations for changed playlists
            # Keep associations for unchanged playlists as-is
            current_changed_playlists = set(current_playlist_names) & changed_playlist_names_set
            current_unchanged_playlists = set(current_playlist_names) - changed_playlist_names_set

            # The new state should be: unchanged playlists + new associations from changed playlists
            expected_final_playlists = current_unchanged_playlists | set(new_playlist_names)
            current_all_playlists = set(current_playlist_names)

            to_add = expected_final_playlists - current_all_playlists
            to_remove = current_all_playlists - expected_final_playlists

            # Only record if there are changes
            if to_add or to_remove:
                track = uow.track_repository.get_by_uri(track_uri)
                if track:
                    track_info = f"{track.artists} - {track.title}"
                    actual_changes[track_uri] = {
                        'track_info': track_info,
                        'to_add': to_add,
                        'to_remove': to_remove
                    }
                    tracks_with_changes.append({
                        'uri': track_uri,  # Changed from track_id to track_uri
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

        # ALSO: Handle tracks that are no longer in any of the changed playlists
        # but still have associations to those playlists in the database
        all_track_uris_in_changed_playlists = set(track_playlist_map.keys())

        # Find tracks that have associations to changed playlists but aren't in the new mapping
        for playlist in changed_playlists:
            current_track_uris_in_playlist = uow.track_playlist_repository.get_uris_for_playlist(playlist.playlist_id)

            for track_uri in current_track_uris_in_playlist:
                if track_uri not in all_track_uris_in_changed_playlists:
                    # This track was removed from this changed playlist
                    track = uow.track_repository.get_by_uri(track_uri)
                    if track:
                        # Only remove association to this specific changed playlist
                        track_info = f"{track.artists} - {track.title}"

                        if track_uri not in actual_changes:
                            actual_changes[track_uri] = {
                                'track_info': track_info,
                                'to_add': set(),
                                'to_remove': {playlist.name}
                            }
                            tracks_with_changes.append({
                                'uri': track_uri,
                                'track_info': track_info,
                                'title': track.title,
                                'artists': track.artists,
                                'add_to': [],
                                'remove_from': [playlist.name]
                            })
                            associations_to_remove += 1

                            # Add to samples for UI
                            samples.append({
                                'track': track_info,
                                'track_info': track_info,
                                'title': track.title,
                                'artists': track.artists,
                                'add_to': [],
                                'remove_from': [playlist.name]
                            })
                        else:
                            # Add to existing changes
                            actual_changes[track_uri]['to_remove'].add(playlist.name)
                            # Update the corresponding entry in tracks_with_changes
                            for change in tracks_with_changes:
                                if change['uri'] == track_uri:
                                    change['remove_from'].append(playlist.name)
                                    break
                            # Update samples as well
                            for sample in samples:
                                if sample['track_info'] == track_info:
                                    sample['remove_from'].append(playlist.name)
                                    break
                            associations_to_remove += 1

    # If there are changed playlists but no actual association changes needed,
    # we still need to update associations_snapshot_id to mark them as processed
    if changed_playlists and not tracks_with_changes:
        print(
            f"\nNo association changes needed, but updating associations_snapshot_id for {len(changed_playlists)} changed playlists...")
        with UnitOfWork() as uow:
            for playlist in changed_playlists:
                db_playlist = uow.playlist_repository.get_by_id(playlist.playlist_id)
                if db_playlist:
                    old_snapshot = db_playlist.associations_snapshot_id
                    db_playlist.associations_snapshot_id = playlist.snapshot_id
                    uow.playlist_repository.update(db_playlist)
                    sync_logger.info(
                        f"Updated associations_snapshot_id for '{playlist.name}': {old_snapshot} -> {playlist.snapshot_id}")
                    print(f"  - Updated '{playlist.name}': {old_snapshot} -> {playlist.snapshot_id}")

        # Return the analysis results with information about changed playlists
        return {
            "tracks_with_changes": tracks_with_changes,
            "associations_to_add": associations_to_add,
            "associations_to_remove": associations_to_remove,
            "samples": samples,
            "all_changes": samples,
            "stats": {
                "tracks_with_playlists": tracks_with_playlists,
                "tracks_without_playlists": len(all_track_uris) - tracks_with_playlists,
                "total_associations": total_associations,
                "unchanged_playlists": len(unchanged_playlists),
                "changed_playlists": len(changed_playlists)
            },
            "changed_playlist_names": changed_playlist_names,
            "changed_playlists": [{"name": p.name, "id": p.playlist_id, "snapshot_id": p.snapshot_id} for p in
                                  changed_playlists],
            "snapshot_ids_updated": True  # Flag to indicate we updated the IDs
        }

    # Return the analysis results with information about changed playlists
    return {
        "tracks_with_changes": tracks_with_changes,
        "associations_to_add": associations_to_add,
        "associations_to_remove": associations_to_remove,
        "samples": samples,
        "all_changes": samples,
        "stats": {
            "tracks_with_playlists": tracks_with_playlists,
            "tracks_without_playlists": len(all_track_uris) - tracks_with_playlists,
            "total_associations": total_associations,
            "unchanged_playlists": len(unchanged_playlists),
            "changed_playlists": len(changed_playlists)
        },
        "changed_playlist_names": changed_playlist_names,
        "changed_playlists": [{"name": p.name, "id": p.playlist_id, "snapshot_id": p.snapshot_id} for p in
                              changed_playlists]
    }


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
            # Updated to use track_uri instead of track_id
            track_uri = change.get('uri', change.get('track_id'))  # Backward compatibility
            track_info = change.get('track_info', f"URI:{track_uri}")
            playlists_to_add = set(change.get('add_to', []))
            playlists_to_remove = set(change.get('remove_from', []))

            with UnitOfWork() as uow:
                # Get current playlist associations BY URI
                current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_uri(track_uri))
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
                result = sync_track_playlist_associations_for_single_track(uow, track_uri, updated_names_set)

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

        # Update associations_snapshot_id for all changed playlists
        if 'changed_playlists' in precomputed_changes:
            with UnitOfWork() as uow:
                for playlist_info in precomputed_changes['changed_playlists']:
                    playlist_id = playlist_info['id']
                    snapshot_id = playlist_info['snapshot_id']
                    playlist_name = playlist_info['name']

                    db_playlist = uow.playlist_repository.get_by_id(playlist_id)
                    if db_playlist:
                        db_playlist.associations_snapshot_id = snapshot_id
                        uow.playlist_repository.update(db_playlist)
                        sync_logger.info(f"Updated associations_snapshot_id for '{playlist_name}' to: {snapshot_id}")

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

    # Get all tracks from database INDEXED BY URI
    with UnitOfWork() as uow:
        all_tracks_db = uow.track_repository.get_all()
        tracks_by_uri = {track.uri: track for track in all_tracks_db if track.uri}
        total_tracks = len(all_tracks_db)
        sync_logger.info(f"Found {total_tracks} tracks in database")

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists_db = uow.playlist_repository.get_all()
        total_playlists = len(all_playlists_db)
        sync_logger.info(f"Found {total_playlists} playlists in database")

    # Fetch all playlists directly from Spotify to ensure associations are fresh
    spotify_client = authenticate_spotify()
    spotify_playlists: List[PlaylistInfo] = fetch_playlists(spotify_client, exclusion_config=exclusion_config)

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
            if force_full_refresh or db_playlist.associations_snapshot_id != playlist.snapshot_id:
                changed_playlists.append(playlist)
                sync_logger.info(
                    f"Playlist '{playlist.name}' associations_snapshot_id changed: {db_playlist.associations_snapshot_id} -> {playlist.snapshot_id}")
            else:
                unchanged_playlists.append(playlist)
                sync_logger.debug(
                    f"Playlist '{playlist.name}' unchanged (associations_snapshot_id: {playlist.snapshot_id})")
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

    # We'll track associations for changed playlists only
    track_playlist_map = {}  # URI -> set of playlist names

    print("Fetching track-playlist associations from Spotify...")

    # Build a list of all track URIs for reference
    all_track_uris = {track.uri for track in all_tracks_db if track.uri}

    # Only process the changed playlists
    for i, playlist in enumerate(changed_playlists, 1):
        playlist_name = playlist.name
        playlist_id = playlist.playlist_id
        print(f"Processing playlist {i}/{len(changed_playlists)}: {playlist_name}")

        # Always force a fresh API call to get the most up-to-date associations
        playlist_track_uris = get_track_uris_for_playlist(spotify_client, playlist_id, force_refresh=True)

        # Log number of local files
        local_files = [uri for uri in playlist_track_uris if uri.startswith('spotify:local:')]
        sync_logger.info(f"Found {len(local_files)} local files in playlist '{playlist_name}'")

        # Log how many tracks were found for this playlist
        valid_tracks = [uri for uri in playlist_track_uris if uri in all_track_uris]
        sync_logger.info(f"Found {len(valid_tracks)} valid tracks in playlist '{playlist_name}'")

        # For each track in this playlist, update its associations
        for track_uri in valid_tracks:
            if track_uri not in track_playlist_map:
                track_playlist_map[track_uri] = set()

            track_playlist_map[track_uri].add(playlist_name)

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

    # Get the set of changed playlist names for filtering
    changed_playlist_names_set = set(playlist.name for playlist in changed_playlists)

    with UnitOfWork() as uow:
        # Check each track to see what will change
        for track_uri, new_playlist_names in track_playlist_map.items():
            # Get current associations for this track BY URI
            current_playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_uri(track_uri)
            current_playlist_names = []

            # Convert playlist IDs to names for comparison
            for pid in current_playlist_ids:
                playlist = uow.playlist_repository.get_by_id(pid)
                if playlist:
                    current_playlist_names.append(playlist.name)

            # IMPORTANT: Only consider associations for changed playlists
            current_changed_playlists = set(current_playlist_names) & changed_playlist_names_set
            current_unchanged_playlists = set(current_playlist_names) - changed_playlist_names_set

            # The new state should be: unchanged playlists + new associations from changed playlists
            expected_final_playlists = current_unchanged_playlists | set(new_playlist_names)
            current_all_playlists = set(current_playlist_names)

            to_add = expected_final_playlists - current_all_playlists
            to_remove = current_all_playlists - expected_final_playlists

            # Only record if there are changes
            if to_add or to_remove:
                track = uow.track_repository.get_by_uri(track_uri)
                if track:
                    actual_changes[track_uri] = {
                        'track_info': f"{track.artists} - {track.title}",
                        'to_add': to_add,
                        'to_remove': to_remove
                    }
                    tracks_with_changes += 1
                    associations_to_add += len(to_add)
                    associations_to_remove += len(to_remove)

        # ALSO: Handle tracks that are no longer in any of the changed playlists
        all_track_uris_in_changed_playlists = set(track_playlist_map.keys())

        for playlist in changed_playlists:
            current_track_uris_in_playlist = uow.track_playlist_repository.get_uris_for_playlist(playlist.playlist_id)

            for track_uri in current_track_uris_in_playlist:
                if track_uri not in all_track_uris_in_changed_playlists:
                    # This track was removed from this changed playlist
                    track = uow.track_repository.get_by_uri(track_uri)
                    if track:
                        if track_uri not in actual_changes:
                            actual_changes[track_uri] = {
                                'track_info': f"{track.artists} - {track.title}",
                                'to_add': set(),
                                'to_remove': {playlist.name}
                            }
                            tracks_with_changes += 1
                            associations_to_remove += 1
                        else:
                            # Add to existing changes
                            actual_changes[track_uri]['to_remove'].add(playlist.name)
                            associations_to_remove += 1

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

        for track_uri, changes in tracks_to_show:
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
        # Update associations_snapshot_id even if no changes (to mark as processed)
        with UnitOfWork() as uow:
            for playlist in changed_playlists:
                db_playlist = uow.playlist_repository.get_by_id(playlist.playlist_id)
                if db_playlist:
                    db_playlist.associations_snapshot_id = playlist.snapshot_id
                    uow.playlist_repository.update(db_playlist)
                    sync_logger.info(
                        f"Updated associations_snapshot_id for '{playlist.name}' to: {playlist.snapshot_id}")

        return {
            "tracks_with_playlists": tracks_with_playlists,
            "tracks_without_playlists": len(all_track_uris) - tracks_with_playlists,
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
                "tracks_without_playlists": len(all_track_uris) - tracks_with_playlists,
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
    for track_uri, changes in actual_changes.items():
        with UnitOfWork() as uow:
            # Get current associations BY URI
            current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_uri(track_uri))

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
            result = sync_track_playlist_associations_for_single_track(uow, track_uri, updated_playlist_names)

            # Log the exact changes made
            if result["added"] > 0 or result["removed"] > 0:
                sync_logger.info(f"Updated associations for '{changes['track_info']}': "
                                 f"added {result['added']}, removed {result['removed']}")

            associations_added += result["added"]
            associations_removed += result["removed"]

            # Print progress for large operations
            if len(actual_changes) > 20 and (list(actual_changes.keys()).index(track_uri) + 1) % 20 == 0:
                print(
                    f"Progress: {list(actual_changes.keys()).index(track_uri) + 1}/{len(actual_changes)} tracks processed")

    # Update associations_snapshot_id for all processed playlists AFTER successful sync
    with UnitOfWork() as uow:
        for playlist in changed_playlists:
            db_playlist = uow.playlist_repository.get_by_id(playlist.playlist_id)
            if db_playlist:
                db_playlist.associations_snapshot_id = playlist.snapshot_id
                uow.playlist_repository.update(db_playlist)
                sync_logger.info(f"Updated associations_snapshot_id for '{playlist.name}' to: {playlist.snapshot_id}")

    # Final statistics
    stats = {
        "tracks_with_playlists": tracks_with_playlists,
        "tracks_without_playlists": len(all_track_uris) - tracks_with_playlists,
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
    print(f"  - {len(all_track_uris) - tracks_with_playlists} tracks have no playlist associations")

    sync_logger.info(
        f"Association sync complete: {associations_added} added, {associations_removed} removed for {len(actual_changes)} tracks")
    return stats


def sync_track_playlist_associations_for_single_track(uow, track_uri, playlist_names):
    """
    Sync track-playlist associations for a single track using URIs.
    Updated to work with URI-based system.

    Args:
        uow: Active unit of work
        track_uri: Spotify URI of the track
        playlist_names: Names of playlists the track should be in

    Returns:
        Dictionary with counts of added, removed, and unchanged associations
    """
    # Normalize playlist names by stripping whitespace
    normalized_playlist_names = [name.strip() if name else name for name in playlist_names]

    # Try to get more information for debugging
    track_info = None
    try:
        track = uow.track_repository.get_by_uri(track_uri)
        if track:
            track_info = f"{track.artists} - {track.title}"
        else:
            track_info = f"URI:{track_uri}"
    except:
        track_info = f"URI:{track_uri}"

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

    # Get current associations BY URI
    current_playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_uri(track_uri))

    # Calculate what needs to be added and removed
    playlist_ids_to_add = set(playlist_ids) - current_playlist_ids
    playlist_ids_to_remove = current_playlist_ids - set(playlist_ids)

    # Add new associations
    for playlist_id in playlist_ids_to_add:
        uow.track_playlist_repository.insert_by_uri(track_uri, playlist_id)
        # Try to get playlist name for better logging
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        playlist_name = playlist.name if playlist else f"ID:{playlist_id}"
        sync_logger.debug(f"Added association: Track {track_info} to Playlist '{playlist_name}'")

    # Remove old associations
    for playlist_id in playlist_ids_to_remove:
        uow.track_playlist_repository.delete_by_uri(track_uri, playlist_id)
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
