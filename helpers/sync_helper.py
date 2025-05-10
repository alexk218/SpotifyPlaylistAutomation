import time
import hashlib
import time
from typing import Dict, Tuple

from drivers.spotify_client import (
    authenticate_spotify,
    fetch_playlists,
    fetch_master_tracks,
    get_playlist_track_ids
)
from helpers.file_helper import parse_local_file_uri, generate_local_track_id, \
    normalize_local_file_uri
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
    existing_playlists = get_db_playlists()
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
    for playlist_name, playlist_id in spotify_playlists:
        # Check if playlist exists in database
        if playlist_id in existing_playlists:
            existing_playlist = existing_playlists[playlist_id]

            # Check if playlist details have changed
            if (existing_playlist.name != playlist_name.strip()):

                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'old_name': existing_playlist.name,
                })
            else:
                unchanged_count += 1
        else:
            # Mark for addition
            playlists_to_add.append({
                'id': playlist_id,
                'name': playlist_name.strip()
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

    if playlists_to_update:
        print("\nPLAYLISTS TO UPDATE:")
        print("===================")
        sorted_updates = sorted(playlists_to_update, key=lambda x: x['name'])
        for playlist in sorted_updates:
            print(f"• {playlist['old_name']} → {playlist['name']}")

    # Ask for confirmation if there are changes (unless auto_confirm is True)
    if (playlists_to_add or playlists_to_update) and not auto_confirm:
        confirmation = input("\nWould you like to proceed with these changes to the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Playlist sync cancelled by user")
            print("Sync cancelled.")
            return 0, 0, unchanged_count
    elif not playlists_to_add and not playlists_to_update:
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
                name=playlist_data['name']
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
            uow.playlist_repository.update(existing_playlist)
            updated_count += 1
            sync_logger.info(f"Updated playlist: {playlist_data['name']} (ID: {playlist_data['id']})")

    print(f"\nPlaylist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged")
    sync_logger.info(
        f"Playlist sync complete: {added_count} added, {updated_count} updated, {unchanged_count} unchanged")

    return added_count, updated_count, unchanged_count


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
    for playlist_name, playlist_id in spotify_playlists:
        # Check if playlist exists in database
        if playlist_id in existing_playlists:
            existing_playlist = existing_playlists[playlist_id]

            # Check if playlist details have changed
            if (existing_playlist.name != playlist_name.strip()):
                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'old_name': existing_playlist.name,
                })
            else:
                unchanged_count += 1
        else:
            # Mark for addition
            playlists_to_add.append({
                'id': playlist_id,
                'name': playlist_name.strip()
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

            # Generate ID
            metadata = {'title': normalized_title, 'artist': normalized_artist}
            track_id = generate_local_track_id(metadata)

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

    return tracks_to_add, tracks_to_update, unchanged_tracks


def analyze_track_playlist_associations(master_playlist_id: str, force_full_refresh: bool = False) -> dict:
    """
    Analyze what changes would be made to track-playlist associations without actually making them.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a full refresh

    Returns:
        Dictionary with statistics about the analysis
    """
    spotify_client = authenticate_spotify()

    # Build a track_id_map for analysis
    all_track_ids = set()
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()
        all_track_ids = {track.track_id for track in tracks}

    # Get all current association data in a single operation
    current_associations = {}
    with UnitOfWork() as uow:
        for track_id in all_track_ids:
            playlist_ids = set(uow.track_playlist_repository.get_playlist_ids_for_track(track_id))
            current_associations[track_id] = playlist_ids

    # Fetch all playlists from Spotify
    user_playlists = fetch_playlists(spotify_client, force_refresh=force_full_refresh)

    # Get current playlist tracks from Spotify - this is expensive but necessary
    spotify_playlist_tracks = {}
    for _, playlist_id in user_playlists:
        if playlist_id != master_playlist_id:  # Skip MASTER playlist
            track_ids = get_playlist_track_ids(spotify_client, playlist_id, force_refresh=force_full_refresh)
            spotify_playlist_tracks[playlist_id] = set(track_ids)

    # Now determine what should change
    tracks_with_changes = []
    associations_to_add = 0
    associations_to_remove = 0

    # For each track in our database, check which playlists it should be in
    for track_id in all_track_ids:
        # Determine which playlists this track should be in according to Spotify
        should_be_in = set()
        for playlist_id, tracks in spotify_playlist_tracks.items():
            if track_id in tracks:
                should_be_in.add(playlist_id)

        # Compare with current associations
        current = current_associations.get(track_id, set())

        to_add = should_be_in - current
        to_remove = current - should_be_in

        if to_add or to_remove:
            # Get track details for reporting
            track_info = None
            with UnitOfWork() as uow:
                track = uow.track_repository.get_by_id(track_id)
                if track:
                    track_info = f"{track.artists} - {track.title}"
                else:
                    track_info = f"Unknown Track (ID: {track_id})"

            # Record this change
            tracks_with_changes.append({
                'track_id': track_id,
                'track_info': track_info,
                'add_to': list(to_add),
                'remove_from': list(to_remove)
            })

            associations_to_add += len(to_add)
            associations_to_remove += len(to_remove)

    # Prepare sample data
    samples = []
    for change in tracks_with_changes[:20]:  # First 20 for display
        # Get playlist names for reporting
        add_names = []
        remove_names = []

        with UnitOfWork() as uow:
            for pid in change['add_to']:
                pl = uow.playlist_repository.get_by_id(pid)
                if pl:
                    add_names.append(pl.name)
                else:
                    add_names.append(f"Unknown Playlist (ID: {pid})")

            for pid in change['remove_from']:
                pl = uow.playlist_repository.get_by_id(pid)
                if pl:
                    remove_names.append(pl.name)
                else:
                    remove_names.append(f"Unknown Playlist (ID: {pid})")

        samples.append({
            'track': change['track_info'],
            'add_to': add_names,
            'remove_from': remove_names
        })

    # Calculate some statistics
    stats = {
        'tracks_with_playlists': len(current_associations),
        'tracks_without_playlists': len(all_track_ids) - len(current_associations),
        'total_associations': sum(len(playlists) for playlists in current_associations.values())
    }

    return {
        "tracks_with_changes": tracks_with_changes,
        "associations_to_add": associations_to_add,
        "associations_to_remove": associations_to_remove,
        "samples": samples,
        "stats": stats
    }


def analyze_master_sync(spotify_client, master_playlist_id: str):
    """
    Analyze what changes would be made by sync_to_master_playlist without executing them.

    Args:
        spotify_client: Authenticated Spotify client
        master_playlist_id: ID of the MASTER playlist

    Returns:
        Dictionary with analysis results
    """
    # Get current tracks in MASTER playlist
    master_track_ids = set(get_playlist_track_ids(spotify_client, master_playlist_id, force_refresh=True))

    # Fetch all playlists
    user_playlists = fetch_playlists(spotify_client, force_refresh=True)

    # Filter playlists safely
    other_playlists = []
    for pl in user_playlists:
        if isinstance(pl, tuple):
            playlist_name = pl[0] if len(pl) > 0 else "Unknown"

            if len(pl) == 2:
                playlist_id = pl[1]
            elif len(pl) >= 3:
                playlist_id = pl[2]
            else:
                continue

            if playlist_id != master_playlist_id:
                other_playlists.append((playlist_name, playlist_id))

    # Track which songs come from which playlists
    new_tracks_by_playlist = {}

    # Process each playlist
    for playlist_name, playlist_id in other_playlists:
        # Get tracks for this playlist
        playlist_track_ids = get_playlist_track_ids(spotify_client, playlist_id, force_refresh=True)

        # Filter out local files and find tracks not in master
        new_track_ids = []
        for track_id in playlist_track_ids:
            if track_id.startswith('spotify:local:') or track_id.startswith('local_'):
                continue

            if track_id not in master_track_ids:
                new_track_ids.append(track_id)

        if not new_track_ids:
            continue

        # Get track details
        playlist_tracks = []
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
                print(f"Error fetching details for track batch: {e}")
                continue

        if playlist_tracks:
            new_tracks_by_playlist[playlist_name] = playlist_tracks

    # Calculate statistics
    total_tracks = sum(len(tracks) for tracks in new_tracks_by_playlist.values())

    # Prepare sample data for display
    sample_playlists = []
    for playlist_name, tracks in sorted(new_tracks_by_playlist.items())[:5]:  # Limit to 5 playlists
        sample_tracks = sorted(tracks, key=lambda x: (x['artists'], x['name']))[:5]  # Limit to 5 tracks per playlist
        sample_playlists.append({
            'name': playlist_name,
            'track_count': len(tracks),
            'sample_tracks': sample_tracks
        })

    # Create analysis result
    analysis = {
        'total_tracks_to_add': total_tracks,
        'playlists_with_new_tracks': len(new_tracks_by_playlist),
        'sample_playlists': sample_playlists,
        'needs_confirmation': total_tracks > 0
    }

    return analysis


def sync_master_tracks_incremental(master_playlist_id: str, force_full_refresh: bool = False) -> Tuple[int, int, int]:
    """
    Incrementally sync tracks from the MASTER playlist to the database.
    Only fetches and updates tracks that have changed. Does NOT update playlist associations.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a full refresh, ignoring existing data

    Returns:
        Tuple of (added, updated, unchanged) counts
    """
    sync_logger.info(f"Starting incremental master tracks sync for playlist {master_playlist_id}")
    print("Starting incremental master tracks sync analysis...")

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

    # Display summary of changes
    print("\nMaster Tracks SYNC ANALYSIS COMPLETE")
    print("=================================")
    print(f"\nTracks to add: {len(tracks_to_add)}")
    print(f"Tracks to update: {len(tracks_to_update)}")
    print(f"Unchanged tracks: {len(unchanged_tracks)}")

    # Display detailed changes
    if tracks_to_add:
        print("\nNEW TRACKS TO ADD:")
        print("=================")
        # Sort by artist for better readability
        sorted_tracks = sorted(tracks_to_add, key=lambda x: x['artists'] + x['title'])
        for i, track in enumerate(sorted_tracks[:10], 1):  # Show first 10
            local_indicator = " (LOCAL)" if track['is_local'] else ""
            print(f"{i}. {track['artists']} - {track['title']} ({track['album']}){local_indicator}")
        if len(sorted_tracks) > 10:
            print(f"...and {len(sorted_tracks) - 10} more tracks")

    if tracks_to_update:
        print("\nTRACKS TO UPDATE:")
        print("================")
        sorted_updates = sorted(tracks_to_update, key=lambda x: x['artists'] + x['title'])
        for i, track in enumerate(sorted_updates[:10], 1):  # Show first 10
            local_indicator = " (LOCAL)" if track['is_local'] else ""
            print(f"{i}. {track['id']}: {track['old_artists']} - {track['old_title']}")
            print(f"   → {track['artists']} - {track['title']}{local_indicator}")
            if track['old_album'] != track['album']:
                print(f"     Album: {track['old_album']} → {track['album']}")
        if len(sorted_updates) > 10:
            print(f"...and {len(sorted_updates) - 10} more tracks")

    # Ask for confirmation
    if tracks_to_add or tracks_to_update:
        confirmation = input("\nWould you like to proceed with these changes to the database? (y/n): ")
        if confirmation.lower() != 'y':
            sync_logger.info("Sync cancelled by user")
            print("Sync cancelled.")
            return 0, 0, len(unchanged_tracks)
    else:
        print("\nNo track changes needed. Database is up to date.")
        return 0, 0, len(unchanged_tracks)

    # If confirmed, apply the changes
    added_count = 0
    updated_count = 0

    print("\nApplying track changes to database...")
    with UnitOfWork() as uow:
        # Add new tracks
        for track_data in tracks_to_add:
            track_id = track_data['id']
            track_title = track_data['title']
            artist_names = track_data['artists']
            album_name = track_data['album']
            added_at = track_data['added_at']
            is_local = track_data['is_local']

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

    print(f"\nTrack sync complete: {added_count} added, {updated_count} updated, {len(unchanged_tracks)} unchanged")
    sync_logger.info(
        f"Track sync complete: {added_count} added, {updated_count} updated, {len(unchanged_tracks)} unchanged"
    )

    return added_count, updated_count, len(unchanged_tracks)


def sync_track_playlist_associations(master_playlist_id: str, force_full_refresh: bool = False) -> Dict[str, int]:
    """
    Sync track-playlist associations for all tracks in the database.
    This function makes direct API calls to Spotify for all tracks and playlists,
    ensuring the most up-to-date associations are stored.

    Args:
        master_playlist_id: ID of the master playlist
        force_full_refresh: Whether to force a fresh API call for each playlist

    Returns:
        Dictionary with statistics about associations
    """
    sync_logger.info("Starting track-playlist association sync")
    print("Starting track-playlist association sync...")

    # Get all tracks from database
    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()
        total_tracks = len(all_tracks)
        sync_logger.info(f"Found {total_tracks} tracks in database")

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists = uow.playlist_repository.get_all()
        total_playlists = len(all_playlists)
        sync_logger.info(f"Found {total_playlists} playlists in database")

    # Fetch all playlists directly from Spotify to ensure associations are fresh
    spotify_client = authenticate_spotify()
    spotify_playlists = fetch_playlists(spotify_client, force_refresh=force_full_refresh)

    # Filter out the master playlist for association lookups
    other_playlists = [pl for pl in spotify_playlists if pl[1] != master_playlist_id]

    print(f"Fetched {len(spotify_playlists)} playlists from Spotify ({len(other_playlists)} excluding MASTER)")

    # We'll track all associations to completely refresh the database
    track_playlist_map = {}

    print("Fetching track-playlist associations from Spotify...")

    # First, build a list of all track IDs for reference
    all_track_ids = {track.track_id for track in all_tracks}

    # Now fetch tracks for each playlist directly from Spotify
    for i, (playlist_name, playlist_id) in enumerate(other_playlists, 1):
        print(f"Processing playlist {i}/{len(other_playlists)}: {playlist_name}")

        # Always force a fresh API call to get the most up-to-date associations
        playlist_track_ids = get_playlist_track_ids(spotify_client, playlist_id, force_refresh=True)

        # Log number of local files
        local_files = [tid for tid in playlist_track_ids if tid.startswith('local_')]
        sync_logger.info(f"Found {len(local_files)} local files in playlist '{playlist_name}'")
        print(f"Found {len(local_files)} local files in playlist '{playlist_name}'")  # Add print for debugging

        # Log how many tracks were found for this playlist
        valid_tracks = [tid for tid in playlist_track_ids if tid in all_track_ids]
        sync_logger.info(f"Found {len(valid_tracks)} valid tracks in playlist '{playlist_name}'")

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
            "no_changes": True
        }

    confirmation = input("\nWould you like to update track-playlist associations in the database? (y/n): ")
    if confirmation.lower() != 'y':
        sync_logger.info("Association sync cancelled by user")
        print("Sync cancelled.")
        return {
            "tracks_with_playlists": tracks_with_playlists,
            "tracks_without_playlists": len(all_track_ids) - tracks_with_playlists,
            "total_associations": total_associations,
            "associations_added": 0,
            "associations_removed": 0
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
        "tracks_with_changes": len(actual_changes)
    }

    print(f"\nTrack-playlist association sync complete:")
    print(f"  - {associations_added} associations added")
    print(f"  - {associations_removed} associations removed")
    print(f"  - {len(actual_changes)} tracks had association changes")
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
    for playlist_name, playlist_id in spotify_playlists:
        # Check if playlist exists in database
        if playlist_id in existing_playlists:
            existing_playlist = existing_playlists[playlist_id]

            # Check if playlist details have changed
            if (existing_playlist.name != playlist_name.strip()):

                # Mark for update
                playlists_to_update.append({
                    'id': playlist_id,
                    'name': playlist_name.strip(),
                    'old_name': existing_playlist.name,
                })
            else:
                unchanged_count += 1
        else:
            # Mark for addition
            playlists_to_add.append({
                'id': playlist_id,
                'name': playlist_name.strip()
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

    # Analyze changes without applying them
    tracks_to_add = []
    tracks_to_update = []
    unchanged_tracks = []

    for track_data in master_tracks:
        track_id, track_title, artist_names, album_name, added_at = track_data

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
                'added_at': added_at
            })

    return tracks_to_add, tracks_to_update, unchanged_tracks
