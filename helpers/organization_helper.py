"""
Optimized organization helpers that minimize Spotify API calls.

This module implements organization functions that use the database as the primary data source.
"""

import os
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple, Union
from os import PathLike
from mutagen.id3 import ID3, ID3NoHeaderError

from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger
from utils.symlink_tracker import tracker
from helpers.file_helper import create_symlink, cleanup_broken_symlinks

organization_logger = setup_logger('organization_helper', 'logs/organization.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def organize_songs_into_playlists(
        master_tracks_dir: Union[str, PathLike[str]],
        playlists_dir: Union[str, PathLike[str]],
        dry_run: bool = False
) -> None:
    """
    Organize songs into playlist folders with symlinks based on database associations.
    Displays planned actions and asks for confirmation before making changes.
    Uses database as the source of truth to completely avoid API calls.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist folders in
        dry_run: If True, only print what would be done, don't make changes
    """
    print("Analyzing playlist organization plan...")
    organization_logger.info("Starting playlist organization analysis")

    # Track planned operations
    planned_operations = {
        'directories_to_create': [],
        'symlinks_to_create': [],
        'tracks_without_trackid': [],
        'tracks_in_no_playlists': []
    }

    # Get all playlist information from the database
    with UnitOfWork() as uow:
        db_playlists = uow.playlist_repository.get_all()

    organization_logger.info(f"Retrieved {len(db_playlists)} playlists from database")

    # Check for playlists directories that need to be created
    for playlist in db_playlists:
        playlist_path = os.path.join(playlists_dir, playlist.name)

        if not os.path.exists(playlist_path):
            planned_operations['directories_to_create'].append(playlist.name)

    # Create a mapping of playlist_id to name for quick lookups
    playlist_names = {playlist.playlist_id: playlist.name for playlist in db_playlists}

    # Track processing stats
    tracks_processed = 0
    tracks_with_trackid = 0

    # Process all MP3 files in the master directory to plan symlinks
    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            if not filename.lower().endswith('.mp3'):
                continue

            tracks_processed += 1
            file_path = os.path.join(root, filename)

            # Extract TrackId from metadata
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' not in tags:
                    organization_logger.warning(f"No TrackId found in metadata for: {filename}")
                    planned_operations['tracks_without_trackid'].append(filename)
                    continue

                track_id = tags['TXXX:TRACKID'].text[0]
                tracks_with_trackid += 1
                organization_logger.debug(f"Found TrackId in {filename}: {track_id}")

                # Get associated playlists from database
                with UnitOfWork() as uow:
                    playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track_id)

                # Convert playlist IDs to names using our mapping
                playlist_names_for_track = [playlist_names[pid] for pid in playlist_ids if pid in playlist_names]

                if not playlist_names_for_track:
                    organization_logger.warning(
                        f"No playlist associations found for track: {filename} (ID: {track_id})")
                    planned_operations['tracks_in_no_playlists'].append(filename)
                    continue

                # Plan symlinks in each associated playlist directory
                for playlist_name in playlist_names_for_track:
                    # Skip MASTER playlist symlinks if it exists
                    if playlist_name.upper() == "MASTER":
                        continue

                    playlist_path = os.path.join(playlists_dir, playlist_name)
                    symlink_path = os.path.join(playlist_path, filename)

                    if not os.path.exists(symlink_path):
                        planned_operations['symlinks_to_create'].append((file_path, symlink_path, playlist_name))

            except ID3NoHeaderError:
                organization_logger.warning(f"No ID3 tags found in: {filename}")
                planned_operations['tracks_without_trackid'].append(filename)
                continue
            except Exception as e:
                organization_logger.error(f"Error processing {filename}: {e}")
                continue

    # Display summary of planned actions
    print("\nPLAYLIST ORGANIZATION PLAN")
    print("=========================")
    print(f"Total tracks processed: {tracks_processed}")
    print(f"Tracks with TrackId: {tracks_with_trackid}")
    print(f"Tracks without TrackId: {len(planned_operations['tracks_without_trackid'])}")
    print(f"Tracks not in any playlists: {len(planned_operations['tracks_in_no_playlists'])}")
    print(f"Playlist directories to create: {len(planned_operations['directories_to_create'])}")
    print(f"Symlinks to create: {len(planned_operations['symlinks_to_create'])}")

    # Show more detailed information
    if planned_operations['directories_to_create']:
        print("\nPLAYLIST DIRECTORIES TO CREATE:")
        print("==============================")
        for dir_name in sorted(planned_operations['directories_to_create']):
            print(f"• {dir_name}")

    # Show sample of symlinks to create, grouped by playlist
    if planned_operations['symlinks_to_create']:
        print("\nSYMLINKS TO CREATE (SAMPLE):")
        print("===========================")

        # Group symlinks by playlist name
        symlinks_by_playlist = {}
        for source, target, playlist in planned_operations['symlinks_to_create']:
            if playlist not in symlinks_by_playlist:
                symlinks_by_playlist[playlist] = []
            symlinks_by_playlist[playlist].append((source, target))

        # Display up to 5 playlists
        playlist_sample = list(sorted(symlinks_by_playlist.keys()))[:5]
        for playlist in playlist_sample:
            print(f"\n• Playlist: {playlist}")
            # Display up to 5 tracks per playlist
            track_sample = symlinks_by_playlist[playlist][:5]
            for source, target in track_sample:
                print(f"  - {os.path.basename(source)}")

            if len(symlinks_by_playlist[playlist]) > 5:
                print(f"  - ... and {len(symlinks_by_playlist[playlist]) - 5} more tracks")

        if len(symlinks_by_playlist) > 5:
            print(f"\n... and {len(symlinks_by_playlist) - 5} more playlists")

    if planned_operations['tracks_without_trackid'] and len(planned_operations['tracks_without_trackid']) <= 10:
        print("\nTRACKS WITHOUT TRACKID:")
        print("======================")
        for filename in sorted(planned_operations['tracks_without_trackid']):
            print(f"• {filename}")
    elif planned_operations['tracks_without_trackid']:
        print("\nTRACKS WITHOUT TRACKID (FIRST 10):")
        print("================================")
        for filename in sorted(planned_operations['tracks_without_trackid'])[:10]:
            print(f"• {filename}")
        print(f"... and {len(planned_operations['tracks_without_trackid']) - 10} more tracks")

    # Skip confirmation if dry run since no changes will be made
    if dry_run:
        print("\nDRY RUN - No changes will be made")
        return

    # Ask for confirmation
    if planned_operations['symlinks_to_create'] or planned_operations['directories_to_create']:
        confirmation = input("\nWould you like to proceed with these playlist organization changes? (y/n): ")
        if confirmation.lower() != 'y':
            organization_logger.info("Organization cancelled by user")
            print("Organization cancelled.")
            return
    else:
        print("\nNo changes needed. All tracks are already organized.")
        return

    # If confirmed, proceed with the actual organization
    with tracker.tracking_session():
        print("\nOrganizing songs into playlist folders...")
        organization_logger.info("Starting to organize songs into playlists")

        # First, clean up any broken symlinks
        cleanup_broken_symlinks(playlists_dir, dry_run=False)

        # Create playlist directories
        for playlist_name in planned_operations['directories_to_create']:
            playlist_path = os.path.join(playlists_dir, playlist_name)
            os.makedirs(playlist_path)
            organization_logger.info(f"Created new playlist directory: {playlist_path}")

        # Create symlinks
        symlinks_created = 0
        for source, target, _ in planned_operations['symlinks_to_create']:
            create_symlink(source, target)
            symlinks_created += 1

            # Show progress every 100 symlinks
            if symlinks_created % 100 == 0:
                print(f"Created {symlinks_created}/{len(planned_operations['symlinks_to_create'])} symlinks...")

        # Print summary
        print("\nOrganization complete!")
        print(f"Created {len(planned_operations['directories_to_create'])} playlist directories")
        print(f"Created {symlinks_created} symlinks")

        organization_logger.info("Playlist organization complete!")
        organization_logger.info(f"Stats: Created directories={len(planned_operations['directories_to_create'])}, " +
                                 f"Created symlinks={symlinks_created}")
