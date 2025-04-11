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
    Uses database as the source of truth to completely avoid API calls.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist folders in
        dry_run: If True, only print what would be done, don't make changes
    """
    with tracker.tracking_session():
        print("Organizing songs into playlist folders with symlinks...")
        organization_logger.info("Starting to organize songs into playlists")

        # First, clean up any broken symlinks
        cleanup_broken_symlinks(playlists_dir, dry_run)

        # Get all playlist information from the database
        with UnitOfWork() as uow:
            db_playlists = uow.playlist_repository.get_all()

        organization_logger.info(f"Retrieved {len(db_playlists)} playlists from database")

        # Create playlist directories
        for playlist in db_playlists:
            playlist_path = os.path.join(playlists_dir, playlist.name)

            if not dry_run:
                if os.path.exists(playlist_path):
                    organization_logger.info(f"Playlist directory already exists: {playlist_path}")
                else:
                    os.makedirs(playlist_path)
                    organization_logger.info(f"Created new playlist directory: {playlist_path}")
            else:
                if os.path.exists(playlist_path):
                    organization_logger.info(f"[DRY RUN] Playlist directory already exists: {playlist_path}")
                else:
                    organization_logger.info(f"[DRY RUN] Would create new playlist directory: {playlist_path}")

        # Create a mapping of playlist_id to name for quick lookups
        playlist_names = {playlist.playlist_id: playlist.name for playlist in db_playlists}

        # Track processing stats
        tracks_processed = 0
        tracks_with_trackid = 0
        tracks_without_trackid = 0
        symlinks_created = 0
        symlinks_skipped = 0
        tracks_in_no_playlists = 0

        # Process all MP3 files in the master directory
        print("\nCreating symlinks...")
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
                        tracks_without_trackid += 1
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
                        tracks_in_no_playlists += 1
                        continue

                    # Create symlinks in each associated playlist directory
                    for playlist_name in playlist_names_for_track:
                        playlist_path = os.path.join(playlists_dir, playlist_name)

                        # Skip MASTER playlist symlinks if it exists
                        if playlist_name.upper() == "MASTER":
                            continue

                        symlink_path = os.path.join(playlist_path, filename)

                        if dry_run:
                            organization_logger.info(f"[DRY RUN] Would create symlink: {symlink_path} -> {file_path}")
                        else:
                            if os.path.exists(symlink_path):
                                organization_logger.debug(f"Symlink already exists: {symlink_path}")
                                symlinks_skipped += 1
                            else:
                                create_symlink(file_path, symlink_path)
                                symlinks_created += 1

                except ID3NoHeaderError:
                    organization_logger.warning(f"No ID3 tags found in: {filename}")
                    tracks_without_trackid += 1
                    continue
                except Exception as e:
                    organization_logger.error(f"Error processing {filename}: {e}")
                    continue

                # Show progress every 100 tracks
                if tracks_processed % 100 == 0:
                    print(f"Processed {tracks_processed} tracks...")

        # Print summary
        print("\nOrganization complete!")
        print(f"Total tracks processed: {tracks_processed}")
        print(f"Tracks with TrackId: {tracks_with_trackid}")
        print(f"Tracks without TrackId: {tracks_without_trackid}")
        print(f"Tracks not in any playlists: {tracks_in_no_playlists}")
        if not dry_run:
            print(f"Symlinks created: {symlinks_created}")
            print(f"Symlinks skipped (already exist): {symlinks_skipped}")

        organization_logger.info("Playlist organization complete!")
        organization_logger.info(f"Stats: Processed={tracks_processed}, With TrackId={tracks_with_trackid}, " +
                                 f"Without TrackId={tracks_without_trackid}, NotInPlaylists={tracks_in_no_playlists}, " +
                                 f"Created={symlinks_created}, Skipped={symlinks_skipped}")
