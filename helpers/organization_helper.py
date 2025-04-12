import os
from os import PathLike
from pathlib import Path
from typing import Union

from mutagen.id3 import ID3

from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

organization_logger = setup_logger('organization_helper', 'logs/organization.log')

# Get the path to the current file
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def organize_songs_into_m3u_playlists(
        master_tracks_dir: Union[str, PathLike[str]],
        playlists_dir: Union[str, PathLike[str]],
        extended: bool = True,
        dry_run: bool = False,
        overwrite: bool = True,
        only_changed: bool = True
) -> None:
    """
    Organize songs into M3U playlist files based on database associations.
    This is an alternative to creating symlinks, which solves the duplicate
    tracks problem when importing to Rekordbox.

    Args:
        only_changed: Only update playlists that have actually changed
        overwrite: Whether to overwrite existing playlist files
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        extended: Whether to use extended M3U format with metadata
        dry_run: If True, only print what would be done, don't create files
    """
    from helpers.m3u_helper import generate_all_m3u_playlists

    print("Analyzing M3U playlist organization plan...")
    organization_logger.info("Starting M3U playlist organization analysis")

    # Get all playlist information from the database
    with UnitOfWork() as uow:
        db_playlists = uow.playlist_repository.get_all()

    organization_logger.info(f"Retrieved {len(db_playlists)} playlists from database")

    # Filter out the MASTER playlist if it exists
    db_playlists = [p for p in db_playlists if p.name.upper() != "MASTER"]

    # Count tracks that have IDs and are in playlists
    track_count = 0
    track_with_id_count = 0

    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            if not filename.lower().endswith('.mp3'):
                continue

            track_count += 1
            file_path = os.path.join(root, filename)

            # Check if file has a TrackId
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    track_with_id_count += 1
            except Exception:
                pass

    print(f"\nFOUND {len(db_playlists)} PLAYLISTS AND {track_count} TRACKS")
    print(f"Tracks with TrackID: {track_with_id_count}")
    print("\nPlaylists that will be generated:")
    print("================================")

    # Get playlist track counts from database for display
    playlist_stats = []
    for playlist in db_playlists:
        with UnitOfWork() as uow:
            tracks = uow.track_playlist_repository.get_track_ids_for_playlist(playlist.playlist_id)
            track_count = len(tracks)
            playlist_stats.append({
                'name': playlist.name,
                'track_count': track_count,
                'id': playlist.playlist_id
            })

    # Sort playlists by track count (descending)
    playlist_stats.sort(key=lambda x: x['track_count'], reverse=True)

    # Display playlists with track counts
    for i, stat in enumerate(playlist_stats):
        print(f"{i + 1}. {stat['name']} ({stat['track_count']} tracks)")

        # Log each playlist to the log file
        organization_logger.info(f"Playlist: {stat['name']} | Tracks: {stat['track_count']} | ID: {stat['id']}")

    # Summarize track distribution
    non_empty = sum(1 for stat in playlist_stats if stat['track_count'] > 0)
    print(f"\nTotal playlists to generate: {len(playlist_stats)}")
    print(f"Non-empty playlists: {non_empty}")
    print(f"Empty playlists: {len(playlist_stats) - non_empty}")

    # Skip confirmation if dry run
    if dry_run:
        print("\nDRY RUN - No files will be created")
        return

    # Check for existing M3U files
    m3u_files_exist = False
    if os.path.exists(playlists_dir):
        existing_files = [f for f in os.listdir(playlists_dir) if f.endswith('.m3u')]
        m3u_files_exist = len(existing_files) > 0

    # If only updating changed playlists, check for changes first
    changed_playlists = []
    if m3u_files_exist and only_changed and not dry_run:
        print("\nChecking for playlist changes...")
        organization_logger.info("Analyzing playlist changes")

        from helpers.m3u_helper import compare_playlist_with_m3u, sanitize_filename

        with UnitOfWork() as uow:
            for playlist in db_playlists:
                if playlist.name.upper() == "MASTER":
                    continue

                safe_name = sanitize_filename(playlist.name, preserve_spaces=True)
                m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

                if os.path.exists(m3u_path):
                    # Pass master_tracks_dir to the compare function to only consider local files
                    has_changes, added, removed = compare_playlist_with_m3u(
                        playlist.playlist_id,
                        m3u_path,
                        master_tracks_dir
                    )

                    if has_changes:
                        changed_playlists.append({
                            'name': playlist.name,
                            'added': len(added),
                            'removed': len(removed)
                        })
                else:
                    # New playlist
                    changed_playlists.append({'name': playlist.name, 'added': 'New', 'removed': 0})

        if changed_playlists:
            print(f"\nFound {len(changed_playlists)} playlists with changes:")
            print("====================================")
            for i, p in enumerate(changed_playlists):
                if p['added'] == 'New':
                    print(f"{i + 1}. {p['name']} (NEW PLAYLIST)")
                else:
                    print(f"{i + 1}. {p['name']} (+{p['added']} tracks, -{p['removed']} tracks)")

            print("\nOnly these playlists will be updated.")
        else:
            print("\nNo playlist changes detected. All playlists are up to date.")
            if not overwrite:
                print("No M3U files will be generated. Use --overwrite to force regeneration.")
                return
            else:
                print("Using --overwrite flag will regenerate all files anyway.")

    # Explain what will happen with existing files
    if m3u_files_exist and overwrite and not only_changed:
        existing_files = [f for f in os.listdir(playlists_dir) if f.endswith('.m3u')]
        if existing_files:
            print(f"\nNOTE: {len(existing_files)} existing M3U files found in the target directory.")
            print("All files will be overwritten with updated versions.")

    # Ask for confirmation
    confirmation = input("\nWould you like to generate M3U playlist files? (y/n): ")
    if confirmation.lower() != 'y':
        organization_logger.info("M3U playlist generation cancelled by user")
        print("Operation cancelled.")
        return

    # Generate all M3U playlists
    print("\nGenerating M3U playlist files...")
    stats = generate_all_m3u_playlists(
        master_tracks_dir=master_tracks_dir,
        playlists_dir=playlists_dir,
        extended=extended,
        skip_master=True,
        overwrite=overwrite,
        only_changed=only_changed,
        changed_playlists=[p['name'] for p in changed_playlists] if only_changed else None
    )

    # Print summary
    print("\nM3U playlist generation complete!")
    if only_changed:
        print(f"Created {stats['playlists_created']} new playlists")
        print(f"Updated {stats['playlists_updated']} existing playlists")
        print(f"Skipped {stats['playlists_unchanged']} unchanged playlists")
    else:
        print(f"Generated {stats['playlists_created'] + stats['playlists_updated']} playlist files")
    print(f"Added {stats['total_tracks_added']} tracks to playlists")

    if stats['empty_playlists']:
        print(f"\n{len(stats['empty_playlists'])} playlists had no tracks found:")
        for playlist in stats['empty_playlists'][:5]:  # Show first 5
            print(f"  - {playlist}")
        if len(stats['empty_playlists']) > 5:
            print(f"  - ... and {len(stats['empty_playlists']) - 5} more")

    organization_logger.info("M3U playlist organization complete!")
