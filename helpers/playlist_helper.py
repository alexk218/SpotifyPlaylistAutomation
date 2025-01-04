import os
from os import PathLike
from typing import Union
from mutagen.id3 import ID3, ID3NoHeaderError
from helpers.file_helper import create_symlink
from sql.helpers.db_helper import fetch_playlists_for_track, fetch_all_playlists_db
from utils.logger import setup_logger

db_logger = setup_logger('db_logger', 'sql/db.log')


# Organizes all songs from 'K:\\tracks_master' by creating symlinks in playlist folders based on db association
# ! CREATES FOLDERS WITH SYMLINKS
def organize_songs_into_playlists(
    master_tracks_dir: Union[str, PathLike[str]],
    playlists_dir: Union[str, PathLike[str]],
    dry_run: bool = False,
    interactive: bool = False
) -> None:
    print("Organizing songs into playlist folders with symlinks...")
    db_logger.info("Starting to organize songs into playlists.")

    # Get all playlists from database to create directories
    playlists = fetch_all_playlists_db()

    # Create playlist directories if they don't exist
    for playlist in playlists:
        playlist_id, playlist_name = playlist
        playlist_path = os.path.join(playlists_dir, playlist_name)

        if not dry_run:
            if os.path.exists(playlist_path):
                db_logger.info(f"Playlist directory already exists: {playlist_path}")
            else:
                os.makedirs(playlist_path)
                db_logger.info(f"Created new playlist directory: {playlist_path}")
        else:
            if os.path.exists(playlist_path):
                db_logger.info(f"[DRY RUN] Playlist directory already exists: {playlist_path}")
            else:
                db_logger.info(f"[DRY RUN] Would create new playlist directory: {playlist_path}")

    # Process each track in the master directory
    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            if not filename.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, filename)

            # Extract TrackId from metadata
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' not in tags:
                    db_logger.warning(f"No TrackId found in metadata for: {filename}")
                    continue

                track_id = tags['TXXX:TRACKID'].text[0]
                db_logger.info(f"Found TrackId in {filename}: {track_id}")

                # Get associated playlists for this track
                associated_playlists = fetch_playlists_for_track(track_id)

                if not associated_playlists:
                    db_logger.warning(f"No playlist associations found for track: {filename} (ID: {track_id})")
                    continue

                # Create symlinks in each associated playlist directory
                for playlist_name in associated_playlists:
                    playlist_path = os.path.join(playlists_dir, playlist_name)
                    symlink_path = os.path.join(playlist_path, filename)

                    if dry_run:
                        db_logger.info(f"[DRY RUN] Would create symlink: {symlink_path} -> {file_path}")
                    else:
                        create_symlink(file_path, symlink_path)

            except ID3NoHeaderError:
                db_logger.warning(f"No ID3 tags found in: {filename}")
                continue
            except Exception as e:
                db_logger.error(f"Error processing {filename}: {e}")
                continue

    db_logger.info("Playlist organization complete!")


def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']
