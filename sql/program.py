import argparse
import sys
import os
import logging
from dotenv import load_dotenv
from helpers.file_helper import embed_track_metadata
from sql.helpers.db_helper import *
from helpers.playlist_helper import organize_songs_into_playlists
from helpers.track_helper import find_track_id_fuzzy, extract_track_id_from_metadata
from utils.logger import setup_logger
import re
import uuid

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
PLAYLISTS_DIRECTORY = os.getenv("PLAYLISTS_DIRECTORY")

db_logger = setup_logger('db_logger', 'sql/db.log')

# logging.basicConfig(filename='db.log', level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    parser = argparse.ArgumentParser(description='Spotify Playlist Database Operations and Song Organization.')
    parser.add_argument('--run-all', action='store_true', help='Clear and insert data into all database tables')
    parser.add_argument('--clear-all', action='store_true', help='Clear all database tables')
    parser.add_argument('--insert-all', action='store_true', help='Insert data into all database tables')
    parser.add_argument('--my_playlists', action='store_true', help='Clear and insert data into Playlists table')
    parser.add_argument('--master_tracks', action='store_true', help='Clear and insert data into Tracks table')
    parser.add_argument('--clear-my-playlists', action='store_true', help='Clear Playlists table')
    parser.add_argument('--insert-my-playlists', action='store_true', help='Insert data into Playlists table')
    parser.add_argument('--clear-master-tracks', action='store_true', help='Clear Tracks table')
    parser.add_argument('--insert-master-tracks', action='store_true', help='Insert data into Tracks and TrackPlaylists tables')
    parser.add_argument('--fetch-master-tracks-db', action='store_true', help='Fetch and display tracks from MASTER playlist in the database')
    parser.add_argument('--fetch-all-playlists-db', action='store_true', help='Fetch and display all playlists from the database')
    parser.add_argument('--organize-songs', action='store_true', help='Organize downloaded songs into playlist folders with symlinks')
    parser.add_argument('--dry-run', action='store_true', help='Simulate the organization process without creating symlinks')
    parser.add_argument('--embed-metadata', action='store_true', help='Embed TrackId into song file metadata')
    parser.add_argument('--interactive', action='store_true', help='Enable interactive mode for low-confidence matches')

    args = parser.parse_args()

    if args.run_all:
        clear_db()
        insert_db()
    if args.clear_all:
        clear_db()
    if args.insert_all:
        insert_db()
    if args.my_playlists:
        clear_playlists()
        insert_playlists()
    if args.master_tracks:
        clear_master_tracks()
        insert_tracks_and_associations()
    if args.clear_my_playlists:
        clear_playlists()
    if args.insert_my_playlists:
        insert_playlists()
    if args.clear_master_tracks:
        clear_master_tracks()
    if args.insert_master_tracks:
        insert_tracks_and_associations()
    if args.fetch_master_tracks_db:
        tracks = fetch_master_tracks_db()
        print("\nMaster Playlist Tracks from Database:")
        for track in tracks:
            track_title, artists, album = track
            print(f"Title: {track_title}, Artists: {artists}, Album: {album}")
            # todo: add Playlist to this. would have to join this table with TrackPlaylists
    if args.fetch_all_playlists_db:
        playlists = fetch_all_playlists_db()
        print("\nAll Playlists from Database:")
        for pl in playlists:
            playlist_id, playlist_name = pl
            print(f"PlaylistId: {playlist_id}, PlaylistName: {playlist_name}")
    if args.embed_metadata:
        embed_track_metadata(MASTER_TRACKS_DIRECTORY)
    if args.organize_songs:
        organize_songs_into_playlists(MASTER_TRACKS_DIRECTORY, PLAYLISTS_DIRECTORY, dry_run=args.dry_run, interactive=args.interactive)


if __name__ == "__main__":
    main()  # keep this uncommented if running in the CLI
    # clear_master_tracks()
    # insert_master_tracks()
    # clear_playlists()
    # insert_playlists()
    # insert_tracks_and_associations()
