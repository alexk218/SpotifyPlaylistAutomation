import argparse
import sys
import os
import logging
from dotenv import load_dotenv
from sql.helpers.db_helper import *

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

logging.basicConfig(filename='db.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    parser = argparse.ArgumentParser(description='Database operations for Spotify playlists.')
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
            # print(f"Title: {track_title}, Artists: {artists}, Album: {album}, Playlist: {playlist_name}")
            # todo: add Playlist to this. would have to join this table with TrackPlaylists


if __name__ == "__main__":
    main()  # keep this uncommented if running in the CLI
    # clear_master_tracks()
    # insert_master_tracks()
    # clear_playlists()
    # insert_playlists()
    # insert_tracks_and_associations()
