# Database connection function
import logging
import os
import argparse

from dotenv import load_dotenv

from sql.helpers.db_helper import clear_db, insert_db, clear_my_playlists, insert_my_playlists, clear_master_tracks, \
    insert_master_tracks

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
    parser.add_argument('--my_playlists', action='store_true', help='Clear and insert data into MyPlaylists table')
    parser.add_argument('--master_tracks', action='store_true', help='Clear and insert data into MasterTracks table')
    parser.add_argument('--clear-my-playlists', action='store_true', help='Clear MyPlaylists table')
    parser.add_argument('--insert-my-playlists', action='store_true', help='Insert data into MyPlaylists table')
    parser.add_argument('--clear-master-tracks', action='store_true', help='Clear MasterTracks table')
    parser.add_argument('--insert-master-tracks', action='store_true', help='Insert data into MasterTracks table')

    args = parser.parse_args()

    if args.run_all:
        clear_db()
        insert_db()
    if args.clear_all:
        clear_db()
    if args.insert_all:
        insert_db()
    if args.my_playlists:
        clear_my_playlists()
        insert_my_playlists()
    if args.master_tracks:
        clear_master_tracks()
        insert_master_tracks()
    if args.clear_my_playlists:
        clear_my_playlists()
    if args.insert_my_playlists:
        insert_my_playlists()
    if args.clear_master_tracks:
        clear_master_tracks()
    if args.insert_master_tracks:
        insert_master_tracks()


if __name__ == "__main__":
    main()
