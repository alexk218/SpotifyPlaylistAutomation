# Database connection function
import logging
import os

import pyodbc
from dotenv import load_dotenv

from drivers.spotify_client import authenticate_spotify, fetch_my_playlists

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

logging.basicConfig(filename='spotify_script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection():
    connection = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=DESKTOP-9GSEQH4\SQL_SERVER;'
        'DATABASE=Playlists;'
        'Trusted_Connection=yes;'
    )
    return connection


def post_my_playlists():
    spotify_client = authenticate_spotify()
    my_playlists = fetch_my_playlists(spotify_client)
    connection = get_db_connection()
    cursor = connection.cursor()

    for playlist_name, playlist_id in my_playlists:
        logging.info(f"Inserting playlist: {playlist_name}")

        cursor.execute("""
                    INSERT INTO MyPlaylists (PlaylistName, AddedDate)
                    VALUES (?, GETDATE())
                """, (playlist_name,))

    connection.commit()
    cursor.close()
    connection.close()


def clear_my_playlists():
    connection = get_db_connection()
    cursor = connection.cursor()
    logging.info("Clearing the MyPlaylists table")
    cursor.execute("DELETE FROM MyPlaylists")
    connection.commit()

    # Reset the identity column
    logging.info("Resetting Id identity seed")
    cursor.execute("DBCC CHECKIDENT ('MyPlaylists', RESEED, 0)")
    connection.commit()

    cursor.close()
    connection.close()


if __name__ == "__main__":
    clear_my_playlists()
    post_my_playlists()
