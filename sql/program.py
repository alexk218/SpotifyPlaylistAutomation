# Database connection function
import logging

import pyodbc

from drivers.spotify_client import authenticate_spotify, fetch_my_playlists

# from drivers.

logging.basicConfig(filename='spotify_script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection():
    connection = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=DESKTOP-9GSEQH4\SQL_SERVER;'
        'DATABASE=Playlists;'
        'Trusted_Connection=yes;'
        # 'UID=your_username;'
        # 'PWD=your_password'
    )
    return connection


def save_to_master_tracks():
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


if __name__ == "__main__":
    save_to_master_tracks()
