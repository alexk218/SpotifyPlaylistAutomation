import logging

import pyodbc

from drivers.spotify_client import authenticate_spotify, fetch_my_playlists

def get_db_connection():
    connection = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=DESKTOP-9GSEQH4\SQL_SERVER;'
        'DATABASE=Playlists;'
        'Trusted_Connection=yes;'
    )
    return connection

def clear_db():
    clear_my_playlists()

def insert_db():
    insert_my_playlists()

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

def insert_my_playlists():
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
