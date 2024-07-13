import logging
import os

import pyodbc
from dotenv import load_dotenv

from drivers.spotify_client import authenticate_spotify, fetch_my_playlists, fetch_master_tracks, \
    find_playlists_for_tracks

load_dotenv()

SERVER_CONNECTION_STRING = os.getenv('SERVER_CONNECTION_STRING')
DATABASE_NAME = os.getenv('DATABASE_NAME')

def get_db_connection():
    connection = pyodbc.connect(
        r'DRIVER={SQL Server};'
        fr'SERVER={SERVER_CONNECTION_STRING}'
        fr'DATABASE={DATABASE_NAME}'
        r'Trusted_Connection=yes;'
    )
    return connection


def clear_db():
    clear_my_playlists()
    clear_master_tracks()


def insert_db():
    insert_my_playlists()
    insert_master_tracks()


def clear_my_playlists():
    print("Clearing MyPlaylists...")
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


def clear_master_tracks():
    print("Clearing MasterTracks...")
    connection = get_db_connection()
    cursor = connection.cursor()
    logging.info("Clearing the MasterTracks table")
    cursor.execute("DELETE FROM MasterTracks")
    connection.commit()

    # Reset the identity column
    logging.info("Resetting Id identity seed")
    cursor.execute("DBCC CHECKIDENT ('MasterTracks', RESEED, 0)")
    connection.commit()

    cursor.close()
    connection.close()


def insert_my_playlists():
    print("Inserting MyPlaylists...")
    spotify_client = authenticate_spotify()
    my_playlists = fetch_my_playlists(spotify_client)
    connection = get_db_connection()
    cursor = connection.cursor()

    for playlist_name, file_path, playlist_id in my_playlists:
        logging.info(f"Inserting playlist: {playlist_name}")

        cursor.execute("""
                      INSERT INTO MyPlaylists (PlaylistName, FilePath, PlaylistId, AddedDate)
                      VALUES (?, ?, ?, GETDATE())
                  """, (playlist_name, file_path, playlist_id))

    connection.commit()
    cursor.close()
    connection.close()


def insert_master_tracks():
    print("Inserting MasterTracks...")
    spotify_client = authenticate_spotify()

    my_playlists = fetch_my_playlists(spotify_client)
    master_tracks = fetch_master_tracks(spotify_client, my_playlists)
    tracks_with_playlists = find_playlists_for_tracks(spotify_client, master_tracks, my_playlists)

    # master_tracks = fetch_master_tracks(spotify_client)
    # tracks_with_playlists = find_playlists_for_tracks(spotify_client, master_tracks)
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for track_name, artist_name, playlists in tracks_with_playlists:
            playlist_str = ", ".join(playlists)
            logging.info(f"Inserting track: {track_name} by {artist_name}, Playlists: {playlist_str}")

            cursor.execute("""
                   INSERT INTO MasterTracks (TrackTitle, Artists, InPlaylists, AddedDate)
                   VALUES (?, ?, ?, GETDATE())
               """, (track_name, artist_name, playlist_str))

        connection.commit()
    except Exception as e:
        logging.error(f"Error inserting tracks: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
