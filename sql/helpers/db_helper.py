import logging

import pyodbc

from drivers.spotify_client import authenticate_spotify, fetch_my_playlists, fetch_master_tracks


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

    for playlist_name, playlist_id in my_playlists:
        logging.info(f"Inserting playlist: {playlist_name}")

        cursor.execute("""
                      INSERT INTO MyPlaylists (PlaylistName, AddedDate)
                      VALUES (?, GETDATE())
                  """, (playlist_name,))

    connection.commit()
    cursor.close()
    connection.close()


def insert_master_tracks():
    print("Inserting MasterTracks...")
    spotify_client = authenticate_spotify()
    master_tracks = fetch_master_tracks(spotify_client)
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for track_name, artist_name in master_tracks:
            logging.info(f"Inserting track: {track_name} by {artist_name}")

            cursor.execute("""
                              INSERT INTO MasterTracks (TrackTitle, Artist, AddedDate)
                              VALUES (?, ?, GETDATE())
                          """, (track_name, artist_name))

        connection.commit()
    except Exception as e:
        logging.error(f"Error inserting tracks: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()