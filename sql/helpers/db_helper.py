import pyodbc
import logging
from dotenv import load_dotenv
from drivers.spotify_client import *
load_dotenv()

SERVER_CONNECTION_STRING = os.getenv('SERVER_CONNECTION_STRING')
DATABASE_NAME = os.getenv('DATABASE_NAME')
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')


def get_db_connection():
    connection = pyodbc.connect(
        r'DRIVER={SQL Server};'
        fr'SERVER={SERVER_CONNECTION_STRING};'
        fr'DATABASE={DATABASE_NAME};'
        r'Trusted_Connection=yes;'
    )
    return connection


def clear_db():
    clear_playlists()
    clear_master_tracks()


def insert_db():
    insert_playlists()
    insert_tracks_and_associations()


def clear_playlists():
    print("Clearing Playlists...")
    connection = get_db_connection()
    cursor = connection.cursor()
    logging.info("Clearing the Playlists table")
    cursor.execute("DELETE FROM Playlists")
    connection.commit()

    cursor.close()
    connection.close()


def clear_master_tracks():
    print("Clearing Tracks...")
    connection = get_db_connection()
    cursor = connection.cursor()
    logging.info("Clearing the Tracks table")
    cursor.execute("DELETE FROM Tracks")
    connection.commit()

    cursor.close()
    connection.close()


def insert_playlists():
    print("Inserting Playlists...")
    spotify_client = authenticate_spotify()
    my_playlists = fetch_playlists(spotify_client)
    connection = get_db_connection()
    cursor = connection.cursor()

    for playlist_name, playlist_description, playlist_id in my_playlists:
        logging.info(f"Inserting playlist: {playlist_name}")

        cursor.execute("""
            INSERT INTO Playlists (PlaylistId, PlaylistName, PlaylistDescription)
            VALUES (?, ?, ?)
        """, (playlist_id, playlist_name, playlist_description))

    connection.commit()
    cursor.close()
    connection.close()


def insert_tracks_and_associations():
    print("Inserting Tracks and Associations...")
    logging.info("Inserting Tracks and Associations...")
    spotify_client = authenticate_spotify()

    # Fetch all tracks from 'MASTER' playlist
    master_tracks = fetch_master_tracks(spotify_client, MASTER_PLAYLIST_ID)

    # Find playlists for each track
    tracks_with_playlists = find_playlists_for_master_tracks(spotify_client, master_tracks, MASTER_PLAYLIST_ID)

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for track in tracks_with_playlists:
            track_id, track_title, artist_names, album_name, playlists = track

            logging.info(f"Inserting track: {track_title} (ID: {track_id})")

            # Insert into Tracks
            cursor.execute("""
                   INSERT INTO Tracks (TrackId, TrackTitle, Artists, Album)
                   VALUES (?, ?, ?, ?)
                   """, (track_id, track_title, artist_names, album_name))

            # Insert into TrackPlaylists
            for playlist_name in playlists:
                # Fetch PlaylistId from Playlists table
                cursor.execute("""
                       SELECT PlaylistId FROM Playlists
                       WHERE PlaylistName = ?
                       """, (playlist_name,))
                result = cursor.fetchone()
                if result:
                    playlist_id = result[0]
                    cursor.execute("""
                           INSERT INTO TrackPlaylists (TrackId, PlaylistId)
                           VALUES (?, ?)
                           """, (track_id, playlist_id))
                else:
                    logging.warning(f"Playlist '{playlist_name}' not found in Playlists table.")

        connection.commit()
    except Exception as e:
        logging.error(f"Error inserting tracks and associations: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# TODO: To be used in the future. Rather than always making API calls. Use the db.
def fetch_master_tracks_db():
    print('Fetching all tracks from Tracks table')
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT t.TrackTitle, t.Artists, t.Album
            FROM Tracks t
        """)
        tracks = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    return tracks
