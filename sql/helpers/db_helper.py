import pyodbc
import logging
from dotenv import load_dotenv
from drivers.spotify_client import *
load_dotenv()

SERVER_CONNECTION_STRING = os.getenv('SERVER_CONNECTION_STRING')
DATABASE_NAME = os.getenv('DATABASE_NAME')
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_logger', 'sql/db.log')

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


# ! CLEAR DB
def clear_playlists():
    print("Clearing Playlists...")
    connection = get_db_connection()
    cursor = connection.cursor()
    db_logger.info("Clearing the Playlists table")
    cursor.execute("DELETE FROM Playlists")
    connection.commit()

    cursor.close()
    connection.close()


def clear_master_tracks():
    print("Clearing Tracks...")
    connection = get_db_connection()
    cursor = connection.cursor()
    db_logger.info("Clearing the Tracks table")
    cursor.execute("DELETE FROM Tracks")
    connection.commit()

    cursor.close()
    connection.close()


# ! INSERT DB
def insert_playlists():
    print("Inserting Playlists...")
    spotify_client = authenticate_spotify()
    my_playlists = fetch_playlists(spotify_client)
    connection = get_db_connection()
    cursor = connection.cursor()

    for playlist_name, playlist_description, playlist_id in my_playlists:
        db_logger.info(f"Inserting playlist: {playlist_name}")

        cursor.execute("""
            INSERT INTO Playlists (PlaylistId, PlaylistName, PlaylistDescription)
            VALUES (?, ?, ?)
        """, (playlist_id, playlist_name, playlist_description))

    connection.commit()
    cursor.close()
    connection.close()


def insert_tracks_and_associations():
    print("Inserting Tracks and Associations...")
    db_logger.info("Inserting Tracks and Associations...")
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

            db_logger.info(f"Inserting track: {track_title} (ID: {track_id})")

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
                    db_logger.warning(f"Playlist '{playlist_name}' not found in Playlists table.")

        connection.commit()
    except Exception as e:
        db_logger.error(f"Error inserting tracks and associations: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# ! SELECT FROM DB
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


# Retrieve all playlists associated with a given TrackId
def fetch_playlists_for_track(track_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT p.PlaylistName
            FROM TrackPlaylists tp
            JOIN Playlists p ON tp.PlaylistId = p.PlaylistId
            WHERE tp.TrackId = ?
        """, (track_id,))
        playlists = [row.PlaylistName for row in cursor.fetchall()]
        db_logger.info(f"Track ID '{track_id}' belongs to playlists: {playlists}")
        return playlists
    except pyodbc.Error as e:
        db_logger.error(f"Error fetching playlists for Track ID '{track_id}': {e}")
        return []
    finally:
        cursor.close()
        connection.close()


# Fetch all playlists from Playlists table
def fetch_all_playlists_db():
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT PlaylistId, PlaylistName FROM Playlists")
        playlists = cursor.fetchall()
        db_logger.info(f"Fetched {len(playlists)} playlists from the database.")
        return playlists
    except pyodbc.Error as e:
        db_logger.error(f"Error fetching all playlists: {e}")
        return []
    finally:
        cursor.close()
        connection.close()


# Fetch all tracks from the Tracks table
def fetch_all_tracks():
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT TrackId, TrackTitle, Artists FROM Tracks")
        tracks = cursor.fetchall()
        db_logger.info(f"Fetched {len(tracks)} tracks from the database.")
        return tracks
    except pyodbc.Error as e:
        db_logger.error(f"Error fetching tracks: {e}")
        return []
    finally:
        cursor.close()
        connection.close()


# Retrieve track details from the database based on TrackId
def fetch_track_details(track_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT TrackTitle, Artists
            FROM Tracks
            WHERE TrackId = ?
        """, (track_id,))
        result = cursor.fetchone()
        if result:
            return {'TrackTitle': result.TrackTitle, 'Artists': result.Artists}
        else:
            db_logger.warning(f"No track details found for Track ID '{track_id}'")
            return None
    except pyodbc.Error as e:
        db_logger.error(f"Error fetching track details for Track ID '{track_id}': {e}")
        return None
    finally:
        cursor.close()
        connection.close()

