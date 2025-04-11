"""
Legacy helper functions for database operations.
These functions use the new UnitOfWork and Repository patterns internally,
but maintain the same API for backward compatibility.
"""

import os
from datetime import datetime
from typing import Optional, List, Tuple

from dotenv import load_dotenv
from drivers.spotify_client import authenticate_spotify, fetch_playlists, fetch_master_tracks, \
    find_playlists_for_master_tracks

from sql.models.track import Track
from sql.models.playlist import Playlist
from sql.core.connection import DatabaseConnection
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

load_dotenv()

MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_helper', 'sql/db.log')


# Used by the application to clear all database tables
def clear_db():
    """Clear all database tables in the correct order."""
    db_logger.info("Clearing all database tables")

    # Use the Unit of Work pattern to ensure transaction integrity
    with UnitOfWork() as uow:
        uow.track_playlist_repository.delete_all()  # Clear associations first (foreign keys)
        uow.playlist_repository.delete_all()  # Clear playlists next
        uow.track_repository.delete_all()  # Clear tracks last

    db_logger.info("All tables cleared successfully")
    print("All tables cleared successfully.")


# Insert data into all database tables
def insert_db():
    """Insert data into all database tables."""
    insert_playlists()
    insert_tracks_and_associations()


# Clear just the playlists table
def clear_playlists():
    """Clear the Playlists table."""
    db_logger.info("Clearing the Playlists table")

    with UnitOfWork() as uow:
        # Need to clear associations first due to foreign key constraints
        uow.track_playlist_repository.delete_all()
        uow.playlist_repository.delete_all()

    db_logger.info("Playlists table cleared successfully")
    print("Playlists table cleared successfully.")


# Clear just the tracks table
def clear_master_tracks():
    """Clear the Tracks table."""
    db_logger.info("Clearing the Tracks table")

    with UnitOfWork() as uow:
        # Need to clear associations first due to foreign key constraints
        uow.track_playlist_repository.delete_all()
        uow.track_repository.delete_all()

    db_logger.info("Tracks table cleared successfully")
    print("Tracks table cleared successfully.")


# Insert playlists from Spotify into the database
def insert_playlists():
    """Insert playlists from Spotify into the database."""
    print("Inserting Playlists...")
    spotify_client = authenticate_spotify()
    my_playlists = fetch_playlists(spotify_client)

    with UnitOfWork() as uow:
        for playlist_name, playlist_description, playlist_id in my_playlists:
            db_logger.info(f"Inserting playlist: {playlist_name}")

            # Create a Playlist domain object
            playlist = Playlist(
                playlist_id=playlist_id,
                name=playlist_name.strip(),
                description=playlist_description
            )

            # Insert using the repository
            uow.playlist_repository.insert(playlist)

    db_logger.info("All playlists inserted successfully")


# Insert tracks and their playlist associations
def insert_tracks_and_associations():
    """Insert tracks and their playlist associations into the database."""
    print("Inserting Tracks and Associations...")
    db_logger.info("Inserting Tracks and Associations...")

    spotify_client = authenticate_spotify()

    # Fetch all tracks from 'MASTER' playlist
    master_tracks = fetch_master_tracks(spotify_client, MASTER_PLAYLIST_ID)

    # Find playlists for each track
    tracks_with_playlists = find_playlists_for_master_tracks(spotify_client, master_tracks, MASTER_PLAYLIST_ID)

    with UnitOfWork() as uow:
        for track_data in tracks_with_playlists:
            track_id, track_title, artist_names, album_name, added_at, playlist_names = track_data

            db_logger.info(f"Inserting track: {track_title} (ID: {track_id})")

            # Create Track domain object
            track = Track(
                track_id=track_id,
                title=track_title,
                artists=artist_names,
                album=album_name,
                added_to_master=added_at
            )

            # Insert the track
            uow.track_repository.insert(track)

            # Associate with playlists
            for playlist_name in playlist_names:
                # Look up the playlist by name
                playlist = uow.playlist_repository.get_by_name(playlist_name)

                if playlist:
                    # Create the association
                    uow.track_playlist_repository.insert(track_id, playlist.playlist_id)
                else:
                    db_logger.warning(f"Playlist '{playlist_name}' not found in database.")


# Fetch all tracks from the database
def fetch_master_tracks_db():
    """
    Fetch all tracks from the Tracks table.

    Returns:
        List of (TrackTitle, Artists, Album) tuples
    """
    print('Fetching all tracks from Tracks table')

    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()
        return [(track.title, track.artists, track.album) for track in tracks]


# Fetch all playlists associated with a track
def fetch_playlists_for_track_db(track_id):
    """
    Retrieve all playlists associated with a given TrackId.

    Args:
        track_id: The track ID to look up

    Returns:
        List of playlist names
    """
    with UnitOfWork() as uow:
        playlists = uow.playlist_repository.get_playlists_for_track(track_id)
        playlist_names = [playlist.name for playlist in playlists]

        db_logger.info(f"Track ID '{track_id}' belongs to playlists: {playlist_names}")
        return playlist_names


# Fetch all playlists from the database
def fetch_all_playlists_db():
    """
    Fetch all playlists from the Playlists table.

    Returns:
        List of (PlaylistId, PlaylistName) tuples
    """
    with UnitOfWork() as uow:
        playlists = uow.playlist_repository.get_all()
        result = [(playlist.playlist_id, playlist.name) for playlist in playlists]

        db_logger.info(f"Fetched {len(result)} playlists from the database.")
        return result


# Fetch all tracks from the database
def fetch_all_tracks_db():
    """
    Fetch all tracks from the Tracks table.

    Returns:
        List of Track objects
    """
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()

        db_logger.info(f"Fetched {len(tracks)} tracks from the database.")
        return tracks


# Fetch details for a specific track
def fetch_track_details_db(track_id):
    """
    Retrieve track details from the database based on TrackId.

    Args:
        track_id: The track ID to look up

    Returns:
        Dictionary with TrackTitle and Artists, or None if not found
    """
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(track_id)

        if track:
            return {'TrackTitle': track.title, 'Artists': track.artists}
        else:
            db_logger.warning(f"No track details found for Track ID '{track_id}'")
            return None


# Get the date a track was added to MASTER playlist
def get_track_added_date(track_id: str) -> Optional[datetime]:
    """
    Get the date a track was added to MASTER playlist from the database.

    Args:
        track_id: The track ID to look up

    Returns:
        Datetime object or None if not found
    """
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(track_id)

        if track:
            return track.added_to_master
        return None


# Count how many MP3 files have a TrackId embedded - This is for file_helper.py compatibility
def count_tracks_with_id_db():
    """
    Get count of tracks with IDs in the database.
    This is mainly a compatibility function for the old API.

    Returns:
        Tuple of (tracks_with_ids, total_tracks)
    """
    with UnitOfWork() as uow:
        return uow.track_repository.get_track_count_with_id()


# Get track IDs that exist in the database
def get_existing_track_ids():
    """
    Get all track IDs from the database.

    Returns:
        Set of track IDs
    """
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()
        return {track.track_id for track in tracks}


# Check if track ID exists in database
def track_id_exists_in_db(track_id):
    """
    Check if a track ID exists in the database.

    Args:
        track_id: The track ID to check

    Returns:
        Boolean indicating if the track exists
    """
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(track_id)
        return track is not None


'''
from datetime import datetime
from typing import Optional
import pyodbc
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
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        connection.autocommit = False  # Start transaction

        clear_track_playlists(cursor)  # Pass cursor to reuse the same transaction
        clear_playlists(cursor)
        clear_master_tracks(cursor)

        connection.commit()  # Commit all changes
        db_logger.info("All tables cleared successfully.")
        print("All tables cleared successfully.")
    except pyodbc.Error as e:
        connection.rollback()  # Rollback all changes on error
        db_logger.error(f"Error clearing database: {e}")
        print(f"Error clearing database: {e}")
    finally:
        cursor.close()
        connection.close()


def insert_db():
    insert_playlists()
    insert_tracks_and_associations()


# ! CLEAR DB
def clear_playlists(cursor=None):
    connection = None
    if cursor is None:
        connection = get_db_connection()
        cursor = connection.cursor()
        close_conn = True
    else:
        close_conn = False
    try:
        print("Clearing Playlists...")
        db_logger.info("Clearing the Playlists table at %s.", datetime.now())
        cursor.execute("DELETE FROM Playlists")
        if close_conn:
            connection.commit()
        db_logger.info("Playlists table cleared successfully at %s.", datetime.now())
        print("Playlists table cleared successfully.")
    except pyodbc.Error as e:
        db_logger.error(f"Error clearing Playlists table at {datetime.now()}: {e}")
        print(f"Error clearing Playlists table: {e}")
        if close_conn:
            connection.rollback()
        raise
    finally:
        if close_conn and cursor and connection:
            cursor.close()
            connection.close()


def clear_master_tracks(cursor=None):
    connection = None
    if cursor is None:
        connection = get_db_connection()
        cursor = connection.cursor()
        close_conn = True
    else:
        close_conn = False
    try:
        print("Clearing Tracks...")
        db_logger.info("Clearing the Tracks table at %s.", datetime.now())
        cursor.execute("DELETE FROM Tracks")
        if close_conn:
            connection.commit()
        db_logger.info("Tracks table cleared successfully at %s.", datetime.now())
        print("Tracks table cleared successfully.")
    except pyodbc.Error as e:
        db_logger.error(f"Error clearing Tracks table at {datetime.now()}: {e}")
        print(f"Error clearing Tracks table: {e}")
        if close_conn:
            connection.rollback()
        raise
    finally:
        if close_conn and cursor and connection:
            cursor.close()
            connection.close()


def clear_track_playlists(cursor=None):
    connection = None
    if cursor is None:
        connection = get_db_connection()
        cursor = connection.cursor()
        close_conn = True
    else:
        close_conn = False
    try:
        print("Clearing TrackPlaylists...")
        db_logger.info("Clearing the TrackPlaylists table at %s.", datetime.now())
        cursor.execute("DELETE FROM TrackPlaylists")
        if close_conn:
            connection.commit()
        db_logger.info("TrackPlaylists table cleared successfully at %s.", datetime.now())
        print("TrackPlaylists table cleared successfully.")
    except pyodbc.Error as e:
        db_logger.error(f"Error clearing TrackPlaylists table at {datetime.now()}: {e}")
        print(f"Error clearing TrackPlaylists table: {e}")
        if close_conn:
            connection.rollback()
        raise
    finally:
        if close_conn and cursor and connection:
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
            VALUES (?, RTRIM(?), ?)
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
            track_id, track_title, artist_names, album_name, added_at, playlists = track

            db_logger.info(f"Inserting track: {track_title} (ID: {track_id})")

            # Insert into Tracks
            cursor.execute("""
                      INSERT INTO Tracks (TrackId, TrackTitle, Artists, Album, AddedToMaster)
                      VALUES (?, ?, ?, ?, ?)
                      """, (track_id, track_title, artist_names, album_name, added_at))

            # Insert into TrackPlaylists
            for playlist_name in playlists:
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
def fetch_playlists_for_track_db(track_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT RTRIM(p.PlaylistName) as PlaylistName
            FROM TrackPlaylists tp
            JOIN Playlists p ON tp.PlaylistId = p.PlaylistId
            WHERE tp.TrackId = ?
        """, (track_id,))
        playlists = [row[0] for row in cursor.fetchall()]
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
        cursor.execute("SELECT PlaylistId, RTRIM(PlaylistName) as PlaylistName FROM Playlists")
        playlists = [(row[0], row[1]) for row in cursor.fetchall()]
        db_logger.info(f"Fetched {len(playlists)} playlists from the database.")
        return playlists
    except pyodbc.Error as e:
        db_logger.error(f"Error fetching all playlists: {e}")
        return []
    finally:
        cursor.close()
        connection.close()


# Fetch all tracks from the Tracks table
def fetch_all_tracks_db():
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
def fetch_track_details_db(track_id):
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


# Get the date a track was added to MASTER playlist from the database
def get_track_added_date(track_id: str) -> Optional[datetime]:
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT AddedToMaster
            FROM Tracks
            WHERE TrackId = ?
        """, (track_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        db_logger.error(f"Error fetching added date for track {track_id}: {e}")
        return None
    finally:
        cursor.close()
        connection.close()
'''