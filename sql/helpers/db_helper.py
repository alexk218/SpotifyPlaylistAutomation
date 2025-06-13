import os
from datetime import datetime
from typing import Optional, Dict

from dotenv import load_dotenv

from sql.core.unit_of_work import UnitOfWork
from sql.models.playlist import Playlist
from sql.models.track import Track
from utils.logger import setup_logger

load_dotenv()

MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_helper', 'sql', 'db_helper.log')


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


def get_db_playlists() -> Dict[str, Playlist]:
    """
    Get all playlists from the database.

    Returns:
        Dictionary of playlist_id to Playlist objects
    """
    with UnitOfWork() as uow:
        playlists = uow.playlist_repository.get_all()
        return {playlist.playlist_id: playlist for playlist in playlists}


def get_db_tracks_by_uri() -> Dict[str, Track]:
    """
    Get all tracks from the database indexed by URI.

    Returns:
        Dictionary of uri to Track objects
    """
    with UnitOfWork() as uow:
        tracks = uow.track_repository.get_all()
        return {track.uri: track for track in tracks if track.uri}
