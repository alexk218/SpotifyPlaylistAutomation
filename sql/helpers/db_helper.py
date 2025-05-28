import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from sql.core.unit_of_work import UnitOfWork
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
