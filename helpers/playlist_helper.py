import os
from pathlib import Path

from utils.logger import setup_logger

MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_logger', 'sql/db.log')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']
