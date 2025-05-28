import json
import os
import re
from pathlib import Path

from utils.logger import setup_logger

MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

db_logger = setup_logger('db_logger', 'sql', 'playlist_helper.log')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent


def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']


def is_forbidden_playlist(name: str, description: str, playlist_id: str, forbidden_playlists=None,
                          forbidden_words=None, description_keywords=None, forbidden_playlist_ids=None) -> bool:
    # Use default lists if not provided
    forbidden_playlists = forbidden_playlists or []
    forbidden_words = forbidden_words or []
    description_keywords = description_keywords or []
    forbidden_playlist_ids = forbidden_playlist_ids or []

    name_lower = name.lower()
    description_lower = description.lower()

    if any(word.lower() in name_lower for word in forbidden_words):
        print(f"Excluding playlist '{name}' due to forbidden word in name.")
        return True

    if name in forbidden_playlists:
        print(f"Excluding playlist '{name}' as it is in forbidden_playlists.")
        return True

    if playlist_id in forbidden_playlist_ids:
        print(f"Excluding playlist '{name}' because ID '{playlist_id}' is in forbidden_playlist_ids.")
        return True

    for keyword in description_keywords:
        # Create a regex pattern to match whole words (case-insensitive)
        pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
        if re.search(pattern, description_lower):
            print(f"Excluding playlist '{name}' because description contains '{keyword}'.")
            return True

    return False


def load_exclusion_config(config_override=None):
    """
    Load exclusion configuration for playlists.

    Args:
        config_override: Optional dictionary with exclusion configuration

    Returns:
        Dictionary with exclusion configuration
    """
    if config_override:
        return config_override

    # Load default config from file
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / 'exclusion_config.json'

    try:
        with config_path.open('r', encoding='utf-8') as config_file:
            return json.load(config_file)
    except Exception as e:
        print(f"Error loading default config: {e}")
        return {
            "forbidden_playlists": [],
            "forbidden_words": [],
            "description_keywords": [],
            "forbidden_playlist_ids": []
        }
