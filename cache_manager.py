"""
Spotify API Cache Manager

This module provides caching mechanisms for Spotify API data to minimize API calls.
It stores API responses in memory and optionally persists them to disk.
"""

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple

from utils.logger import setup_logger

cache_logger = setup_logger('cache_manager', 'cache/spotify_cache.log')


class SpotifyCache:
    """
    Cache manager for Spotify API responses.
    Provides in-memory caching with optional persistence to disk.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SpotifyCache, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.cache_dir = Path('cache')
        self.cache_dir.mkdir(exist_ok=True)

        # In-memory cache
        self._playlist_cache = {}
        self._track_cache = {}
        self._playlist_tracks_cache = {}

        # Cache expiry settings (in seconds)
        self.expiry = {
            'playlists': 3600,  # 1 hour
            'tracks': 86400,  # 24 hours
            'playlist_tracks': 3600  # 1 hour
        }

        # Load cache from disk if available
        self._load_cache()

        self._initialized = True
        cache_logger.info("Spotify cache initialized")

    def _load_cache(self):
        """Load cached data from disk."""
        try:
            # Load playlists cache
            playlist_cache_file = self.cache_dir / 'playlists_cache.json'
            if playlist_cache_file.exists():
                with open(playlist_cache_file, 'r', encoding='utf-8') as f:
                    self._playlist_cache = json.load(f)
                cache_logger.info(f"Loaded playlists cache with {len(self._playlist_cache)} entries")

            # Load tracks cache
            track_cache_file = self.cache_dir / 'tracks_cache.json'
            if track_cache_file.exists():
                with open(track_cache_file, 'r', encoding='utf-8') as f:
                    self._track_cache = json.load(f)
                cache_logger.info(f"Loaded tracks cache with {len(self._track_cache)} entries")

            # Load playlist tracks cache
            playlist_tracks_cache_file = self.cache_dir / 'playlist_tracks_cache.json'
            if playlist_tracks_cache_file.exists():
                with open(playlist_tracks_cache_file, 'r', encoding='utf-8') as f:
                    self._playlist_tracks_cache = json.load(f)
                cache_logger.info(f"Loaded playlist_tracks cache with {len(self._playlist_tracks_cache)} entries")

        except Exception as e:
            cache_logger.error(f"Error loading cache from disk: {e}")

    def _save_cache(self, cache_type: str):
        """Save cache to disk."""
        try:
            if cache_type == 'playlists':
                cache_file = self.cache_dir / 'playlists_cache.json'
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self._playlist_cache, f)
                cache_logger.debug(f"Saved playlists cache to disk")

            elif cache_type == 'tracks':
                cache_file = self.cache_dir / 'tracks_cache.json'
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self._track_cache, f)
                cache_logger.debug(f"Saved tracks cache to disk")

            elif cache_type == 'playlist_tracks':
                cache_file = self.cache_dir / 'playlist_tracks_cache.json'
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self._playlist_tracks_cache, f)
                cache_logger.debug(f"Saved playlist_tracks cache to disk")

        except Exception as e:
            cache_logger.error(f"Error saving {cache_type} cache to disk: {e}")

    def _is_cache_valid(self, timestamp: float, cache_type: str) -> bool:
        """Check if cache entry is still valid based on expiry time."""
        expiry_time = self.expiry.get(cache_type, 3600)  # Default 1 hour
        return (time.time() - timestamp) < expiry_time

    def get_playlists(self) -> Optional[List[Tuple[str, str, str]]]:
        """
        Get playlists from cache if available and valid.

        Returns:
            List of tuples (playlist_name, playlist_id) or None if cache is invalid
        """
        if 'data' in self._playlist_cache and 'timestamp' in self._playlist_cache:
            if self._is_cache_valid(self._playlist_cache['timestamp'], 'playlists'):
                cache_logger.info("Returning playlists from cache")
                return self._playlist_cache['data']

        cache_logger.info("Playlists cache invalid or missing")
        return None

    def cache_playlists(self, playlists: List[Tuple[str, str, str]]):
        """
        Cache playlists data.

        Args:
            playlists: List of tuples (playlist_name, playlist_id)
        """
        self._playlist_cache = {
            'timestamp': time.time(),
            'data': playlists
        }
        cache_logger.info(f"Cached {len(playlists)} playlists")
        self._save_cache('playlists')

    def get_master_tracks(self, master_playlist_id: str) -> Optional[List[Tuple[str, str, str, str, datetime]]]:
        """
        Get master tracks from cache if available and valid.

        Args:
            master_playlist_id: ID of the master playlist

        Returns:
            List of master tracks or None if cache is invalid
        """
        cache_key = f"master_{master_playlist_id}"
        if cache_key in self._track_cache and 'timestamp' in self._track_cache[cache_key]:
            if self._is_cache_valid(self._track_cache[cache_key]['timestamp'], 'tracks'):
                cache_logger.info("Returning master tracks from cache")
                return self._track_cache[cache_key]['data']

        cache_logger.info("Master tracks cache invalid or missing")
        return None

    def cache_master_tracks(self, master_playlist_id: str, tracks: List[Tuple[str, str, str, str, datetime]]):
        """
        Cache master tracks data.

        Args:
            master_playlist_id: ID of the master playlist
            tracks: List of track data tuples
        """
        cache_key = f"master_{master_playlist_id}"
        self._track_cache[cache_key] = {
            'timestamp': time.time(),
            'data': tracks
        }
        cache_logger.info(f"Cached {len(tracks)} master tracks for playlist {master_playlist_id}")
        self._save_cache('tracks')

    def get_playlist_tracks(self, playlist_id: str) -> Optional[List[str]]:
        """
        Get tracks for a specific playlist from cache if available and valid.

        Args:
            playlist_id: ID of the playlist

        Returns:
            List of track IDs or None if cache is invalid
        """
        if playlist_id in self._playlist_tracks_cache and 'timestamp' in self._playlist_tracks_cache[playlist_id]:
            if self._is_cache_valid(self._playlist_tracks_cache[playlist_id]['timestamp'], 'playlist_tracks'):
                cache_logger.info(f"Returning tracks for playlist {playlist_id} from cache")
                return self._playlist_tracks_cache[playlist_id]['data']

        cache_logger.info(f"Tracks cache for playlist {playlist_id} invalid or missing")
        return None

    def cache_playlist_tracks(self, playlist_id: str, track_ids: List[str]):
        """
        Cache track IDs for a specific playlist.

        Args:
            playlist_id: ID of the playlist
            track_ids: List of track IDs in the playlist
        """
        self._playlist_tracks_cache[playlist_id] = {
            'timestamp': time.time(),
            'data': track_ids
        }
        cache_logger.info(f"Cached {len(track_ids)} tracks for playlist {playlist_id}")
        self._save_cache('playlist_tracks')

    def invalidate_playlist_cache(self, playlist_id: Optional[str] = None):
        """
        Invalidate playlist cache.

        Args:
            playlist_id: Optional specific playlist ID to invalidate, or None to invalidate all playlists
        """
        if playlist_id:
            if playlist_id in self._playlist_tracks_cache:
                del self._playlist_tracks_cache[playlist_id]
                cache_logger.info(f"Invalidated cache for playlist {playlist_id}")
                self._save_cache('playlist_tracks')
        else:
            self._playlist_cache = {}
            self._playlist_tracks_cache = {}
            cache_logger.info("Invalidated all playlist caches")
            self._save_cache('playlists')
            self._save_cache('playlist_tracks')

    def invalidate_tracks_cache(self, master_playlist_id: Optional[str] = None):
        """
        Invalidate tracks cache.

        Args:
            master_playlist_id: Optional specific master playlist ID to invalidate, or None to invalidate all tracks
        """
        if master_playlist_id:
            cache_key = f"master_{master_playlist_id}"
            if cache_key in self._track_cache:
                del self._track_cache[cache_key]
                cache_logger.info(f"Invalidated cache for master tracks {master_playlist_id}")
        else:
            self._track_cache = {}
            cache_logger.info("Invalidated all tracks cache")

        self._save_cache('tracks')

    def clear_all_caches(self):
        """Clear all caches from memory and disk."""
        self._playlist_cache = {}
        self._track_cache = {}
        self._playlist_tracks_cache = {}

        cache_logger.info("Cleared all caches from memory")

        # Delete cache files
        try:
            for cache_file in self.cache_dir.glob('*_cache.json'):
                cache_file.unlink()
            cache_logger.info("Deleted all cache files from disk")
        except Exception as e:
            cache_logger.error(f"Error deleting cache files: {e}")


# Singleton instance
spotify_cache = SpotifyCache()
