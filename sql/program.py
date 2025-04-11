import argparse
import os
from dotenv import load_dotenv

from sql.helpers.db_helper import clear_db
from drivers.spotify_client import sync_to_master_playlist, sync_unplaylisted_to_unsorted, authenticate_spotify
from helpers.file_helper import embed_track_metadata, remove_all_track_ids, count_tracks_with_id, cleanup_tracks, \
    validate_song_lengths, cleanup_broken_symlinks
from helpers.organization_helper import organize_songs_into_playlists
from helpers.sync_helper import sync_playlists_incremental, sync_master_tracks_incremental
from helpers.validation_helper import validate_master_tracks, validate_playlist_symlinks, \
    validate_playlist_symlinks
from utils.logger import setup_logger
from cache_manager import spotify_cache

load_dotenv()

MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
PLAYLISTS_DIRECTORY = os.getenv("PLAYLISTS_DIRECTORY")
QUARANTINE_DIRECTORY = os.getenv("QUARANTINE_DIRECTORY")
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')
UNSORTED_PLAYLIST_ID = os.getenv("UNSORTED_PLAYLIST_ID")

program_logger = setup_logger('program', 'sql/program.log')


def main():
    parser = argparse.ArgumentParser(
        description='Spotify Playlist Database Operations and Song Organization (Optimized Version).')

    # Database operations
    parser.add_argument('--clear-db', action='store_true', help='Clear all database tables')
    parser.add_argument('--sync-playlists', action='store_true', help='Sync playlists incrementally')
    parser.add_argument('--sync-tracks', action='store_true', help='Sync master tracks incrementally')
    parser.add_argument('--sync-all', action='store_true', help='Sync playlists and master tracks incrementally')
    parser.add_argument('--force-refresh', action='store_true', help='Force full refresh from Spotify API')

    # File operations
    parser.add_argument('--organize-songs', action='store_true',
                        help='Organize downloaded songs into playlist folders with symlinks')
    parser.add_argument('--dry-run', action='store_true',
                        help='Simulate the organization process without creating symlinks')
    parser.add_argument('--embed-metadata', action='store_true', help='Embed TrackId into song file metadata')
    parser.add_argument('--interactive', action='store_true', help='Enable interactive mode for fuzzy matching')
    parser.add_argument('--remove-track-ids', action='store_true', help='Remove TrackId from all MP3 files')
    parser.add_argument('--cleanup-tracks', action='store_true',
                        help='Clean up unwanted files from tracks_master directory by moving them to quarantine')
    parser.add_argument('--cleanup-symlinks', action='store_true',
                        help='Remove broken symlinks from playlists_master directory')

    # Validation
    parser.add_argument('--count-track-ids', action='store_true', help='Count MP3 files with TrackId')
    parser.add_argument('--validate-tracks', action='store_true',
                        help='Validate local tracks against database information')
    parser.add_argument('--validate-lengths', action='store_true',
                        help='Validate song lengths and report songs shorter than 5 minutes')
    parser.add_argument('--validate-playlists', action='store_true',
                        help='Validate playlist symlinks against database information')
    parser.add_argument('--validate-all', action='store_true', help='Run all validation checks')

    # Cache management
    parser.add_argument('--clear-cache', action='store_true', help='Clear all cached Spotify API data')

    # Spotify sync
    parser.add_argument('--sync-master', action='store_true',
                        help='Sync all tracks from all playlists to MASTER playlist')
    parser.add_argument('--sync-unplaylisted', action='store_true',
                        help='Sync unplaylisted Liked Songs to UNSORTED playlist')

    args = parser.parse_args()

    # * Database operations
    if args.clear_db:
        clear_db()
        print("All database tables cleared successfully.")

    if args.sync_all or args.sync_playlists:
        sync_playlists_incremental(force_full_refresh=args.force_refresh)

    if args.sync_all or args.sync_tracks:
        sync_master_tracks_incremental(MASTER_PLAYLIST_ID, force_full_refresh=args.force_refresh)

    # * File operations
    if args.organize_songs:
        organize_songs_into_playlists(MASTER_TRACKS_DIRECTORY, PLAYLISTS_DIRECTORY, dry_run=args.dry_run)

    if args.embed_metadata:
        embed_track_metadata(MASTER_TRACKS_DIRECTORY, interactive=args.interactive)

    if args.remove_track_ids:
        remove_all_track_ids(MASTER_TRACKS_DIRECTORY)

    if args.cleanup_tracks:
        cleanup_tracks(MASTER_TRACKS_DIRECTORY, QUARANTINE_DIRECTORY)

    if args.cleanup_symlinks:
        cleanup_broken_symlinks(PLAYLISTS_DIRECTORY, dry_run=args.dry_run)

    # * Validation
    if args.count_track_ids:
        count_tracks_with_id(MASTER_TRACKS_DIRECTORY)

    if args.validate_tracks or args.validate_all:
        validate_master_tracks(MASTER_TRACKS_DIRECTORY)

    if args.validate_lengths or args.validate_all:
        validate_song_lengths(MASTER_TRACKS_DIRECTORY)

    if args.validate_playlists or args.validate_all:
        validate_playlist_symlinks(PLAYLISTS_DIRECTORY)

    # * Cache management
    if args.clear_cache:
        spotify_cache.clear_all_caches()
        print("All Spotify API caches cleared.")

    # * Spotify sync
    if args.sync_master:
        # ! Lots of API calls
        spotify_client = authenticate_spotify()
        sync_to_master_playlist(spotify_client, MASTER_PLAYLIST_ID)

    if args.sync_unplaylisted:
        # ! Lots of API calls
        spotify_client = authenticate_spotify()
        sync_unplaylisted_to_unsorted(spotify_client, UNSORTED_PLAYLIST_ID)


if __name__ == "__main__":
    main()
