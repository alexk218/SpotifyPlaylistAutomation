import argparse
import os
from dotenv import load_dotenv

from sql.helpers.db_helper import clear_db
from drivers.spotify_client import sync_to_master_playlist, sync_unplaylisted_to_unsorted, authenticate_spotify
from helpers.file_helper import validate_song_lengths
from helpers.sync_helper import sync_playlists_to_db, sync_tracks_to_db, \
    sync_track_playlist_associations_to_db
from helpers.validation_helper import validate_master_tracks
from utils.logger import setup_logger

load_dotenv()

MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
MASTER_TRACKS_DIRECTORY_SSD = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
QUARANTINE_DIRECTORY = os.getenv("QUARANTINE_DIRECTORY")
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')
UNSORTED_PLAYLIST_ID = os.getenv("UNSORTED_PLAYLIST_ID")
M3U_PLAYLISTS_DIRECTORY_SSD = os.getenv("M3U_PLAYLISTS_DIRECTORY_SSD")
M3U_PLAYLISTS_DIRECTORY = os.getenv("M3U_PLAYLISTS_DIRECTORY")

program_logger = setup_logger('program', 'sql', 'program.log')


def main():
    parser = argparse.ArgumentParser(
        description='Spotify Playlist Database Operations and Song Organization (Optimized Version).')

    # Database operations
    parser.add_argument('--clear-db', action='store_true', help='Clear all database tables')
    parser.add_argument('--sync-playlists', action='store_true', help='Sync playlists incrementally')
    parser.add_argument('--sync-tracks', action='store_true', help='Sync master tracks incrementally')
    parser.add_argument('--sync-associations', action='store_true', help='Sync track-playlist associations')
    parser.add_argument('--sync-all', action='store_true',
                        help='Sync playlists, tracks, and associations incrementally')
    parser.add_argument('--force-refresh', action='store_true', help='Force full refresh from Spotify API')

    # SSD option for all file operations
    parser.add_argument('--ssd', action='store_true', help='Use SSD directories instead of standard directories')

    # File operations
    parser.add_argument('--generate-m3u', action='store_true',
                        help='Generate M3U playlist files that reference original tracks (for Rekordbox)')
    parser.add_argument('--m3u-dir', type=str, default=None,
                        help='Override directory where M3U playlist files will be created')
    parser.add_argument('--no-extended-m3u', action='store_true',
                        help='Generate simple M3U files without extended track information')
    parser.add_argument('--no-overwrite', action='store_true',
                        help='Do not overwrite existing M3U files')
    parser.add_argument('--all-playlists', action='store_true',
                        help='Process all playlists, not just the ones that changed')
    parser.add_argument('--dry-run', action='store_true',
                        help='Simulate the organization process without creating symlinks or files')
    parser.add_argument('--embed-metadata', action='store_true', help='Embed TrackId into song file metadata')
    parser.add_argument('--remove-track-ids', action='store_true', help='Remove TrackId from all MP3 files')
    parser.add_argument('--cleanup-tracks', action='store_true',
                        help='Clean up unwanted files from tracks_master directory by moving them to quarantine')

    # Validation
    parser.add_argument('--count-track-ids', action='store_true', help='Count MP3 files with TrackId')
    parser.add_argument('--validate-tracks', action='store_true',
                        help='Validate local tracks against database information')
    parser.add_argument('--validate-lengths', action='store_true',
                        help='Validate song lengths and report songs shorter than 5 minutes')
    parser.add_argument('--validate-all', action='store_true', help='Run all validation checks')

    # Spotify sync
    parser.add_argument('--sync-master', action='store_true',
                        help='Sync all tracks from all playlists to MASTER playlist')
    parser.add_argument('--sync-unplaylisted', action='store_true',
                        help='Sync unplaylisted Liked Songs to UNSORTED playlist')

    args = parser.parse_args()

    # Determine which directories to use based on --ssd flag
    tracks_dir = MASTER_TRACKS_DIRECTORY_SSD if args.ssd else MASTER_TRACKS_DIRECTORY
    m3u_dir = M3U_PLAYLISTS_DIRECTORY_SSD if args.ssd else M3U_PLAYLISTS_DIRECTORY

    # Override m3u_dir if explicitly provided
    if args.m3u_dir:
        m3u_dir = args.m3u_dir

    # * Database operations
    if args.clear_db:
        clear_db()
        print("All database tables cleared successfully.")

    if args.sync_all or args.sync_playlists:
        sync_playlists_to_db(force_full_refresh=args.force_refresh)

    if args.sync_all or args.sync_tracks:
        sync_tracks_to_db(MASTER_PLAYLIST_ID, force_full_refresh=args.force_refresh)

    if args.sync_all or args.sync_associations:
        sync_track_playlist_associations_to_db(MASTER_PLAYLIST_ID, force_full_refresh=args.force_refresh)

    if args.validate_tracks or args.validate_all:
        validate_master_tracks(tracks_dir)

    if args.validate_lengths or args.validate_all:
        validate_song_lengths(tracks_dir)

    # * Spotify sync
    if args.sync_master:
        # ! Lots of API calls
        spotify_client = authenticate_spotify()
        sync_to_master_playlist(spotify_client, MASTER_PLAYLIST_ID, )

    if args.sync_unplaylisted:
        # ! Lots of API calls
        spotify_client = authenticate_spotify()
        sync_unplaylisted_to_unsorted(spotify_client, UNSORTED_PLAYLIST_ID)


if __name__ == "__main__":
    main()
