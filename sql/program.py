import argparse
from helpers.file_helper import embed_track_metadata, remove_all_track_ids, count_tracks_with_id, cleanup_tracks, \
    validate_song_lengths, cleanup_broken_symlinks
from helpers.playlist_helper import organize_songs_into_playlists, validate_master_tracks, validate_playlist_symlinks, \
    validate_playlist_symlinks_quick
from sql.helpers.db_helper import *
from utils.logger import setup_logger

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
PLAYLISTS_DIRECTORY = os.getenv("PLAYLISTS_DIRECTORY")
QUARANTINE_DIRECTORY = os.getenv("QUARANTINE_DIRECTORY")
VALIDATION_LOGS_DIR = os.getenv("VALIDATION_LOGS_DIR")
METADATA_LOGS_DIR = os.getenv("METADATA_LOGS_DIR")
UNSORTED_PLAYLIST_ID = os.getenv("UNSORTED_PLAYLIST_ID")

db_logger = setup_logger('db_logger', 'sql/db.log')


def main():
    parser = argparse.ArgumentParser(description='Spotify Playlist Database Operations and Song Organization.')
    parser.add_argument('--run-all', action='store_true', help='Clear and insert data into all database tables')
    parser.add_argument('--clear-all', action='store_true', help='Clear all database tables')
    parser.add_argument('--insert-all', action='store_true', help='Insert data into all database tables')
    parser.add_argument('--my_playlists', action='store_true', help='Clear and insert data into Playlists table')
    parser.add_argument('--master_tracks', action='store_true', help='Clear and insert data into Tracks table')
    parser.add_argument('--clear-my-playlists', action='store_true', help='Clear Playlists table')
    parser.add_argument('--insert-my-playlists', action='store_true', help='Insert data into Playlists table')
    parser.add_argument('--clear-master-tracks', action='store_true', help='Clear Tracks table')
    parser.add_argument('--insert-master-tracks', action='store_true', help='Insert data into Tracks and TrackPlaylists tables')
    parser.add_argument('--fetch-master-tracks-db', action='store_true', help='Fetch and display tracks from MASTER playlist in the database')
    parser.add_argument('--fetch-all-playlists-db', action='store_true', help='Fetch and display all playlists from the database')
    parser.add_argument('--organize-songs', action='store_true', help='Organize downloaded songs into playlist folders with symlinks')
    parser.add_argument('--dry-run', action='store_true', help='Simulate the organization process without creating symlinks')
    parser.add_argument('--embed-metadata', action='store_true', help='Embed TrackId into song file metadata')
    parser.add_argument('--interactive', action='store_true', help='Enable interactive mode for fuzzy matching')
    parser.add_argument('--remove-track-ids', action='store_true', help='Remove TrackId from all MP3 files')
    parser.add_argument('--count-track-ids', action='store_true', help='Count MP3 files with TrackId')
    parser.add_argument('--cleanup-tracks', action='store_true', help='Clean up unwanted files from tracks_master directory by moving them to quarantine')
    parser.add_argument('--sync-master', action='store_true', help='Sync all tracks from all playlists to MASTER playlist')
    parser.add_argument('--validate-tracks', action='store_true', help='Validate local tracks against MASTER playlist')
    parser.add_argument('--validate-lengths', action='store_true', help='Validate song lengths and report songs shorter than 5 minutes')
    parser.add_argument('--validate-playlists', action='store_true', help='Validate playlist symlinks against Spotify playlists')
    parser.add_argument('--validate-playlists-quick', action='store_true', help='Quick validation of playlist symlinks (TrackIds only)')
    parser.add_argument('--validate-all', action='store_true', help='Run all validation checks (validate-tracks, validate-lengths, validate-playlists')
    parser.add_argument('--sync-unplaylisted', action='store_true', help='Sync unplaylisted Liked Songs to UNSORTED playlist')
    parser.add_argument('--cleanup-symlinks', action='store_true', help='Remove broken symlinks from playlists_master directory')

    args = parser.parse_args()

    if args.run_all:
        clear_db()
        insert_db()
    if args.clear_all:
        clear_db()
    if args.insert_all:
        insert_db()
    if args.my_playlists:
        clear_playlists()
        insert_playlists()
    if args.master_tracks:
        clear_master_tracks()
        insert_tracks_and_associations()
    if args.clear_my_playlists:
        clear_playlists()
    if args.insert_my_playlists:
        insert_playlists()
    if args.clear_master_tracks:
        clear_master_tracks()
    if args.insert_master_tracks:
        insert_tracks_and_associations()
    if args.fetch_master_tracks_db:
        tracks = fetch_master_tracks_db()
        print("\nMaster Playlist Tracks from Database:")
        for track in tracks:
            track_title, artists, album = track
            print(f"Title: {track_title}, Artists: {artists}, Album: {album}")
            # todo: add Playlist to this. would have to join this table with TrackPlaylists
    if args.fetch_all_playlists_db:
        playlists = fetch_all_playlists_db()
        print("\nAll Playlists from Database:")
        for pl in playlists:
            playlist_id, playlist_name = pl
            print(f"PlaylistId: {playlist_id}, PlaylistName: {playlist_name}")
    if args.embed_metadata:
        # * DB must be up-to-date
        embed_track_metadata(MASTER_TRACKS_DIRECTORY, interactive=args.interactive)
    if args.remove_track_ids:
        remove_all_track_ids(MASTER_TRACKS_DIRECTORY)
    if args.count_track_ids:
        count_tracks_with_id(MASTER_TRACKS_DIRECTORY)
    if args.organize_songs:
        organize_songs_into_playlists(MASTER_TRACKS_DIRECTORY, PLAYLISTS_DIRECTORY, dry_run=args.dry_run)
    if args.cleanup_tracks:
        cleanup_tracks(MASTER_TRACKS_DIRECTORY, QUARANTINE_DIRECTORY)
    if args.validate_tracks:
        # ! Warning: Lots of API requests
        validate_master_tracks(MASTER_TRACKS_DIRECTORY)
    if args.validate_lengths:
        validate_song_lengths(MASTER_TRACKS_DIRECTORY)
    if args.validate_playlists:
        # ! Warning: Lots of API requests
        validate_playlist_symlinks(PLAYLISTS_DIRECTORY)
    if args.validate_playlists_quick:
        # ! Warning: Lots of API requests
        validate_playlist_symlinks_quick(PLAYLISTS_DIRECTORY)
    if args.validate_all:
        # ! Warning: Lots of API requests
        validate_master_tracks(MASTER_TRACKS_DIRECTORY)
        validate_song_lengths(MASTER_TRACKS_DIRECTORY)
        validate_playlist_symlinks(PLAYLISTS_DIRECTORY)
    if args.sync_master:
        # ! Warning: Lots of API requests
        spotify_client = authenticate_spotify()
        sync_to_master_playlist(spotify_client, MASTER_PLAYLIST_ID)
    if args.sync_unplaylisted:
        # ! Warning: Lots of API requests
        spotify_client = authenticate_spotify()
        sync_unplaylisted_to_unsorted(spotify_client, UNSORTED_PLAYLIST_ID)
    if args.cleanup_symlinks:
        cleanup_broken_symlinks(PLAYLISTS_DIRECTORY, dry_run=args.dry_run)


if __name__ == "__main__":
    main()  # keep this uncommented if running in the CLI
    # clear_master_tracks()
    # insert_master_tracks()
    # clear_playlists()
    # insert_playlists()
    # insert_tracks_and_associations()
