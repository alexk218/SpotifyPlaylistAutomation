from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import sys
import traceback
from datetime import datetime
import glob
from dotenv import load_dotenv

# Ensure we can import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your existing modules
from sql.helpers.db_helper import clear_db
from drivers.spotify_client import authenticate_spotify, sync_to_master_playlist, sync_unplaylisted_to_unsorted
from helpers.file_helper import embed_track_metadata, remove_all_track_ids, count_tracks_with_id, cleanup_tracks, \
    validate_song_lengths
from helpers.organization_helper import organize_songs_into_m3u_playlists
from helpers.sync_helper import sync_playlists_incremental, sync_master_tracks_incremental
from helpers.validation_helper import validate_master_tracks
from utils.logger import setup_logger
from cache_manager import spotify_cache

# Load environment variables
load_dotenv()

# Get environment variables
MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
MASTER_TRACKS_DIRECTORY_SSD = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
PLAYLISTS_DIRECTORY = os.getenv("PLAYLISTS_DIRECTORY")
QUARANTINE_DIRECTORY = os.getenv("QUARANTINE_DIRECTORY")
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')
UNSORTED_PLAYLIST_ID = os.getenv("UNSORTED_PLAYLIST_ID")
M3U_PLAYLISTS_DIRECTORY = os.getenv("M3U_PLAYLISTS_DIRECTORY_SSD")

# Setup logging
api_logger = setup_logger('api', 'logs/api.log')

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Spotify client (will be initialized when needed)
spotify_client = None


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get the current status of connections and key directories"""
    global spotify_client

    try:
        # Check if Spotify client exists and can get current user
        spotify_connected = False
        user_info = None

        if spotify_client:
            try:
                user_info = spotify_client.current_user()
                spotify_connected = True
            except:
                spotify_connected = False

        return jsonify({
            'success': True,
            'spotify_connected': spotify_connected,
            'spotify_user': user_info,
            'directories': {
                'master_tracks': MASTER_TRACKS_DIRECTORY,
                'master_tracks_ssd': MASTER_TRACKS_DIRECTORY_SSD,
                'playlists': PLAYLISTS_DIRECTORY,
                'quarantine': QUARANTINE_DIRECTORY,
                'm3u_playlists': M3U_PLAYLISTS_DIRECTORY
            }
        })
    except Exception as e:
        api_logger.error(f"Error getting status: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/spotify/connect', methods=['POST'])
def connect_spotify():
    """Connect to Spotify API"""
    global spotify_client

    try:
        spotify_client = authenticate_spotify()
        user_info = spotify_client.current_user()
        api_logger.info(f"Connected to Spotify as user: {user_info['id']}")

        return jsonify({
            'success': True,
            'user': {
                'id': user_info['id'],
                'display_name': user_info['display_name'],
                'email': user_info.get('email')
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to connect to Spotify: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/db/clear', methods=['POST'])
def clear_database():
    """Clear all database tables"""
    try:
        clear_db()
        api_logger.info("Database cleared successfully")
        return jsonify({'success': True})
    except Exception as e:
        api_logger.error(f"Failed to clear database: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/sync/playlists', methods=['POST'])
def sync_playlists():
    """Sync playlists incrementally"""
    force_refresh = request.json.get('force_refresh', False)
    confirm = request.json.get('confirm', False)

    if not confirm:
        # First run the analysis to check what changes would be made
        try:
            from helpers.sync_helper import analyze_playlists_changes
            added, updated, unchanged, changes_details = analyze_playlists_changes(force_full_refresh=force_refresh)

            return jsonify({
                'success': True,
                'needs_confirmation': True,
                'analysis': {
                    'added': added,
                    'updated': updated,
                    'unchanged': unchanged,
                    'details': changes_details
                }
            })
        except Exception as e:
            api_logger.error(f"Failed to analyze playlist changes: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    # If confirm=True, proceed with the actual sync
    try:
        added, updated, unchanged = sync_playlists_incremental(force_full_refresh=force_refresh, auto_confirm=True)
        api_logger.info(f"Playlists sync complete: {added} added, {updated} updated, {unchanged} unchanged")

        return jsonify({
            'success': True,
            'stats': {
                'added': added,
                'updated': updated,
                'unchanged': unchanged
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to sync playlists: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/sync/tracks', methods=['POST'])
def sync_tracks():
    """Sync master tracks incrementally"""
    force_refresh = request.json.get('force_refresh', False)

    try:
        added, updated, unchanged = sync_master_tracks_incremental(MASTER_PLAYLIST_ID, force_full_refresh=force_refresh)
        api_logger.info(f"Master tracks sync complete: {added} added, {updated} updated, {unchanged} unchanged")

        return jsonify({
            'success': True,
            'stats': {
                'added': added,
                'updated': updated,
                'unchanged': unchanged
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to sync master tracks: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/sync/all', methods=['POST'])
def sync_all():
    """Sync playlists and master tracks incrementally"""
    force_refresh = request.json.get('force_refresh', False)

    try:
        playlists_added, playlists_updated, playlists_unchanged = sync_playlists_incremental(
            force_full_refresh=force_refresh)
        tracks_added, tracks_updated, tracks_unchanged = sync_master_tracks_incremental(MASTER_PLAYLIST_ID,
                                                                                        force_full_refresh=force_refresh)

        api_logger.info(
            f"All sync complete. Playlists: {playlists_added} added, {playlists_updated} updated, {playlists_unchanged} unchanged. " +
            f"Tracks: {tracks_added} added, {tracks_updated} updated, {tracks_unchanged} unchanged.")

        return jsonify({
            'success': True,
            'stats': {
                'playlists': {
                    'added': playlists_added,
                    'updated': playlists_updated,
                    'unchanged': playlists_unchanged
                },
                'tracks': {
                    'added': tracks_added,
                    'updated': tracks_updated,
                    'unchanged': tracks_unchanged
                }
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to sync all: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/spotify/sync-to-master', methods=['POST'])
def sync_master():
    """Sync all tracks from all playlists to MASTER playlist"""
    global spotify_client

    if not spotify_client:
        return jsonify({'success': False, 'error': 'Not connected to Spotify'}), 400

    try:
        # The actual implementation would call your sync function
        # This could be a long operation, consider using background tasks
        sync_to_master_playlist(spotify_client, MASTER_PLAYLIST_ID)

        return jsonify({'success': True})
    except Exception as e:
        api_logger.error(f"Failed to sync to master playlist: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/spotify/sync-unplaylisted', methods=['POST'])
def sync_unplaylisted():
    """Sync unplaylisted Liked Songs to UNSORTED playlist"""
    global spotify_client

    if not spotify_client:
        return jsonify({'success': False, 'error': 'Not connected to Spotify'}), 400

    try:
        # The actual implementation would call your sync function
        sync_unplaylisted_to_unsorted(spotify_client, UNSORTED_PLAYLIST_ID)

        return jsonify({'success': True})
    except Exception as e:
        api_logger.error(f"Failed to sync unplaylisted tracks: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """Clear all Spotify API caches"""
    try:
        spotify_cache.clear_all_caches()
        api_logger.info("Cleared all Spotify API caches")
        return jsonify({'success': True})
    except Exception as e:
        api_logger.error(f"Failed to clear cache: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/generate-m3u', methods=['POST'])
def generate_m3u():
    """Generate M3U playlist files"""
    options = request.json
    extended = options.get('extended', True)
    overwrite = options.get('overwrite', True)
    all_playlists = options.get('allPlaylists', False)

    try:
        organize_songs_into_m3u_playlists(
            MASTER_TRACKS_DIRECTORY_SSD,
            M3U_PLAYLISTS_DIRECTORY,
            extended=extended,
            overwrite=overwrite,
            only_changed=not all_playlists
        )

        api_logger.info(
            f"Generated M3U playlists. Extended: {extended}, Overwrite: {overwrite}, All playlists: {all_playlists}")
        return jsonify({'success': True})
    except Exception as e:
        api_logger.error(f"Failed to generate M3U playlists: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/embed-metadata', methods=['POST'])
def metadata_embed():
    """Embed TrackId into song file metadata"""
    interactive = request.json.get('interactive', False)

    try:
        successful_count, total_files = embed_track_metadata(MASTER_TRACKS_DIRECTORY, interactive=interactive)

        api_logger.info(f"Embedded metadata in {successful_count} of {total_files} files")
        return jsonify({
            'success': True,
            'stats': {
                'successful': successful_count,
                'total': total_files
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to embed metadata: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/count-track-ids', methods=['GET'])
def track_ids_count():
    """Count MP3 files with TrackId"""
    try:
        tracks_with_id, total_count = count_tracks_with_id(MASTER_TRACKS_DIRECTORY_SSD)

        api_logger.info(f"Counted {tracks_with_id} of {total_count} files with TrackId")
        return jsonify({
            'success': True,
            'stats': {
                'with_id': tracks_with_id,
                'total': total_count,
                'percentage': round(tracks_with_id / total_count * 100, 2) if total_count > 0 else 0
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to count track ids: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/remove-track-ids', methods=['POST'])
def remove_ids():
    """Remove TrackId from all MP3 files"""
    try:
        removed_count = remove_all_track_ids(MASTER_TRACKS_DIRECTORY)

        api_logger.info(f"Removed TrackId from {removed_count} files")
        return jsonify({
            'success': True,
            'stats': {
                'removed': removed_count
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to remove track ids: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/cleanup-tracks', methods=['POST'])
def cleanup():
    """Clean up unwanted files"""
    try:
        cleanup_tracks(MASTER_TRACKS_DIRECTORY, QUARANTINE_DIRECTORY)

        api_logger.info(f"Cleaned up unwanted files")
        return jsonify({'success': True})
    except Exception as e:
        api_logger.error(f"Failed to clean up tracks: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/validation/tracks', methods=['POST'])
def validate_tracks():
    """Validate local tracks against database information"""
    try:
        results = validate_master_tracks(MASTER_TRACKS_DIRECTORY_SSD)

        api_logger.info(f"Validated tracks: {results}")
        return jsonify({
            'success': True,
            'results': results
        })
    except Exception as e:
        api_logger.error(f"Failed to validate tracks: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/validation/song-lengths', methods=['POST'])
def validate_lengths():
    """Validate song lengths"""
    min_length = request.json.get('min_length', 5)  # Default to 5 minutes

    try:
        short_songs, total_files = validate_song_lengths(MASTER_TRACKS_DIRECTORY_SSD, min_length_minutes=min_length)

        api_logger.info(f"Validated song lengths: {short_songs} short songs out of {total_files} total")
        return jsonify({
            'success': True,
            'results': {
                'short_songs': short_songs,
                'total_files': total_files,
                'percentage': round(short_songs / total_files * 100, 2) if total_files > 0 else 0
            }
        })
    except Exception as e:
        api_logger.error(f"Failed to validate song lengths: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/logs/files', methods=['GET'])
def get_log_files():
    """Get a list of available log files"""
    try:
        # Get all log files in the logs directory and subdirectories
        log_files = []
        log_dirs = ['logs', 'logs/deduplication_reports', 'logs/m3u_validation', 'logs/master_validation', 'sql']

        for log_dir in log_dirs:
            if os.path.exists(log_dir):
                for file in glob.glob(f"{log_dir}/*.log"):
                    log_files.append(file)

        # Sort by modification time (newest first)
        log_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Return just the filenames for the dropdown
        log_filenames = [os.path.basename(f) for f in log_files]

        return jsonify({
            'success': True,
            'files': log_filenames,
            'full_paths': log_files
        })
    except Exception as e:
        api_logger.error(f"Failed to get log files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/logs/content/<path:filename>', methods=['GET'])
def get_log_content(filename):
    """Get the content of a log file"""
    try:
        # For security, only allow log files from specific directories
        log_dirs = ['logs', 'logs/deduplication_reports', 'logs/m3u_validation', 'logs/master_validation', 'sql']
        log_file_path = None

        for log_dir in log_dirs:
            potential_path = os.path.join(log_dir, filename)
            if os.path.exists(potential_path):
                log_file_path = potential_path
                break

        if not log_file_path:
            return jsonify({'success': False, 'error': 'Log file not found'}), 404

        with open(log_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        return jsonify({
            'success': True,
            'content': content,
            'path': log_file_path
        })
    except Exception as e:
        api_logger.error(f"Failed to get log content: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
