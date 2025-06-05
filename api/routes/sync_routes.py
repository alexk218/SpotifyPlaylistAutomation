from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import sync_service

bp = Blueprint('sync', __name__, url_prefix='/api/sync')


@bp.route('/master', methods=['POST'])
def sync_master_playlist():
    """Sync all tracks from all playlists to MASTER playlist."""
    master_playlist_id = request.json.get('master_playlist_id') or current_app.config['MASTER_PLAYLIST_ID']

    if not master_playlist_id:
        return jsonify({
            "success": False,
            "message": "Master playlist ID not provided or found in environment"
        }), 400

    try:
        result = sync_service.sync_master_playlist(master_playlist_id, request.json)
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error starting sync: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}"
        }), 500


@bp.route('/unplaylisted', methods=['POST'])
def sync_unplaylisted_tracks():
    """Sync unplaylisted tracks to UNSORTED playlist."""
    unsorted_playlist_id = request.json.get('unsorted_playlist_id') or current_app.config.get('UNSORTED_PLAYLIST_ID')

    if not unsorted_playlist_id:
        return jsonify({
            "success": False,
            "message": "Unsorted playlist ID not provided or found in environment"
        }), 400

    try:
        result = sync_service.sync_unplaylisted_tracks(unsorted_playlist_id)
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error starting sync: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}"
        }), 500


@bp.route('/database', methods=['POST'])
def sync_database():
    """Sync database with Spotify data."""
    action = request.json.get('action', 'all')
    force_refresh = request.json.get('force_refresh', False)
    is_confirmed = request.json.get('confirmed', False)
    master_playlist_id = request.json.get('master_playlist_id') or current_app.config['MASTER_PLAYLIST_ID']
    precomputed_changes = request.json.get('precomputed_changes_from_analysis')
    stage = request.json.get('stage', 'start')

    exclusion_config = sync_service.get_exclusion_config(request.json)

    try:
        result = sync_service.orchestrate_db_sync(
            action,
            master_playlist_id,
            force_refresh,
            is_confirmed,
            precomputed_changes,
            exclusion_config,
            stage
        )
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error in sync_database: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}",
            "traceback": error_str
        }), 500
