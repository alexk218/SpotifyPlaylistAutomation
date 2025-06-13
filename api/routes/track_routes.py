from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import track_service

bp = Blueprint('tracks', __name__, url_prefix='/api/tracks')


@bp.route('/search', methods=['GET'])
def search_tracks():
    """Search for tracks that match the query."""
    query = request.args.get('query', '')
    limit = int(request.args.get('limit', 20))
    search_type = request.args.get('type', 'general')  # 'general' or 'matching'

    try:
        if search_type == 'matching':
            # Search Tracks db for file-track matching
            results = track_service.search_tracks_db_for_matching(query, limit)
        else:
            # For general file system search
            master_tracks_dir = request.args.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
            results = track_service.search_tracks_file_system(master_tracks_dir, query)

        return jsonify({"success": True, "results": results})
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error searching tracks: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/match', methods=['POST'])
def fuzzy_match_track():
    """Find potential Spotify track matches for a local file."""
    file_name = request.json.get('fileName')
    current_track_id = request.json.get('currentTrackId')

    if not file_name:
        return jsonify({"success": False, "message": "No file name provided"}), 400

    try:
        matches = track_service.fuzzy_match_track(file_name, current_track_id)
        return jsonify({"success": True, **matches})
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error in fuzzy match: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/mapping', methods=['POST'])
def manage_file_mappings():
    """Manage file-to-track mappings (analysis and creation)."""
    master_tracks_dir = request.json.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    confirmed = request.json.get('confirmed', False)
    precomputed_changes = request.json.get('precomputed_changes_from_analysis')
    confidence_threshold = request.json.get('confidence_threshold', 0.75)
    user_selections = request.json.get('user_selections', [])

    if not master_tracks_dir:
        return jsonify({
            "success": False,
            "message": "Master tracks directory not specified"
        }), 400

    try:
        result = track_service.orchestrate_file_mapping(
            master_tracks_dir,
            confirmed,
            precomputed_changes,
            confidence_threshold,
            user_selections
        )
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error managing file mappings: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('', methods=['DELETE'])
def delete_file():
    """Delete a file from the filesystem."""
    file_path = request.json.get('file_path')

    if not file_path:
        return jsonify({
            "success": False,
            "message": "file_path is required"
        }), 400

    try:
        filename = track_service.delete_file(file_path)
        return jsonify({
            "success": True,
            "message": f"File deleted: {filename}"
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error deleting file: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/compare', methods=['GET'])
def direct_tracks_compare():
    """Directly compare Spotify tracks with local tracks from the database."""
    master_tracks_dir = request.args.get('master_tracks_dir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']

    if not master_tracks_dir:
        return jsonify({
            "success": False,
            "message": f"Master tracks directory does not exist: {master_tracks_dir}"
        }), 400

    try:
        result = track_service.direct_tracks_compare(master_tracks_dir)
        return jsonify({
            "success": True,
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error in direct tracks compare: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/download', methods=['POST'])
def download_track():
    """Download a track using spotDL and create file mapping."""
    uri = request.json.get('uri')
    download_dir = request.json.get('download_dir') or current_app.config.get('MASTER_TRACKS_DIRECTORY_SSD')

    if not uri:
        return jsonify({
            "success": False,
            "message": "Track URI is required"
        }), 400

    try:
        result = track_service.download_and_map_track(uri, download_dir)
        return jsonify({
            "success": True,
            "message": f"Successfully downloaded and mapped track",
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error downloading track: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/download-batch', methods=['POST'])
def download_batch():
    """Download multiple tracks using spotDL and create file mappings."""
    uris = request.json.get('uris', [])
    download_dir = request.json.get('download_dir') or current_app.config.get('MASTER_TRACKS_DIRECTORY_SSD')

    if not uris:
        return jsonify({
            "success": False,
            "message": "Track URIs are required"
        }), 400

    try:
        result = track_service.download_all_missing_tracks(uris, download_dir)
        return jsonify({
            "success": True,
            "message": f"Batch download completed: {result['success_count']} successful, {result['failure_count']} failed",
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error in batch download: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/cleanup-mappings', methods=['POST'])
def cleanup_stale_mappings():
    """Clean up file mappings that point to non-existent files."""
    try:
        result = track_service.cleanup_stale_file_mappings()
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error cleaning up mappings: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/duplicates/detect', methods=['GET'])
def detect_duplicate_tracks():
    """Generate a report of duplicate tracks without making changes."""
    try:
        from api.services.duplicate_track_service import get_duplicate_tracks_report

        result = get_duplicate_tracks_report()
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error detecting duplicates: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/duplicates/cleanup', methods=['POST'])
def cleanup_duplicate_tracks():
    """Clean up duplicate tracks by removing duplicates and merging playlist associations."""
    try:
        from api.services.duplicate_track_service import detect_and_cleanup_duplicate_tracks

        data = request.get_json() or {}
        dry_run = data.get('dry_run', False)

        result = detect_and_cleanup_duplicate_tracks(dry_run=dry_run)
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error cleaning up duplicates: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
