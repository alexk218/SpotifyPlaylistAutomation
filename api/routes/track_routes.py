from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import track_service
from api.services.duplicate_track_service import get_duplicate_tracks_report, detect_and_cleanup_duplicate_tracks
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

bp = Blueprint('tracks', __name__, url_prefix='/api/tracks')

tracks_logger = setup_logger('tracks_helper', 'tracks', 'tracks.log')


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


@bp.route('/duplicates/cleanup', methods=['POST'])
def cleanup_duplicates():
    """
    Endpoint to clean up duplicate tracks with optional user selections.
    """
    try:
        from api.services.duplicate_track_service import detect_and_cleanup_duplicate_tracks

        data = request.get_json() or {}
        dry_run = data.get('dry_run', False)
        user_selections = data.get('user_selections')  # Optional dict of group_id -> selected_uri

        # Pass user_selections only if it's provided and not empty
        if user_selections and isinstance(user_selections, dict) and len(user_selections) > 0:
            result = detect_and_cleanup_duplicate_tracks(dry_run=dry_run, user_selections=user_selections)
        else:
            result = detect_and_cleanup_duplicate_tracks(dry_run=dry_run)

        if result["success"]:
            action = "analyzed" if dry_run else "cleaned up"
            tracks_logger.info(f"Duplicate cleanup {action}: {result.get('tracks_removed', 0)} tracks processed")
        else:
            tracks_logger.error(f"Duplicate cleanup failed: {result['message']}")

        return jsonify(result)

    except Exception as e:
        error_msg = f"Error in duplicate cleanup endpoint: {str(e)}"
        tracks_logger.error(error_msg)
        return jsonify({
            "success": False,
            "message": "Internal server error during duplicate cleanup",
            "error": str(e)
        }), 500


@bp.route('/duplicates/detect', methods=['GET'])
def detect_duplicates():
    """
    Endpoint to detect duplicate tracks with playlist names displayed.
    """
    try:
        from api.services.duplicate_track_service import get_duplicate_tracks_report

        result = get_duplicate_tracks_report()

        if result["success"]:
            tracks_logger.info(f"Duplicate detection completed: {result['total_groups']} groups found")
        else:
            tracks_logger.error(f"Duplicate detection failed: {result['message']}")

        return jsonify(result)

    except Exception as e:
        error_msg = f"Error in duplicate detection endpoint: {str(e)}"
        tracks_logger.error(error_msg)
        return jsonify({
            "success": False,
            "message": "Internal server error during duplicate detection",
            "error": str(e)
        }), 500


@bp.route('/duplicates/apply-selections', methods=['POST'])
def apply_duplicate_selections():
    """
    Endpoint to apply user selections for duplicate groups and perform cleanup.
    """
    try:
        from api.services.duplicate_track_service import apply_user_selections_and_cleanup

        data = request.get_json() or {}
        user_selections = data.get('user_selections', {})
        dry_run = data.get('dry_run', False)

        if not user_selections:
            return jsonify({
                "success": False,
                "message": "No user selections provided"
            }), 400

        result = apply_user_selections_and_cleanup(user_selections, dry_run=dry_run)

        if result["success"]:
            action = "analyzed with selections" if dry_run else "cleaned up with user selections"
            tracks_logger.info(f"Duplicate cleanup {action}: {result.get('tracks_removed', 0)} tracks processed")
        else:
            tracks_logger.error(f"Duplicate cleanup with selections failed: {result['message']}")

        return jsonify(result)

    except Exception as e:
        error_msg = f"Error in duplicate selections endpoint: {str(e)}"
        tracks_logger.error(error_msg)
        return jsonify({
            "success": False,
            "message": "Internal server error during duplicate cleanup with selections",
            "error": str(e)
        }), 500


@bp.route('/cleanup-mappings', methods=['DELETE'])
def clear_all_file_mappings():
    """Clear all file mappings and reset auto-increment counter."""
    try:
        with UnitOfWork() as uow:
            deleted_count = uow.file_track_mapping_repository.delete_all_and_reset()

        return jsonify({
            "success": True,
            "message": f"Cleared {deleted_count} file mappings and reset counter",
            "deleted_count": deleted_count
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error clearing file mappings: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
