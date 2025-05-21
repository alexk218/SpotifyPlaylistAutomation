# api/routes/validation_routes.py
from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import validation_service

bp = Blueprint('validation', __name__, url_prefix='/api/validation')


@bp.route('/tracks', methods=['GET'])
def validate_tracks():
    """Validate local tracks against database information."""
    master_tracks_dir = request.args.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']

    try:
        result = validation_service.validate_tracks(master_tracks_dir)
        return jsonify({"success": True, "stats": result})
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error validating tracks: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/playlists', methods=['GET'])
def validate_playlists_m3u():
    """Validate playlists against M3U files."""
    master_tracks_dir = request.args.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    playlists_dir = request.args.get('playlistsDir')

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified"
        }), 400

    try:
        result = validation_service.validate_playlists_m3u(master_tracks_dir, playlists_dir)
        return jsonify({
            "success": True,
            "summary": result["summary"],
            "playlist_analysis": result["playlist_analysis"]
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error validating playlists: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/track-metadata', methods=['GET'])
def validate_track_metadata():
    """Validate track metadata in the master tracks directory."""
    master_tracks_dir = request.args.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    confidence_threshold = float(request.args.get('confidence_threshold', 0.75))

    try:
        result = validation_service.validate_track_metadata(master_tracks_dir, confidence_threshold)
        return jsonify({
            "success": True,
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error validating track metadata: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
