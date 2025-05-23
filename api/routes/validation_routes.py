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

    try:
        result = validation_service.validate_track_metadata(master_tracks_dir)
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


@bp.route('/short-tracks', methods=['GET'])
def validate_short_tracks():
    """Validate tracks that are shorter than minimum length."""
    master_tracks_dir = request.args.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    min_length_minutes = float(request.args.get('minLengthMinutes', 5))

    try:
        result = validation_service.validate_short_tracks(master_tracks_dir, min_length_minutes)
        return jsonify({"success": True, **result})
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error validating short tracks: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/search-extended-versions', methods=['POST'])
def search_extended_versions():
    """Search for extended versions of a specific track using Discogs."""
    data = request.get_json()
    artist = data.get('artist')
    title = data.get('title')
    current_duration = data.get('currentDuration')

    if not all([artist, title, current_duration]):
        return jsonify({
            "success": False,
            "message": "Artist, title, and currentDuration are required"
        }), 400

    try:
        result = validation_service.search_extended_versions_for_track(artist, title, current_duration)
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error searching extended versions: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
