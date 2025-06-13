from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import playlist_service

bp = Blueprint('playlists', __name__, url_prefix='/api/playlists')


@bp.route('/analysis', methods=['GET', 'POST'])
def analyze_m3u_generation():
    """Analyze which playlists would be generated without making changes."""
    master_tracks_dir = request.json.get('masterTracksDir', None) if request.is_json else request.args.get(
        'masterTracksDir')
    master_tracks_dir = master_tracks_dir or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']

    playlists_dir = request.json.get('playlistsDir', None) if request.is_json else request.args.get('playlistsDir')

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified"
        }), 400

    try:
        result = playlist_service.analyze_m3u_generation(master_tracks_dir, playlists_dir)

        return jsonify({
            "success": True,
            "message": f"Ready to generate {result['total_playlists']} M3U playlists.",
            "needs_confirmation": result['total_playlists'] > 0,
            "details": result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error analyzing M3U playlists: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/generate', methods=['POST'])
def generate_playlists():
    """Generate M3U playlist files for selected playlists."""
    master_tracks_dir = request.json.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    playlists_dir = request.json.get('playlistsDir')
    extended = request.json.get('extended', True)
    overwrite = request.json.get('overwrite', True)
    confirmed = request.json.get('confirmed', False)
    playlists_to_update = request.json.get('playlists_to_update', [])

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified"
        }), 400

    # If not confirmed, just run analysis
    if not confirmed:
        return analyze_m3u_generation()

    try:
        result = playlist_service.generate_playlists(
            master_tracks_dir,
            playlists_dir,
            playlists_to_update,
            extended,
            overwrite
        )

        return jsonify({
            "success": True,
            "message": f"Successfully regenerated {result['playlists_updated']} M3U playlists. {result['playlists_failed']} failed.",
            "playlists_updated": result['playlists_updated'],
            "playlists_failed": result['playlists_failed'],
            "updated_playlists": result['updated_playlists'],
            "total_processed": result['total_playlists_to_update']
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error generating M3U playlists: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/<playlist_id>/regenerate', methods=['POST'])
def regenerate_playlist(playlist_id):
    """Regenerate a single M3U playlist."""
    master_tracks_dir = request.json.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    playlists_dir = request.json.get('playlistsDir')
    extended = request.json.get('extended', True)
    overwrite = request.json.get('overwrite', True)
    force = request.json.get('force', True)

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified"
        }), 400

    try:
        result = playlist_service.regenerate_playlist(
            playlist_id,
            master_tracks_dir,
            playlists_dir,
            extended,
            force
        )

        return jsonify({
            "success": True,
            "message": f"Successfully regenerated playlist: {result['stats']['playlist_name']}",
            "result": result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error regenerating playlist: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
