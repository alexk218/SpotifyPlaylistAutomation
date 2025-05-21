from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import rekordbox_service

bp = Blueprint('rekordbox', __name__, url_prefix='/api/rekordbox')


@bp.route('/generate-xml', methods=['POST'])
def generate_rekordbox_xml():
    """Generate a Rekordbox XML file from M3U playlists."""
    playlists_dir = request.json.get('playlistsDir')
    output_xml_path = request.json.get('rekordboxXmlPath')
    rating_data = request.json.get('ratingData', {})
    master_tracks_dir = request.json.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified"
        }), 400

    if not output_xml_path:
        return jsonify({
            "success": False,
            "message": "Output XML path not specified"
        }), 400

    try:
        result = rekordbox_service.generate_rekordbox_xml(
            playlists_dir,
            output_xml_path,
            master_tracks_dir,
            rating_data
        )

        return jsonify({
            "success": True,
            "message": f"Successfully generated rekordbox XML with {result['total_tracks']} tracks and {result['total_playlists']} playlists. Applied ratings to {result['total_rated']} tracks."
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error generating rekordbox XML: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
