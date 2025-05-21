from flask import Blueprint, request, jsonify, current_app
import traceback
from api.services import track_service

bp = Blueprint('tracks', __name__, url_prefix='/api/tracks')


@bp.route('/search', methods=['GET'])
def search_tracks():
    """Search for tracks that match the query."""
    query = request.args.get('query', '')
    master_tracks_dir = request.args.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']

    try:
        results = track_service.search_tracks(master_tracks_dir, query)
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


@bp.route('/metadata', methods=['PUT'])
def update_track_id():
    """Update the track ID in a file's metadata."""
    file_path = request.json.get('file_path')
    new_track_id = request.json.get('new_track_id')

    if not file_path or not new_track_id:
        return jsonify({
            "success": False,
            "message": "Both file_path and new_track_id are required"
        }), 400

    try:
        result = track_service.update_track_id(file_path, new_track_id)
        return jsonify({
            "success": True,
            "message": f"Successfully updated TrackId from '{result['old_track_id']}' to '{result['new_track_id']}'",
            "old_track_id": result['old_track_id'],
            "new_track_id": result['new_track_id']
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error correcting track ID: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/metadata', methods=['DELETE'])
def remove_track_id():
    """Remove track ID from a file's metadata."""
    file_path = request.json.get('file_path')

    if not file_path:
        return jsonify({
            "success": False,
            "message": "file_path is required"
        }), 400

    try:
        old_track_id = track_service.remove_track_id(file_path)
        return jsonify({
            "success": True,
            "message": f"Successfully removed TrackId '{old_track_id}' from file",
            "old_track_id": old_track_id
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error removing track ID: {e}")
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


@bp.route('/metadata/embed/analyze', methods=['POST'])
def analyze_embedding_metadata():
    """Analyze files that need track ID embedding."""
    master_tracks_dir = request.json.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    auto_confirm_threshold = request.json.get('auto_confirm_threshold', 0.75)

    try:
        result = track_service.analyze_embedding_metadata(master_tracks_dir, auto_confirm_threshold)

        return jsonify({
            "success": True,
            "message": f"Found {len(result['files_without_id'])} files without TrackId out of {result['total_files']} total files. Auto-matched {len(result['auto_matched_files'])} files.",
            "needs_confirmation": result['needs_confirmation'],
            "requires_fuzzy_matching": result['requires_fuzzy_matching'],
            "details": {
                "files_to_process": result['files_without_id'],
                "total_files": result['total_files'],
                "auto_matched_files": result['auto_matched_files']
            }
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error analyzing metadata embedding: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/metadata/embed', methods=['POST'])
def embed_metadata():
    """Embed track IDs in multiple files based on user selections."""
    master_tracks_dir = request.json.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    confirmed = request.json.get('confirmed', False)
    user_selections = request.json.get('userSelections', [])
    skipped_files = request.json.get('skippedFiles', [])

    # If not confirmed, run analysis
    if not confirmed:
        return analyze_embedding_metadata()

    try:
        result = track_service.embed_metadata_batch(master_tracks_dir, user_selections)

        return jsonify({
            "success": True,
            "message": f"Embedded TrackId into {result['successful_embeds']} files. {result['failed_embeds']} files failed.",
            "results": result['results'],
            "successful_embeds": result['successful_embeds'],
            "failed_embeds": result['failed_embeds'],
            "skipped_files": len(skipped_files)
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error embedding metadata: {e}")
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
    master_playlist_id = request.args.get('master_playlist_id') or current_app.config['MASTER_PLAYLIST_ID']

    if not master_tracks_dir:
        return jsonify({
            "success": False,
            "message": f"Master tracks directory does not exist: {master_tracks_dir}"
        }), 400

    try:
        result = track_service.direct_tracks_compare(master_tracks_dir, master_playlist_id)
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
