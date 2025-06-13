import json
import os
import traceback
from flask import Blueprint, request, jsonify, current_app
from api.services import validation_service
from sql.core.unit_of_work import UnitOfWork

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


# TODO: move logic to service
@bp.route('/extract-file-mappings', methods=['POST'])
def extract_file_mappings():
    """Extract URI mappings from a list of file paths."""
    data = request.get_json()
    file_paths = data.get('filePaths', [])

    if not file_paths:
        return jsonify({
            "success": False,
            "message": "No file paths provided"
        }), 400

    try:
        mappings = []
        with UnitOfWork() as uow:
            for file_path in file_paths:
                uri = uow.file_track_mapping_repository.get_uri_by_file_path(file_path)
                mappings.append({
                    'file_path': file_path,
                    'uri': uri
                })

        return jsonify({
            "success": True,
            "mappings": mappings
        })

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error extracting file mappings: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


# TODO: move this to track_routes
@bp.route('/remove-file-mapping', methods=['POST'])
def remove_file_mapping():
    """Remove mapping for a specific file path."""
    data = request.get_json()
    file_path = data.get('filePath')

    if not file_path:
        return jsonify({
            "success": False,
            "message": "File path is required"
        }), 400

    try:
        with UnitOfWork() as uow:
            success = uow.file_track_mapping_repository.delete_by_file_path(file_path)
            if success:
                uow.commit()
                return jsonify({
                    "success": True,
                    "message": "Mapping removed successfully"
                })
            else:
                return jsonify({
                    "success": False,
                    "message": "No mapping found for the specified file"
                })

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error removing file mapping: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/remove-duplicate-mapping', methods=['POST'])
def remove_duplicate_mapping():
    """Remove a specific file mapping from duplicates."""
    data = request.get_json()
    file_path = data.get('filePath')
    uri = data.get('uri')

    if not file_path:
        return jsonify({
            "success": False,
            "message": "File path is required"
        }), 400

    try:
        with UnitOfWork() as uow:
            # Verify this is actually a duplicate before removing
            files_with_uri = uow.file_track_mapping_repository.get_files_by_uri(uri)

            if len(files_with_uri) <= 1:
                return jsonify({
                    "success": False,
                    "message": "This is not a duplicate mapping"
                })

            success = uow.file_track_mapping_repository.delete_by_file_path(file_path)
            if success:
                uow.commit()
                return jsonify({
                    "success": True,
                    "message": "Duplicate mapping removed successfully"
                })
            else:
                return jsonify({
                    "success": False,
                    "message": "No mapping found for the specified file"
                })

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error removing duplicate mapping: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


# TODO: move this logic to service
@bp.route('/create-file-mapping', methods=['POST'])
def create_file_mapping():
    """Create a new file mapping between a file and a Spotify URI."""
    data = request.get_json()
    file_path = data.get('filePath')
    uri = data.get('uri')

    if not file_path or not uri:
        return jsonify({
            "success": False,
            "message": "File path and URI are required"
        }), 400

    try:
        with UnitOfWork() as uow:
            # Verify the URI exists in the tracks table
            track = uow.track_repository.get_by_uri(uri)
            if not track:
                return jsonify({
                    "success": False,
                    "message": "Track URI not found in database"
                })

            # Verify file exists
            if not os.path.exists(file_path):
                return jsonify({
                    "success": False,
                    "message": "File not found"
                })

            # Check if mapping already exists for this file
            existing_uri = uow.file_track_mapping_repository.get_uri_by_file_path(file_path)
            if existing_uri:
                if existing_uri == uri:
                    return jsonify({
                        "success": True,
                        "message": "Mapping already exists"
                    })
                else:
                    # Remove old mapping first
                    uow.file_track_mapping_repository.delete_by_file_path(file_path)

            # Create new mapping
            uow.file_track_mapping_repository.add_mapping_by_uri(file_path, uri)
            uow.commit()

            return jsonify({
                "success": True,
                "message": "File mapping created successfully",
                "track_info": f"{track.artists} - {track.title}"
            })

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error creating file mapping: {e}")
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


@bp.route('/extract-track-ids', methods=['POST'])
def extract_track_ids():
    """Extract TrackIds from a list of file paths."""
    data = request.get_json()
    file_paths = data.get('filePaths', [])

    if not file_paths:
        return jsonify({
            "success": False,
            "message": "No file paths provided"
        }), 400

    try:
        from mutagen.id3 import ID3, ID3NoHeaderError

        track_ids = []
        for file_path in file_paths:
            track_id = None
            try:
                if file_path.lower().endswith('.mp3'):
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' in tags:
                        track_id = tags['TXXX:TRACKID'].text[0]
            except (ID3NoHeaderError, Exception) as e:
                print(f"Error reading TrackId from {file_path}: {e}")

            track_ids.append({
                'file_path': file_path,
                'track_id': track_id
            })

        return jsonify({
            "success": True,
            "track_ids": track_ids
        })

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error extracting track IDs: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/create-extended-versions-playlist', methods=['POST'])
def create_extended_versions_playlist():
    """Create a Spotify playlist from tracks with extended versions."""
    data = request.get_json()
    track_ids = data.get('trackIds', [])
    playlist_name = data.get('playlistName', 'Extended Versions Playlist')
    playlist_description = data.get('playlistDescription', 'Tracks with extended versions available')

    if not track_ids:
        return jsonify({
            "success": False,
            "message": "No track IDs provided"
        }), 400

    try:
        result = validation_service.create_playlist_from_track_ids(
            track_ids, playlist_name, playlist_description
        )
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error creating extended versions playlist: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/playlist-organization', methods=['GET'])
def get_playlist_organization():
    """Get all playlists for organization (excluding forbidden ones)."""
    try:
        # Get exclusion settings and playlists directory from request
        exclusion_settings = request.args.get('exclusionSettings')
        playlists_dir = request.args.get('playlistsDir')
        force_reload = request.args.get('forceReload', 'false').lower() == 'true'  # NEW

        if exclusion_settings:
            exclusion_settings = json.loads(exclusion_settings)
        else:
            exclusion_settings = {}

        result = validation_service.get_playlists_for_organization(exclusion_settings, playlists_dir,
                                                                   force_reload)  # MODIFIED
        return jsonify({
            "success": True,
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error getting playlists for organization: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/playlist-organization/preview', methods=['POST'])
def preview_playlist_organization():
    """Preview what changes will be made to the file system."""
    data = request.get_json()
    playlists_dir = data.get('playlistsDir')
    new_structure = data.get('newStructure')

    if not playlists_dir or not new_structure:
        return jsonify({
            "success": False,
            "message": "Playlists directory and new structure are required"
        }), 400

    try:
        result = validation_service.preview_playlist_reorganization(playlists_dir, new_structure)
        return jsonify({
            "success": True,
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error previewing playlist organization: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/playlist-organization/apply', methods=['POST'])
def apply_playlist_organization():
    """Apply the new playlist organization to the file system."""
    data = request.get_json()
    playlists_dir = data.get('playlistsDir')
    master_tracks_dir = data.get('masterTracksDir') or current_app.config['MASTER_TRACKS_DIRECTORY_SSD']
    new_structure = data.get('newStructure')
    create_backup = data.get('createBackup', True)

    if not playlists_dir or not new_structure:
        return jsonify({
            "success": False,
            "message": "Playlists directory and new structure are required"
        }), 400

    try:
        result = validation_service.apply_playlist_reorganization(
            playlists_dir, master_tracks_dir, new_structure, create_backup
        )
        return jsonify({
            "success": True,
            **result
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error applying playlist organization: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@bp.route('/cleanup-orphaned-playlists', methods=['POST'])
def cleanup_orphaned_playlists():
    """Clean up orphaned M3U files that don't correspond to database playlists."""
    data = request.get_json()
    playlists_dir = data.get('playlistsDir')
    dry_run = data.get('dryRun', False)

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified"
        }), 400

    try:
        result = validation_service.cleanup_orphaned_playlists(playlists_dir, dry_run)
        return jsonify(result)
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error cleaning up orphaned playlists: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500
