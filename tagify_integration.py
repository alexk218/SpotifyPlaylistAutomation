#!/usr/bin/env python
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import traceback
import argparse
import Levenshtein
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from mutagen.id3 import ID3, ID3NoHeaderError
from flask import redirect
from mutagen.mp3 import MP3

from drivers.spotify_client import authenticate_spotify, get_playlist_track_ids, fetch_playlists, \
    sync_to_master_playlist, config_path
from helpers.file_helper import embed_track_id
from helpers.sync_helper import analyze_playlists_changes, analyze_tracks_changes, analyze_track_playlist_associations, \
    sync_playlists_to_db, sync_tracks_to_db, sync_track_playlist_associations_to_db
from m3u_to_rekordbox import RekordboxXmlGenerator
from sql.core.unit_of_work import UnitOfWork
from helpers.m3u_helper import build_track_id_mapping, generate_m3u_playlist, find_local_file_path_with_extensions, \
    get_m3u_track_ids
from helpers.m3u_helper import sanitize_filename
from sql.helpers.db_helper import clear_db

# Load environment variables
load_dotenv()

# Define paths
PROJECT_ROOT = Path(__file__).resolve().parent

# Add project root to Python path for imports
sys.path.insert(0, str(PROJECT_ROOT))

from helpers.validation_helper import validate_master_tracks

app = Flask(__name__)
CORS(app, origins=["https://xpui.app.spotify.com", "https://open.spotify.com", "http://localhost:4000", "*"])
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
HELPERS_DIR = PROJECT_ROOT / "helpers"

LOCAL_TRACKS_SERVER_PATH = SCRIPTS_DIR / "local_tracks_server.py"
EMBED_METADATA_SCRIPT = HELPERS_DIR / "file_helper.py"

# Get environment variables
MASTER_TRACKS_DIRECTORY_SSD = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
MASTER_PLAYLIST_ID = os.getenv("MASTER_PLAYLIST_ID")
DEFAULT_PORT = 8765


@app.route('/status')
def get_status():
    return jsonify({
        "status": "running",
        "version": "1.0",
        "env_vars": {
            "MASTER_TRACKS_DIRECTORY_SSD": MASTER_TRACKS_DIRECTORY_SSD,
            "MASTER_PLAYLIST_ID": MASTER_PLAYLIST_ID
        }
    })


@app.route('/api/direct-tracks-compare', methods=['GET'])
def api_direct_tracks_compare():
    """
    Directly compare Spotify tracks with local tracks from the database.
    Returns information about missing tracks without requiring a cache file.
    """
    try:
        # Get master playlist ID (from query param or environment)
        playlist_id = request.args.get('master_playlist_id') or MASTER_PLAYLIST_ID

        if not playlist_id:
            return jsonify({
                "success": False,
                "message": "Master playlist ID not provided"
            }), 400

        # 1. Get all tracks from the master playlist in the database
        with UnitOfWork() as uow:
            master_tracks = uow.track_repository.get_all()

            # Convert to a list of dicts for JSON serialization
            master_tracks_list = []
            for track in master_tracks:
                master_tracks_list.append({
                    'id': track.track_id,
                    'name': track.title,
                    'artists': track.artists,
                    'album': track.album,
                    'added_at': track.added_to_master.isoformat() if track.added_to_master else None
                })

            # 2. Get all local tracks (tracks that have paths associated with them)
            # Create a set of track IDs that are verified to exist locally
            local_track_ids = set()
            local_tracks_info = []

            # Scan the master tracks directory to find which files have TrackIds
            master_tracks_dir = request.args.get('master_tracks_dir') or MASTER_TRACKS_DIRECTORY_SSD

            if not master_tracks_dir or not os.path.exists(master_tracks_dir):
                return jsonify({
                    "success": False,
                    "message": f"Master tracks directory does not exist: {master_tracks_dir}"
                }), 400

            # Scan local files to find which ones have TrackIds embedded
            for root, _, files in os.walk(master_tracks_dir):
                for filename in files:
                    if not filename.lower().endswith('.mp3'):
                        continue

                    file_path = os.path.join(root, filename)

                    # Check if this file has a TrackId
                    try:
                        try:
                            tags = ID3(file_path)
                            if 'TXXX:TRACKID' in tags:
                                track_id = tags['TXXX:TRACKID'].text[0]
                                local_track_ids.add(track_id)
                                local_tracks_info.append({
                                    'path': file_path,
                                    'filename': filename,
                                    'track_id': track_id,
                                    'size': os.path.getsize(file_path),
                                    'modified': os.path.getmtime(file_path)
                                })
                        except ID3NoHeaderError:
                            pass
                    except Exception as e:
                        print(f"Error reading ID3 tags from {file_path}: {e}")

            # 3. Compare to find missing tracks
            missing_tracks = []
            for track in master_tracks_list:
                # Skip tracks without an ID (shouldn't happen but just in case)
                if not track['id']:
                    continue

                # Skip tracks that are local files
                if track['id'].startswith('local_'):
                    continue

                # If track ID is not in local tracks, it's missing
                if track['id'] not in local_track_ids:
                    missing_tracks.append(track)

            # Sort missing tracks by added_at date, newest first
            missing_tracks.sort(
                key=lambda x: x['added_at'] if x['added_at'] else '0',
                reverse=True
            )

            # 4. Return the results
            return jsonify({
                "success": True,
                "database_time": datetime.now().isoformat(),
                "master_tracks": master_tracks_list,
                "local_tracks": {
                    "count": len(local_track_ids),
                    "tracks": local_tracks_info[:100]  # Limit to avoid huge payloads
                },
                "missing_tracks": missing_tracks,
                "music_directory": master_tracks_dir,
                "master_playlist_id": playlist_id
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


@app.route('/api/embed-metadata', methods=['POST'])
def api_embed_metadata():
    master_tracks_dir = request.json.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
    confirmed = request.json.get('confirmed', False)
    user_selections = request.json.get('userSelections', [])
    skipped_files = request.json.get('skippedFiles', [])
    auto_confirm_threshold = request.json.get('auto_confirm_threshold', 0.75)

    if not master_tracks_dir:
        return jsonify({
            "success": False,
            "message": "Master tracks directory not specified in request or environment"
        }), 400

    # If we've received confirmation with user selections, actually embed the metadata
    if confirmed and user_selections:
        try:
            successful_embeds = 0
            failed_embeds = 0
            results = []

            for selection in user_selections:
                file_name = selection.get('fileName')
                track_id = selection.get('trackId')
                confidence = selection.get('confidence')

                # Find the full file path
                file_path = None
                for root, _, files in os.walk(master_tracks_dir):
                    if file_name in files:
                        file_path = os.path.join(root, file_name)
                        break

                if file_path and track_id:
                    # Use the embed_track_id function from file_helper.py
                    success = embed_track_id(file_path, track_id)

                    if success:
                        successful_embeds += 1
                        results.append({
                            "file": file_name,
                            "track_id": track_id,
                            "success": True
                        })
                    else:
                        failed_embeds += 1
                        results.append({
                            "file": file_name,
                            "track_id": track_id,
                            "success": False,
                            "reason": "Failed to write to file"
                        })
                else:
                    failed_embeds += 1
                    results.append({
                        "file": file_name,
                        "track_id": track_id,
                        "success": False,
                        "reason": "File not found or track ID missing"
                    })

            return jsonify({
                "success": True,
                "message": f"Embedded TrackId into {successful_embeds} files. {failed_embeds} files failed.",
                "results": results,
                "successful_embeds": successful_embeds,
                "failed_embeds": failed_embeds,
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

    # If not confirmed, analyze files that need processing
    try:
        # Get list of all files first without making changes
        all_files = []
        total_files = 0
        files_without_id = []
        auto_matched_files = []

        for root, _, files in os.walk(master_tracks_dir):
            for file in files:
                if not file.lower().endswith('.mp3'):
                    continue

                total_files += 1
                file_path = os.path.join(root, file)

                # Check if file has TrackId
                has_id = False
                try:
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' in tags:
                        has_id = True
                except Exception:
                    pass

                if not has_id:
                    # Try to find high confidence exact matches
                    exact_match = None
                    with UnitOfWork() as uow:
                        tracks_db = uow.track_repository.get_all()
                        for track in tracks_db:
                            if track.is_local and (track.title == file or track.title == os.path.splitext(file)[0]):
                                # Found exact match
                                exact_match = {
                                    'fileName': file,
                                    'trackId': track.track_id,
                                    'confidence': 1.0  # 100% confidence
                                }
                                break

                    if exact_match and exact_match['confidence'] >= auto_confirm_threshold:
                        # Auto-match this file - embed the TrackId immediately
                        success = embed_track_id(file_path, exact_match['trackId'])
                        if success:
                            auto_matched_files.append(exact_match)
                        else:
                            # If embedding failed, add to manual process list
                            files_without_id.append(file)
                    else:
                        # No high confidence match, add to manual process list
                        files_without_id.append(file)

        # Return analysis with files that need processing
        return jsonify({
            "success": True,
            "message": f"Found {len(files_without_id)} files without TrackId out of {total_files} total files. Auto-matched {len(auto_matched_files)} files.",
            "needs_confirmation": len(files_without_id) > 0,
            "requires_fuzzy_matching": len(files_without_id) > 0,
            "details": {
                "files_to_process": files_without_id,
                "total_files": total_files,
                "auto_matched_files": auto_matched_files
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


@app.route('/api/fuzzy-match-track', methods=['POST'])
def api_fuzzy_match_track():
    try:
        file_name = request.json.get('fileName')
        master_tracks_dir = request.json.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
        current_track_id = request.json.get('currentTrackId')

        print(f"Received fuzzy match request for file: {file_name}")
        print(f"Using master tracks dir: {master_tracks_dir}")

        if not file_name:
            return jsonify({"success": False, "message": "No file name provided"}), 400

        # Load tracks from database for matching
        with UnitOfWork() as uow:
            try:
                tracks_db = uow.track_repository.get_all()
                print(f"Successfully loaded {len(tracks_db)} tracks from database")
            except Exception as e:
                print(f"Database error: {e}")
                return jsonify({"success": False, "message": f"Database error: {str(e)}"}), 500

        # DIRECT MATCH FOR LOCAL FILES - First try an exact file name match
        exact_file_matches = []
        for track in tracks_db:
            if track.is_local:
                # Compare the file name directly
                if track.title == file_name or track.title == os.path.splitext(file_name)[0]:
                    # Exact file name match - give this highest priority
                    exact_file_matches.append({
                        'track_id': track.track_id,
                        'ratio': 1.0,  # Perfect match
                        'artist': track.artists or "",
                        'title': track.title,
                        'album': track.album or "Local Files",
                        'is_local': True,
                        'match_type': 'exact_file'
                    })
                    print(f"Found EXACT file name match: {track.title}")

        # If we found exact matches, return them immediately
        if exact_file_matches:
            print(f"Returning {len(exact_file_matches)} exact file matches for {file_name}")
            return jsonify({
                "success": True,
                "file_name": file_name,
                "original_artist": "",
                "original_title": os.path.splitext(file_name)[0],
                "matches": exact_file_matches
            })

        # Extract artist and title from filename
        try:
            name_part = os.path.splitext(file_name)[0]

            # Try standard "Artist - Title" format first
            if " - " in name_part:
                artist, track_title = name_part.split(" - ", 1)
            else:
                # Handle files without the separator
                artist = ""
                track_title = name_part
                print(f"NOTE: '{file_name}' doesn't follow 'Artist - Title' format. Will try to match by title only.")
        except ValueError:
            print(f"Filename format issue: {file_name}")
            artist = ""
            track_title = name_part

        # Normalize for comparison
        normalized_artist = artist.lower().replace('&', 'and')
        normalized_title = track_title.lower()

        # Original artist and title for display
        original_artist = artist
        original_title = track_title

        # Handle remix information
        remix_info = ""
        if "remix" in normalized_title.lower():
            remix_parts = normalized_title.lower().split("remix")
            normalized_title = remix_parts[0].strip()
            remix_info = "remix" + remix_parts[1] if len(remix_parts) > 1 else "remix"

        # Calculate matches
        matches = []
        print(f"Calculating fuzzy matches for '{file_name}'")

        # IMPORTANT: First, check for matching local files - Add this block
        local_tracks = [track for track in tracks_db if track.is_local]
        print(f"Found {len(local_tracks)} local tracks in database")

        # Try to match against local files first
        for track in local_tracks:
            # For local files, just compare titles since they might not have proper artist-title format
            db_title = track.title.lower()

            # Normalize both titles
            clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
            db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

            # Calculate similarity
            similarity = Levenshtein.ratio(clean_normalized_title, db_title_clean)

            # Add high similarity boost for exact file name match
            if clean_normalized_title == db_title_clean or file_name.lower() == track.title.lower():
                similarity = 1.0

            # Include if reasonable match
            if similarity >= 0.5:
                matches.append({
                    'track_id': track.track_id,
                    'ratio': similarity,
                    'artist': track.artists or "Local File",
                    'title': track.title,
                    'album': track.album or "Local Files",
                    'is_local': True
                })

        # Then process regular Spotify tracks
        for track in tracks_db:
            # Skip local files as we already processed them
            if track.is_local:
                continue

            # Process artists and titles - rest of your existing matching logic
            db_artists = track.artists.lower().replace('&', 'and')
            db_title = track.title.lower()
            track_id = track.track_id

            # Split artists and normalize
            db_artist_list = [a.strip() for a in db_artists.split(',')]
            expanded_artists = []
            for db_artist in db_artist_list:
                if ' and ' in db_artist:
                    expanded_artists.extend([a.strip() for a in db_artist.split(' and ')])
                else:
                    expanded_artists.append(db_artist)

            # Artist match calculation
            artist_ratios = []
            if artist:  # Only do artist matching if we have an artist name
                artist_ratios = [Levenshtein.ratio(normalized_artist, db_artist) for db_artist in expanded_artists]
                artist_ratio = max(artist_ratios) if artist_ratios else 0

                # Perfect match bonus
                if any(normalized_artist == db_artist for db_artist in expanded_artists):
                    artist_ratio = 1.0
            else:
                artist_ratio = 0.0

            # Clean titles for better matching
            clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
            db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

            # Create title variations for matching
            title_variations = [
                db_title,
                db_title_clean,
                db_title.replace(' - ', ' ').replace("'s", "s")
            ]

            # Find best title match
            title_ratios = [Levenshtein.ratio(clean_normalized_title, var) for var in title_variations]
            title_ratio = max(title_ratios)

            # Perfect match bonus
            if clean_normalized_title in [var.lower() for var in title_variations]:
                title_ratio = 1.0

            # Remix bonus
            remix_bonus = 0
            if any(x in track_title.lower() for x in ['remix', 'edit', 'mix', 'version']) and \
                    any(x in db_title for x in ['remix', 'edit', 'mix', 'version']):
                remix_bonus = 0.1

                # Extra bonus for same remixer
                remix_pattern = r'\(([^)]+)(remix|edit|version|mix)\)'
                local_remix_match = re.search(remix_pattern, track_title.lower())
                db_remix_match = re.search(remix_pattern, db_title.lower())

                if local_remix_match and db_remix_match and local_remix_match.group(1) == db_remix_match.group(1):
                    remix_bonus += 0.1

            # Calculate overall match score
            if artist:
                overall_ratio = (artist_ratio * 0.6 + title_ratio * 0.3 + remix_bonus)
            else:
                overall_ratio = (title_ratio * 0.9 + remix_bonus)

            # Return if over threshold
            if overall_ratio >= 0.45:  # Lower threshold for showing more options
                matches.append({
                    'track_id': track_id,
                    'ratio': overall_ratio,
                    'artist': track.artists,
                    'title': track.title,
                    'album': track.album,
                    'is_local': False
                })

        # Filter out the current track ID from matches to avoid suggesting the same ID
        if current_track_id:
            matches = [match for match in matches if match['track_id'] != current_track_id]

        # Sort by match quality
        matches.sort(key=lambda x: x['ratio'], reverse=True)

        # Limit to top 8 matches
        top_matches = matches[:8]

        print(f"Found {len(top_matches)} potential matches for '{file_name}'")

        # Return the original file info and potential matches
        print(f"Returning {len(top_matches)} matches for {file_name}")
        return jsonify({
            "success": True,
            "file_name": file_name,
            "original_artist": original_artist,
            "original_title": original_title,
            "matches": top_matches
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error in fuzzy match: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@app.route('/api/generate-m3u', methods=['POST'])
def api_generate_m3u():
    data = request.json
    master_tracks_dir = data.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
    playlists_dir = data.get('playlistsDir')
    extended = data.get('extended', True)
    overwrite = data.get('overwrite', True)
    confirmed = data.get('confirmed', False)
    # Add this to receive the list of playlists needing updates
    playlists_to_update = data.get('playlists_to_update', [])

    if not master_tracks_dir:
        return jsonify({
            "success": False,
            "message": "Master tracks directory not specified in request or environment"
        }), 400

    if not playlists_dir:
        return jsonify({
            "success": False,
            "message": "Playlists directory not specified in request"
        }), 400

    try:
        # First, analyze what will be generated
        with UnitOfWork() as uow:
            db_playlists = uow.playlist_repository.get_all()

        # Filter out the MASTER playlist if it exists
        db_playlists = [p for p in db_playlists if p.name.upper() != "MASTER"]

        # Get track counts for each playlist for display
        playlist_stats = []
        for playlist in db_playlists:
            with UnitOfWork() as uow:
                tracks = uow.track_playlist_repository.get_track_ids_for_playlist(playlist.playlist_id)
                track_count = len(tracks)
                playlist_stats.append({
                    'name': playlist.name,
                    'track_count': track_count,
                    'id': playlist.playlist_id
                })

        # Sort playlists by track count (descending)
        playlist_stats.sort(key=lambda x: x['track_count'], reverse=True)

        # If confirmed parameter is true, actually generate the M3U files
        if confirmed:

            # Build track ID mapping for efficiency
            track_id_map = build_track_id_mapping(master_tracks_dir)

            # Build a comprehensive map of all M3U files with multiple keys for robust matching
            m3u_files_map = {}
            for root, dirs, files in os.walk(playlists_dir):
                for file in files:
                    if file.lower().endswith('.m3u'):
                        file_path = os.path.join(root, file)
                        file_name = os.path.splitext(file)[0]

                        # Map original name
                        m3u_files_map[file_name] = os.path.relpath(root, playlists_dir)

                        # Map lowercase name
                        m3u_files_map[file_name.lower()] = os.path.relpath(root, playlists_dir)

                        # Map sanitized name
                        sanitized_name = sanitize_filename(file_name, preserve_spaces=True)
                        m3u_files_map[sanitized_name] = os.path.relpath(root, playlists_dir)
                        m3u_files_map[sanitized_name.lower()] = os.path.relpath(root, playlists_dir)

            # Convert root paths to empty string
            for key, value in m3u_files_map.items():
                if value == '.' or value == './':
                    m3u_files_map[key] = ''

            # Log the mapping for debugging
            print(f"Found {len(m3u_files_map)} M3U files in various folders")

            # Generate only the playlists that need updating
            success_count = 0
            failed_count = 0
            updated_playlists = []

            for playlist_id in playlists_to_update:
                try:
                    # Get the playlist details to determine correct filename
                    with UnitOfWork() as uow:
                        playlist = uow.playlist_repository.get_by_id(playlist_id)
                        if not playlist:
                            print(f"Playlist ID {playlist_id} not found in database, skipping")
                            continue

                    playlist_name = playlist.name
                    print(f"Processing playlist: '{playlist_name}' (ID: {playlist_id})")

                    # Sanitize the playlist name
                    safe_name = sanitize_filename(playlist_name, preserve_spaces=True)
                    print(f"Sanitized name: '{safe_name}'")

                    # Try various name patterns to find existing location
                    playlist_folder = ''
                    search_patterns = [
                        safe_name,
                        safe_name.lower(),
                        playlist_name,
                        playlist_name.lower()
                    ]

                    # Log search patterns
                    print(f"Searching for patterns: {search_patterns}")

                    # Find existing location using multiple patterns
                    for pattern in search_patterns:
                        if pattern in m3u_files_map:
                            playlist_folder = m3u_files_map[pattern]
                            print(f"Found existing location for pattern '{pattern}': {playlist_folder}")
                            break

                    # If not found with exact patterns, try partial matching
                    if not playlist_folder:
                        for filename, location in m3u_files_map.items():
                            if (safe_name.lower() in filename.lower() or
                                    playlist_name.lower() in filename.lower()):
                                playlist_folder = location
                                print(f"Found location using partial match: {playlist_folder}")
                                break

                    # Determine the M3U path
                    if playlist_folder:
                        # Ensure the subfolder exists
                        folder_path = os.path.join(playlists_dir, playlist_folder)
                        os.makedirs(folder_path, exist_ok=True)
                        m3u_path = os.path.join(folder_path, f"{safe_name}.m3u")
                        print(f"Will generate in existing location: {m3u_path}")
                    else:
                        # New playlist - generate in root directory
                        m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")
                        print(f"New playlist - will generate in root: {m3u_path}")

                    # Ensure the directory exists
                    os.makedirs(os.path.dirname(m3u_path), exist_ok=True)

                    # Generate the M3U file
                    tracks_found, tracks_added = generate_m3u_playlist(
                        playlist_name=playlist.name,
                        playlist_id=playlist_id,
                        master_tracks_dir=master_tracks_dir,
                        m3u_path=m3u_path,
                        extended=extended,
                        overwrite=overwrite,
                        track_id_map=track_id_map
                    )

                    # Verify the file was created
                    if os.path.exists(m3u_path):
                        print(f"Verified playlist was generated at: {m3u_path}")
                    else:
                        print(f"WARNING: Failed to verify playlist generation at: {m3u_path}")

                    success_count += 1
                    updated_playlists.append({
                        'id': playlist_id,
                        'name': playlist.name,
                        'tracks_added': tracks_added,
                        'location': playlist_folder or 'root',
                        'm3u_path': m3u_path
                    })
                except Exception as e:
                    failed_count += 1
                    print(f"Failed to regenerate playlist {playlist_id}: {str(e)}")
                    print(traceback.format_exc())

            return jsonify({
                "success": True,
                "message": f"Successfully regenerated {success_count} M3U playlists. {failed_count} failed.",
                "stats": {
                    "playlists_updated": success_count,
                    "playlists_failed": failed_count,
                    "updated_playlists": updated_playlists,
                    "total_playlists_to_update": len(playlists_to_update)
                }
            })
        else:
            # Just return analysis without making changes
            return jsonify({
                "success": True,
                "message": f"Ready to generate {len(playlist_stats)} M3U playlists.",
                "needs_confirmation": len(playlist_stats) > 0,
                "details": {
                    "playlists": playlist_stats,
                    "total_playlists": len(playlist_stats),
                    "playlists_with_tracks": sum(1 for p in playlist_stats if p['track_count'] > 0)
                }
            })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error analyzing/generating M3U playlists: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@app.route('/api/generate-rekordbox-xml', methods=['POST'])
def api_generate_rekordbox_xml():
    playlists_dir = request.json.get('playlistsDir')
    output_xml_path = request.json.get('rekordboxXmlPath')
    rating_data = request.json.get('ratingData', {})
    master_tracks_dir = request.json.get('masterTracksDir')

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

    if not master_tracks_dir:
        return jsonify({
            "success": False,
            "message": "Master tracks directory not specified"
        }), 400

    try:
        generator = RekordboxXmlGenerator(playlists_dir, master_tracks_dir, rating_data)
        total_tracks, total_playlists, total_rated = generator.generate(output_xml_path)

        return jsonify({
            "success": True,
            "message": f"Successfully generated rekordbox XML with {total_tracks} tracks and {total_playlists} playlists. Applied ratings to {total_rated} tracks."
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


@app.route('/api/validate-tracks', methods=['POST'])
def api_validate_tracks():
    master_tracks_dir = request.json.get('masterTracksDir')

    try:
        result = validate_master_tracks(master_tracks_dir)
        return jsonify({"success": True, "stats": result})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/sync-to-master', methods=['POST'])
def api_sync_to_master():
    try:
        # Get the master playlist ID
        master_playlist_id = request.json.get('master_playlist_id')
        if not master_playlist_id:
            master_playlist_id = MASTER_PLAYLIST_ID

        if not master_playlist_id:
            return jsonify({
                "success": False,
                "message": "Master playlist ID not provided or found in environment"
            }), 400

        exclusion_config = get_exclusion_config(request.json)

        spotify_client = authenticate_spotify()

        response = {
            "success": True,
            "message": "Sync to master playlist started. This operation runs in the background and may take several minutes."
        }

        def background_sync():
            try:
                sync_to_master_playlist(spotify_client, master_playlist_id, exclusion_config)
            except Exception as e:
                error_str = traceback.format_exc()
                print(f"Error in background sync: {e}")
                print(error_str)

        thread = threading.Thread(target=background_sync)
        thread.daemon = True
        thread.start()

        return jsonify(response)

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error starting sync: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}"
        }), 500


@app.route('/api/analyze-playlists', methods=['POST'])
def api_analyze_playlists_changes():
    try:
        force_refresh = request.json.get('force_refresh', False)

        exclusion_config = get_exclusion_config(request.json)
        print(f"Using exclusion config in analyze-playlists: {exclusion_config}")

        # Get analysis without executing
        added_count, updated_count, unchanged_count, deleted_count, changes_details = analyze_playlists_changes(
            force_full_refresh=force_refresh, exclusion_config=exclusion_config
        )

        return jsonify({
            "success": True,
            "message": f"Analysis complete: {added_count} to add, {updated_count} to update, {deleted_count} to delete, {unchanged_count} unchanged",
            "stats": {
                "added": added_count,
                "updated": updated_count,
                "unchanged": unchanged_count,
                "deleted": deleted_count
            },
            "details": {
                "to_add": changes_details['to_add'],
                "to_update": changes_details['to_update'],
                "to_delete": changes_details['to_delete']
            },
            "needs_confirmation": added_count > 0 or updated_count > 0 or deleted_count > 0
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error analyzing playlists: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}",
            "traceback": error_str
        }), 500


@app.route('/api/analyze-tracks', methods=['POST'])
def api_analyze_tracks():
    try:
        master_playlist_id = request.json.get('master_playlist_id') or MASTER_PLAYLIST_ID
        force_refresh = request.json.get('force_refresh', False)

        exclusion_config = get_exclusion_config(request.json)
        print(f"Using exclusion config in analyze-tracks: {exclusion_config}")

        # Get analysis without executing
        tracks_to_add, tracks_to_update, unchanged_tracks = analyze_tracks_changes(
            master_playlist_id,
            force_full_refresh=force_refresh
        )

        # Include ALL tracks for display
        all_tracks_to_add = []
        for track in tracks_to_add:
            all_tracks_to_add.append({
                "id": track.get('id'),
                "artists": track['artists'],
                "title": track['title'],
                "album": track.get('album', 'Unknown Album'),
                "is_local": track.get('is_local', False),
                "added_at": track.get('added_at')
            })

        all_tracks_to_update = []
        for track in tracks_to_update:
            all_tracks_to_update.append({
                "id": track.get('id'),
                "old_artists": track['old_artists'],
                "old_title": track['old_title'],
                "old_album": track.get('old_album', 'Unknown Album'),
                "artists": track['artists'],
                "title": track['title'],
                "album": track.get('album', 'Unknown Album'),
                "is_local": track.get('is_local', False)
            })

        return jsonify({
            "success": True,
            "message": f"Analysis complete: {len(tracks_to_add)} to add, {len(tracks_to_update)} to update, {len(unchanged_tracks)} unchanged",
            "stats": {
                "added": len(tracks_to_add),
                "updated": len(tracks_to_update),
                "unchanged": len(unchanged_tracks)
            },
            "details": {
                "all_items_to_add": all_tracks_to_add,  # Full list for pagination
                "to_add": all_tracks_to_add[:20],  # First 20 for immediate display
                "to_add_total": len(tracks_to_add),
                "all_items_to_update": all_tracks_to_update,  # Full list for pagination
                "to_update": all_tracks_to_update[:20],  # First 20 for immediate display
                "to_update_total": len(tracks_to_update)
            },
            "needs_confirmation": len(tracks_to_add) > 0 or len(tracks_to_update) > 0
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error analyzing tracks: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}",
            "traceback": error_str
        }), 500


@app.route('/api/analyze-associations', methods=['POST'])
def api_analyze_associations():
    try:
        master_playlist_id = request.json.get('master_playlist_id') or MASTER_PLAYLIST_ID
        force_refresh = request.json.get('force_refresh', False)

        exclusion_config = get_exclusion_config(request.json)
        print(f"Using exclusion config in analyze-associations: {exclusion_config}")

        # Get the analysis results
        changes = analyze_track_playlist_associations(
            master_playlist_id,
            force_full_refresh=force_refresh,
            exclusion_config=exclusion_config
        )

        # Make sure we include all the data needed for the UI
        formatted_changes = {
            "tracks_with_changes": changes['tracks_with_changes'],
            "associations_to_add": changes['associations_to_add'],
            "associations_to_remove": changes['associations_to_remove'],
            "samples": changes['samples'],
            "all_changes": changes.get('all_changes', changes['samples'])  # Use all_changes if available
        }

        return jsonify({
            "success": True,
            "message": f"Analysis complete: {changes['associations_to_add']} to add, {changes['associations_to_remove']} to remove, affecting {len(changes['tracks_with_changes'])} tracks",
            "stats": changes['stats'],
            "details": formatted_changes,
            "needs_confirmation": changes['associations_to_add'] > 0 or changes['associations_to_remove'] > 0
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error analyzing associations: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}",
            "traceback": error_str
        }), 500


@app.route('/api/sync-database', methods=['POST'])
def api_sync_database():
    data = request.json
    action = data.get('action', 'all')
    force_refresh = data.get('force_refresh', False)
    is_confirmed = data.get('confirmed', False)

    exclusion_config = get_exclusion_config(request.json)
    print(f"Using exclusion config in sync-database: {exclusion_config}")

    try:
        if action == 'clear':
            clear_db()
            return jsonify({"success": True, "message": "Database cleared successfully"})

        elif action == 'playlists':
            # If not confirmed, return the analysis result instead
            if not is_confirmed:
                # Call analyze endpoint and return that result
                return redirect('/api/analyze-playlists', code=307)  # 307 preserves POST method

            # Otherwise, proceed with execution
            playlist_changes_from_analysis = data.get('precomputed_changes_from_analysis')
            added, updated, unchanged, deleted = sync_playlists_to_db(
                force_full_refresh=force_refresh,
                auto_confirm=True,
                precomputed_changes=playlist_changes_from_analysis,
                exclusion_config=exclusion_config
            )
            return jsonify({
                "success": True,
                "message": f"Playlists synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
                "stats": {
                    "added": added,
                    "updated": updated,
                    "unchanged": unchanged,
                    "deleted": deleted
                }
            })

        elif action == 'tracks':
            # If not confirmed, return the analysis result instead
            if not is_confirmed:
                # Call analyze endpoint and return that result
                return redirect('/api/analyze-tracks', code=307)

            # Otherwise, proceed with execution
            master_playlist_id = data.get('master_playlist_id') or MASTER_PLAYLIST_ID
            track_changes_from_analysis = data.get('precomputed_changes_from_analysis')
            if track_changes_from_analysis:
                if 'all_items_to_add' in track_changes_from_analysis:
                    for track in track_changes_from_analysis['all_items_to_add']:
                        # Ensure all required fields exist
                        if 'album' not in track:
                            track['album'] = 'Unknown Album'
                        if 'added_at' not in track:
                            track['added_at'] = None

                if 'all_items_to_update' in track_changes_from_analysis:
                    for track in track_changes_from_analysis['all_items_to_update']:
                        if 'album' not in track:
                            track['album'] = 'Unknown Album'
                        if 'old_album' not in track:
                            track['old_album'] = 'Unknown Album'
            added, updated, unchanged, deleted = sync_tracks_to_db(
                master_playlist_id,
                force_full_refresh=force_refresh,
                auto_confirm=True,
                precomputed_changes=track_changes_from_analysis
            )
            return jsonify({
                "success": True,
                "message": f"Tracks synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
                "stats": {
                    "added": added,
                    "updated": updated,
                    "unchanged": unchanged,
                    "deleted": deleted
                }
            })

        elif action == 'associations':
            # If not confirmed, return the analysis result instead
            if not is_confirmed:
                # Call analyze endpoint and return that result
                return redirect('/api/analyze-associations', code=307)

            # Otherwise, proceed with execution
            master_playlist_id = data.get('master_playlist_id') or MASTER_PLAYLIST_ID
            associations_changes_from_analysis = data.get('precomputed_changes_from_analysis')
            stats = sync_track_playlist_associations_to_db(
                master_playlist_id,
                force_full_refresh=force_refresh,
                auto_confirm=True,
                precomputed_changes=associations_changes_from_analysis,
                exclusion_config=exclusion_config
            )
            return jsonify({
                "success": True,
                "message": f"Associations synced: {stats['associations_added']} added, {stats['associations_removed']} removed",
                "stats": stats
            })

        elif action == 'all':
            # For 'all', process sequentially, handling one stage at a time
            stage = data.get('stage', 'start')  # 'start', 'playlists', 'tracks', 'associations', 'complete'

            if stage == 'start':
                # Just return initial instructions to begin with playlists
                return jsonify({
                    "success": True,
                    "action": "all",
                    "stage": "start",
                    "next_stage": "playlists",
                    "message": "Starting sequential sync process..."
                })

            elif stage == 'playlists':
                # Handle playlist stage
                if not is_confirmed:
                    # Analyze playlists
                    playlists_added, playlists_updated, playlists_unchanged, playlists_deleted, playlists_details = analyze_playlists_changes(
                        force_full_refresh=force_refresh, exclusion_config=exclusion_config
                    )

                    return jsonify({
                        "success": True,
                        "action": "all",
                        "stage": "playlists",
                        "message": f"Analysis complete: {playlists_added} to add, {playlists_updated} to update, "
                                   f"{playlists_deleted} to delete, {playlists_unchanged} unchanged",
                        "stats": {
                            "added": playlists_added,
                            "updated": playlists_updated,
                            "unchanged": playlists_unchanged,
                            "deleted": playlists_deleted
                        },
                        "details": playlists_details,
                        "next_stage": "tracks",
                        "needs_confirmation": playlists_added > 0 or playlists_updated > 0 or playlists_deleted > 0
                    })
                else:
                    # Sync playlists
                    precomputed_changes_raw = data.get('precomputed_changes_from_analysis')
                    precomputed_changes = precomputed_changes_raw

                    # Add snapshot_id to to_add items if missing
                    if precomputed_changes and 'to_add' in precomputed_changes:
                        for playlist in precomputed_changes['to_add']:
                            if 'snapshot_id' not in playlist:
                                # Set a default value or fetch it if available
                                playlist['snapshot_id'] = playlist.get('snapshot_id', '')

                    # Add snapshot_id to to_update items if missing
                    if precomputed_changes and 'to_update' in precomputed_changes:
                        for playlist in precomputed_changes['to_update']:
                            if 'snapshot_id' not in playlist:
                                # Set a default value or fetch it if available
                                playlist['snapshot_id'] = playlist.get('old_snapshot_id', '')

                    added, updated, unchanged, deleted = sync_playlists_to_db(
                        force_full_refresh=force_refresh,
                        auto_confirm=True,
                        precomputed_changes=precomputed_changes,
                        exclusion_config=exclusion_config
                    )

                    return jsonify({
                        "success": True,
                        "action": "all",
                        "stage": "playlists",
                        "step": "sync_complete",
                        "message": f"Playlists synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
                        "stats": {
                            "added": added,
                            "updated": updated,
                            "unchanged": unchanged,
                            "deleted": deleted
                        },
                        "next_stage": "tracks"
                    })

            elif stage == 'tracks':
                # Handle tracks stage
                master_playlist_id = data.get('master_playlist_id') or MASTER_PLAYLIST_ID

                if not is_confirmed:
                    # Analyze tracks
                    tracks_to_add, tracks_to_update, tracks_unchanged = analyze_tracks_changes(
                        master_playlist_id, force_full_refresh=force_refresh
                    )

                    # Format tracks for display - ALL tracks, not just samples
                    all_tracks_to_add = []
                    for track in tracks_to_add:
                        all_tracks_to_add.append({
                            "id": track.get('id'),
                            "artists": track['artists'],
                            "title": track['title'],
                            "album": track.get('album', 'Unknown Album'),
                            "is_local": track.get('is_local', False),
                            "added_at": track.get('added_at')
                        })

                    all_tracks_to_update = []
                    for track in tracks_to_update:
                        all_tracks_to_update.append({
                            "id": track.get('id'),
                            "old_artists": track['old_artists'],
                            "old_title": track['old_title'],
                            "old_album": track.get('old_album', 'Unknown Album'),
                            "artists": track['artists'],
                            "title": track['title'],
                            "album": track.get('album', 'Unknown Album'),
                            "is_local": track.get('is_local', False)
                        })

                    return jsonify({
                        "success": True,
                        "action": "all",
                        "stage": "tracks",
                        "message": f"Analysis complete: {len(tracks_to_add)} to add, {len(tracks_to_update)} to update, "
                                   f"{len(tracks_unchanged)} unchanged",
                        "stats": {
                            "added": len(tracks_to_add),
                            "updated": len(tracks_to_update),
                            "unchanged": len(tracks_unchanged)
                        },
                        "details": {
                            "all_items_to_add": all_tracks_to_add,
                            "to_add": all_tracks_to_add[:20],  # First 20 for immediate display
                            "to_add_total": len(tracks_to_add),
                            "all_items_to_update": all_tracks_to_update,
                            "to_update": all_tracks_to_update[:20],
                            "to_update_total": len(tracks_to_update)
                        },
                        "next_stage": "associations",
                        "needs_confirmation": len(tracks_to_add) > 0 or len(tracks_to_update) > 0
                    })
                else:
                    # Sync tracks
                    precomputed_changes = data.get('precomputed_changes_from_analysis')
                    added, updated, unchanged, deleted = sync_tracks_to_db(
                        master_playlist_id,
                        force_full_refresh=force_refresh,
                        auto_confirm=True,
                        precomputed_changes=precomputed_changes
                    )

                    return jsonify({
                        "success": True,
                        "action": "all",
                        "stage": "tracks",
                        "step": "sync_complete",
                        "message": f"Tracks synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
                        "stats": {
                            "added": added,
                            "updated": updated,
                            "unchanged": unchanged,
                            "deleted": deleted
                        },
                        "next_stage": "associations"
                    })

            elif stage == 'associations':
                # Handle associations stage
                master_playlist_id = data.get('master_playlist_id') or MASTER_PLAYLIST_ID

                if not is_confirmed:
                    # Analyze associations
                    associations_changes = analyze_track_playlist_associations(
                        master_playlist_id,
                        force_full_refresh=force_refresh,
                        exclusion_config=exclusion_config
                    )

                    return jsonify({
                        "success": True,
                        "action": "all",
                        "stage": "associations",
                        "message": f"Analysis complete: {associations_changes['associations_to_add']} to add, "
                                   f"{associations_changes['associations_to_remove']} to remove, "
                                   f"affecting {len(associations_changes['tracks_with_changes'])} tracks",
                        "stats": associations_changes['stats'],
                        "details": {
                            "tracks_with_changes": associations_changes['tracks_with_changes'],
                            "associations_to_add": associations_changes['associations_to_add'],
                            "associations_to_remove": associations_changes['associations_to_remove'],
                            "samples": associations_changes['samples'],
                            "all_changes": associations_changes.get("tracks_with_changes", [])  # Full list of changes
                        },
                        "next_stage": "complete",
                        "needs_confirmation": associations_changes['associations_to_add'] > 0 or
                                              associations_changes['associations_to_remove'] > 0
                    })
                else:
                    # Sync associations
                    precomputed_changes = data.get('precomputed_changes_from_analysis')
                    stats = sync_track_playlist_associations_to_db(
                        master_playlist_id,
                        force_full_refresh=force_refresh,
                        auto_confirm=True,
                        precomputed_changes=precomputed_changes,
                        exclusion_config=exclusion_config
                    )

                    return jsonify({
                        "success": True,
                        "action": "all",
                        "stage": "associations",
                        "step": "sync_complete",
                        "message": f"Associations synced: {stats['associations_added']} added, {stats['associations_removed']} removed",
                        "stats": stats,
                        "next_stage": "complete"
                    })

            elif stage == 'complete':
                # Final completion stage
                return jsonify({
                    "success": True,
                    "action": "all",
                    "stage": "complete",
                    "message": "Sequential database sync completed successfully",
                })

            else:
                # Handle unknown stage
                return jsonify({
                    "success": False,
                    "message": f"Unknown stage: {stage}",
                }), 400

        else:
            return jsonify({"success": False, "message": "Invalid action"}), 400


    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error in sync_database: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}",
            "traceback": error_str
        }), 500


@app.route('/api/validate-track-metadata', methods=['POST'])
def api_validate_track_metadata():
    try:
        # Get parameters from request
        master_tracks_dir = request.json.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
        confidence_threshold = request.json.get('confidence_threshold', 0.75)

        if not master_tracks_dir:
            return jsonify({
                "success": False,
                "message": "Master tracks directory not specified"
            }), 400

        # Track statistics
        total_files = 0
        files_with_track_id = 0
        files_without_track_id = 0
        potential_mismatches = []
        duplicate_track_ids = {}
        files_missing_trackid = []

        # Get all tracks from database for comparison
        with UnitOfWork() as uow:
            db_tracks = uow.track_repository.get_all()
            db_tracks_by_id = {track.track_id: track for track in db_tracks}

        # Create a map of IDs to expected filenames for comparison
        expected_filenames = {}
        for track_id, track in db_tracks_by_id.items():
            artist = track.get_primary_artist()
            title = track.title
            expected_filename = f"{artist} - {title}"
            expected_filenames[track_id] = expected_filename.lower()

        # Scan local files
        for root, _, files in os.walk(master_tracks_dir):
            for file in files:
                if not file.lower().endswith('.mp3'):
                    continue

                total_files += 1
                file_path = os.path.join(root, file)
                filename_no_ext = os.path.splitext(file)[0].lower()

                try:
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' in tags:
                        track_id = tags['TXXX:TRACKID'].text[0]
                        files_with_track_id += 1

                        # Add to duplicate detection
                        if track_id in duplicate_track_ids:
                            duplicate_track_ids[track_id].append(file)
                        else:
                            duplicate_track_ids[track_id] = [file]

                        # Check if track_id exists in database
                        if track_id in db_tracks_by_id:
                            db_track = db_tracks_by_id[track_id]
                            expected_filename = expected_filenames[track_id]

                            # Calculate filename similarity
                            similarity = Levenshtein.ratio(filename_no_ext, expected_filename)

                            # Flag potential mismatches
                            if similarity < confidence_threshold:
                                try:
                                    audio = MP3(file_path)
                                    duration = audio.info.length  # Duration in seconds
                                    duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                                except Exception as e:
                                    duration = 0
                                    duration_formatted = "Unknown"
                                    print(f"Error getting duration for {file_path}: {e}")
                                potential_mismatches.append({
                                    'file': file,
                                    'track_id': track_id,
                                    'embedded_artist_title': f"{db_track.artists} - {db_track.title}",
                                    'filename': filename_no_ext,
                                    'confidence': similarity,
                                    'full_path': file_path,
                                    'duration': duration,
                                    'duration_formatted': duration_formatted
                                })
                        else:
                            try:
                                audio = MP3(file_path)
                                duration = audio.info.length
                                duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                            except Exception as e:
                                duration = 0
                                duration_formatted = "Unknown"
                                print(f"Error getting duration for {file_path}: {e}")
                            # Track ID not found in database
                            potential_mismatches.append({
                                'file': file,
                                'track_id': track_id,
                                'embedded_artist_title': "Unknown (TrackId not in database)",
                                'filename': filename_no_ext,
                                'confidence': 0,
                                'full_path': file_path,
                                'reason': 'track_id_not_in_db',
                                'duration': duration,
                                'duration_formatted': duration_formatted
                            })
                    else:
                        try:
                            audio = MP3(file_path)
                            duration = audio.info.length
                            duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                        except Exception as e:
                            duration = 0
                            duration_formatted = "Unknown"
                            print(f"Error getting duration for {file_path}: {e}")

                        files_without_track_id += 1
                        # Add to list of files missing TrackId
                        files_missing_trackid.append({
                            'file': file,
                            'track_id': None,
                            'embedded_artist_title': "No TrackId",
                            'filename': filename_no_ext,
                            'confidence': 0,
                            'full_path': file_path,
                            'reason': 'missing_track_id',
                            'duration': duration,
                            'duration_formatted': duration_formatted
                        })
                except Exception as e:
                    try:
                        audio = MP3(file_path)
                        duration = audio.info.length
                        duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                    except Exception as e:
                        duration = 0
                        duration_formatted = "Unknown"
                        print(f"Error getting duration for {file_path}: {e}")
                    files_without_track_id += 1
                    # Add to list of files missing TrackId with error message
                    files_missing_trackid.append({
                        'file': file,
                        'track_id': None,
                        'embedded_artist_title': f"Error: {str(e)}",
                        'filename': filename_no_ext,
                        'confidence': 0,
                        'full_path': file_path,
                        'reason': 'error_reading_tags',
                        'duration': duration,
                        'duration_formatted': duration_formatted
                    })

        # Filter duplicate_track_ids to only include actual duplicates
        real_duplicates = {}
        for track_id, files in duplicate_track_ids.items():
            if len(files) > 1:
                # Get track title from database
                if track_id in db_tracks_by_id:
                    track = db_tracks_by_id[track_id]
                    artist = track.get_primary_artist()
                    track_title = f"{artist} - {track.title}"

                # Create detailed file information for each duplicate
                file_details = []
                for file in files:
                    file_path = os.path.join(master_tracks_dir, file)
                    if os.path.exists(file_path):
                        # Get file duration
                        try:
                            audio = MP3(file_path)
                            duration = audio.info.length
                            duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                        except Exception as e:
                            duration = 0
                            duration_formatted = "Unknown"
                            print(f"Error getting duration for {file_path}: {e}")

                        file_details.append({
                            'filename': file,
                            'path': file_path,
                            'duration': duration,
                            'duration_formatted': duration_formatted
                        })

                real_duplicates[track_id] = {
                    'track_title': track_title,
                    'files': file_details
                }

        # Sort potential mismatches by confidence
        potential_mismatches.sort(key=lambda x: x['confidence'])

        return jsonify({
            "success": True,
            "summary": {
                "total_files": total_files,
                "files_with_track_id": files_with_track_id,
                "files_without_track_id": files_without_track_id,
                "potential_mismatches": len(potential_mismatches),
                "duplicate_track_ids": len(real_duplicates)
            },
            "potential_mismatches": potential_mismatches,
            "files_missing_trackid": files_missing_trackid,
            "duplicate_track_ids": real_duplicates
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


@app.route('/api/validate-playlists', methods=['POST'])
def api_validate_playlists_m3u():
    try:
        # Get parameters from request
        master_tracks_dir = request.json.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
        playlists_dir = request.json.get('playlistsDir')

        if not master_tracks_dir:
            return jsonify({
                "success": False,
                "message": "Master tracks directory not specified"
            }), 400

        if not playlists_dir:
            return jsonify({
                "success": False,
                "message": "Playlists directory not specified"
            }), 400

        # Build track ID mapping first for efficiency
        track_id_map = build_track_id_mapping(master_tracks_dir)

        filename_to_db_track_id = {}
        with UnitOfWork() as uow:
            local_tracks = [t for t in uow.track_repository.get_all() if t.is_local]
            for track in local_tracks:
                title = track.title or ''
                # If title exists, use it for matching
                if title:
                    # Add normalized versions of the title for better matching
                    normalized_title = title.lower().replace(' ', '_')
                    filename_to_db_track_id[normalized_title] = track.track_id
                    # Also try without extension
                    basename = os.path.splitext(normalized_title)[0]
                    filename_to_db_track_id[basename] = track.track_id

        print(f"Built filename to database track ID mapping with {len(filename_to_db_track_id)} entries")

        # Get all playlists from database
        with UnitOfWork() as uow:
            db_playlists = uow.playlist_repository.get_all()
            # Filter out the MASTER playlist
            db_playlists = [p for p in db_playlists if p.name.upper() != "MASTER"]  # TODO: create env variable?

        # Find all M3U files in all subdirectories
        m3u_files = {}  # Dict of {sanitized_name: m3u_path}

        for root, dirs, files in os.walk(playlists_dir):
            for file in files:
                if file.lower().endswith('.m3u'):
                    sanitized_name = os.path.splitext(file)[0]
                    m3u_files[sanitized_name] = os.path.join(root, file)

        # Analyze each playlist's integrity
        playlist_analysis = []

        for playlist in db_playlists:
            playlist_name = playlist.name
            playlist_id = playlist.playlist_id

            # Check if this playlist has an M3U file (in any subdirectory)
            safe_name = sanitize_filename(playlist_name)
            m3u_path = m3u_files.get(safe_name)
            playlist_has_m3u_file = m3u_path is not None

            # Get all track-playlist associations from the database
            with UnitOfWork() as uow:
                all_track_ids_in_playlist_db = set(
                    uow.track_playlist_repository.get_track_ids_for_playlist(playlist_id))
                expected_tracks = []

                # Get details for all tracks in the playlist - not just local ones
                for track_id in all_track_ids_in_playlist_db:
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
                        expected_tracks.append({
                            'id': track_id,
                            'title': track.title or '',
                            'artists': track.artists or '',
                            'album': track.album or '',
                            'is_local': track.is_local,
                            'has_local_file': track_id in track_id_map
                        })

            # Track which database IDs actually exist locally
            local_track_files = set()

            # For Spotify tracks (non-local), check the track_id_map
            for track_id in all_track_ids_in_playlist_db:
                if not track_id.startswith('local_'):
                    if track_id in track_id_map:
                        local_track_files.add(track_id)
                else:
                    # For local tracks, we need to find them by name
                    with UnitOfWork() as uow:
                        track = uow.track_repository.get_by_id(track_id)
                        if track:
                            # Get the title and artists, handling NULL values
                            title = track.title or ''
                            artists = track.artists or ''

                            # First, try using both title and artists
                            local_path = None
                            if title and artists:
                                local_path = find_local_file_path_with_extensions(
                                    title, artists, master_tracks_dir,
                                    extensions=['.mp3', '.wav', '.aiff']
                                )

                            # If not found, try with just the title
                            if not local_path and title:
                                local_path = find_local_file_path_with_extensions(
                                    title, '', master_tracks_dir,
                                    extensions=['.mp3', '.wav', '.aiff']
                                )

                            if local_path:
                                local_track_files.add(track_id)

            # Process M3U file if it exists
            m3u_track_ids = set()
            if playlist_has_m3u_file:
                # Get tracks in the M3U file - this will include virtual IDs for WAV/AIFF files
                # We will store virtual IDs separately to avoid double counting
                virtual_track_ids = set()
                actual_track_ids = set()

                # First get all track IDs including virtual ones
                all_ids = get_m3u_track_ids(m3u_path, track_id_map)

                # Separate virtual IDs from actual IDs
                for track_id in all_ids:
                    if track_id.startswith('local_wav_aiff_'):
                        virtual_track_ids.add(track_id)
                    else:
                        actual_track_ids.add(track_id)

                # Start with actual track IDs
                m3u_track_ids = actual_track_ids

                # Look for database matches for WAV/AIFF files
                # But ONLY if they weren't already matched
                wav_aiff_matches_found = set()
                with open(m3u_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        # Skip comment lines and empty lines
                        if line.startswith('#') or not line.strip():
                            continue

                        file_path = os.path.normpath(line.strip())
                        if os.path.exists(file_path):
                            file_ext = os.path.splitext(file_path.lower())[1]
                            if file_ext in ['.wav', '.aiff']:
                                # For WAV/AIFF files, try to match by filename
                                filename = os.path.basename(file_path)
                                basename = os.path.splitext(filename)[0].lower().replace(' ', '_')

                                # Generate the virtual ID to check if it's already counted
                                virtual_id = f"local_wav_aiff_{basename}"

                                # Try to find in our mapping
                                if basename in filename_to_db_track_id:
                                    db_track_id = filename_to_db_track_id[basename]

                                    # If this is the first time we've seen this file, add the database ID
                                    if virtual_id in virtual_track_ids and virtual_id not in wav_aiff_matches_found:
                                        m3u_track_ids.add(db_track_id)
                                        wav_aiff_matches_found.add(virtual_id)

            # These are tracks that should be in the M3U but aren't
            missing_track_ids = local_track_files - m3u_track_ids

            # These are tracks in the M3U that shouldn't be there
            unexpected_track_ids = m3u_track_ids - all_track_ids_in_playlist_db

            # Get details for missing tracks (only those with local files)
            missing_tracks = []
            for track_id in missing_track_ids:
                with UnitOfWork() as uow:
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
                        missing_tracks.append({
                            'id': track_id,
                            'title': track.title,
                            'artists': track.artists,
                            'album': track.album or '',
                            'is_local': track.is_local,
                            'has_local_file': True
                        })

            # Get details for unexpected tracks
            unexpected_tracks = []
            for track_id in unexpected_track_ids:
                with UnitOfWork() as uow:
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
                        unexpected_tracks.append({
                            'id': track_id,
                            'title': track.title,
                            'artists': track.artists,
                            'album': track.album or '',
                            'is_local': track.is_local,
                            'has_local_file': True
                        })

            # Also get tracks that are in the playlist but have no local files
            not_downloaded_tracks = []
            for track in expected_tracks:
                if track['id'] not in local_track_files:
                    not_downloaded_tracks.append(track)

            # Calculate the total discrepancy
            total_discrepancy = len(all_track_ids_in_playlist_db) - len(m3u_track_ids)
            identified_discrepancy = len(missing_tracks) + len(unexpected_tracks) + len(not_downloaded_tracks)
            unidentified_discrepancy = abs(total_discrepancy) - identified_discrepancy

            # The playlist needs an update if there's any discrepancy whatsoever
            needs_update = (len(m3u_track_ids) != len(all_track_ids_in_playlist_db) or  # Total count mismatch
                            len(missing_tracks) > 0 or  # Missing tracks that should be included
                            len(unexpected_tracks) > 0 or  # Unexpected tracks that shouldn't be there
                            not playlist_has_m3u_file)  # Missing M3U file

            m3u_location = ""
            if playlist_has_m3u_file and m3u_path:
                rel_path = os.path.relpath(os.path.dirname(m3u_path), playlists_dir)
                if rel_path == ".":
                    m3u_location = "root"
                else:
                    m3u_location = rel_path

            playlist_analysis.append({
                'name': playlist_name,
                'id': playlist_id,
                'has_m3u': playlist_has_m3u_file,
                'needs_update': needs_update,
                'total_associations': len(all_track_ids_in_playlist_db),
                'tracks_with_local_files': len(local_track_files),
                'm3u_track_count': len(m3u_track_ids),
                'tracks_missing_from_m3u': missing_tracks,
                'unexpected_tracks_in_m3u': unexpected_tracks,
                'total_discrepancy': total_discrepancy,
                'identified_discrepancy': identified_discrepancy,
                'unidentified_discrepancy': unidentified_discrepancy,
                'not_downloaded_tracks': not_downloaded_tracks,
                'location': m3u_location,
            })

        # Count playlists needing updates
        playlists_needing_update = sum(1 for p in playlist_analysis if p['needs_update'])
        missing_m3u_files = sum(1 for p in playlist_analysis if not p['has_m3u'])

        # Sort by issue severity
        playlist_analysis.sort(key=lambda x: (
            not x['has_m3u'],  # Missing M3U playlist file completely
            abs(x['total_discrepancy']),  # Then by total discrepancy
            len(x['tracks_missing_from_m3u']) + len(x['unexpected_tracks_in_m3u']),
            # Then by number of identified issues
            x['name']  # Then alphabetically
        ), reverse=True)

        return jsonify({
            "success": True,
            "summary": {
                "total_playlists": len(playlist_analysis),
                "playlists_needing_update": playlists_needing_update,
                "missing_m3u_files": missing_m3u_files
            },
            "playlist_analysis": playlist_analysis
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


@app.route('/api/correct-track-id', methods=['POST'])
def api_correct_track_id():
    try:
        file_path = request.json.get('file_path')
        new_track_id = request.json.get('new_track_id')

        if not file_path or not new_track_id:
            return jsonify({
                "success": False,
                "message": "Both file_path and new_track_id are required"
            }), 400

        # Check if new track ID exists in database
        with UnitOfWork() as uow:
            track = uow.track_repository.get_by_id(new_track_id)
            if not track:
                return jsonify({
                    "success": False,
                    "message": f"Track ID '{new_track_id}' not found in database"
                }), 400

        # Get existing track ID if any
        old_track_id = None
        try:
            tags = ID3(file_path)
            if 'TXXX:TRACKID' in tags:
                old_track_id = tags['TXXX:TRACKID'].text[0]
        except Exception:
            pass

        success = embed_track_id(file_path, new_track_id)

        if success:
            return jsonify({
                "success": True,
                "message": f"Successfully updated TrackId from '{old_track_id}' to '{new_track_id}'",
                "old_track_id": old_track_id,
                "new_track_id": new_track_id
            })
        else:
            return jsonify({
                "success": False,
                "message": f"Failed to update TrackId in file: {file_path}"
            }), 500
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error correcting track ID: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


@app.route('/api/regenerate-playlist', methods=['POST'])
def api_regenerate_playlist():
    try:
        # Get parameters from request
        master_tracks_dir = request.json.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
        playlists_dir = request.json.get('playlistsDir')
        playlist_id = request.json.get('playlist_id')
        extended = request.json.get('extended', True)
        force = request.json.get('force', True)

        if not master_tracks_dir:
            return jsonify({
                "success": False,
                "message": "Master tracks directory not specified"
            }), 400

        if not playlists_dir:
            return jsonify({
                "success": False,
                "message": "Playlists directory not specified"
            }), 400

        if not playlist_id:
            return jsonify({
                "success": False,
                "message": "Playlist ID not specified"
            }), 400

        # Always force overwrite when regenerating
        overwrite = True

        # Log detailed information for debugging
        print(f"Regenerating playlist {playlist_id} at {playlists_dir}")
        print(f"Master tracks directory: {master_tracks_dir}")

        # Get the playlist name for more useful messages
        playlist_name = None
        with UnitOfWork() as uow:
            playlist = uow.playlist_repository.get_by_id(playlist_id)
            if not playlist:
                return jsonify({
                    "success": False,
                    "message": f"Playlist ID {playlist_id} not found in database"
                }), 404

            playlist_name = playlist.name

        # Sanitize the playlist name for file matching
        safe_name = sanitize_filename(playlist_name, preserve_spaces=True)

        # LOG EVERYTHING FOR DEBUGGING
        print(f"Playlist name: '{playlist_name}'")
        print(f"Sanitized name: '{safe_name}'")

        # IMPROVED SEARCH: Build a case-insensitive lookup for files
        m3u_files_map = {}
        for root, dirs, files in os.walk(playlists_dir):
            for file in files:
                if file.lower().endswith('.m3u'):
                    file_lower = file.lower()
                    name_without_ext = os.path.splitext(file)[0]
                    name_without_ext_lower = name_without_ext.lower()
                    # Map both lowercase filename and sanitized lowercase name to the full path
                    m3u_files_map[file_lower] = os.path.join(root, file)
                    m3u_files_map[name_without_ext_lower + '.m3u'] = os.path.join(root, file)
                    # Also map sanitized version to handle special characters
                    sanitized_lower = sanitize_filename(name_without_ext, preserve_spaces=True).lower() + '.m3u'
                    m3u_files_map[sanitized_lower] = os.path.join(root, file)

        # Try multiple search patterns to find the file
        existing_m3u_path = None
        search_patterns = [
            f"{safe_name}.m3u",
            f"{safe_name.lower()}.m3u",
            f"{playlist_name}.m3u",
            f"{playlist_name.lower()}.m3u"
        ]

        # Log all patterns we're searching for
        print(f"Searching for patterns: {search_patterns}")
        print(f"Available M3U files: {list(m3u_files_map.keys())}")

        for pattern in search_patterns:
            if pattern.lower() in m3u_files_map:
                existing_m3u_path = m3u_files_map[pattern.lower()]
                print(f"Found existing M3U file with pattern '{pattern}' at: {existing_m3u_path}")
                break

        # FALLBACK: If we still can't find it, do a more lenient search
        if not existing_m3u_path:
            # Try a more flexible match (checking if playlist name is contained in any m3u filename)
            for filename, filepath in m3u_files_map.items():
                name_part = os.path.splitext(filename)[0].lower()
                if safe_name.lower() in name_part or playlist_name.lower() in name_part:
                    existing_m3u_path = filepath
                    print(f"Found existing M3U using partial match at: {existing_m3u_path}")
                    break

        # If not found, it's a new playlist - use the root directory
        if not existing_m3u_path:
            existing_m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")
            print(f"No existing file found. Will create at: {existing_m3u_path}")

        # If force is specified and the file exists, remove it
        if force and existing_m3u_path and os.path.exists(existing_m3u_path):
            try:
                os.remove(existing_m3u_path)
                print(f"Forcibly removed existing M3U file: {existing_m3u_path}")
            except Exception as e:
                print(f"Error removing existing M3U file: {e}")

        # Build track ID mapping for efficiency
        track_id_map = build_track_id_mapping(master_tracks_dir)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(existing_m3u_path), exist_ok=True)

        # Regenerate the playlist
        try:
            tracks_found, tracks_added = generate_m3u_playlist(
                playlist_name=playlist_name,
                playlist_id=playlist_id,
                master_tracks_dir=master_tracks_dir,
                m3u_path=existing_m3u_path,
                extended=extended,
                overwrite=overwrite,
                track_id_map=track_id_map
            )

            if os.path.exists(existing_m3u_path):
                print(f"Verified playlist was regenerated at: {existing_m3u_path}")
            else:
                print(f"WARNING: Failed to verify playlist generation at: {existing_m3u_path}")

            # Get location relative to the playlists directory for display
            m3u_location = "root"
            if existing_m3u_path != os.path.join(playlists_dir, f"{safe_name}.m3u"):
                rel_path = os.path.relpath(os.path.dirname(existing_m3u_path), playlists_dir)
                if rel_path != ".":
                    m3u_location = rel_path

            result = {
                'success': True,
                'message': f'Successfully regenerated playlist: {playlist_name} with {tracks_added} tracks',
                'stats': {
                    'playlist_name': playlist_name,
                    'tracks_found': tracks_found,
                    'tracks_added': tracks_added,
                    'm3u_path': existing_m3u_path,
                    'location': m3u_location,
                    'file_size': os.path.getsize(existing_m3u_path) if os.path.exists(existing_m3u_path) else 0
                }
            }
        except Exception as e:
            print(f"Error in generate_m3u_playlist: {e}")
            raise

        # If we're still here, regeneration was successful
        return jsonify({
            "success": True,
            "message": f"Successfully regenerated playlist: {playlist_name or playlist_id}",
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


@app.route('/api/remove-track-id', methods=['POST'])
def api_remove_track_id():
    try:
        file_path = request.json.get('file_path')

        if not file_path:
            return jsonify({
                "success": False,
                "message": "file_path is required"
            }), 400

        if not os.path.exists(file_path):
            return jsonify({
                "success": False,
                "message": f"File not found: {file_path}"
            }), 404

        # Get existing track ID if any for reporting
        old_track_id = None
        try:
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    old_track_id = tags['TXXX:TRACKID'].text[0]
                    # Remove the TrackId
                    tags.delall('TXXX:TRACKID')
                    tags.save(file_path)
                else:
                    return jsonify({
                        "success": False,
                        "message": f"No TrackId found in file: {file_path}"
                    }), 400
            except ID3NoHeaderError:
                return jsonify({
                    "success": False,
                    "message": f"No ID3 tags found in file: {file_path}"
                }), 400
        except Exception as e:
            return jsonify({
                "success": False,
                "message": f"Error removing TrackId: {str(e)}"
            }), 500

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


@app.route('/api/delete-file', methods=['POST'])
def api_delete_file():
    try:
        file_path = request.json.get('file_path')

        if not file_path:
            return jsonify({
                "success": False,
                "message": "file_path is required"
            }), 400

        if not os.path.exists(file_path):
            return jsonify({
                "success": False,
                "message": f"File not found: {file_path}"
            }), 404

        # Delete the file
        os.remove(file_path)

        return jsonify({
            "success": True,
            "message": f"File deleted: {os.path.basename(file_path)}"
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


@app.route('/api/search-tracks', methods=['POST'])
def api_search_tracks():
    try:
        # Get parameters from request
        master_tracks_dir = request.json.get('masterTracksDir') or MASTER_TRACKS_DIRECTORY_SSD
        query = request.json.get('query', '').lower()

        if not master_tracks_dir:
            return jsonify({
                "success": False,
                "message": "Master tracks directory not specified"
            }), 400

        if not query:
            return jsonify({
                "success": True,
                "results": []
            })

        # Results array
        results = []

        # Scan the files in the master directory
        for root, _, files in os.walk(master_tracks_dir):
            for file in files:
                if not file.lower().endswith('.mp3'):
                    continue

                file_path = os.path.join(root, file)
                filename_no_ext = os.path.splitext(file)[0].lower()

                # Check if query is in filename
                if query in filename_no_ext:
                    # Get TrackId if present
                    track_id = None
                    embedded_artist_title = "Unknown"
                    try:
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]

                        # Try to get artist and title from ID3 tags
                        artist = ""
                        title = ""
                        if 'TPE1' in tags:  # Artist
                            artist = str(tags['TPE1'])
                        if 'TIT2' in tags:  # Title
                            title = str(tags['TIT2'])

                        if artist and title:
                            embedded_artist_title = f"{artist} - {title}"
                        else:
                            embedded_artist_title = filename_no_ext
                    except Exception as e:
                        print(f"Error reading ID3 tags from {file_path}: {e}")

                    results.append({
                        'file': file,
                        'track_id': track_id,
                        'embedded_artist_title': embedded_artist_title,
                        'filename': filename_no_ext,
                        'confidence': 1.0 if track_id else 0,
                        'full_path': file_path
                    })

        # Sort results by relevance (tracks with IDs first, then by filename match)
        results.sort(key=lambda x: (x['track_id'] is None, x['file'].lower().find(query)))

        return jsonify({
            "success": True,
            "results": results
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error searching tracks: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


def get_exclusion_config(request_json=None):
    # Default config from file
    default_config = {}
    try:
        with config_path.open('r', encoding='utf-8') as config_file:
            default_config = json.load(config_file)
    except Exception as e:
        print(f"Error loading default config: {e}")
        default_config = {
            "forbidden_playlists": [],
            "forbidden_words": [],
            "description_keywords": []
        }

    # If request contains playlist settings, use those instead
    if request_json and 'playlistSettings' in request_json:
        client_settings = request_json['playlistSettings']

        # Create a new config based on client settings
        config = {
            "forbidden_playlists": [],
            "forbidden_words": [],
            "description_keywords": [],
            "forbidden_playlist_ids": []
        }

        # Map client-side settings to server-side format
        if 'excludedKeywords' in client_settings:
            config['forbidden_words'] = client_settings['excludedKeywords']

        if 'excludedPlaylistIds' in client_settings:
            config['forbidden_playlist_ids'] = client_settings['excludedPlaylistIds']

        if 'excludeByDescription' in client_settings:
            config['description_keywords'] = client_settings['excludeByDescription']

        return config

    # If no client settings, return default
    return default_config


def run_command(command, wait=True):
    """Run a command and optionally wait for it to complete."""
    print(f"Running: {' '.join(str(c) for c in command)}")

    if wait:
        result = subprocess.run(command, check=False)
        return result.returncode
    else:
        # Run in background
        if sys.platform == 'win32':
            # Windows requires shell=True for background processes
            subprocess.Popen(' '.join(str(c) for c in command), shell=True)
        else:
            # Unix/Linux/Mac
            subprocess.Popen(command)
        return 0


def start_local_tracks_server(port=DEFAULT_PORT, cache_path=None):
    """Start the local tracks server."""
    print("\n=== Starting Local Tracks Server ===")

    command = [sys.executable, str(LOCAL_TRACKS_SERVER_PATH), "--port", str(port)]
    if cache_path:
        command.extend(["--cache-path", cache_path])

    # Run server in background
    return run_command(command, wait=False)


def main():
    parser = argparse.ArgumentParser(description="Tagify Integration Tools")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Server command
    server_parser = subparsers.add_parser("server", help="Start local tracks server")
    server_parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                               help="Port to run server on (default 8765)")

    # All-in-one command
    all_parser = subparsers.add_parser("all", help="Run server")
    all_parser.add_argument("--music-dir", type=str, default=MASTER_TRACKS_DIRECTORY_SSD,
                            help="Directory containing music files (default from .env)")
    all_parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                            help="Port to run server on (default 8765)")

    args = parser.parse_args()

    if args.command == "server":
        start_local_tracks_server(args.port, args.cache_path)
        # Keep script running while server is active
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nServer stopped by user")

    elif args.command == "all":
        # Run all processes in sequence
        print("\n=== Running Tagify Integration ===")

        # Start server
        start_local_tracks_server(args.port, args.cache_dir)

        # Keep script running while server is active
        print("\n=== Integration Complete ===")
        print(f"Local tracks server running at http://localhost:{args.port}")
        print("Your Spicetify Tagify app can now access your local tracks data")
        print("Press Ctrl+C to stop the server")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nServer stopped by user")

    else:
        parser.print_help()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Local Tracks Server")
    parser.add_argument("--port", type=int, default=8765, help="Port to run server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to run server on")
    parser.add_argument("--cache-path", type=str, help="Path to the cache file or directory")

    args = parser.parse_args()

    print(f"Starting local tracks server on {args.host}:{args.port}")
    if args.cache_path:
        print(f"Using cache path: {args.cache_path}")

    app.run(host=args.host, port=args.port, debug=True, use_reloader=not os.environ.get('FLASK_DEBUG') == '0')
