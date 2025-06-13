import json
import os
import re
import shutil
from datetime import datetime
from typing import Any, Dict, List

import Levenshtein
from mutagen.mp3 import MP3

from api.constants.file_extensions import SUPPORTED_AUDIO_EXTENSIONS
from helpers.m3u_helper import (
    sanitize_filename,
    generate_m3u_playlist, build_uri_to_file_mapping_from_database, get_m3u_track_uris_from_file,
    get_playlists_track_uris_batch, get_all_tracks_metadata_by_uri
)
from helpers.validation_helper import validate_master_tracks
from sql.core.unit_of_work import UnitOfWork


def validate_file_mappings(master_tracks_dir):
    """
    Validate track file mappings using the FileTrackMappings table.
    """
    # Track statistics
    total_files = 0
    files_with_mapping = 0
    files_without_mapping = 0
    potential_mismatches = []
    files_missing_mapping = []

    # Get all tracks and mappings from database
    with UnitOfWork() as uow:
        db_tracks = uow.track_repository.get_all()
        all_mappings = uow.file_track_mapping_repository.get_all()

        # Create lookup dictionaries
        db_tracks_by_uri = {track.uri: track for track in db_tracks}
        mapping_by_path = {mapping.file_path: mapping for mapping in all_mappings if mapping.is_active}
        mappings_by_uri = {}

        # Group mappings by URI to detect duplicates
        for mapping in all_mappings:
            if mapping.is_active and mapping.uri:
                if mapping.uri not in mappings_by_uri:
                    mappings_by_uri[mapping.uri] = []
                mappings_by_uri[mapping.uri].append(mapping)

    # Create expected filenames for comparison
    expected_filenames = {}
    for uri, track in db_tracks_by_uri.items():
        artist = track.get_primary_artist() if hasattr(track, 'get_primary_artist') else track.artists.split(',')[
            0].strip()
        title = track.title
        expected_filename = f"{artist} - {title}"
        expected_filenames[uri] = expected_filename.lower()

    # Scan local files
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext not in SUPPORTED_AUDIO_EXTENSIONS:
                continue

            total_files += 1
            file_path = os.path.join(root, file)
            filename_no_ext = os.path.splitext(file)[0].lower()

            # Get file duration
            try:
                if file.lower().endswith('.mp3'):
                    audio = MP3(file_path)
                    duration = audio.info.length
                    duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                else:
                    duration = 0
                    duration_formatted = "Unknown"
            except Exception:
                duration = 0
                duration_formatted = "Unknown"

            # Check if file has mapping
            if file_path in mapping_by_path:
                mapping = mapping_by_path[file_path]
                files_with_mapping += 1

                # Check if URI exists in database
                if mapping.uri in db_tracks_by_uri:
                    db_track = db_tracks_by_uri[mapping.uri]
                    expected_filename = expected_filenames[mapping.uri]

                    # Calculate similarity between actual and expected filename
                    similarity = 1 - (Levenshtein.distance(filename_no_ext, expected_filename) /
                                      max(len(filename_no_ext), len(expected_filename)))

                    # Flag potential mismatches (low similarity)
                    if similarity < 0.7:
                        confidence = round(similarity, 1)
                        potential_mismatches.append({
                            'file': file,
                            'uri': mapping.uri,
                            'track_info': f"{db_track.artists} - {db_track.title}",
                            'filename': filename_no_ext,
                            'confidence': confidence,
                            'full_path': file_path,
                            'reason': 'filename_mismatch',
                            'duration': duration,
                            'duration_formatted': duration_formatted
                        })
            else:
                files_without_mapping += 1
                # Add to list of files missing mapping
                files_missing_mapping.append({
                    'file': file,
                    'uri': None,
                    'track_info': "No Mapping",
                    'filename': filename_no_ext,
                    'confidence': 0,
                    'full_path': file_path,
                    'reason': 'missing_mapping',
                    'duration': duration,
                    'duration_formatted': duration_formatted
                })

    # Find duplicate mappings (multiple files mapped to same URI)
    real_duplicates = {}
    for uri, mappings_list in mappings_by_uri.items():
        if len(mappings_list) > 1:
            # Filter out inactive mappings and non-existent files
            active_mappings = [m for m in mappings_list if m.is_active and os.path.exists(m.file_path)]

            if len(active_mappings) > 1:
                # Get track title from database
                track_title = "Unknown"
                if uri in db_tracks_by_uri:
                    track = db_tracks_by_uri[uri]
                    artist = track.get_primary_artist() if hasattr(track, 'get_primary_artist') else \
                        track.artists.split(',')[0].strip()
                    track_title = f"{artist} - {track.title}"

                # Create detailed file information for each duplicate
                file_details = []
                for mapping in active_mappings:
                    try:
                        if mapping.file_path.lower().endswith('.mp3'):
                            audio = MP3(mapping.file_path)
                            duration = audio.info.length
                            duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                        else:
                            duration = 0
                            duration_formatted = "Unknown"
                    except Exception:
                        duration = 0
                        duration_formatted = "Unknown"

                    file_details.append({
                        'filename': os.path.basename(mapping.file_path),
                        'path': mapping.file_path,
                        'duration': duration,
                        'duration_formatted': duration_formatted,
                        'file_size': mapping.file_size or (
                            os.path.getsize(mapping.file_path) if os.path.exists(mapping.file_path) else 0),
                        'last_modified': mapping.last_modified if mapping.last_modified else "Unknown"
                    })

                real_duplicates[uri] = {
                    'track_title': track_title,
                    'files': file_details,
                    'uri': uri
                }
    # Sort potential mismatches by confidence
    potential_mismatches.sort(key=lambda x: x['confidence'])

    return {
        "summary": {
            "total_files": total_files,
            "files_with_track_id": files_with_mapping,
            "files_without_track_id": files_without_mapping,
            "potential_mismatches": len(potential_mismatches),
            "duplicate_track_ids": len(real_duplicates)
        },
        "potential_mismatches": potential_mismatches,
        "files_missing_trackid": files_missing_mapping,
        "duplicate_track_ids": real_duplicates
    }


def validate_playlists_m3u(playlists_dir):
    """
    Validate M3U playlists against db information using URI-based FileTrackMappings system

    Args:
        playlists_dir: Directory containing m3u playlist files

    Returns:
        Dictionary with validation results
    """
    uri_to_file_map = build_uri_to_file_mapping_from_database()

    # Get all playlists from database
    with UnitOfWork() as uow:
        db_playlists = uow.playlist_repository.get_all()
        # Filter out the MASTER playlist
        db_playlists = [p for p in db_playlists if p.name.upper() != "MASTER"]

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

        # Get all track URIs for this playlist from database
        with UnitOfWork() as uow:
            # Use the new URI-based TrackPlaylists table
            track_uris = uow.track_playlist_repository.get_uris_for_playlist(playlist_id)
            all_track_uris_in_playlist_db = set(track_uris) if track_uris else set()

        # Find which tracks have local files available
        local_track_files = set()
        not_downloaded_tracks = []

        for uri in all_track_uris_in_playlist_db:
            if uri in uri_to_file_map:
                # Check if the file actually exists
                file_path = uri_to_file_map[uri]
                if os.path.exists(file_path):
                    local_track_files.add(uri)
                else:
                    # File mapping exists but file is missing
                    not_downloaded_tracks.append({
                        'uri': uri,
                        'expected_path': file_path,
                        'reason': 'file_missing'
                    })
            else:
                # No file mapping exists for this track
                not_downloaded_tracks.append({
                    'uri': uri,
                    'expected_path': None,
                    'reason': 'no_mapping'
                })

        # Process M3U file if it exists
        m3u_track_uris = set()
        if playlist_has_m3u_file:
            m3u_track_uris = get_m3u_track_uris_from_file(m3u_path, uri_to_file_map)

        # Compare database vs M3U to find discrepancies
        tracks_missing_from_m3u = local_track_files - m3u_track_uris
        unexpected_tracks_in_m3u = m3u_track_uris - all_track_uris_in_playlist_db

        # Calculate discrepancy metrics
        total_discrepancy = len(m3u_track_uris) - len(local_track_files)
        identified_discrepancy = len(tracks_missing_from_m3u) + len(unexpected_tracks_in_m3u)
        unidentified_discrepancy = abs(total_discrepancy) - identified_discrepancy

        # A playlist needs update if there are any discrepancies or missing files
        needs_update = (
                not playlist_has_m3u_file or
                bool(tracks_missing_from_m3u) or
                bool(unexpected_tracks_in_m3u) or
                len(m3u_track_uris) != len(local_track_files)
        )

        # Determine relative location of M3U file
        m3u_location = "root"
        if playlist_has_m3u_file:
            rel_path = os.path.relpath(os.path.dirname(m3u_path), playlists_dir)
            if rel_path != ".":
                m3u_location = rel_path

        # Convert sets to lists for JSON serialization
        missing_tracks = [{'uri': uri} for uri in tracks_missing_from_m3u]
        unexpected_tracks = [{'uri': uri} for uri in unexpected_tracks_in_m3u]

        playlist_analysis.append({
            'name': playlist_name,
            'id': playlist_id,
            'has_m3u': playlist_has_m3u_file,
            'needs_update': needs_update,
            'total_associations': len(all_track_uris_in_playlist_db),
            'tracks_with_local_files': len(local_track_files),
            'm3u_track_count': len(m3u_track_uris),
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
        len(x['tracks_missing_from_m3u']) + len(x['unexpected_tracks_in_m3u']),  # Then by number of identified issues
        x['name']  # Then alphabetically
    ), reverse=True)

    return {
        "summary": {
            "total_playlists": len(playlist_analysis),
            "playlists_needing_update": playlists_needing_update,
            "missing_m3u_files": missing_m3u_files
        },
        "playlist_analysis": playlist_analysis
    }


def validate_tracks(master_tracks_dir):
    """
    Validate local tracks against database information.

    Args:
        master_tracks_dir: Directory containing master tracks

    Returns:
        Dictionary with validation results
    """
    result = validate_master_tracks(master_tracks_dir)

    return result


def validate_short_tracks(master_tracks_dir, min_length_minutes=5):
    """
    Validate tracks that are shorter than the minimum length.
    Note: This only scans local files - no external API calls are made.
    """
    import time
    start_time = time.time()

    # Track statistics
    total_files = 0
    short_tracks = []
    min_length_seconds = min_length_minutes * 60
    processed_files = 0

    print(f"\nScanning directory for tracks shorter than {min_length_minutes} minutes...")
    print(f"Directory: {master_tracks_dir}")

    # Count total MP3 files first for progress tracking
    total_mp3_files = 0
    for root, _, files in os.walk(master_tracks_dir):
        total_mp3_files += sum(1 for file in files if file.lower().endswith('.mp3'))

    print(f"Found {total_mp3_files} MP3 files to process...")

    # Scan all MP3 files
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            processed_files += 1
            file_path = os.path.join(root, file)

            # Progress logging every 1000 files
            if processed_files % 1000 == 0:
                print(f"Processed {processed_files}/{total_mp3_files} files...")

            try:
                audio = MP3(file_path)
                if audio is None:
                    continue

                length = audio.info.length

                if length < min_length_seconds:
                    # Extract artist and title from filename
                    filename_no_ext = os.path.splitext(file)[0]

                    # Try to parse "Artist - Title" format
                    if " - " in filename_no_ext:
                        artist, title = filename_no_ext.split(" - ", 1)
                    else:
                        artist = "Unknown Artist"
                        title = filename_no_ext

                    # Get TrackId if present (simplified - don't import inside loop)
                    track_id = None
                    try:
                        from mutagen.id3 import ID3
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]
                    except Exception:
                        pass

                    short_track_info = {
                        'file': file,
                        'full_path': file_path,
                        'artist': artist.strip(),
                        'title': title.strip(),
                        'duration_seconds': length,
                        'duration_formatted': f"{int(length // 60)}:{int(length % 60):02d}",
                        'track_id': track_id,
                        # Note: Extended version search will be done on-demand per track
                        'extended_versions_found': [],
                        'has_longer_versions': False,
                        'discogs_search_completed': False,
                        'search_error': None
                    }

                    short_tracks.append(short_track_info)

            except Exception as e:
                # Don't print every error to avoid spam
                if processed_files % 100 == 0:  # Only log errors occasionally
                    print(f"Error processing {file}: {e}")

    # Sort by duration (shortest first)
    short_tracks.sort(key=lambda x: x['duration_seconds'])

    elapsed_time = time.time() - start_time
    print(f"Scan complete! Found {len(short_tracks)} short tracks out of {total_files} total files")
    print(f"Processing took {elapsed_time:.2f} seconds")

    return {
        "summary": {
            "total_files": total_files,
            "short_tracks": len(short_tracks),
            "min_length_minutes": min_length_minutes,
            "processing_time_seconds": elapsed_time
        },
        "short_tracks": short_tracks[:100]  # Limit initial response to first 100 for performance
    }


def search_extended_versions_for_track(artist, title, current_duration):
    """
    Search for extended versions of a specific track using Discogs.
    """
    try:
        import os
        api_token = os.getenv('DISCOGS_API_TOKEN')

        from helpers.discogs_helper import DiscogsClient

        client = DiscogsClient(api_token=api_token)
        all_versions = client.search_releases(artist, title)

        # Determine search status
        track_found_on_discogs = len(all_versions) > 0

        # Filter for versions longer than current
        extended_versions = []
        for version in all_versions:
            if version['duration_seconds'] > current_duration + 30:  # At least 30 seconds longer
                extended_versions.append(version)

        # Sort by duration (longest first)
        extended_versions.sort(key=lambda x: x['duration_seconds'], reverse=True)

        has_longer_versions = len(extended_versions) > 0

        # Determine status message
        if not track_found_on_discogs:
            status_message = "Track not found on Discogs"
            status_type = "not_found"
        elif has_longer_versions:
            status_message = f"Found {len(extended_versions)} extended version(s)"
            status_type = "extended_found"
        else:
            status_message = f"Track found on Discogs but no extended versions available (found {len(all_versions)} version(s))"
            status_type = "no_extended"

        return {
            "success": True,
            "extended_versions": extended_versions,
            "has_longer_versions": has_longer_versions,
            "track_found_on_discogs": track_found_on_discogs,
            "total_versions_found": len(all_versions),
            "search_completed": True,
            "status_message": status_message,
            "status_type": status_type
        }
    except Exception as e:
        return {
            "success": False,
            "extended_versions": [],
            "has_longer_versions": False,
            "track_found_on_discogs": False,
            "total_versions_found": 0,
            "search_completed": True,
            "status_message": f"Search failed: {str(e)}",
            "status_type": "error",
            "error": str(e)
        }


def create_playlist_from_track_ids(track_ids, playlist_name, playlist_description):
    """
    Create a Spotify playlist from a list of track IDs.

    Args:
        track_ids: List of Spotify track IDs
        playlist_name: Name for the new playlist
        playlist_description: Description for the new playlist

    Returns:
        Dictionary with creation results
    """
    from drivers.spotify_client import authenticate_spotify
    from sql.core.unit_of_work import UnitOfWork

    try:
        # Get Spotify client
        spotify_client = authenticate_spotify()

        # Get current user ID
        user = spotify_client.current_user()
        user_id = user['id']

        # Create the playlist
        playlist = spotify_client.user_playlist_create(
            user=user_id,
            name=playlist_name,
            description=playlist_description,
            public=False  # Create as private playlist
        )

        playlist_id = playlist['id']

        # Filter out any None or empty track IDs
        valid_track_ids = [tid for tid in track_ids if tid and tid.strip()]

        if not valid_track_ids:
            return {
                "success": False,
                "message": "No valid track IDs found"
            }

        # Get track details for better error reporting
        track_details = {}
        with UnitOfWork() as uow:
            for track_id in valid_track_ids:
                track = uow.track_repository.get_by_id(track_id)
                if track:
                    track_details[track_id] = {
                        'artist': track.artists,
                        'title': track.title,
                        'album': track.album or 'Unknown Album'
                    }
                else:
                    track_details[track_id] = {
                        'artist': 'Unknown Artist',
                        'title': 'Unknown Title',
                        'album': 'Unknown Album'
                    }

        # Convert track IDs to Spotify URIs
        track_uris = [f"spotify:track:{track_id}" for track_id in valid_track_ids]

        # Add tracks to playlist in batches (Spotify allows max 100 tracks per request)
        batch_size = 100
        tracks_added = 0
        failed_tracks = []
        successful_tracks = []

        for i in range(0, len(track_uris), batch_size):
            batch = track_uris[i:i + batch_size]
            batch_track_ids = valid_track_ids[i:i + batch_size]

            try:
                spotify_client.playlist_add_items(playlist_id, batch)
                tracks_added += len(batch)
                # Add all tracks in this batch to successful list
                for track_id in batch_track_ids:
                    successful_tracks.append({
                        'track_id': track_id,
                        **track_details[track_id]
                    })
            except Exception as batch_error:
                print(f"Batch failed, trying individual tracks: {batch_error}")
                # If batch fails, try individual tracks
                for j, uri in enumerate(batch):
                    track_id = batch_track_ids[j]
                    try:
                        spotify_client.playlist_add_items(playlist_id, [uri])
                        tracks_added += 1
                        successful_tracks.append({
                            'track_id': track_id,
                            **track_details[track_id]
                        })
                    except Exception as track_error:
                        failed_tracks.append({
                            'track_id': track_id,
                            'uri': uri,
                            'error': str(track_error),
                            **track_details[track_id]
                        })
                        print(
                            f"Failed to add track {uri} ({track_details[track_id]['artist']} - {track_details[track_id]['title']}): {track_error}")

        result = {
            "success": True,
            "message": f"Successfully created playlist with {tracks_added} tracks",
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "playlist_url": f"https://open.spotify.com/playlist/{playlist_id}",
            "tracks_added": tracks_added,
            "tracks_requested": len(valid_track_ids),
            "failed_tracks_count": len(failed_tracks),
            "failed_tracks": failed_tracks,
            "successful_tracks": successful_tracks
        }

        if failed_tracks:
            result["message"] += f" ({len(failed_tracks)} tracks failed to add)"

        return result

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to create playlist: {str(e)}"
        }


# TODO: fix
def get_playlists_for_organization(exclusion_settings, playlists_dir=None, force_reload=False):
    """
    Get all playlists for organization, applying exclusion rules.

    Args:
        exclusion_settings: Dictionary with exclusion configuration from frontend
        playlists_dir: Directory containing M3U playlists (optional)
        force_reload: Force reload from saved structure without verification

    Returns:
        Dictionary with all non-excluded playlists and current organization
    """
    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists = uow.playlist_repository.get_all()
        # Filter out the MASTER playlist
        all_playlists = [p for p in all_playlists if p.name.upper() != "MASTER"]

    # Apply exclusion logic based on frontend settings
    filtered_playlists = []

    for playlist in all_playlists:
        if _should_exclude_playlist(playlist, exclusion_settings):
            continue

        # Get track count for this playlist
        with UnitOfWork() as uow:
            track_count = len(uow.track_playlist_repository.get_uris_for_playlist(playlist.playlist_id))

        filtered_playlists.append({
            'id': playlist.playlist_id,
            'name': playlist.name,
            'track_count': track_count,
            'description': getattr(playlist, 'description', '') or ''
        })

    # Get current organization from existing structure (if any)
    current_organization = _get_current_organization_structure(playlists_dir, force_reload)  # MODIFIED

    return {
        "playlists": filtered_playlists,
        "current_organization": current_organization,
        "total_playlists": len(filtered_playlists)
    }


def _should_exclude_playlist(playlist, exclusion_settings):
    """Apply exclusion logic similar to spotify_client.py"""
    if not exclusion_settings:
        return False

    name = playlist.name
    description = getattr(playlist, 'description', '') or ''

    # Check excluded keywords
    excluded_keywords = exclusion_settings.get('excludedKeywords', [])
    for keyword in excluded_keywords:
        if keyword.lower() in name.lower():
            return True

    # Check excluded playlist IDs
    excluded_ids = exclusion_settings.get('excludedPlaylistIds', [])
    if playlist.playlist_id in excluded_ids:
        return True

    # Check description keywords
    description_exclusions = exclusion_settings.get('excludeByDescription', [])
    for keyword in description_exclusions:
        # Create a regex pattern to match whole words (case-insensitive)
        pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
        if re.search(pattern, description.lower()):
            return True

    return False


def _get_current_organization_structure(playlists_dir=None, force_reload=False):
    """Get the current organization structure by scanning the actual directory."""
    if not playlists_dir:
        return {
            "folders": {},
            "root_playlists": [],
            "structure_version": "1.0"
        }

    # First, get all current playlist names from the directory
    current_playlist_names = set()
    if os.path.exists(playlists_dir):
        for root, dirs, files in os.walk(playlists_dir):
            for file in files:
                if file.lower().endswith('.m3u'):
                    playlist_name = os.path.splitext(file)[0]
                    current_playlist_names.add(playlist_name)

    # Try to load from saved structure file
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    if os.path.exists(structure_file):
        try:
            with open(structure_file, 'r', encoding='utf-8') as f:
                saved_structure = json.load(f)

                # If force_reload is True, skip verification and return saved structure
                if force_reload:
                    print("Force reload requested - using saved structure without verification")
                    return saved_structure

                # NEW: Instead of strict verification, merge saved structure with current playlists
                print(f"Merging saved structure with {len(current_playlist_names)} current playlists")
                merged_structure = _merge_saved_structure_with_current_playlists(
                    playlists_dir, saved_structure, current_playlist_names
                )

                # Save the merged structure back to file if there were changes
                if merged_structure != saved_structure:
                    print("Saving merged structure back to file")
                    merged_structure["last_updated"] = datetime.now().isoformat()
                    with open(structure_file, 'w', encoding='utf-8') as f:
                        json.dump(merged_structure, f, indent=2, ensure_ascii=False)

                return merged_structure

        except Exception as e:
            print(f"Error reading saved structure: {e}")

    # Fallback: scan the actual directory structure
    return _scan_directory_structure(playlists_dir)


def _scan_directory_structure(playlists_dir):
    """Scan the actual directory structure and build organization."""
    if not os.path.exists(playlists_dir):
        return {
            "folders": {},
            "root_playlists": [],
            "structure_version": "1.0"
        }

    folders = {}
    root_playlists = []

    # Walk through the directory structure
    for root, dirs, files in os.walk(playlists_dir):
        # Get relative path from playlists_dir
        rel_path = os.path.relpath(root, playlists_dir)
        if rel_path == '.':
            rel_path = ''

        # Find M3U files in this directory
        m3u_files = [f for f in files if f.lower().endswith('.m3u')]
        playlist_names = [os.path.splitext(f)[0] for f in m3u_files]

        if rel_path == '':
            # Root directory playlists
            root_playlists.extend(playlist_names)
        else:
            # Folder playlists
            # Normalize path separators
            folder_path = rel_path.replace('\\', '/')
            if folder_path not in folders:
                folders[folder_path] = {"playlists": []}
            folders[folder_path]["playlists"].extend(playlist_names)

    return {
        "folders": folders,
        "root_playlists": root_playlists,
        "structure_version": "1.0"
    }


def _verify_structure_matches_directory(playlists_dir, structure):
    """Verify that the saved structure still matches the actual directory."""
    try:
        actual_structure = _scan_directory_structure(playlists_dir)

        # Simple verification - check if major structure elements match
        actual_folders = set(actual_structure["folders"].keys())
        saved_folders = set(structure.get("folders", {}).keys())

        actual_root = set(actual_structure["root_playlists"])
        saved_root = set(structure.get("root_playlists", []))

        # If there's a significant difference, the structure is outdated
        folder_diff = len(actual_folders.symmetric_difference(saved_folders))
        root_diff = len(actual_root.symmetric_difference(saved_root))

        # Allow some tolerance for small differences
        return folder_diff <= 2 and root_diff <= 2

    except Exception:
        return False


def preview_playlist_reorganization(playlists_dir, new_structure):
    """
    Preview what changes will be made to the file system.

    Args:
        playlists_dir: Directory containing current M3U playlists
        new_structure: New organization structure

    Returns:
        Dictionary with preview of changes
    """
    changes = {
        "folders_to_create": [],
        "folders_to_remove": [],
        "files_to_move": [],
        "files_to_create": [],
        "files_to_remove": [],
        "backup_location": None
    }

    # Analyze current structure
    current_files = {}
    if os.path.exists(playlists_dir):
        for root, dirs, files in os.walk(playlists_dir):
            for file in files:
                if file.lower().endswith('.m3u'):
                    rel_path = os.path.relpath(root, playlists_dir)
                    playlist_name = os.path.splitext(file)[0]
                    current_files[playlist_name] = rel_path if rel_path != '.' else ''

    # Determine what folders need to be created
    folders_in_new_structure = set()
    for folder_path in new_structure.get('folders', {}):
        if folder_path:  # Skip empty root path
            folders_in_new_structure.add(folder_path)
            # Add parent folders too
            parts = folder_path.split('/')
            for i in range(1, len(parts)):
                parent_path = '/'.join(parts[:i])
                folders_in_new_structure.add(parent_path)

    # Check which folders need to be created
    for folder_path in folders_in_new_structure:
        full_folder_path = os.path.join(playlists_dir, folder_path)
        if not os.path.exists(full_folder_path):
            changes["folders_to_create"].append(folder_path)

    # Determine file movements/creations
    all_new_playlist_locations = {}

    # Root playlists
    for playlist_name in new_structure.get('root_playlists', []):
        all_new_playlist_locations[playlist_name] = ''

    # Folder playlists
    for folder_path, folder_data in new_structure.get('folders', {}).items():
        for playlist_name in folder_data.get('playlists', []):
            all_new_playlist_locations[playlist_name] = folder_path

    # Compare with current locations
    for playlist_name, new_location in all_new_playlist_locations.items():
        current_location = current_files.get(playlist_name)

        if current_location is None:
            # File doesn't exist, needs to be created
            new_path = os.path.join(new_location, f"{playlist_name}.m3u") if new_location else f"{playlist_name}.m3u"
            changes["files_to_create"].append({
                "playlist": playlist_name,
                "path": new_path
            })
        elif current_location != new_location:
            # File exists but needs to be moved
            old_path = os.path.join(current_location,
                                    f"{playlist_name}.m3u") if current_location else f"{playlist_name}.m3u"
            new_path = os.path.join(new_location, f"{playlist_name}.m3u") if new_location else f"{playlist_name}.m3u"
            changes["files_to_move"].append({
                "playlist": playlist_name,
                "from": old_path,
                "to": new_path
            })

    # Check for files that will be orphaned (exist but not in new structure)
    for playlist_name, current_location in current_files.items():
        if playlist_name not in all_new_playlist_locations:
            old_path = os.path.join(current_location,
                                    f"{playlist_name}.m3u") if current_location else f"{playlist_name}.m3u"
            changes["files_to_remove"].append({
                "playlist": playlist_name,
                "path": old_path
            })

    # Set backup location (in m3u_playlists_backup directory)
    backup_base_dir = os.path.join(os.path.dirname(playlists_dir), "m3u_playlists_backup")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    changes["backup_location"] = os.path.join(backup_base_dir, f"backup_{timestamp}")

    return changes


def apply_playlist_reorganization(
        playlists_dir: str,
        master_tracks_dir: str,
        new_structure: Dict[str, Any],
        create_backup: bool = True
) -> Dict[str, Any]:
    """
    Apply the new playlist organization to the file system using URI-based logic.

    This version eliminates the N+1 query problem and uses batch operations for efficiency.

    Args:
        playlists_dir: Directory containing M3U playlists
        master_tracks_dir: Directory containing master tracks (kept for compatibility)
        new_structure: New organization structure
        create_backup: Whether to create a backup before making changes

    Returns:
        Dictionary with results of the operation
    """
    results = {
        "backup_created": False,
        "backup_location": None,
        "folders_created": 0,
        "files_moved": 0,
        "files_created": 0,
        "files_removed": 0,
        "errors": []
    }

    try:
        print(f"Starting optimized playlist reorganization...")

        # Create backup if requested
        if create_backup and os.path.exists(playlists_dir):
            backup_location = create_playlist_backup(playlists_dir)
            results["backup_created"] = True
            results["backup_location"] = backup_location
            print(f"Created backup at: {backup_location}")

        # Ensure playlists directory exists
        os.makedirs(playlists_dir, exist_ok=True)

        # Build URI-to-file mapping once for all playlists
        print("Building optimized URI-to-file mapping...")
        uri_to_file_map = build_uri_to_file_mapping_from_database()
        print(f"Loaded {len(uri_to_file_map)} URI-to-file mappings")

        # Create all necessary folders first
        folders_created_count = create_folder_structure(playlists_dir, new_structure)
        results["folders_created"] = folders_created_count

        # Build complete playlist location mapping
        all_playlist_locations = build_playlist_location_mapping(new_structure)
        print(f"Processing {len(all_playlist_locations)} playlists in new structure")

        # OPTIMIZATION: Get all required playlist data in batch operations
        playlist_names = list(all_playlist_locations.keys())
        playlists_data = get_playlists_data_batch(playlist_names)

        if not playlists_data:
            results["errors"].append("No valid playlists found in database")
            return results

        # OPTIMIZATION: Get all track URIs for all playlists in batch
        playlist_ids = [p['playlist_id'] for p in playlists_data.values()]
        playlist_track_uris = get_playlists_track_uris_batch(playlist_ids)

        # OPTIMIZATION: Get all unique track metadata in batch
        all_uris = set()
        for uris in playlist_track_uris.values():
            all_uris.update(uris)

        print(f"Loading metadata for {len(all_uris)} unique tracks...")
        tracks_metadata = get_all_tracks_metadata_by_uri(list(all_uris))

        # Process each playlist using pre-loaded data
        files_created_count = process_playlists_with_batch_data(
            playlists_dir,
            all_playlist_locations,
            playlists_data,
            playlist_track_uris,
            tracks_metadata,
            uri_to_file_map,
            results
        )
        results["files_created"] = files_created_count

        # Remove old files that are no longer needed
        files_removed_count = cleanup_old_playlist_files(
            playlists_dir,
            all_playlist_locations,
            results
        )
        results["files_removed"] = files_removed_count

        # Save the new structure for future use
        save_playlist_structure(playlists_dir, new_structure)

        print(f"Organization complete. Created {results['files_created']} files, "
              f"removed {results['files_removed']} files, "
              f"created {results['folders_created']} folders")

    except Exception as e:
        error_msg = f"Fatal error during reorganization: {str(e)}"
        results["errors"].append(error_msg)
        print(error_msg)
        raise

    return results


def create_playlist_backup(playlists_dir: str) -> str:
    """Create a backup of the existing playlists directory."""
    backup_base_dir = os.path.join(os.path.dirname(playlists_dir), "m3u_playlists_backup")
    os.makedirs(backup_base_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_location = os.path.join(backup_base_dir, f"backup_{timestamp}")

    shutil.copytree(playlists_dir, backup_location)
    return backup_location


def create_folder_structure(playlists_dir: str, new_structure: Dict[str, Any]) -> int:
    """Create all necessary folders for the new structure."""
    folders_created = 0
    folders_in_new_structure = set()

    # Collect all folder paths
    for folder_path in new_structure.get('folders', {}):
        if folder_path:  # Skip empty root path
            folders_in_new_structure.add(folder_path)
            # Add parent folders too
            parts = folder_path.split('/')
            for i in range(1, len(parts)):
                parent_path = '/'.join(parts[:i])
                folders_in_new_structure.add(parent_path)

    # Create folders in order (parents first)
    for folder_path in sorted(folders_in_new_structure):
        full_folder_path = os.path.join(playlists_dir, folder_path)
        if not os.path.exists(full_folder_path):
            os.makedirs(full_folder_path, exist_ok=True)
            folders_created += 1
            print(f"Created folder: {folder_path}")

    return folders_created


def build_playlist_location_mapping(new_structure: Dict[str, Any]) -> Dict[str, str]:
    """Build mapping of playlist names to their target folders."""
    all_playlist_locations = {}

    # Root playlists
    for playlist_name in new_structure.get('root_playlists', []):
        all_playlist_locations[playlist_name] = ''

    # Folder playlists
    for folder_path, folder_data in new_structure.get('folders', {}).items():
        for playlist_name in folder_data.get('playlists', []):
            all_playlist_locations[playlist_name] = folder_path

    return all_playlist_locations


def get_playlists_data_batch(playlist_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get playlist data for multiple playlists in batch operations.

    Args:
        playlist_names: List of playlist names to find

    Returns:
        Dictionary mapping playlist_name to playlist data
    """
    playlists_data = {}

    with UnitOfWork() as uow:
        # Get all playlists at once
        all_playlists = uow.playlist_repository.get_all()

        # Create lookup by name
        playlists_by_name = {p.name: p for p in all_playlists}

        # Find requested playlists
        for playlist_name in playlist_names:
            if playlist_name in playlists_by_name:
                playlist = playlists_by_name[playlist_name]
                playlists_data[playlist_name] = {
                    'playlist_id': playlist.playlist_id,
                    'name': playlist.name,
                    'playlist_object': playlist
                }
            else:
                print(f"Warning: Playlist '{playlist_name}' not found in database")

    return playlists_data


def process_playlists_with_batch_data(
        playlists_dir: str,
        all_playlist_locations: Dict[str, str],
        playlists_data: Dict[str, Dict[str, Any]],
        playlist_track_uris: Dict[str, List[str]],
        tracks_metadata: Dict[str, Dict[str, Any]],
        uri_to_file_map: Dict[str, str],
        results: Dict[str, Any]
) -> int:
    """
    OPTIMIZED: Process all playlists using pre-loaded batch data.

    Args:
        playlists_dir: Base playlists directory
        all_playlist_locations: Mapping of playlist names to target folders
        playlists_data: Pre-loaded playlist data
        playlist_track_uris: Pre-loaded track URIs for each playlist
        tracks_metadata: Pre-loaded track metadata
        uri_to_file_map: Pre-loaded URI to file mapping
        results: Results dictionary to update with errors

    Returns:
        Number of files created
    """
    files_created = 0

    for playlist_name, target_folder in all_playlist_locations.items():
        try:
            # Check if we have data for this playlist
            if playlist_name not in playlists_data:
                results["errors"].append(f"Playlist '{playlist_name}' not found in database")
                continue

            playlist_data = playlists_data[playlist_name]
            playlist_id = playlist_data['playlist_id']

            # Determine target path
            safe_name = sanitize_filename(playlist_name, preserve_spaces=True)
            if target_folder:
                target_dir = os.path.join(playlists_dir, target_folder)
                target_path = os.path.join(target_dir, f"{safe_name}.m3u")
            else:
                target_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

            # Check if file already exists at target location
            if os.path.exists(target_path):
                print(f"File already exists at target location: {target_path}")
                continue

            # Get track URIs for this playlist
            track_uris = playlist_track_uris.get(playlist_id, [])

            # Build metadata subset for this playlist
            playlist_tracks_metadata = {
                uri: tracks_metadata[uri]
                for uri in track_uris
                if uri in tracks_metadata
            }

            # Generate M3U file using optimized function
            tracks_found, tracks_added = generate_m3u_playlist(
                playlist_name=playlist_name,
                playlist_id=playlist_id,
                m3u_path=target_path,
                extended=True,
                overwrite=True,
                uri_to_file_map=uri_to_file_map,
                tracks_metadata=playlist_tracks_metadata
            )

            if tracks_added > 0:
                files_created += 1
                print(f"Created M3U for '{playlist_name}' with {tracks_added} tracks at: {target_path}")
            else:
                results["errors"].append(f"No tracks found for playlist '{playlist_name}'")

        except Exception as e:
            error_msg = f"Error processing playlist '{playlist_name}': {str(e)}"
            results["errors"].append(error_msg)
            print(error_msg)

    return files_created


def cleanup_old_playlist_files(
        playlists_dir: str,
        all_playlist_locations: Dict[str, str],
        results: Dict[str, Any]
) -> int:
    """Remove old M3U files that are no longer needed in the new structure."""
    files_removed = 0

    if not os.path.exists(playlists_dir):
        return files_removed

    for root, dirs, files in os.walk(playlists_dir):
        for file in files:
            if file.lower().endswith('.m3u'):
                playlist_name = os.path.splitext(file)[0]
                current_path = os.path.join(root, file)

                # Check if this file is in the new structure
                if playlist_name in all_playlist_locations:
                    expected_folder = all_playlist_locations[playlist_name]
                    safe_name = sanitize_filename(playlist_name, preserve_spaces=True)
                    expected_path = os.path.join(
                        playlists_dir,
                        expected_folder,
                        f"{safe_name}.m3u"
                    ) if expected_folder else os.path.join(playlists_dir, f"{safe_name}.m3u")

                    # If current path is not the expected path, remove it
                    if os.path.normpath(current_path) != os.path.normpath(expected_path):
                        try:
                            os.remove(current_path)
                            files_removed += 1
                            print(f"Removed old file: {current_path}")
                        except Exception as e:
                            results["errors"].append(f"Error removing file {current_path}: {str(e)}")
                else:
                    # Playlist not in new structure, remove it
                    try:
                        os.remove(current_path)
                        files_removed += 1
                        print(f"Removed orphaned file: {current_path}")
                    except Exception as e:
                        results["errors"].append(f"Error removing orphaned file {current_path}: {str(e)}")

    return files_removed


def save_playlist_structure(playlists_dir: str, new_structure: Dict[str, Any]) -> None:
    """Save the new playlist structure to a JSON file for future use."""
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    structure_to_save = {
        "folders": new_structure.get('folders', {}),
        "root_playlists": new_structure.get('root_playlists', []),
        "structure_version": "1.0",
        "last_updated": datetime.now().isoformat()
    }

    with open(structure_file, 'w', encoding='utf-8') as f:
        json.dump(structure_to_save, f, indent=2, ensure_ascii=False)

    print(f"Saved playlist structure to: {structure_file}")


def _merge_saved_structure_with_current_playlists(playlists_dir, saved_structure, all_playlist_names):
    """
    Merge saved structure with current playlists, adding new ones to root and removing missing ones.

    Args:
        playlists_dir: Directory containing M3U files
        saved_structure: The saved structure from .playlist_structure.json
        all_playlist_names: Set of all playlist names that currently exist

    Returns:
        Merged structure that preserves organization but includes new playlists
    """
    merged_structure = {
        "folders": {},
        "root_playlists": [],
        "structure_version": saved_structure.get("structure_version", "1.0"),
        "last_updated": saved_structure.get("last_updated")
    }

    # Track which playlists we've accounted for
    accounted_playlists = set()

    # Copy folder structure, filtering out playlists that no longer exist
    for folder_path, folder_data in saved_structure.get("folders", {}).items():
        existing_playlists = [
            playlist for playlist in folder_data.get("playlists", [])
            if playlist in all_playlist_names
        ]

        if existing_playlists:  # Only keep folders that have playlists
            merged_structure["folders"][folder_path] = {
                "playlists": existing_playlists
            }
            accounted_playlists.update(existing_playlists)

    # Copy root playlists, filtering out ones that no longer exist
    existing_root_playlists = [
        playlist for playlist in saved_structure.get("root_playlists", [])
        if playlist in all_playlist_names
    ]
    merged_structure["root_playlists"] = existing_root_playlists
    accounted_playlists.update(existing_root_playlists)

    # Add any new playlists to root
    new_playlists = all_playlist_names - accounted_playlists
    if new_playlists:
        print(f"Found {len(new_playlists)} new playlists, adding to root: {list(new_playlists)}")
        merged_structure["root_playlists"].extend(sorted(new_playlists))

    # Remove empty folders
    merged_structure["folders"] = {
        path: data for path, data in merged_structure["folders"].items()
        if data.get("playlists")
    }

    return merged_structure


def cleanup_orphaned_playlists(playlists_dir, dry_run=False):
    """
    Detect and optionally remove M3U files that don't correspond to any database playlist.
    Also clean up the playlist structure file.

    Args:
        playlists_dir: Directory containing M3U playlists
        dry_run: If True, only report what would be cleaned up without making changes

    Returns:
        Dictionary with cleanup results
    """
    if not os.path.exists(playlists_dir):
        return {
            "success": False,
            "message": "Playlists directory does not exist"
        }

    # Get all current playlist names from database (excluding MASTER)
    with UnitOfWork() as uow:
        db_playlists = uow.playlist_repository.get_all()
        current_playlist_names = set(p.name for p in db_playlists if p.name.upper() != "MASTER")

    # Find all M3U files
    existing_m3u_files = {}  # {playlist_name: file_path}
    for root, dirs, files in os.walk(playlists_dir):
        for file in files:
            if file.lower().endswith('.m3u'):
                playlist_name = os.path.splitext(file)[0]
                file_path = os.path.join(root, file)
                existing_m3u_files[playlist_name] = file_path

    # Identify orphaned files (M3U files without corresponding database playlist)
    orphaned_files = {}
    for playlist_name, file_path in existing_m3u_files.items():
        if playlist_name not in current_playlist_names:
            orphaned_files[playlist_name] = file_path

    # Load current structure file
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    current_structure = None
    if os.path.exists(structure_file):
        try:
            with open(structure_file, 'r', encoding='utf-8') as f:
                current_structure = json.load(f)
        except Exception as e:
            print(f"Error loading structure file: {e}")

    # Identify orphaned entries in structure file
    orphaned_in_structure = []
    if current_structure:
        # Check root playlists
        for playlist_name in current_structure.get('root_playlists', []):
            if playlist_name not in current_playlist_names:
                orphaned_in_structure.append(('root', playlist_name))

        # Check folder playlists
        for folder_path, folder_data in current_structure.get('folders', {}).items():
            for playlist_name in folder_data.get('playlists', []):
                if playlist_name not in current_playlist_names:
                    orphaned_in_structure.append((folder_path, playlist_name))

    results = {
        "success": True,
        "dry_run": dry_run,
        "orphaned_files": orphaned_files,
        "orphaned_in_structure": orphaned_in_structure,
        "files_deleted": 0,
        "structure_cleaned": False,
        "current_playlists": len(current_playlist_names),
        "existing_files": len(existing_m3u_files)
    }

    if dry_run:
        results[
            "message"] = f"Found {len(orphaned_files)} orphaned M3U files and {len(orphaned_in_structure)} orphaned structure entries"
        return results

    # Actually perform cleanup
    deleted_files = []

    # Delete orphaned M3U files
    for playlist_name, file_path in orphaned_files.items():
        try:
            os.remove(file_path)
            deleted_files.append(playlist_name)
            print(f"Deleted orphaned M3U file: {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

    results["files_deleted"] = len(deleted_files)

    # Clean up structure file
    if current_structure and orphaned_in_structure:
        # Clean root playlists
        current_structure["root_playlists"] = [
            name for name in current_structure.get("root_playlists", [])
            if name in current_playlist_names
        ]

        # Clean folder playlists
        for folder_path in list(current_structure.get("folders", {}).keys()):
            folder_data = current_structure["folders"][folder_path]
            cleaned_playlists = [
                name for name in folder_data.get("playlists", [])
                if name in current_playlist_names
            ]

            if cleaned_playlists:
                current_structure["folders"][folder_path]["playlists"] = cleaned_playlists
            else:
                # Remove empty folders
                del current_structure["folders"][folder_path]

        # Save cleaned structure
        try:
            current_structure["last_updated"] = datetime.now().isoformat()
            with open(structure_file, 'w', encoding='utf-8') as f:
                json.dump(current_structure, f, indent=2, ensure_ascii=False)
            results["structure_cleaned"] = True
            print("Cleaned playlist structure file")
        except Exception as e:
            print(f"Error saving cleaned structure: {e}")

    results[
        "message"] = f"Cleanup complete: {len(deleted_files)} files deleted, structure {'cleaned' if results['structure_cleaned'] else 'unchanged'}"
    return results
