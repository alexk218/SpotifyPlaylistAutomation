import json
import os
import re
import shutil

import Levenshtein
from mutagen.id3 import ID3
from mutagen.mp3 import MP3
from datetime import datetime

from helpers.m3u_helper import (
    build_track_id_mapping,
    sanitize_filename,
    get_m3u_track_ids,
    find_local_file_path_with_extensions, generate_m3u_playlist
)
from helpers.validation_helper import validate_master_tracks
from sql.core.unit_of_work import UnitOfWork


def validate_track_metadata(master_tracks_dir):
    """
    Validate track metadata in the master tracks directory.

    Args:
        master_tracks_dir: Directory containing master tracks

    Returns:
        Dictionary with validation results
    """
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
        artist = track.get_primary_artist() if hasattr(track, 'get_primary_artist') else track.artists.split(',')[
            0].strip()
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
                    except Exception:
                        duration = 0
                        duration_formatted = "Unknown"

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
                except Exception:
                    duration = 0
                    duration_formatted = "Unknown"
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
    for track_id, files_list in duplicate_track_ids.items():
        if len(files_list) > 1:
            # Get track title from database
            track_title = "Unknown"
            if track_id in db_tracks_by_id:
                track = db_tracks_by_id[track_id]
                artist = track.get_primary_artist() if hasattr(track, 'get_primary_artist') else \
                    track.artists.split(',')[0].strip()
                track_title = f"{artist} - {track.title}"

            # Create detailed file information for each duplicate
            file_details = []
            for file in files_list:
                file_path = os.path.join(master_tracks_dir, file)
                if os.path.exists(file_path):
                    # Get file duration
                    try:
                        audio = MP3(file_path)
                        duration = audio.info.length
                        duration_formatted = f"{int(duration // 60)}:{int(duration % 60):02d}"
                    except Exception:
                        duration = 0
                        duration_formatted = "Unknown"

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

    return {
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
    }


def validate_playlists_m3u(master_tracks_dir, playlists_dir):
    """
    Validate M3U playlists against database information.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory containing playlist files

    Returns:
        Dictionary with validation results
    """
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
    # Use the existing validate_master_tracks function
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


def get_playlists_for_organization(exclusion_settings, playlists_dir=None):
    """
    Get all playlists for organization, applying exclusion rules.

    Args:
        exclusion_settings: Dictionary with exclusion configuration from frontend
        playlists_dir: Directory containing M3U playlists (optional)

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
            track_count = len(uow.track_playlist_repository.get_track_ids_for_playlist(playlist.playlist_id))

        filtered_playlists.append({
            'id': playlist.playlist_id,
            'name': playlist.name,
            'track_count': track_count,
            'description': getattr(playlist, 'description', '') or ''
        })

    # Get current organization from existing structure (if any)
    current_organization = _get_current_organization_structure(playlists_dir)

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


def _get_current_organization_structure(playlists_dir=None):
    """Get the current organization structure by scanning the actual directory."""
    if not playlists_dir:
        # Try to get from a default location or return empty if not available
        return {
            "folders": {},
            "root_playlists": [],
            "structure_version": "1.0"
        }

    # First try to load from saved structure file
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    if os.path.exists(structure_file):
        try:
            with open(structure_file, 'r', encoding='utf-8') as f:
                saved_structure = json.load(f)
                # Verify the structure still matches the actual directory
                if _verify_structure_matches_directory(playlists_dir, saved_structure):
                    return saved_structure
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

    # Set backup location
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    changes["backup_location"] = f"{playlists_dir}_backup_{timestamp}"

    return changes


def apply_playlist_reorganization(playlists_dir, master_tracks_dir, new_structure, create_backup=True):
    """
    Apply the new playlist organization to the file system.

    Args:
        playlists_dir: Directory containing M3U playlists
        master_tracks_dir: Directory containing master tracks
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
        # Create backup if requested
        if create_backup and os.path.exists(playlists_dir):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_location = f"{playlists_dir}_backup_{timestamp}"
            shutil.copytree(playlists_dir, backup_location)
            results["backup_created"] = True
            results["backup_location"] = backup_location
            print(f"Created backup at: {backup_location}")

        # Ensure playlists directory exists
        os.makedirs(playlists_dir, exist_ok=True)

        # Build track ID mapping for M3U generation
        track_id_map = build_track_id_mapping(master_tracks_dir)

        # Create all necessary folders first
        folders_in_new_structure = set()
        for folder_path in new_structure.get('folders', {}):
            if folder_path:  # Skip empty root path
                folders_in_new_structure.add(folder_path)
                # Add parent folders too
                parts = folder_path.split('/')
                for i in range(1, len(parts)):
                    parent_path = '/'.join(parts[:i])
                    folders_in_new_structure.add(parent_path)

        for folder_path in sorted(folders_in_new_structure):  # Sort to create parents first
            full_folder_path = os.path.join(playlists_dir, folder_path)
            if not os.path.exists(full_folder_path):
                os.makedirs(full_folder_path, exist_ok=True)
                results["folders_created"] += 1
                print(f"Created folder: {folder_path}")

        # Generate/move M3U files according to new structure
        all_playlist_locations = {}

        # Root playlists
        for playlist_name in new_structure.get('root_playlists', []):
            all_playlist_locations[playlist_name] = ''

        # Folder playlists
        for folder_path, folder_data in new_structure.get('folders', {}).items():
            for playlist_name in folder_data.get('playlists', []):
                all_playlist_locations[playlist_name] = folder_path

        # Process each playlist
        for playlist_name, target_folder in all_playlist_locations.items():
            try:
                # Find playlist in database
                with UnitOfWork() as uow:
                    playlists = uow.playlist_repository.get_all()
                    playlist = next((p for p in playlists if p.name == playlist_name), None)

                    if not playlist:
                        results["errors"].append(f"Playlist '{playlist_name}' not found in database")
                        continue

                # Determine target path
                if target_folder:
                    target_dir = os.path.join(playlists_dir, target_folder)
                    target_path = os.path.join(target_dir, f"{playlist_name}.m3u")
                else:
                    target_path = os.path.join(playlists_dir, f"{playlist_name}.m3u")

                # Check if file already exists at target location
                if os.path.exists(target_path):
                    print(f"File already exists at target location: {target_path}")
                    continue

                # Generate M3U file at new location
                tracks_found, tracks_added = generate_m3u_playlist(
                    playlist_name=playlist.name,
                    playlist_id=playlist.playlist_id,
                    master_tracks_dir=master_tracks_dir,
                    m3u_path=target_path,
                    extended=True,
                    overwrite=True,
                    track_id_map=track_id_map
                )

                if tracks_added > 0:
                    results["files_created"] += 1
                    print(f"Created M3U for '{playlist_name}' with {tracks_added} tracks at: {target_path}")
                else:
                    results["errors"].append(f"No tracks found for playlist '{playlist_name}'")

            except Exception as e:
                error_msg = f"Error processing playlist '{playlist_name}': {str(e)}"
                results["errors"].append(error_msg)
                print(error_msg)

        # Remove old files that are no longer needed
        if os.path.exists(playlists_dir):
            for root, dirs, files in os.walk(playlists_dir):
                for file in files:
                    if file.lower().endswith('.m3u'):
                        playlist_name = os.path.splitext(file)[0]
                        current_path = os.path.join(root, file)

                        # Check if this file is in the new structure
                        if playlist_name in all_playlist_locations:
                            expected_folder = all_playlist_locations[playlist_name]
                            expected_path = os.path.join(playlists_dir, expected_folder,
                                                         file) if expected_folder else os.path.join(playlists_dir, file)

                            # If current path is not the expected path, remove it
                            if os.path.normpath(current_path) != os.path.normpath(expected_path):
                                try:
                                    os.remove(current_path)
                                    results["files_removed"] += 1
                                    print(f"Removed old file: {current_path}")
                                except Exception as e:
                                    results["errors"].append(f"Error removing file {current_path}: {str(e)}")
                        else:
                            # Playlist not in new structure, remove it
                            try:
                                os.remove(current_path)
                                results["files_removed"] += 1
                                print(f"Removed orphaned file: {current_path}")
                            except Exception as e:
                                results["errors"].append(f"Error removing orphaned file {current_path}: {str(e)}")

        # Save the new structure for future use
        structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
        with open(structure_file, 'w', encoding='utf-8') as f:
            json.dump(new_structure, f, indent=2, ensure_ascii=False)

        print(
            f"Organization complete. Created {results['files_created']} files, removed {results['files_removed']} files")

    except Exception as e:
        results["errors"].append(f"Fatal error during reorganization: {str(e)}")
        raise

    return results
