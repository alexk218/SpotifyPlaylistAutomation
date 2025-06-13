import json
import os
from typing import List, Any, Dict

from helpers.m3u_helper import (
    build_uri_to_file_mapping_from_database,
    generate_multiple_playlists, sanitize_filename, get_all_tracks_metadata_by_uri, generate_m3u_playlist
)
from sql.core.unit_of_work import UnitOfWork


def analyze_m3u_generation(master_tracks_dir: str, playlists_dir: str) -> Dict[str, Any]:
    """
    OPTIMIZED: Analyze which playlists would be generated without making changes.

    This version uses batch operations to eliminate N+1 queries.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in

    Returns:
        Dictionary with analysis results
    """
    # Build URI-to-file mapping once
    uri_to_file_map = build_uri_to_file_mapping_from_database()

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists = uow.playlist_repository.get_all()
        # Filter out the MASTER playlist
        playlists = [p for p in all_playlists if p.name.upper() != "MASTER"]

        # Get track counts for all playlists in batch
        playlist_ids = [p.playlist_id for p in playlists]
        playlist_track_counts = get_playlist_track_counts_batch(playlist_ids, uri_to_file_map)

    # Create a list of playlist info for the UI
    playlists_info = []
    for playlist in playlists:
        track_count, local_track_count = playlist_track_counts.get(
            playlist.playlist_id, (0, 0)
        )

        playlists_info.append({
            'name': playlist.name,
            'id': playlist.playlist_id,
            'track_count': track_count,
            'local_track_count': local_track_count
        })

    # Sort by name for better display
    playlists_info.sort(key=lambda x: x['name'])

    return {
        'total_playlists': len(playlists_info),
        'playlists': playlists_info,
        'master_tracks_dir': master_tracks_dir,
        'playlists_dir': playlists_dir
    }


def generate_playlists(
        master_tracks_dir: str,
        playlists_dir: str,
        playlists_to_update: List[str],
        extended: bool = True,
        overwrite: bool = True
) -> Dict[str, Any]:
    """
    Generate M3U playlist files for selected playlists with batch operations.

    This version eliminates the N+1 query problem by:
    1. Loading all required data in batch operations
    2. Using optimized file existence checking
    3. Eliminating redundant database connections
    4. Pre-building all mappings once

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        playlists_to_update: List of playlist IDs to update
        extended: Whether to use extended M3U format
        overwrite: Whether to overwrite existing files

    Returns:
        Dictionary with generation results
    """
    print(f"Starting playlist generation for {len(playlists_to_update)} playlists...")

    # Load saved playlist structure first (for determining output locations)
    saved_structure = None
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    if os.path.exists(structure_file):
        try:
            with open(structure_file, 'r', encoding='utf-8') as f:
                saved_structure = json.load(f)
                print(f"Loaded saved playlist structure with {len(saved_structure.get('folders', {}))} folders")
        except Exception as e:
            print(f"Error loading saved structure: {e}")

    # Build playlist location mapping from saved structure or fall back to scanning
    playlist_location_map = {}
    if saved_structure:
        # Use saved structure to determine locations
        for playlist_name in saved_structure.get('root_playlists', []):
            playlist_location_map[playlist_name] = ''

        for folder_path, folder_data in saved_structure.get('folders', {}).items():
            for playlist_name in folder_data.get('playlists', []):
                playlist_location_map[playlist_name] = folder_path

        print(f"Using saved structure for {len(playlist_location_map)} playlists")
    else:
        # Fall back to scanning existing structure (original logic, but optimized)
        playlist_location_map = build_playlist_location_mapping(playlists_dir)

    # Get playlist information in batch
    playlists_info = []
    with UnitOfWork() as uow:
        # Get all playlists at once
        all_playlists = uow.playlist_repository.get_all()
        playlists_dict = {p.playlist_id: p for p in all_playlists}

        # Filter to only the playlists we want to update
        target_playlists = []
        for playlist_id in playlists_to_update:
            if playlist_id in playlists_dict:
                target_playlists.append(playlists_dict[playlist_id])

    # Prepare playlist generation info
    for playlist in target_playlists:
        playlist_name = playlist.name
        playlist_id = playlist.playlist_id

        # Determine output path
        safe_name = sanitize_filename(playlist_name, preserve_spaces=True)

        # Determine the folder location
        playlist_folder = playlist_location_map.get(playlist_name, '')

        if not playlist_folder:
            # Try pattern matching if not found in saved structure
            playlist_folder = find_playlist_folder_by_patterns(playlist_name, playlist_location_map)

        # Determine the M3U path
        if playlist_folder:
            folder_path = os.path.join(playlists_dir, playlist_folder)
            os.makedirs(folder_path, exist_ok=True)
            m3u_path = os.path.join(folder_path, f"{safe_name}.m3u")
        else:
            m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

        # Ensure the directory exists
        os.makedirs(os.path.dirname(m3u_path), exist_ok=True)

        playlists_info.append({
            'id': playlist_id,
            'name': playlist_name,
            'm3u_path': m3u_path,
            'folder': playlist_folder or 'root'
        })

    # Generate all playlists using optimized batch method
    print(f"Generating {len(playlists_info)} playlists using optimized batch processing...")
    results = generate_multiple_playlists(
        playlists_to_generate=playlists_info,
        extended=extended,
        overwrite=overwrite
    )

    # Process results
    success_count = 0
    failed_count = 0
    updated_playlists = []

    for result in results:
        if result['success']:
            success_count += 1
            updated_playlists.append({
                'id': result['id'],
                'name': result['name'],
                'tracks_added': result['tracks_added'],
                'location': result.get('folder', 'root'),
                'm3u_path': result['m3u_path']
            })
        else:
            failed_count += 1
            print(f"Failed to regenerate playlist {result['id']}: {result.get('error', 'Unknown error')}")

    print(f"Completed playlist generation: {success_count} successful, {failed_count} failed")

    return {
        "playlists_updated": success_count,
        "playlists_failed": failed_count,
        "updated_playlists": updated_playlists,
        "total_playlists_to_update": len(playlists_to_update)
    }


def build_playlist_location_mapping(playlists_dir: str) -> Dict[str, str]:
    """
    OPTIMIZED: Build mapping of playlist names to their folder locations.

    Args:
        playlists_dir: Base playlists directory

    Returns:
        Dictionary mapping playlist names to relative folder paths
    """
    playlist_location_map = {}

    # Walk directory tree once and build all mappings
    for root, dirs, files in os.walk(playlists_dir):
        rel_path = os.path.relpath(root, playlists_dir)
        if rel_path == '.':
            rel_path = ''

        for file in files:
            if file.lower().endswith('.m3u'):
                file_name = os.path.splitext(file)[0]

                # Map multiple variations for better matching
                variations = [
                    file_name,
                    file_name.lower(),
                    sanitize_filename(file_name, preserve_spaces=True),
                    sanitize_filename(file_name, preserve_spaces=True).lower()
                ]

                for variation in variations:
                    playlist_location_map[variation] = rel_path

    return playlist_location_map


def find_playlist_folder_by_patterns(playlist_name: str, playlist_location_map: Dict[str, str]) -> str:
    """
    Find playlist folder using pattern matching.

    Args:
        playlist_name: Name of the playlist to find
        playlist_location_map: Dictionary of known playlist locations

    Returns:
        Folder path or empty string if not found
    """
    safe_name = sanitize_filename(playlist_name, preserve_spaces=True)

    # Try exact matches first
    for pattern in [playlist_name, safe_name, playlist_name.lower(), safe_name.lower()]:
        if pattern in playlist_location_map:
            return playlist_location_map[pattern]

    # Try partial matching
    for filename, location in playlist_location_map.items():
        if (safe_name.lower() in filename.lower() or
                playlist_name.lower() in filename.lower()):
            return location

    return ''


def get_playlist_track_counts_batch(
        playlist_ids: List[str],
        uri_to_file_map: Dict[str, str]
) -> Dict[str, tuple]:
    """
    Get track counts for multiple playlists in batch.

    Args:
        playlist_ids: List of playlist IDs
        uri_to_file_map: Pre-built URI to file mapping

    Returns:
        Dictionary mapping playlist_id to (total_tracks, local_tracks) tuple
    """
    if not playlist_ids:
        return {}

    playlist_counts = {}

    with UnitOfWork() as uow:
        # Get all track URIs for all playlists in one query
        placeholders = ','.join(['?' for _ in playlist_ids])
        query = f"""
            SELECT PlaylistId, Uri 
            FROM TrackPlaylists 
            WHERE PlaylistId IN ({placeholders})
        """

        results = uow.track_playlist_repository.fetch_all(query, playlist_ids)

        # Group by playlist and count
        for playlist_id in playlist_ids:
            playlist_counts[playlist_id] = (0, 0)  # (total, local)

        playlist_uris = {}
        for row in results:
            if row.PlaylistId not in playlist_uris:
                playlist_uris[row.PlaylistId] = []
            playlist_uris[row.PlaylistId].append(row.Uri)

        # Count tracks for each playlist
        for playlist_id, uris in playlist_uris.items():
            total_tracks = len(uris)
            local_tracks = sum(1 for uri in uris if uri in uri_to_file_map)
            playlist_counts[playlist_id] = (total_tracks, local_tracks)

    return playlist_counts


def regenerate_playlist(
        playlist_id: str,
        master_tracks_dir: str,
        playlists_dir: str,
        extended: bool = True,
        force: bool = True
) -> Dict[str, Any]:
    """
    OPTIMIZED: Regenerate a single playlist using batch operations where possible.

    Args:
        playlist_id: The playlist ID to regenerate
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        extended: Whether to use extended M3U format
        force: Whether to force overwrite existing files

    Returns:
        Dictionary with regeneration results
    """
    # Get the playlist information
    with UnitOfWork() as uow:
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        if not playlist:
            raise ValueError(f"Playlist ID {playlist_id} not found in database")

    playlist_name = playlist.name
    safe_name = sanitize_filename(playlist_name, preserve_spaces=True)

    # Determine target location (using same logic as batch generation)
    target_m3u_path = None

    # Try to load saved playlist structure first
    saved_structure = None
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    if os.path.exists(structure_file):
        try:
            with open(structure_file, 'r', encoding='utf-8') as f:
                saved_structure = json.load(f)
        except Exception as e:
            print(f"Error loading saved structure: {e}")

    # Determine target location
    playlist_folder = ''
    if saved_structure:
        # Check saved structure
        for pname in saved_structure.get('root_playlists', []):
            if pname == playlist_name:
                playlist_folder = ''
                break

        if not playlist_folder:
            for folder_path, folder_data in saved_structure.get('folders', {}).items():
                if playlist_name in folder_data.get('playlists', []):
                    playlist_folder = folder_path
                    break

    # Determine the M3U path
    if playlist_folder:
        folder_path = os.path.join(playlists_dir, playlist_folder)
        os.makedirs(folder_path, exist_ok=True)
        target_m3u_path = os.path.join(folder_path, f"{safe_name}.m3u")
    else:
        target_m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

    # If force is specified and the file exists, remove it
    if force and target_m3u_path and os.path.exists(target_m3u_path):
        try:
            os.remove(target_m3u_path)
            print(f"Forcibly removed existing M3U file: {target_m3u_path}")
        except Exception as e:
            print(f"Error removing existing M3U file: {e}")

    # Build optimized mappings
    uri_to_file_map = build_uri_to_file_mapping_from_database()

    # Get track URIs for this playlist
    with UnitOfWork() as uow:
        track_uris = uow.track_playlist_repository.get_uris_for_playlist(playlist_id)

    # Get track metadata if extended format is needed
    tracks_metadata = None
    if extended and track_uris:
        tracks_metadata = get_all_tracks_metadata_by_uri(track_uris)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(target_m3u_path), exist_ok=True)

    # Regenerate the playlist using optimized function
    tracks_found, tracks_added = generate_m3u_playlist(
        playlist_name=playlist_name,
        playlist_id=playlist_id,
        m3u_path=target_m3u_path,
        extended=extended,
        overwrite=True,
        uri_to_file_map=uri_to_file_map,
        tracks_metadata=tracks_metadata
    )

    # Get location relative to the playlists directory for display
    m3u_location = "root"
    if target_m3u_path != os.path.join(playlists_dir, f"{safe_name}.m3u"):
        rel_path = os.path.relpath(os.path.dirname(target_m3u_path), playlists_dir)
        if rel_path != ".":
            m3u_location = rel_path

    return {
        'success': True,
        'message': f'Successfully regenerated playlist: {playlist_name} with {tracks_added} tracks',
        'stats': {
            'playlist_name': playlist_name,
            'tracks_found': tracks_found,
            'tracks_added': tracks_added,
            'm3u_path': target_m3u_path,
            'location': m3u_location,
            'file_size': os.path.getsize(target_m3u_path) if os.path.exists(target_m3u_path) else 0
        }
    }
