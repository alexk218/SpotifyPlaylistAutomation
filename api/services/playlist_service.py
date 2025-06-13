import os
from helpers.m3u_helper import (
    build_track_id_mapping,
    generate_all_m3u_playlists,
    regenerate_single_playlist, build_uri_to_file_mapping, build_uri_to_file_mapping_from_database
)
from sql.core.unit_of_work import UnitOfWork


def analyze_m3u_generation(master_tracks_dir, playlists_dir):
    """
    Analyze which playlists would be generated without making changes.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in

    Returns:
        Dictionary with analysis results
    """
    # Build track ID mapping first for efficiency
    track_id_map = build_track_id_mapping(master_tracks_dir)

    # Get all playlists from database
    with UnitOfWork() as uow:
        all_playlists = uow.playlist_repository.get_all()
        # Filter out the MASTER playlist
        playlists = [p for p in all_playlists if p.name.upper() != "MASTER"]

    # Create a list of playlist info for the UI
    playlists_info = []
    for playlist in playlists:
        # Get tracks for this playlist
        with UnitOfWork() as uow:
            track_ids = uow.track_playlist_repository.get_track_ids_for_playlist(playlist.playlist_id)

            # Count how many tracks actually have local files
            local_track_count = sum(1 for tid in track_ids if tid in track_id_map)

        playlists_info.append({
            'name': playlist.name,
            'id': playlist.playlist_id,
            'track_count': len(track_ids),
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


def generate_playlists(master_tracks_dir, playlists_dir, playlists_to_update, extended=True, overwrite=True):
    from helpers.m3u_helper import build_track_id_mapping, generate_m3u_playlist, sanitize_filename
    from sql.core.unit_of_work import UnitOfWork
    import os
    import json

    # Build URI-to-file mapping for efficiency
    uri_to_file_map = build_uri_to_file_mapping_from_database()

    # Try to load saved playlist structure first
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
        # Fall back to scanning existing structure (original logic)
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

            # Determine playlist location
            playlist_folder = ''

            if saved_structure:
                # Use saved structure location
                playlist_folder = playlist_location_map.get(playlist_name, '')
                if playlist_folder:
                    print(f"Using saved structure location for '{playlist_name}': {playlist_folder}")
                else:
                    print(f"Playlist '{playlist_name}' not found in saved structure, placing in root")
            else:
                # Use original scanning logic for fallback
                search_patterns = [
                    safe_name,
                    safe_name.lower(),
                    playlist_name,
                    playlist_name.lower()
                ]

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
                print(f"Will generate in location: {m3u_path}")
            else:
                # Generate in root directory
                m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")
                print(f"Will generate in root: {m3u_path}")

            # Ensure the directory exists
            os.makedirs(os.path.dirname(m3u_path), exist_ok=True)

            # Generate the M3U file
            tracks_found, tracks_added = generate_m3u_playlist(
                playlist_name=playlist.name,
                playlist_id=playlist_id,
                m3u_path=m3u_path,
                extended=extended,
                overwrite=overwrite,
                uri_to_file_map=uri_to_file_map,
            )

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
            import traceback
            print(traceback.format_exc())

    return {
        "playlists_updated": success_count,
        "playlists_failed": failed_count,
        "updated_playlists": updated_playlists,
        "total_playlists_to_update": len(playlists_to_update)
    }


def regenerate_playlist(playlist_id, master_tracks_dir, playlists_dir, extended=True, force=True):
    from helpers.m3u_helper import build_track_id_mapping, generate_m3u_playlist, sanitize_filename
    from sql.core.unit_of_work import UnitOfWork
    import os
    import json

    # Get the playlist name for more useful messages
    playlist_name = None
    with UnitOfWork() as uow:
        playlist = uow.playlist_repository.get_by_id(playlist_id)
        if not playlist:
            raise ValueError(f"Playlist ID {playlist_id} not found in database")

        playlist_name = playlist.name

    # Sanitize the playlist name for file matching
    safe_name = sanitize_filename(playlist_name, preserve_spaces=True)

    # NEW: Try to load saved playlist structure first
    saved_structure = None
    structure_file = os.path.join(playlists_dir, '.playlist_structure.json')
    if os.path.exists(structure_file):
        try:
            with open(structure_file, 'r', encoding='utf-8') as f:
                saved_structure = json.load(f)
                print(f"Found saved playlist structure for single playlist regeneration")
        except Exception as e:
            print(f"Error loading saved structure: {e}")

    # Determine target location
    target_m3u_path = None

    if saved_structure:
        # Check if playlist is in saved structure
        playlist_folder = ''

        # Check root playlists
        if playlist_name in saved_structure.get('root_playlists', []):
            playlist_folder = ''
            print(f"Found '{playlist_name}' in saved structure (root)")
        else:
            # Check folders
            for folder_path, folder_data in saved_structure.get('folders', {}).items():
                if playlist_name in folder_data.get('playlists', []):
                    playlist_folder = folder_path
                    print(f"Found '{playlist_name}' in saved structure (folder: {folder_path})")
                    break

        # If found in saved structure, use that location
        if playlist_name in saved_structure.get('root_playlists', []) or any(
                playlist_name in folder_data.get('playlists', [])
                for folder_data in saved_structure.get('folders', {}).values()
        ):
            if playlist_folder:
                folder_path = os.path.join(playlists_dir, playlist_folder)
                os.makedirs(folder_path, exist_ok=True)
                target_m3u_path = os.path.join(folder_path, f"{safe_name}.m3u")
            else:
                target_m3u_path = os.path.join(playlists_dir, f"{safe_name}.m3u")

            print(f"Using saved structure location: {target_m3u_path}")

    # Fallback to scanning if not found in saved structure
    if not target_m3u_path:
        print(f"Playlist '{playlist_name}' not found in saved structure, scanning existing files...")

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

        target_m3u_path = existing_m3u_path

    # If force is specified and the file exists, remove it
    if force and target_m3u_path and os.path.exists(target_m3u_path):
        try:
            os.remove(target_m3u_path)
            print(f"Forcibly removed existing M3U file: {target_m3u_path}")
        except Exception as e:
            print(f"Error removing existing M3U file: {e}")

    # Build track ID mapping for efficiency
    uri_to_file_map = build_uri_to_file_mapping()

    # Ensure the directory exists
    os.makedirs(os.path.dirname(target_m3u_path), exist_ok=True)

    # Regenerate the playlist
    tracks_found, tracks_added = generate_m3u_playlist(
        playlist_name=playlist_name,
        playlist_id=playlist_id,
        m3u_path=target_m3u_path,
        extended=extended,
        overwrite=True,
        uri_to_file_map=uri_to_file_map
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
