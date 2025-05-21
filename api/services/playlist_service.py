import os
from helpers.m3u_helper import (
    build_track_id_mapping,
    generate_all_m3u_playlists,
    regenerate_single_playlist
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


def generate_playlists(master_tracks_dir, playlists_dir, playlists_to_update=None, extended=True, overwrite=True):
    """
    Generate M3U playlist files for selected playlists.

    Args:
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        playlists_to_update: List of playlist IDs to update (if None, update all)
        extended: Whether to use extended M3U format with metadata
        overwrite: Whether to overwrite existing playlist files

    Returns:
        Dictionary with generation results
    """
    # Create the playlists directory if it doesn't exist
    os.makedirs(playlists_dir, exist_ok=True)

    # Build track ID mapping first for efficiency
    track_id_map = build_track_id_mapping(master_tracks_dir)

    # Generate the playlists
    stats = generate_all_m3u_playlists(
        master_tracks_dir=master_tracks_dir,
        playlists_dir=playlists_dir,
        extended=extended,
        skip_master=True,
        overwrite=overwrite,
        only_changed=playlists_to_update is not None,
        changed_playlists=None if playlists_to_update is None else playlists_to_update,
        track_id_map=track_id_map
    )

    # Reformat stats for the API response
    return {
        'playlists_updated': stats['playlists_created'] + stats['playlists_updated'],
        'playlists_unchanged': stats['playlists_unchanged'],
        'playlists_failed': 0,  # Add error tracking if needed
        'empty_playlists': stats['empty_playlists'],
        'total_tracks_added': stats['total_tracks_added']
    }


def regenerate_playlist(playlist_id, master_tracks_dir, playlists_dir, extended=True, force=True):
    """
    Regenerate a single M3U playlist.

    Args:
        playlist_id: ID of the playlist to regenerate
        master_tracks_dir: Directory containing master tracks
        playlists_dir: Directory to create playlist files in
        extended: Whether to use extended M3U format with metadata
        force: Whether to force regeneration even if no changes detected

    Returns:
        Dictionary with regeneration results
    """
    # Create the playlists directory if it doesn't exist
    os.makedirs(playlists_dir, exist_ok=True)

    # Call the helper function to regenerate the playlist
    result = regenerate_single_playlist(
        playlist_id=playlist_id,
        master_tracks_dir=master_tracks_dir,
        playlists_dir=playlists_dir,
        extended=extended,
        overwrite=True  # Always overwrite for single playlist regeneration
    )

    return result
