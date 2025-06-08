import threading
import traceback
from typing import Optional, Dict, Any

from dotenv import load_dotenv

from api.models.sync_responses import SyncResponse, SyncStats, SyncDetails, create_execution_response, \
    format_playlist_item, PlaylistSyncDetails, create_analysis_response, format_track_item, TrackSyncDetails, \
    format_association_item, AssociationSyncDetails, normalize_precomputed_changes
from drivers.spotify_client import (
    authenticate_spotify, sync_to_master_playlist, sync_unplaylisted_to_unsorted, fetch_playlists
)
from helpers.playlist_helper import load_exclusion_config
from helpers.sync_helper import (
    analyze_playlists_changes, analyze_tracks_changes, analyze_track_playlist_associations,
    sync_playlists_to_db, sync_tracks_to_db, sync_track_playlist_associations_to_db
)
from sql.core.unit_of_work import UnitOfWork

load_dotenv()


def get_exclusion_config(request_json=None):
    """
    Get exclusion configuration from HTTP request format.
    """
    # If request contains playlist settings, extract and use those
    if request_json and 'playlistSettings' in request_json:
        client_settings = request_json['playlistSettings']

        return {
            "forbidden_playlists": [],
            "forbidden_words": client_settings.get('excludedKeywords', []),
            "description_keywords": client_settings.get('excludeByDescription', []),
            "forbidden_playlist_ids": client_settings.get('excludedPlaylistIds', [])
        }

    # Otherwise load default config
    return load_exclusion_config()


def sync_master_playlist(master_playlist_id, request_json=None):
    """
    Sync all tracks from all playlists to MASTER playlist.
    Supports both optimized (changed playlists only) and full refresh modes.

    Args:
        master_playlist_id: ID of the master playlist
        request_json: Optional dictionary with request data (including force_refresh flag)

    Returns:
        Success status
    """
    exclusion_config = get_exclusion_config(request_json)
    force_refresh = request_json.get('force_refresh', False) if request_json else False

    try:
        spotify_client = authenticate_spotify()

        # Get all playlists from database for snapshot comparison
        with UnitOfWork() as uow:
            db_playlists = uow.playlist_repository.get_all()
            db_playlists_dict = {p.playlist_id: p for p in db_playlists}

        # Fetch current playlists from Spotify to get current snapshot_ids
        spotify_playlists = fetch_playlists(spotify_client, exclusion_config=exclusion_config)

        # Filter out MASTER playlist
        spotify_playlists = [p for p in spotify_playlists if p.playlist_id != master_playlist_id]

        if force_refresh:
            # Full refresh mode - process ALL playlists
            playlists_to_process = spotify_playlists
            print(f"Force refresh mode: Processing all {len(playlists_to_process)} playlists...")

            message = f"Full master playlist sync started (processing all {len(playlists_to_process)} playlists). This operation runs in the background and may take several minutes."
        else:
            # Optimized mode - only process changed playlists
            changed_playlists = []
            unchanged_playlists = []

            for spotify_playlist in spotify_playlists:
                playlist_id = spotify_playlist.playlist_id
                current_snapshot = spotify_playlist.snapshot_id

                # Check if this playlist exists in DB and if snapshot has changed
                if playlist_id in db_playlists_dict:
                    db_playlist = db_playlists_dict[playlist_id]
                    if db_playlist.master_sync_snapshot_id != current_snapshot:
                        changed_playlists.append(spotify_playlist)
                    else:
                        unchanged_playlists.append(spotify_playlist)
                else:
                    # New playlist not in database - include it
                    changed_playlists.append(spotify_playlist)

            playlists_to_process = changed_playlists
            print(
                f"Optimized master sync: {len(changed_playlists)} playlists changed, {len(unchanged_playlists)} unchanged")

            if not changed_playlists:
                return {
                    "success": True,
                    "message": "No playlists have changed since last master sync. No action needed."
                }

            message = f"Optimized master playlist sync started. Processing {len(changed_playlists)} changed playlists (skipping {len(unchanged_playlists)} unchanged). This operation runs in the background and may take several minutes."

        # Start a background thread for the sync operation
        def background_sync():
            try:
                sync_to_master_playlist(spotify_client, master_playlist_id, playlists_to_process)
            except Exception as e:
                error_str = traceback.format_exc()
                print(f"Error in background sync: {e}")
                print(error_str)

        thread = threading.Thread(target=background_sync)
        thread.daemon = True
        thread.start()

        return {
            "success": True,
            "message": message
        }

    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error starting sync: {e}")
        print(error_str)
        raise RuntimeError(f"Error: {str(e)}")


def sync_unplaylisted_tracks(unsorted_playlist_id):
    """
    Sync unplaylisted tracks to UNSORTED playlist.

    Args:
        unsorted_playlist_id: ID of the unsorted playlist

    Returns:
        Success status
    """
    try:
        spotify_client = authenticate_spotify()

        # Start a background thread for this operation
        def background_sync():
            try:
                sync_unplaylisted_to_unsorted(spotify_client, unsorted_playlist_id)
            except Exception as e:
                error_str = traceback.format_exc()
                print(f"Error in background sync: {e}")
                print(error_str)

        thread = threading.Thread(target=background_sync)
        thread.daemon = True
        thread.start()

        return {
            "success": True,
            "message": "Sync of unplaylisted tracks started. This operation runs in the background and may take several minutes."
        }
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error starting sync: {e}")
        print(error_str)
        raise RuntimeError(f"Error: {str(e)}")


def orchestrate_db_sync(
        action: str,
        master_playlist_id: str,
        force_refresh: bool,
        is_confirmed: bool,
        precomputed_changes: Optional[Dict[str, Any]] = None,
        exclusion_config: Optional[Dict[str, Any]] = None,
        stage: str = 'start'
) -> Dict[str, Any]:
    """
    Handle database sync operations with consistent response structure.

    Args:
        action: Type of sync operation ('playlists', 'tracks', 'associations', 'all', 'clear')
        master_playlist_id: ID of the master playlist
        force_refresh: Whether to force a full refresh
        is_confirmed: Whether the operation is confirmed
        precomputed_changes: Optional dictionary with precomputed changes
        exclusion_config: Optional dictionary with exclusion configs
        stage: Stage in the sync process for 'all' operations

    Returns:
        Dictionary with standardized sync results (SyncResponse.to_dict())
    """
    if action == 'clear':
        from sql.helpers.db_helper import clear_db
        clear_db()
        return SyncResponse(
            success=True,
            action="clear",
            stage="sync_complete",
            message="Database cleared successfully",
            stats=SyncStats(),
            details=SyncDetails(operation_type="clear")
        ).to_dict()

    elif action == 'playlists':
        return handle_playlists_sync(force_refresh, is_confirmed, precomputed_changes, exclusion_config)

    elif action == 'tracks':
        return handle_tracks_sync(master_playlist_id, force_refresh, is_confirmed, precomputed_changes)

    elif action == 'associations':
        return handle_associations_sync(master_playlist_id, force_refresh, is_confirmed, precomputed_changes,
                                        exclusion_config)

    elif action == 'all':
        return handle_sequential_sync(stage, master_playlist_id, force_refresh, is_confirmed, precomputed_changes,
                                      exclusion_config)

    else:
        raise ValueError(f"Invalid action: {action}")


def handle_playlists_sync(force_refresh, is_confirmed, precomputed_changes, exclusion_config):
    """Handle playlist sync operations."""
    if not is_confirmed:
        added_count, updated_count, unchanged_count, deleted_count, changes_details = analyze_playlists_changes(
            force_full_refresh=force_refresh, exclusion_config=exclusion_config
        )

        formatted_to_add = [format_playlist_item(item) for item in changes_details['to_add']]
        formatted_to_update = [format_playlist_item(item) for item in changes_details['to_update']]
        formatted_to_delete = [format_playlist_item(item) for item in changes_details['to_delete']]

        stats = SyncStats(
            added=added_count,
            updated=updated_count,
            deleted=deleted_count,
            unchanged=unchanged_count
        )

        details = PlaylistSyncDetails(
            items_to_add=formatted_to_add,
            items_to_update=formatted_to_update,
            items_to_delete=formatted_to_delete,
            total_items_to_add=added_count,
            total_items_to_update=updated_count,
            total_items_to_delete=deleted_count
        )

        return create_analysis_response("playlists", stats, details).to_dict()

    else:
        normalized_changes = normalize_precomputed_changes(precomputed_changes)

        added, updated, unchanged, deleted = sync_playlists_to_db(
            force_full_refresh=force_refresh,
            skip_confirmation=True,
            precomputed_changes=normalized_changes,
            exclusion_config=exclusion_config
        )

        stats = SyncStats(
            added=added,
            updated=updated,
            deleted=deleted,
            unchanged=unchanged
        )

        return create_execution_response("playlists", stats).to_dict()


def handle_tracks_sync(master_playlist_id, force_refresh, is_confirmed, precomputed_changes):
    """Handle track sync operations."""
    if not is_confirmed:
        tracks_to_add, tracks_to_update, unchanged_tracks, tracks_to_delete = analyze_tracks_changes(
            master_playlist_id
        )

        formatted_to_add = [format_track_item(track) for track in tracks_to_add]
        formatted_to_update = [format_track_item(track) for track in tracks_to_update]
        formatted_to_delete = [format_track_item(track) for track in tracks_to_delete]

        stats = SyncStats(
            added=len(tracks_to_add),
            updated=len(tracks_to_update),
            deleted=len(tracks_to_delete),
            unchanged=len(unchanged_tracks) if isinstance(unchanged_tracks, list) else unchanged_tracks
        )

        details = TrackSyncDetails(
            items_to_add=formatted_to_add,
            items_to_update=formatted_to_update,
            items_to_delete=formatted_to_delete,
            total_items_to_add=len(tracks_to_add),
            total_items_to_update=len(tracks_to_update),
            total_items_to_delete=len(tracks_to_delete)
        )

        return create_analysis_response("tracks", stats, details).to_dict()

    else:
        normalized_changes = normalize_precomputed_changes(precomputed_changes)

        added, updated, unchanged, deleted = sync_tracks_to_db(
            master_playlist_id,
            force_full_refresh=force_refresh,
            auto_confirm=True,
            precomputed_changes=normalized_changes
        )

        stats = SyncStats(
            added=added,
            updated=updated,
            deleted=deleted,
            unchanged=unchanged
        )

        return create_execution_response("tracks", stats).to_dict()


def handle_associations_sync(master_playlist_id, force_refresh, is_confirmed, precomputed_changes, exclusion_config):
    """Handle association sync operations."""
    if not is_confirmed:
        associations_changes = analyze_track_playlist_associations(
            master_playlist_id,
            force_full_refresh=force_refresh,
            exclusion_config=exclusion_config
        )

        formatted_changes = [format_association_item(item) for item in associations_changes['tracks_with_changes']]

        stats = SyncStats(
            added=associations_changes['associations_to_add'],
            deleted=associations_changes['associations_to_remove'],
            # For associations, we use added/deleted instead of updated
            updated=0,
            unchanged=0
        )

        details = AssociationSyncDetails(
            items_to_add=[],  # Associations don't have simple "items to add"
            items_to_update=[],
            items_to_delete=[],
            tracks_with_changes=formatted_changes,
            changed_playlists=associations_changes.get('changed_playlists', []),
            associations_to_add=associations_changes['associations_to_add'],
            associations_to_remove=associations_changes['associations_to_remove'],
            total_items_to_add=len(formatted_changes),
            total_items_to_update=0,
            total_items_to_delete=0
        )

        message = f"Analysis complete: {associations_changes['associations_to_add']} associations to add, {associations_changes['associations_to_remove']} to remove, affecting {len(associations_changes['tracks_with_changes'])} tracks"

        return create_analysis_response("associations", stats, details, message).to_dict()

    else:
        normalized_changes = normalize_precomputed_changes(precomputed_changes)

        sync_stats = sync_track_playlist_associations_to_db(
            master_playlist_id,
            force_full_refresh=force_refresh,
            auto_confirm=True,
            precomputed_changes=normalized_changes,
            exclusion_config=exclusion_config
        )

        stats = SyncStats(
            added=sync_stats['associations_added'],
            deleted=sync_stats['associations_removed'],
            updated=0,
            unchanged=0
        )

        message = f"Associations synced: {sync_stats['associations_added']} added, {sync_stats['associations_removed']} removed"

        return create_execution_response("associations", stats, message).to_dict()


def handle_sequential_sync(stage, master_playlist_id, force_refresh, is_confirmed, precomputed_changes,
                           exclusion_config):
    """Handle sequential 'all' sync operations."""
    if stage == 'start':
        return SyncResponse(
            success=True,
            action="all",
            stage="start",
            message="Starting sequential sync process...",
            stats=SyncStats(),
            details=SyncDetails(operation_type="all"),
            next_stage="playlists"
        ).to_dict()

    elif stage == 'playlists':
        if not is_confirmed:
            # Analysis phase for playlists in sequential mode
            response = handle_playlists_sync(force_refresh, False, None, exclusion_config)
            # Modify response for sequential context
            response['action'] = 'all'
            response['stage'] = 'playlists'  # Override the stage to match sequential expectations
            response['next_stage'] = 'tracks'
            response['needs_confirmation'] = True  # Always show confirmation in sequential mode
            return response
        else:
            # Execution phase for playlists in sequential mode
            # Normalize precomputed changes before passing to handle function
            normalized_changes = normalize_precomputed_changes(precomputed_changes)
            response = handle_playlists_sync(force_refresh, True, normalized_changes, exclusion_config)
            response['action'] = 'all'
            response['stage'] = 'sync_complete'  # Keep sync_complete for execution
            response['next_stage'] = 'tracks'
            return response

    elif stage == 'tracks':
        if not is_confirmed:
            # Analysis phase for tracks in sequential mode
            response = handle_tracks_sync(master_playlist_id, force_refresh, False, None)
            response['action'] = 'all'
            response['stage'] = 'tracks'  # Override the stage to match sequential expectations
            response['next_stage'] = 'associations'
            response['needs_confirmation'] = True
            return response
        else:
            # Execution phase for tracks in sequential mode
            # Normalize precomputed changes before passing to handle function
            normalized_changes = normalize_precomputed_changes(precomputed_changes)
            response = handle_tracks_sync(master_playlist_id, force_refresh, True, normalized_changes)
            response['action'] = 'all'
            response['stage'] = 'sync_complete'  # Keep sync_complete for execution
            response['next_stage'] = 'associations'
            return response

    elif stage == 'associations':
        if not is_confirmed:
            # Analysis phase for associations in sequential mode
            response = handle_associations_sync(master_playlist_id, force_refresh, False, None, exclusion_config)
            response['action'] = 'all'
            response['stage'] = 'associations'  # Override the stage to match sequential expectations
            response['next_stage'] = 'complete'
            response['needs_confirmation'] = True
            return response
        else:
            # Execution phase for associations in sequential mode
            # Normalize precomputed changes before passing to handle function
            normalized_changes = normalize_precomputed_changes(precomputed_changes)
            response = handle_associations_sync(master_playlist_id, force_refresh, True, normalized_changes,
                                                exclusion_config)
            response['action'] = 'all'
            response['stage'] = 'sync_complete'  # Keep sync_complete for execution
            response['next_stage'] = 'complete'
            return response

    elif stage == 'complete':
        return SyncResponse(
            success=True,
            action="all",
            stage="complete",
            message="Sequential database sync completed successfully",
            stats=SyncStats(),
            details=SyncDetails(operation_type="all")
        ).to_dict()

    else:
        raise ValueError(f"Unknown stage: {stage}")
