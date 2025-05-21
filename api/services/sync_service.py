import json
import os
import threading
import traceback
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

from drivers.spotify_client import (
    authenticate_spotify, sync_to_master_playlist, sync_unplaylisted_to_unsorted
)
from helpers.sync_helper import (
    analyze_playlists_changes, analyze_tracks_changes, analyze_track_playlist_associations,
    sync_playlists_to_db, sync_tracks_to_db, sync_track_playlist_associations_to_db
)

load_dotenv()


def get_exclusion_config(request_json=None):
    """
    Get the exclusion configuration for playlists.

    Args:
        request_json: Optional dictionary with request data

    Returns:
        Dictionary with exclusion configuration
    """
    # Find the path to the exclusion config
    project_root = Path(__file__).resolve().parent.parent.parent
    config_path = project_root / 'exclusion_config.json'

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


def sync_master_playlist(master_playlist_id, request_json=None):
    """
    Sync all tracks from all playlists to MASTER playlist.

    Args:
        master_playlist_id: ID of the master playlist
        request_json: Optional dictionary with request data

    Returns:
        Success status
    """
    exclusion_config = get_exclusion_config(request_json)

    try:
        spotify_client = authenticate_spotify()

        # Start a background thread for this operation
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

        return {
            "success": True,
            "message": "Sync to master playlist started. This operation runs in the background and may take several minutes."
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


def handle_db_sync(action, master_playlist_id, force_refresh, is_confirmed, precomputed_changes=None,
                   exclusion_config=None):
    """
    Handle database sync operations.

    Args:
        action: Type of sync operation ('playlists', 'tracks', 'associations', 'all', 'clear')
        master_playlist_id: ID of the master playlist
        force_refresh: Whether to force a full refresh
        is_confirmed: Whether the operation is confirmed
        precomputed_changes: Optional dictionary with precomputed changes
        exclusion_config: Optional dictionary with exclusion configuration

    Returns:
        Dictionary with sync results
    """
    if action == 'clear':
        from sql.helpers.db_helper import clear_db
        clear_db()
        return {"success": True, "message": "Database cleared successfully"}

    elif action == 'playlists':
        # If not confirmed, return the analysis result
        if not is_confirmed:
            added_count, updated_count, unchanged_count, deleted_count, changes_details = analyze_playlists_changes(
                force_full_refresh=force_refresh, exclusion_config=exclusion_config
            )

            return {
                "success": True,
                "action": "playlists",
                "stage": "analysis",
                "message": f"Analysis complete: {added_count} to add, {updated_count} to update, {deleted_count} to delete, {unchanged_count} unchanged",
                "stats": {
                    "added": added_count,
                    "updated": updated_count,
                    "unchanged": unchanged_count,
                    "deleted": deleted_count
                },
                "details": changes_details,
                "needs_confirmation": added_count > 0 or updated_count > 0 or deleted_count > 0
            }

        # Otherwise, proceed with execution
        playlist_changes_from_analysis = precomputed_changes
        added, updated, unchanged, deleted = sync_playlists_to_db(
            force_full_refresh=force_refresh,
            auto_confirm=True,
            precomputed_changes=playlist_changes_from_analysis,
            exclusion_config=exclusion_config
        )
        return {
            "success": True,
            "action": "playlists",
            "stage": "sync_complete",
            "message": f"Playlists synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
            "stats": {
                "added": added,
                "updated": updated,
                "unchanged": unchanged,
                "deleted": deleted
            }
        }

    elif action == 'tracks':
        # If not confirmed, return the analysis result
        if not is_confirmed:
            tracks_to_add, tracks_to_update, unchanged_tracks = analyze_tracks_changes(
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

            return {
                "success": True,
                "action": "tracks",
                "stage": "analysis",
                "message": f"Analysis complete: {len(tracks_to_add)} to add, {len(tracks_to_update)} to update, {len(unchanged_tracks)} unchanged",
                "stats": {
                    "added": len(tracks_to_add),
                    "updated": len(tracks_to_update),
                    "unchanged": len(unchanged_tracks)
                },
                "details": {
                    "all_items_to_add": all_tracks_to_add,
                    "to_add": all_tracks_to_add[:20],  # First 20 for immediate display
                    "to_add_total": len(tracks_to_add),
                    "all_items_to_update": all_tracks_to_update,
                    "to_update": all_tracks_to_update[:20],
                    "to_update_total": len(tracks_to_update)
                },
                "needs_confirmation": len(tracks_to_add) > 0 or len(tracks_to_update) > 0
            }

        # Otherwise, proceed with execution
        track_changes_from_analysis = precomputed_changes
        added, updated, unchanged, deleted = sync_tracks_to_db(
            master_playlist_id,
            force_full_refresh=force_refresh,
            auto_confirm=True,
            precomputed_changes=track_changes_from_analysis
        )
        return {
            "success": True,
            "action": "tracks",
            "stage": "sync_complete",
            "message": f"Tracks synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
            "stats": {
                "added": added,
                "updated": updated,
                "unchanged": unchanged,
                "deleted": deleted
            }
        }

    elif action == 'associations':
        # If not confirmed, return the analysis result
        if not is_confirmed:
            associations_changes = analyze_track_playlist_associations(
                master_playlist_id,
                force_full_refresh=force_refresh,
                exclusion_config=exclusion_config
            )

            return {
                "success": True,
                "action": "associations",
                "stage": "analysis",
                "message": f"Analysis complete: {associations_changes['associations_to_add']} to add, "
                           f"{associations_changes['associations_to_remove']} to remove, "
                           f"affecting {len(associations_changes['tracks_with_changes'])} tracks",
                "stats": associations_changes['stats'],
                "details": {
                    "tracks_with_changes": associations_changes['tracks_with_changes'],
                    "associations_to_add": associations_changes['associations_to_add'],
                    "associations_to_remove": associations_changes['associations_to_remove'],
                    "samples": associations_changes['samples'],
                    "all_changes": associations_changes.get('all_changes', associations_changes['samples'])
                },
                "needs_confirmation": associations_changes['associations_to_add'] > 0 or
                                      associations_changes['associations_to_remove'] > 0
            }

        # Otherwise, proceed with execution
        associations_changes_from_analysis = precomputed_changes
        stats = sync_track_playlist_associations_to_db(
            master_playlist_id,
            force_full_refresh=force_refresh,
            auto_confirm=True,
            precomputed_changes=associations_changes_from_analysis,
            exclusion_config=exclusion_config
        )
        return {
            "success": True,
            "action": "associations",
            "stage": "sync_complete",
            "message": f"Associations synced: {stats['associations_added']} added, {stats['associations_removed']} removed",
            "stats": stats
        }

    elif action == 'all':
        # For 'all', handle one stage at a time
        stage = precomputed_changes.get('stage', 'start') if precomputed_changes else 'start'

        if stage == 'start':
            # Just return initial instructions to begin with playlists
            return {
                "success": True,
                "action": "all",
                "stage": "start",
                "next_stage": "playlists",
                "message": "Starting sequential sync process..."
            }

        elif stage == 'playlists':
            # Process the playlists stage
            playlists_result = handle_db_sync(
                'playlists',
                master_playlist_id,
                force_refresh,
                is_confirmed,
                precomputed_changes.get('precomputed_changes_from_analysis') if precomputed_changes else None,
                exclusion_config
            )

            # Add next stage info only if this is a complete sync operation
            if is_confirmed and 'stage' in playlists_result and playlists_result['stage'] == 'sync_complete':
                playlists_result["next_stage"] = "tracks"
            return playlists_result

        elif stage == 'tracks':
            # Process the tracks stage
            tracks_result = handle_db_sync(
                'tracks',
                master_playlist_id,
                force_refresh,
                is_confirmed,
                precomputed_changes.get('precomputed_changes_from_analysis') if precomputed_changes else None,
                exclusion_config
            )

            # Add next stage info only if this is a complete sync operation
            if is_confirmed and 'stage' in tracks_result and tracks_result['stage'] == 'sync_complete':
                tracks_result["next_stage"] = "associations"
            return tracks_result

        elif stage == 'associations':
            # Process the associations stage
            associations_result = handle_db_sync(
                'associations',
                master_playlist_id,
                force_refresh,
                is_confirmed,
                precomputed_changes.get('precomputed_changes_from_analysis') if precomputed_changes else None,
                exclusion_config
            )

            # Add next stage info only if this is a complete sync operation
            if is_confirmed and 'stage' in associations_result and associations_result['stage'] == 'sync_complete':
                associations_result["next_stage"] = "complete"
            return associations_result

        elif stage == 'complete':
            # Final completion stage
            return {
                "success": True,
                "action": "all",
                "stage": "complete",
                "message": "Sequential database sync completed successfully"
            }

        else:
            # Handle unknown stage
            raise ValueError(f"Unknown stage: {stage}")

    else:
        # Invalid action
        raise ValueError(f"Invalid action: {action}")
