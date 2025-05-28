import json
import threading
import traceback
from pathlib import Path

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
                   exclusion_config=None, stage='start'):
    """
    Handle database sync operations.

    Args:
        action: Type of sync operation ('playlists', 'tracks', 'associations', 'all', 'clear')
        master_playlist_id: ID of the master playlist
        force_refresh: Whether to force a full refresh (not implemented yet...)
        is_confirmed: Whether the operation is confirmed (if confirmed - perform sync operation, otherwise just analyze)
        precomputed_changes: Optional dictionary with precomputed changes (obtained from analysis)
        exclusion_config: Optional dictionary with exclusion configs (which Spotify playlists to exclude)
        stage: when syncing 'all' - specifies which stage in the sync process (playlists/tracks/associations)

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
        # For 'all', process sequentially, handling one stage at a time
        if stage == 'start':
            return {
                "success": True,
                "action": "all",
                "stage": "start",
                "next_stage": "playlists",
                "message": "Starting sequential sync process..."
            }

        elif stage == 'playlists':
            if not is_confirmed:
                # Analyze playlists
                playlists_added, playlists_updated, playlists_unchanged, playlists_deleted, playlists_details = analyze_playlists_changes(
                    force_full_refresh=force_refresh, exclusion_config=exclusion_config
                )

                return {
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
                    "needs_confirmation": True  # Always show confirmation, even if no changes
                }
            else:
                # Execute playlists sync
                added, updated, unchanged, deleted = sync_playlists_to_db(
                    force_full_refresh=force_refresh,
                    auto_confirm=True,
                    precomputed_changes=precomputed_changes,
                    exclusion_config=exclusion_config
                )

                return {
                    "success": True,
                    "action": "all",
                    "stage": "sync_complete",  # Match old route behavior
                    "message": f"Playlists synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
                    "stats": {
                        "added": added,
                        "updated": updated,
                        "unchanged": unchanged,
                        "deleted": deleted
                    },
                    "next_stage": "tracks"
                }

        elif stage == 'tracks':
            if not is_confirmed:
                # Analyze tracks - make sure to capture ALL changes including deletions
                tracks_to_add, tracks_to_update, tracks_unchanged = analyze_tracks_changes(
                    master_playlist_id, force_full_refresh=force_refresh
                )

                # Format tracks for display
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

                # Get tracks to delete by calling the sync function in analysis mode
                # This ensures we capture the same logic as the actual sync
                try:
                    from helpers.sync_helper import get_db_tracks
                    from drivers.spotify_client import authenticate_spotify, fetch_master_tracks

                    existing_tracks = get_db_tracks()
                    spotify_client = authenticate_spotify()
                    master_tracks = fetch_master_tracks(spotify_client, master_playlist_id, force_refresh=force_refresh)

                    # Find tracks to delete (same logic as in sync_tracks_to_db)
                    master_track_ids = set()
                    for track_data in master_tracks:
                        track_id, track_title, artist_names, album_name, added_at = track_data
                        if track_id is None:  # Local file
                            from helpers.file_helper import generate_local_track_id
                            normalized_title = ''.join(c for c in track_title if c.isalnum() or c in ' &-_')
                            normalized_artist = ''.join(c for c in artist_names if c.isalnum() or c in ' &-_')
                            metadata = {'title': normalized_title, 'artist': normalized_artist}
                            track_id = generate_local_track_id(metadata)
                        master_track_ids.add(track_id)

                    tracks_to_delete = []
                    for track_id, track in existing_tracks.items():
                        if track_id not in master_track_ids:
                            tracks_to_delete.append({
                                'id': track_id,
                                'title': track.title,
                                'artists': track.artists,
                                'album': track.album,
                                'is_local': getattr(track, 'is_local', False)
                            })
                except Exception as e:
                    print(f"Error analyzing tracks to delete: {e}")
                    tracks_to_delete = []

                return {
                    "success": True,
                    "action": "all",
                    "stage": "tracks",
                    "message": f"Analysis complete: {len(tracks_to_add)} to add, {len(tracks_to_update)} to update, "
                               f"{len(tracks_to_delete)} to delete, {len(tracks_unchanged)} unchanged",
                    "stats": {
                        "added": len(tracks_to_add),
                        "updated": len(tracks_to_update),
                        "unchanged": len(tracks_unchanged),
                        "deleted": len(tracks_to_delete)
                    },
                    "details": {
                        "all_items_to_add": all_tracks_to_add,
                        "to_add": all_tracks_to_add[:20],
                        "to_add_total": len(tracks_to_add),
                        "all_items_to_update": all_tracks_to_update,
                        "to_update": all_tracks_to_update[:20],
                        "to_update_total": len(tracks_to_update),
                        "all_items_to_delete": tracks_to_delete,
                        "to_delete": tracks_to_delete[:20],
                        "to_delete_total": len(tracks_to_delete)
                    },
                    "next_stage": "associations",
                    "needs_confirmation": True  # Always show confirmation
                }
            else:
                # Execute tracks sync
                added, updated, unchanged, deleted = sync_tracks_to_db(
                    master_playlist_id,
                    force_full_refresh=force_refresh,
                    auto_confirm=True,
                    precomputed_changes=precomputed_changes
                )

                return {
                    "success": True,
                    "action": "all",
                    "stage": "sync_complete",
                    "message": f"Tracks synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
                    "stats": {
                        "added": added,
                        "updated": updated,
                        "unchanged": unchanged,
                        "deleted": deleted
                    },
                    "next_stage": "associations"
                }

        elif stage == 'associations':
            if not is_confirmed:
                # Analyze associations
                associations_changes = analyze_track_playlist_associations(
                    master_playlist_id,
                    force_full_refresh=force_refresh,
                    exclusion_config=exclusion_config
                )

                return {
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
                        "all_changes": associations_changes.get('tracks_with_changes', [])
                    },
                    "next_stage": "complete",
                    "needs_confirmation": True  # Always show confirmation
                }
            else:
                # Execute associations sync
                stats = sync_track_playlist_associations_to_db(
                    master_playlist_id,
                    force_full_refresh=force_refresh,
                    auto_confirm=True,
                    precomputed_changes=precomputed_changes,
                    exclusion_config=exclusion_config
                )

                return {
                    "success": True,
                    "action": "all",
                    "stage": "sync_complete",
                    "message": f"Associations synced: {stats['associations_added']} added, {stats['associations_removed']} removed",
                    "stats": stats,
                    "next_stage": "complete"
                }

        elif stage == 'complete':
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
