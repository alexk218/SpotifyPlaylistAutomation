from abc import ABC, abstractmethod
from typing import Dict, Any

from helpers.sync_helper import analyze_playlists_changes, sync_playlists_to_db, analyze_tracks_changes, \
    analyze_track_playlist_associations, sync_track_playlist_associations_to_db, sync_tracks_to_db


class SyncOperation(ABC):
    def __init__(self, force_refresh: bool = False, exclusion_config: dict = None):
        self.force_refresh = force_refresh
        self.exclusion_config = exclusion_config

    @abstractmethod
    def analyze(self, **kwargs) -> Dict[str, Any]:
        """Analyze what changes would be made"""
        pass

    @abstractmethod
    def execute(self, precomputed_changes: Dict = None, **kwargs) -> Dict[str, Any]:
        """Execute the changes"""
        pass

    def get_action_name(self) -> str:
        return self.__class__.__name__.lower().replace('syncoperation', '')


class PlaylistSyncOperation(SyncOperation):
    def analyze(self, **kwargs):
        added, updated, unchanged, deleted, details = analyze_playlists_changes(
            force_full_refresh=self.force_refresh,
            exclusion_config=self.exclusion_config
        )
        return {
            "success": True,
            "action": "playlists",
            "stage": "analysis",
            "message": f"Analysis complete: {added} to add, {updated} to update, {deleted} to delete, {unchanged} unchanged",
            "stats": {"added": added, "updated": updated, "unchanged": unchanged, "deleted": deleted},
            "details": details,
            "needs_confirmation": added > 0 or updated > 0 or deleted > 0
        }

    def execute(self, precomputed_changes=None, **kwargs):
        added, updated, unchanged, deleted = sync_playlists_to_db(
            force_full_refresh=self.force_refresh,
            auto_confirm=True,
            precomputed_changes=precomputed_changes,
            exclusion_config=self.exclusion_config
        )
        return {
            "success": True,
            "action": "playlists",
            "stage": "sync_complete",
            "message": f"Playlists synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
            "stats": {"added": added, "updated": updated, "unchanged": unchanged, "deleted": deleted}
        }


class TrackSyncOperation(SyncOperation):
    def analyze(self, master_playlist_id: str, **kwargs):
        tracks_to_add, tracks_to_update, unchanged_tracks, tracks_to_delete = analyze_tracks_changes(
            master_playlist_id, force_full_refresh=self.force_refresh
        )

        # Format tracks for display
        formatted_details = self._format_track_details(tracks_to_add, tracks_to_update, tracks_to_delete)

        return {
            "success": True,
            "action": "tracks",
            "stage": "analysis",
            "message": f"Analysis complete: {len(tracks_to_add)} to add, {len(tracks_to_update)} to update, {len(tracks_to_delete)} to delete, {len(unchanged_tracks)} unchanged",
            "stats": {
                "added": len(tracks_to_add),
                "updated": len(tracks_to_update),
                "deleted": len(tracks_to_delete),
                "unchanged": len(unchanged_tracks)
            },
            "details": formatted_details,
            "needs_confirmation": len(tracks_to_add) > 0 or len(tracks_to_update) > 0 or len(tracks_to_delete) > 0
        }

    def execute(self, master_playlist_id: str, precomputed_changes=None, **kwargs):
        track_changes = self._extract_track_changes(precomputed_changes)
        added, updated, unchanged, deleted = sync_tracks_to_db(
            master_playlist_id,
            force_full_refresh=self.force_refresh,
            auto_confirm=True,
            precomputed_changes=track_changes
        )
        return {
            "success": True,
            "action": "tracks",
            "stage": "sync_complete",
            "message": f"Tracks synced: {added} added, {updated} updated, {unchanged} unchanged, {deleted} deleted",
            "stats": {"added": added, "updated": updated, "unchanged": unchanged, "deleted": deleted}
        }

    def _format_track_details(self, tracks_to_add, tracks_to_update, tracks_to_delete):
        """Format track data for API response"""
        format_track = lambda track: {
            "id": track.get('id'),
            "artists": track['artists'],
            "title": track['title'],
            "album": track.get('album', 'Unknown Album'),
            "is_local": track.get('is_local', False),
            "added_at": track.get('added_at')
        }

        all_tracks_to_add = [format_track(track) for track in tracks_to_add]
        all_tracks_to_update = [
            {**format_track(track),
             "old_artists": track['old_artists'],
             "old_title": track['old_title'],
             "old_album": track.get('old_album', 'Unknown Album'),
             "changes": track['changes']}
            for track in tracks_to_update
        ]
        all_tracks_to_delete = [format_track(track) for track in tracks_to_delete]

        return {
            "all_items_to_add": all_tracks_to_add,
            "to_add": all_tracks_to_add[:20],
            "to_add_total": len(tracks_to_add),
            "all_items_to_update": all_tracks_to_update,
            "to_update": all_tracks_to_update[:20],
            "to_update_total": len(tracks_to_update),
            "all_items_to_delete": all_tracks_to_delete,
            "to_delete": all_tracks_to_delete[:20],
            "to_delete_total": len(tracks_to_delete)
        }

    def _extract_track_changes(self, precomputed_changes):
        """Extract properly structured changes from analysis details"""
        if 'details' in precomputed_changes:
            return {
                'tracks_to_add': precomputed_changes['details'].get('all_items_to_add', []),
                'tracks_to_update': precomputed_changes['details'].get('all_items_to_update', []),
                'tracks_to_delete': precomputed_changes['details'].get('all_items_to_delete', []),
                'unchanged_tracks': precomputed_changes['stats'].get('unchanged', 0)
            }
        return precomputed_changes


class AssociationSyncOperation(SyncOperation):
    def analyze(self, master_playlist_id: str, **kwargs):
        associations_changes = analyze_track_playlist_associations(
            master_playlist_id,
            force_full_refresh=self.force_refresh,
            exclusion_config=self.exclusion_config
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
            "needs_confirmation": associations_changes['associations_to_add'] > 0 or associations_changes[
                'associations_to_remove'] > 0
        }

    def execute(self, master_playlist_id: str, precomputed_changes=None, **kwargs):
        stats = sync_track_playlist_associations_to_db(
            master_playlist_id,
            force_full_refresh=self.force_refresh,
            auto_confirm=True,
            precomputed_changes=precomputed_changes,
            exclusion_config=self.exclusion_config
        )
        return {
            "success": True,
            "action": "associations",
            "stage": "sync_complete",
            "message": f"Associations synced: {stats['associations_added']} added, {stats['associations_removed']} removed",
            "stats": stats
        }
