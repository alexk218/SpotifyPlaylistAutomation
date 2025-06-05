"""
Standardized response models for sync operations.
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict


@dataclass
class SyncStats:
    """Standardized statistics for all sync operations."""
    added: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0


@dataclass
class SyncDetails:
    """Base class for sync operation details."""
    operation_type: str
    items_to_add: List[Dict[str, Any]] = None
    items_to_update: List[Dict[str, Any]] = None
    items_to_delete: List[Dict[str, Any]] = None
    preview_limit: int = 20
    total_items_to_add: int = 0
    total_items_to_update: int = 0
    total_items_to_delete: int = 0

    def __post_init__(self):
        # Initialize empty lists if None
        if self.items_to_add is None:
            self.items_to_add = []
        if self.items_to_update is None:
            self.items_to_update = []
        if self.items_to_delete is None:
            self.items_to_delete = []

        # Set totals based on actual list lengths if not provided
        if self.total_items_to_add == 0:
            self.total_items_to_add = len(self.items_to_add)
        if self.total_items_to_update == 0:
            self.total_items_to_update = len(self.items_to_update)
        if self.total_items_to_delete == 0:
            self.total_items_to_delete = len(self.items_to_delete)


@dataclass
class PlaylistSyncDetails(SyncDetails):
    """Details specific to playlist sync operations."""
    operation_type: str = "playlists"


@dataclass
class TrackSyncDetails(SyncDetails):
    """Details specific to track sync operations."""
    operation_type: str = "tracks"


@dataclass
class AssociationSyncDetails(SyncDetails):
    """Details specific to association sync operations."""
    operation_type: str = "associations"
    tracks_with_changes: List[Dict[str, Any]] = None
    changed_playlists: List[Dict[str, Any]] = None
    associations_to_add: int = 0
    associations_to_remove: int = 0

    def __post_init__(self):
        super().__post_init__()
        if self.tracks_with_changes is None:
            self.tracks_with_changes = []
        if self.changed_playlists is None:
            self.changed_playlists = []


@dataclass
class SyncResponse:
    """Standardized response for all sync operations."""
    success: bool
    action: str  # 'playlists', 'tracks', 'associations', 'all'
    stage: str  # 'analysis', 'sync_complete', 'start'
    message: str
    stats: SyncStats
    details: SyncDetails
    needs_confirmation: bool = False
    next_stage: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert details to dict properly
        result['details'] = asdict(self.details)
        result['stats'] = asdict(self.stats)
        return result


def create_analysis_response(
        action: str,
        stats: SyncStats,
        details: SyncDetails,
        message: str = None,
        next_stage: str = None
) -> SyncResponse:
    """Helper function to create standardized analysis responses."""
    if message is None:
        message = f"Analysis complete: {stats.added} to add, {stats.updated} to update, {stats.deleted} to delete, {stats.unchanged} unchanged"

    needs_confirmation = stats.added > 0 or stats.updated > 0 or stats.deleted > 0

    return SyncResponse(
        success=True,
        action=action,
        stage="analysis",
        message=message,
        stats=stats,
        details=details,
        needs_confirmation=needs_confirmation,
        next_stage=next_stage
    )


def create_execution_response(
        action: str,
        stats: SyncStats,
        message: str = None,
        next_stage: str = None
) -> SyncResponse:
    """Helper function to create standardized execution responses."""
    if message is None:
        if action == "playlists":
            message = f"Playlists synced: {stats.added} added, {stats.updated} updated, {stats.unchanged} unchanged, {stats.deleted} deleted"
        elif action == "tracks":
            message = f"Tracks synced: {stats.added} added, {stats.updated} updated, {stats.unchanged} unchanged, {stats.deleted} deleted"
        elif action == "associations":
            message = f"Associations synced: {stats.added} added, {stats.deleted} removed"
        else:
            message = f"Sync complete: {stats.added} added, {stats.updated} updated, {stats.unchanged} unchanged, {stats.deleted} deleted"

    # Create minimal details for execution responses
    details = SyncDetails(
        operation_type=action,
        items_to_add=[],
        items_to_update=[],
        items_to_delete=[]
    )

    return SyncResponse(
        success=True,
        action=action,
        stage="sync_complete",
        message=message,
        stats=stats,
        details=details,
        needs_confirmation=False,
        next_stage=next_stage
    )


# NEW: Helper function to convert SyncResponse to flattened precomputed changes
def create_flattened_precomputed_changes(sync_response: SyncResponse) -> Dict[str, Any]:
    """
    Convert a SyncResponse to the flattened format expected by sync functions.

    Args:
        sync_response: The SyncResponse from analysis phase

    Returns:
        Dictionary in flattened format for sync functions
    """
    details = sync_response.details
    stats = sync_response.stats

    # Base flattened structure that all sync functions expect
    flattened = {
        'items_to_add': details.items_to_add if details.items_to_add else [],
        'items_to_update': details.items_to_update if details.items_to_update else [],
        'items_to_delete': details.items_to_delete if details.items_to_delete else [],
        'unchanged_count': stats.unchanged
    }

    # Add association-specific fields if this is an association sync
    if isinstance(details, AssociationSyncDetails):
        flattened.update({
            'tracks_with_changes': details.tracks_with_changes,
            'changed_playlists': details.changed_playlists,
            'associations_to_add': details.associations_to_add,
            'associations_to_remove': details.associations_to_remove
        })

    return flattened


# NEW: Helper function to convert client precomputed changes to flattened format
def normalize_precomputed_changes(precomputed_changes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert any format of precomputed changes to the standardized flattened format.
    Handles both nested (SyncResponse) and already-flattened formats.

    Args:
        precomputed_changes: Dictionary that might be nested or flattened

    Returns:
        Dictionary in standardized flattened format
    """
    if not precomputed_changes:
        return {}

    # If it's already flattened (has items_to_add directly), return as-is
    if 'items_to_add' in precomputed_changes:
        return precomputed_changes

    # If it's nested (SyncResponse format), flatten it
    if 'details' in precomputed_changes and 'stats' in precomputed_changes:
        details = precomputed_changes['details']
        stats = precomputed_changes['stats']

        flattened = {
            'items_to_add': details.get('items_to_add', []),
            'items_to_update': details.get('items_to_update', []),
            'items_to_delete': details.get('items_to_delete', []),
            'unchanged_count': stats.get('unchanged', 0)
        }

        # Add association-specific fields if present
        if details.get('operation_type') == 'associations':
            flattened.update({
                'tracks_with_changes': details.get('tracks_with_changes', []),
                'changed_playlists': details.get('changed_playlists', []),
                'associations_to_add': details.get('associations_to_add', 0),
                'associations_to_remove': details.get('associations_to_remove', 0)
            })

        return flattened

    # Fallback: try to extract what we can
    return {
        'items_to_add': precomputed_changes.get('items_to_add', []),
        'items_to_update': precomputed_changes.get('items_to_update', []),
        'items_to_delete': precomputed_changes.get('items_to_delete', []),
        'unchanged_count': precomputed_changes.get('unchanged_count', 0)
    }


def format_playlist_item(playlist_data: Dict[str, Any]) -> Dict[str, Any]:
    """Format playlist data for consistent API responses."""
    return {
        "id": playlist_data.get('id'),
        "name": playlist_data.get('name', ''),
        "old_name": playlist_data.get('old_name'),
        "snapshot_id": playlist_data.get('snapshot_id'),
        "old_snapshot_id": playlist_data.get('old_snapshot_id')
    }


def format_track_item(track_data: Dict[str, Any]) -> Dict[str, Any]:
    """Format track data for consistent API responses."""
    return {
        "id": track_data.get('id'),
        "artists": track_data.get('artists', 'Unknown Artist'),
        "title": track_data.get('title', 'Unknown Title'),
        "album": track_data.get('album', 'Unknown Album'),
        "is_local": track_data.get('is_local', False),
        "added_at": track_data.get('added_at'),
        "old_artists": track_data.get('old_artists'),
        "old_title": track_data.get('old_title'),
        "old_album": track_data.get('old_album'),
        "changes": track_data.get('changes', [])
    }


def format_association_item(association_data: Dict[str, Any]) -> Dict[str, Any]:
    """Format association data for consistent API responses."""
    return {
        "track_id": association_data.get('track_id'),
        "track_info": association_data.get('track_info'),
        "title": association_data.get('title'),
        "artists": association_data.get('artists'),
        "add_to": association_data.get('add_to', []),
        "remove_from": association_data.get('remove_from', [])
    }
