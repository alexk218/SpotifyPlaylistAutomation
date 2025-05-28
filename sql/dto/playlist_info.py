from dataclasses import dataclass


@dataclass
class PlaylistInfo:
    name: str
    playlist_id: str
    snapshot_id: str
