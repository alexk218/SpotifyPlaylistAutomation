from contextlib import contextmanager
from typing import List, Tuple
from utils.logger import setup_logger
import os

# Set up logger for symlink tracking
SYMLINK_LOG_FILE = os.path.join(os.path.dirname(__file__), '..', 'symlinks.log')
symlink_logger = setup_logger('symlink_tracker', SYMLINK_LOG_FILE)


class SymlinkTracker:
    def __init__(self):
        self.created_symlinks: List[Tuple[str, str]] = []

    @contextmanager
    def tracking_session(self):
        self.created_symlinks.clear()
        try:
            yield self  # code pauses here while the 'with' block runs in organize_songs_into_playlists
        finally:
            # after the 'with' block finishes...
            if self.created_symlinks:
                summary = ["=== Summary of Created Symlinks ==="]
                for link, target in self.created_symlinks:
                    summary.append(f"{link} -> {target}")
                summary.append(f"Total new symlinks created: {len(self.created_symlinks)}")
                symlink_logger.info("\n".join(summary))


tracker = SymlinkTracker()
