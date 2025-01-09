from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from utils.logger import setup_logger
import os

# Set up logger for symlink tracking
# SYMLINK_LOG_FILE = os.path.join(os.path.dirname(__file__), '..', 'symlinks.log')
# symlink_logger = setup_logger('symlink_tracker', SYMLINK_LOG_FILE)
db_logger = setup_logger('db_logger', 'sql/db.log')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent

class SymlinkTracker:
    def __init__(self):
        self.created_symlinks: List[Tuple[str, str]] = []
        self.removed_symlinks: List[str] = []

    def log_symlink_operations(self, timestamp: str) -> None:
        """Generate a detailed log file of all symlink operations"""
        # Create logs directory and subdirectory
        logs_dir = project_root / 'logs'
        logs_dir.mkdir(exist_ok=True)
        symlinks_logs_dir = logs_dir / 'symlinks'
        symlinks_logs_dir.mkdir(exist_ok=True)

        # Create log file
        log_path = symlinks_logs_dir / f'symlink_operations_{timestamp}.log'

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("Symlink Operations Report\n")
            f.write("=======================\n\n")

            # Log removed symlinks
            f.write("=== Removed Broken Symlinks ===\n")
            if self.removed_symlinks:
                f.write(f"Total symlinks removed: {len(self.removed_symlinks)}\n\n")
                for link in self.removed_symlinks:
                    f.write(f"Removed: {link}\n")
            else:
                f.write("No broken symlinks were removed\n")

            # Log created symlinks
            f.write("\n=== Created Symlinks ===\n")
            if self.created_symlinks:
                f.write(f"Total new symlinks created: {len(self.created_symlinks)}\n\n")
                for link, target in self.created_symlinks:
                    f.write(f"Link: {link}\n")
                    f.write(f"Target: {target}\n\n")
            else:
                f.write("No new symlinks were created\n")

        db_logger.info(f"Symlink operations log saved to: {log_path}")

    @contextmanager
    def tracking_session(self):
        """Context manager for tracking symlink operations"""
        # Clear previous tracking data
        self.created_symlinks.clear()
        self.removed_symlinks.clear()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            yield self
        finally:
            # Generate log file with results
            self.log_symlink_operations(timestamp)


tracker = SymlinkTracker()
