#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define paths
PROJECT_ROOT = Path(__file__).resolve().parent

# Add project root to Python path for imports
sys.path.insert(0, str(PROJECT_ROOT))

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
HELPERS_DIR = PROJECT_ROOT / "helpers"

LOCAL_TRACKS_SERVER_PATH = SCRIPTS_DIR / "local_tracks_server.py"
SYNC_LOCAL_TRACKS_PATH = SCRIPTS_DIR / "sync_local_tracks.py"
EMBED_METADATA_SCRIPT = HELPERS_DIR / "file_helper.py"

# Get environment variables
MASTER_TRACKS_DIRECTORY_SSD = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
LOCAL_TRACKS_CACHE_DIRECTORY = os.getenv("LOCAL_TRACKS_CACHE_DIRECTORY")
DEFAULT_PORT = 8765


def run_command(command, wait=True):
    """Run a command and optionally wait for it to complete."""
    print(f"Running: {' '.join(str(c) for c in command)}")

    if wait:
        result = subprocess.run(command, check=False)
        return result.returncode
    else:
        # Run in background
        if sys.platform == 'win32':
            # Windows requires shell=True for background processes
            subprocess.Popen(' '.join(str(c) for c in command), shell=True)
        else:
            # Unix/Linux/Mac
            subprocess.Popen(command)
        return 0


def generate_local_tracks_cache(music_dir, output_dir):
    """Generate local tracks cache file."""
    print("\n=== Generating Local Tracks Cache ===")

    command = [sys.executable, str(SYNC_LOCAL_TRACKS_PATH),
               "--music-dir", music_dir,
               "--output-dir", output_dir]

    return run_command(command)


def start_local_tracks_server(port=DEFAULT_PORT, cache_path=None):
    """Start the local tracks server."""
    print("\n=== Starting Local Tracks Server ===")

    command = [sys.executable, str(LOCAL_TRACKS_SERVER_PATH), "--port", str(port)]
    if cache_path:
        command.extend(["--cache-path", cache_path])

    # Run server in background
    return run_command(command, wait=False)


def main():
    parser = argparse.ArgumentParser(description="Tagify Integration Tools")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Generate cache command
    cache_parser = subparsers.add_parser("cache", help="Generate local tracks cache")
    cache_parser.add_argument("--music-dir", type=str, default=MASTER_TRACKS_DIRECTORY_SSD,
                              help="Directory containing music files (default from .env)")
    cache_parser.add_argument("--output-dir", type=str, default=LOCAL_TRACKS_CACHE_DIRECTORY,
                              help="Directory to save cache files (default from .env)")

    # Server command
    server_parser = subparsers.add_parser("server", help="Start local tracks server")
    server_parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                               help="Port to run server on (default 8765)")
    server_parser.add_argument("--cache-path", type=str, default=LOCAL_TRACKS_CACHE_DIRECTORY,
                               help="Path to cache directory (default from .env)")

    # All-in-one command
    all_parser = subparsers.add_parser("all", help="Run cache and server in sequence")
    all_parser.add_argument("--music-dir", type=str, default=MASTER_TRACKS_DIRECTORY_SSD,
                            help="Directory containing music files (default from .env)")
    all_parser.add_argument("--cache-dir", type=str, default=LOCAL_TRACKS_CACHE_DIRECTORY,
                            help="Directory to save cache files (default from .env)")
    all_parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                            help="Port to run server on (default 8765)")

    args = parser.parse_args()

    if args.command == "cache":
        generate_local_tracks_cache(args.music_dir, args.output_dir)

    elif args.command == "server":
        start_local_tracks_server(args.port, args.cache_path)
        # Keep script running while server is active
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nServer stopped by user")

    elif args.command == "all":
        # Run all processes in sequence
        print("\n=== Running Tagify Integration ===")

        # 1. Generate cache
        generate_local_tracks_cache(args.music_dir, args.cache_dir)

        # 2. Start server
        start_local_tracks_server(args.port, args.cache_dir)

        # Keep script running while server is active
        print("\n=== Integration Complete ===")
        print(f"Local tracks server running at http://localhost:{args.port}")
        print("Your Spicetify Tagify app can now access your local tracks data")
        print("Press Ctrl+C to stop the server")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nServer stopped by user")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
