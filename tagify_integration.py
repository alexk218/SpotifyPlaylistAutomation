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

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
HELPERS_DIR = PROJECT_ROOT / "helpers"

LOCAL_TRACKS_SERVER_PATH = SCRIPTS_DIR / "local_tracks_server.py"
SYNC_LOCAL_TRACKS_PATH = SCRIPTS_DIR / "sync_local_tracks.py"
EMBED_METADATA_SCRIPT = HELPERS_DIR / "file_helper.py"

# Get environment variables
MASTER_TRACKS_DIRECTORY = os.getenv("MASTER_TRACKS_DIRECTORY")
LOCAL_TRACKS_CACHE_DIRECTORY = os.getenv("LOCAL_TRACKS_CACHE_DIRECTORY")
DEFAULT_PORT = 8765


def run_command(command, wait=True):
    """Run a command and optionally wait for it to complete."""
    print(f"Running: {' '.join(command)}")

    if wait:
        result = subprocess.run(command, check=False)
        return result.returncode
    else:
        # Run in background
        if sys.platform == 'win32':
            # Windows requires shell=True for background processes
            subprocess.Popen(' '.join(command), shell=True)
        else:
            # Unix/Linux/Mac
            subprocess.Popen(command)
        return 0


def embed_metadata(interactive=False):
    """Embed TrackId metadata into MP3 files."""
    print("\n=== Embedding Spotify TrackIds into MP3 files ===")

    command = [sys.executable, str(EMBED_METADATA_SCRIPT), "--embed-metadata"]
    if interactive:
        command.append("--interactive")

    return run_command(command)


def generate_local_tracks_cache(music_dir=None, output_dir=None, master_only=True, report=True):
    """Generate local tracks cache file."""
    print("\n=== Generating Local Tracks Cache ===")

    command = [sys.executable, str(SYNC_LOCAL_TRACKS_PATH)]

    if music_dir:
        command.extend(["--music-dir", music_dir])
    if output_dir:
        command.extend(["--output-dir", output_dir])
    if master_only:
        command.append("--master-only")
    if report:
        command.append("--report")

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

    # Main command groups
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Embed metadata command
    embed_parser = subparsers.add_parser("embed", help="Embed Spotify TrackIds into MP3 files")
    embed_parser.add_argument("--interactive", action="store_true", help="Enable interactive mode for fuzzy matching")

    # Generate cache command
    cache_parser = subparsers.add_parser("cache", help="Generate local tracks cache")
    cache_parser.add_argument("--music-dir", type=str, help="Directory containing music files")
    cache_parser.add_argument("--output-dir", type=str, help="Directory to save cache files")
    cache_parser.add_argument("--all-tracks", action="store_true", help="Include all tracks, not just MASTER playlist")
    cache_parser.add_argument("--no-report", action="store_true", help="Don't generate missing tracks report")

    # Server command
    server_parser = subparsers.add_parser("server", help="Start local tracks server")
    server_parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to run server on")
    server_parser.add_argument("--cache-path", type=str, help="Path to cache directory")

    # All-in-one command
    all_parser = subparsers.add_parser("all", help="Run embed, cache, and server in sequence")
    all_parser.add_argument("--interactive", action="store_true", help="Enable interactive mode for embedding")
    all_parser.add_argument("--music-dir", type=str, help="Directory containing music files")
    all_parser.add_argument("--cache-dir", type=str, help="Directory to save cache files")
    all_parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to run server on")
    all_parser.add_argument("--all-tracks", action="store_true", help="Include all tracks, not just MASTER playlist")

    args = parser.parse_args()

    if args.command == "embed":
        embed_metadata(args.interactive)

    elif args.command == "cache":
        generate_local_tracks_cache(
            args.music_dir,
            args.output_dir,
            not args.all_tracks,
            not args.no_report
        )

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
        print("\n=== Running Complete Tagify Integration ===")

        music_dir = args.music_dir or MASTER_TRACKS_DIRECTORY
        cache_dir = args.cache_dir or LOCAL_TRACKS_CACHE_DIRECTORY

        # 1. Embed metadata
        embed_metadata(args.interactive)

        # 2. Generate cache
        generate_local_tracks_cache(
            music_dir,
            cache_dir,
            not args.all_tracks,
            True  # Always generate report in all-in-one mode
        )

        # 3. Start server
        start_local_tracks_server(args.port, cache_dir)

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
