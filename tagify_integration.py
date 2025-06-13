import argparse
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

# Load environment variables
load_dotenv()

# Define paths
PROJECT_ROOT = Path(__file__).resolve().parent

# Add project root to Python path for imports
sys.path.insert(0, str(PROJECT_ROOT))

USE_NEW_API = os.getenv('USE_NEW_API', 'true').lower() in ('true', '1', 'yes')

if USE_NEW_API:
    try:
        # Use the new API structure
        from api.app import create_app

        app = create_app()
        print("Successfully loaded new API structure")
    except Exception as e:
        print(f"Error loading new API structure: {e}")
        print(traceback.format_exc())
        sys.exit(1)
else:
    app = Flask(__name__)
    CORS(app, origins=["https://xpui.app.spotify.com", "https://open.spotify.com", "http://localhost:4000", "*"])
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
HELPERS_DIR = PROJECT_ROOT / "helpers"

LOCAL_TRACKS_SERVER_PATH = SCRIPTS_DIR / "local_tracks_server.py"
EMBED_METADATA_SCRIPT = HELPERS_DIR / "file_helper.py"

# Get environment variables
MASTER_TRACKS_DIRECTORY_SSD = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
MASTER_PLAYLIST_ID = os.getenv("MASTER_PLAYLIST_ID")
DEFAULT_PORT = 8765


@app.route('/api/delete-file', methods=['POST'])
def api_delete_file():
    try:
        file_path = request.json.get('file_path')

        if not file_path:
            return jsonify({
                "success": False,
                "message": "file_path is required"
            }), 400

        if not os.path.exists(file_path):
            return jsonify({
                "success": False,
                "message": f"File not found: {file_path}"
            }), 404

        # Delete the file
        os.remove(file_path)

        return jsonify({
            "success": True,
            "message": f"File deleted: {os.path.basename(file_path)}"
        })
    except Exception as e:
        error_str = traceback.format_exc()
        print(f"Error deleting file: {e}")
        print(error_str)
        return jsonify({
            "success": False,
            "message": str(e),
            "traceback": error_str
        }), 500


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

    # Server command
    server_parser = subparsers.add_parser("server", help="Start local tracks server")
    server_parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                               help="Port to run server on (default 8765)")

    # All-in-one command
    all_parser = subparsers.add_parser("all", help="Run server")
    all_parser.add_argument("--music-dir", type=str, default=MASTER_TRACKS_DIRECTORY_SSD,
                            help="Directory containing music files (default from .env)")
    all_parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                            help="Port to run server on (default 8765)")

    args = parser.parse_args()

    if args.command == "server":
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

        # Start server
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Local Tracks Server")
    parser.add_argument("--port", type=int, default=8765, help="Port to run server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to run server on")
    parser.add_argument("--cache-path", type=str, help="Path to the cache file or directory")

    args = parser.parse_args()

    print(f"Starting local tracks server on {args.host}:{args.port}")
    if args.cache_path:
        print(f"Using cache path: {args.cache_path}")

    app.run(host=args.host, port=args.port, debug=True, use_reloader=not os.environ.get('FLASK_DEBUG') == '0')
