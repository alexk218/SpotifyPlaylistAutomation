import json
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from pathlib import Path
from datetime import datetime
import argparse
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Default values (can be overridden by environment variables or command line args)
DEFAULT_PORT = 8765
DEFAULT_CACHE_PATH = os.getenv("LOCAL_TRACKS_CACHE_DIRECTORY", "")


class LocalTracksRequestHandler(BaseHTTPRequestHandler):
    cache_path = ""
    last_modified_time = 0
    cache_data = None

    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS preflight"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '86400')  # 24 hours
        self.end_headers()

    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            # Check if we need to reload the cache
            self.reload_cache_if_needed()

            # Return the cache data
            self.wfile.write(json.dumps(self.cache_data).encode())

        elif self.path == '/status':
            # Status endpoint for checking if server is running
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            status = {
                "status": "running",
                "time": datetime.now().isoformat(),
                "cache_path": self.cache_path,
                "cache_file": self.get_latest_cache_file(),
                "tracks_count": len(self.cache_data.get("tracks", [])) if self.cache_data else 0
            }
            self.wfile.write(json.dumps(status).encode())

        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Not found"}).encode())

    @classmethod
    def get_latest_cache_file(cls):
        """Find the latest cache file in the cache directory"""
        latest_file = os.path.join(cls.cache_path, "local_tracks_cache.json")

        # First check if the "latest" file exists
        if os.path.exists(latest_file):
            return latest_file

        # Otherwise look for the most recent dated file
        cache_files = []
        for file in os.listdir(cls.cache_path):
            if file.startswith("local_tracks_cache_") and file.endswith(".json"):
                file_path = os.path.join(cls.cache_path, file)
                cache_files.append((file_path, os.path.getmtime(file_path)))

        # Sort by modification time (newest first)
        cache_files.sort(key=lambda x: x[1], reverse=True)
        return cache_files[0][0] if cache_files else None

    @classmethod
    def reload_cache_if_needed(cls):
        """Check if cache file has changed and reload if necessary"""
        cache_file = cls.get_latest_cache_file()

        if not cache_file:
            cls.cache_data = {"error": "No cache file found", "tracks": []}
            return

        current_mtime = os.path.getmtime(cache_file)

        # Reload if file has changed or we haven't loaded it yet
        if current_mtime > cls.last_modified_time or cls.cache_data is None:
            print(f"Loading cache file: {cache_file}")
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cls.cache_data = json.load(f)
                cls.last_modified_time = current_mtime
                print(f"Loaded {len(cls.cache_data.get('tracks', []))} tracks from cache")
            except Exception as e:
                print(f"Error loading cache file: {e}")
                cls.cache_data = {"error": str(e), "tracks": []}


def run_server(port, cache_path):
    """Run the HTTP server"""
    # Set up the request handler with the cache path
    LocalTracksRequestHandler.cache_path = cache_path

    # Validate the cache path
    if not os.path.isdir(cache_path):
        print(f"Error: Cache directory {cache_path} does not exist.")
        print("Creating directory...")
        try:
            os.makedirs(cache_path, exist_ok=True)
        except Exception as e:
            print(f"Failed to create directory: {e}")
            return

    # Load initial cache
    LocalTracksRequestHandler.reload_cache_if_needed()

    # Create and start the server
    server = HTTPServer(('localhost', port), LocalTracksRequestHandler)
    print(f"Local tracks server running at http://localhost:{port}")
    print(f"Using cache directory: {cache_path}")
    print(f"Press Ctrl+C to stop the server")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Server stopped by user")
    finally:
        server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local tracks HTTP server for Tagify Spicetify app")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"Port to run the server on (default: {DEFAULT_PORT})")
    parser.add_argument("--cache-path", type=str, default=DEFAULT_CACHE_PATH,
                        help=f"Path to the directory containing cache files (default: {DEFAULT_CACHE_PATH})")

    args = parser.parse_args()

    print("=== Tagify Local Tracks Server ===")
    print(f"Starting server on port {args.port}")

    run_server(args.port, args.cache_path)
