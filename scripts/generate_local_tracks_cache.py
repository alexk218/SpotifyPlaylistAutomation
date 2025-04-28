#!/usr/bin/env python
import argparse
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from mutagen.id3 import ID3, ID3NoHeaderError

MASTER_TRACKS_DIRECTORY_SSD = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
LOCAL_TRACKS_CACHE_DIRECTORY = os.getenv("LOCAL_TRACKS_CACHE_DIRECTORY")


# Function to extract TrackId from MP3 files
def extract_track_id(file_path):
    """Extract Spotify TrackId from MP3 file metadata."""
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            return tags['TXXX:TRACKID'].text[0]
        return None
    except ID3NoHeaderError:
        return None
    except Exception as e:
        print(f"Error extracting TrackId from {file_path}: {e}")
        return None


def scan_directory(directory):
    """Scan directory for MP3 files and extract Spotify TrackIds."""
    local_tracks = []
    total_files = 0
    files_with_track_id = 0

    print(f"Scanning directory: {directory}")

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.mp3'):
                total_files += 1
                file_path = os.path.join(root, file)

                # Get file stats
                try:
                    file_stats = os.stat(file_path)
                    file_size = file_stats.st_size
                    modified_time = file_stats.st_mtime
                except Exception as e:
                    print(f"Error getting stats for {file_path}: {e}")
                    continue

                # Extract track ID
                track_id = extract_track_id(file_path)

                if track_id:
                    files_with_track_id += 1
                    # Extract artist and title from filename
                    filename = os.path.splitext(file)[0]
                    artist = ""
                    title = filename

                    # Common pattern: "Artist - Title"
                    if " - " in filename:
                        parts = filename.split(" - ", 1)
                        artist = parts[0].strip()
                        title = parts[1].strip()

                    # Add to our tracks list
                    local_tracks.append({
                        "path": file_path,
                        "filename": file,
                        "track_id": track_id,
                        "size": file_size,
                        "modified": modified_time,
                        "artist": artist,
                        "title": title
                    })

                # Show progress for large libraries
                if total_files % 100 == 0:
                    print(f"Processed {total_files} files, found {files_with_track_id} with TrackIds")

    print(f"Scan complete: {total_files} total files, {files_with_track_id} with TrackIds")
    return local_tracks, total_files, files_with_track_id


def generate_cache(music_directory, output_path):
    """Generate a cache file of all local tracks with their Spotify TrackIds."""
    start_time = time.time()

    # Scan the directory
    local_tracks, total_files, files_with_track_id = scan_directory(music_directory)

    # Prepare output data
    cache_data = {
        "generated": datetime.now().isoformat(),
        "music_directory": music_directory,
        "total_files": total_files,
        "files_with_track_id": files_with_track_id,
        "tracks": local_tracks
    }

    # Determine output filename
    date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"local_tracks_cache_{date_suffix}.json"
    output_file = os.path.join(output_path, filename)

    latest_file = os.path.join(output_path, "local_tracks_cache.json")
    with open(latest_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2)
    print(f"Latest version also saved to: {latest_file}")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2)

    elapsed_time = time.time() - start_time
    print(f"Cache generated in {elapsed_time:.2f} seconds")
    print(f"Cache file saved to: {output_file}")

    return output_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a cache of local music files with Spotify TrackIds")
    parser.add_argument("--no-date", action="store_true", help="Don't append date to filename")

    args = parser.parse_args()

    cache_file = generate_cache(MASTER_TRACKS_DIRECTORY_SSD, LOCAL_TRACKS_CACHE_DIRECTORY)
    print(f"Complete! Use this cache file in your Spicetify app: {cache_file}")
