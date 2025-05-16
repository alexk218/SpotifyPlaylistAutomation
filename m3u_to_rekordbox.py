import datetime
import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET
import math
from pathlib import Path
from mutagen.id3 import ID3, ID3NoHeaderError

from sql.core.unit_of_work import UnitOfWork


def generate_rekordbox_xml_from_m3us(m3u_root_folder, output_xml_path, master_tracks_dir, rating_data=None):
    """
    Generate a Rekordbox XML file from all M3U playlists in a folder structure.
    The folder structure within m3u_root_folder will be preserved in Rekordbox.
    All playlists will be placed under a root folder called "m3u".

    Args:
        m3u_root_folder: Root directory containing M3U playlists and subfolders
        output_xml_path: Path to write the output XML file
    """
    print(f"Reading M3U playlists from: {m3u_root_folder}")

    def validate_rating_data(rating_data):
        """
        Analyze the rating data and report statistics about available ratings.
        """
        if not rating_data:
            print("No rating data to validate.")
            return

        # Track counts
        spotify_tracks_with_ratings = 0
        spotify_tracks_with_energy = 0
        local_tracks_with_ratings = 0
        local_tracks_with_energy = 0

        # Process each track in rating data
        for track_uri, track_data in rating_data.items():
            is_spotify_track = track_uri.startswith("spotify:track:")
            is_local_track = track_uri.startswith("spotify:local:")

            has_rating = 'rating' in track_data and track_data['rating'] is not None
            has_energy = 'energy' in track_data and track_data['energy'] is not None

            if is_spotify_track:
                if has_rating:
                    spotify_tracks_with_ratings += 1
                if has_energy:
                    spotify_tracks_with_energy += 1
            elif is_local_track:
                if has_rating:
                    local_tracks_with_ratings += 1
                if has_energy:
                    local_tracks_with_energy += 1

        # Print summary
        print("\n===== RATING DATA VALIDATION =====")
        print(f"Total tracks with rating data: {len(rating_data)}")
        print(f"Spotify tracks with star ratings: {spotify_tracks_with_ratings}")
        print(f"Spotify tracks with energy ratings: {spotify_tracks_with_energy}")
        print(f"Local tracks with star ratings: {local_tracks_with_ratings}")
        print(f"Local tracks with energy ratings: {local_tracks_with_energy}")
        print("==================================\n")

        return {
            "total": len(rating_data),
            "spotify_with_ratings": spotify_tracks_with_ratings,
            "spotify_with_energy": spotify_tracks_with_energy,
            "local_with_ratings": local_tracks_with_ratings,
            "local_with_energy": local_tracks_with_energy
        }

    # Log rating data for debugging
    if rating_data:
        print(f"Received rating data for {len(rating_data)} tracks")
        validation_results = validate_rating_data(rating_data)

        # Track which rating keys failed to be applied
        rating_keys_with_success = set()
        rating_keys_with_failure = {}

    else:
        print("No rating data provided")

    def extract_track_id_from_file(file_path):
        """Extract the TrackId from an MP3 file's metadata."""
        try:
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    return tags['TXXX:TRACKID'].text[0]
            except ID3NoHeaderError:
                pass
        except Exception as e:
            print(f"Error reading ID3 tags from {file_path}: {e}")
        return None

    # Create a reverse mapping: file_path -> track_id
    # This will be used to match tracks in the M3U files with their IDs
    file_to_track_id_map = {}

    # Also create a direct mapping: track_id -> file_path
    # This will help us find files that match ratings data
    track_id_to_file_map = {}

    print(f"Building track ID mapping from directory: {master_tracks_dir}")
    total_files = 0
    files_with_id = 0

    # First, create a list to track which tracks got ratings and which didn't
    successful_matches = []
    failed_matches = []

    # Before processing the tracks, create a set of all track IDs from your rating data
    rating_track_ids = set()
    for uri in rating_data.keys():
        # Extract just the ID part from the URI
        if uri.startswith("spotify:track:"):
            track_id = uri.split("spotify:track:")[1]
            rating_track_ids.add(track_id)

    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if file.lower().endswith('.mp3'):
                total_files += 1
                file_path = os.path.join(root, file)

                # Normalize the file path to use forward slashes for consistency
                normalized_file_path = file_path.replace('\\', '/')

                # Extract track ID if present
                track_id = extract_track_id_from_file(file_path)
                if track_id:
                    files_with_id += 1
                    file_to_track_id_map[normalized_file_path] = track_id
                    track_id_to_file_map[track_id] = normalized_file_path

    print(f"Found {files_with_id} files with embedded TrackIds out of {total_files} total MP3 files")

    # Dictionary to store all M3U files with their relative folder paths
    m3u_files_with_paths = {}
    total_m3u_count = 0

    # Walk through all directories and collect M3U files
    for root, dirs, files in os.walk(m3u_root_folder):
        for file in files:
            if file.lower().endswith('.m3u'):
                # Calculate the relative path from the root folder
                rel_path = os.path.relpath(root, m3u_root_folder)
                if rel_path == '.':  # Files in the root directory
                    rel_path = ''

                # Store the full path and its location in the folder hierarchy
                full_path = os.path.join(root, file)
                playlist_name = os.path.splitext(file)[0]

                m3u_files_with_paths[(rel_path, playlist_name)] = full_path
                total_m3u_count += 1

    if not m3u_files_with_paths:
        print("No M3U files found in the specified folder or subfolders.")
        return 0, 0

    print(f"Found {total_m3u_count} M3U playlists in {len(set(p[0] for p in m3u_files_with_paths))} folders")

    # Read all M3U files and build content dictionary
    m3u_data = {}
    for (folder_path, playlist_name), file_path in m3u_files_with_paths.items():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                m3u_data[(folder_path, playlist_name)] = content
        except UnicodeDecodeError:
            # Try with a different encoding if UTF-8 fails
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    content = f.read()
                    m3u_data[(folder_path, playlist_name)] = content
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue

    # Create XML structure
    root = ET.Element("DJ_PLAYLISTS")
    root.set("Version", "1.0.0")

    product = ET.SubElement(root, "PRODUCT")
    product.set("Name", "rekordbox")
    product.set("Version", "6.0.0")
    product.set("Company", "Pioneer DJ")

    collection = ET.SubElement(root, "COLLECTION")

    playlists = ET.SubElement(root, "PLAYLISTS")

    # Create ROOT folder
    root_folder = ET.SubElement(playlists, "NODE")
    root_folder.set("Name", "ROOT")
    root_folder.set("Type", "0")

    # Create "m3u" folder inside ROOT
    m3u_root_node = ET.SubElement(root_folder, "NODE")
    m3u_root_node.set("Name", "m3u")
    m3u_root_node.set("Type", "0")

    # Process all playlists
    track_id_counter = 1
    all_tracks = {}  # File path -> Track ID mapping
    created_playlists = []
    tracks_with_ratings = 0

    # First build a complete map of all tracks across all playlists
    for (folder_path, playlist_name), m3u_content in m3u_data.items():
        lines = m3u_content.strip().split('\n')
        i = 0

        processed_tracks = 0

        while i < len(lines):
            line = lines[i].strip()

            if line.startswith('#EXTINF:'):
                # Parse EXTINF line
                info_parts = line[8:].split(',', 1)
                duration = info_parts[0] if len(info_parts) > 0 else "0"
                try:
                    duration = int(float(duration))
                except ValueError:
                    duration = 0

                track_info = info_parts[1].strip() if len(info_parts) > 1 else ""

                # Get file path from next line
                i += 1
                if i < len(lines) and not lines[i].startswith('#'):
                    file_path = lines[i].strip()

                    # Clean up file path - ensure consistent forward slashes
                    file_path = file_path.replace('\\', '/')

                    # Ensure each track has a unique ID based on its path
                    if file_path not in all_tracks:
                        artist = "Unknown"
                        title = os.path.basename(file_path)

                        # Extract artist and title from track info
                        if " - " in track_info:
                            parts = track_info.split(" - ", 1)
                            artist = parts[0].strip()
                            title = parts[1].strip()

                        # Generate a unique key for this track
                        path_hash = hashlib.md5(file_path.encode()).hexdigest()[:8]
                        key = f"{artist}_{title}_{path_hash}"

                        # Try to get the embedded track ID if available
                        embedded_track_id = file_to_track_id_map.get(file_path)

                        all_tracks[file_path] = {
                            'id': track_id_counter,
                            'key': key,
                            'info': track_info,
                            'title': title,
                            'artist': artist,
                            'duration': duration,
                            'path': file_path,
                            'embedded_track_id': embedded_track_id
                        }
                        track_id_counter += 1

            i += 1

    # Add all tracks to the collection
    for file_path, track_data in all_tracks.items():
        track_elem = ET.SubElement(collection, "TRACK")

        # Set required attributes
        track_elem.set("TrackID", str(track_data['id']))
        track_elem.set("Name", track_data['title'])
        track_elem.set("Artist", track_data['artist'])
        track_elem.set("Composer", "")
        track_elem.set("Album", "")
        track_elem.set("Grouping", "")
        track_elem.set("Genre", "")
        track_elem.set("TotalTime", str(track_data['duration']))
        track_elem.set("DiscNumber", "0")
        track_elem.set("TrackNumber", "0")
        track_elem.set("Year", "")
        track_elem.set("AverageBpm", "0")
        track_elem.set("DateAdded", datetime.datetime.now().strftime("%Y-%m-%d"))
        track_elem.set("BitRate", "320")
        track_elem.set("SampleRate", "44100")
        track_elem.set("Comments", "")
        track_elem.set("PlayCount", "0")
        track_elem.set("Rating", "0")

        # Apply ratings and energy if available
        if rating_data and track_data.get('embedded_track_id'):
            embedded_id = track_data['embedded_track_id']

            # Create the primary Spotify URI format
            spotify_uri = f"spotify:track:{embedded_id}"

            # Debug output for troubleshooting
            debug_track_id_match(embedded_id, rating_data.keys(), file_path, track_data['title'])

            found_rating = False
            rating_entry = None

            # First attempt: Direct match with spotify URI format
            if spotify_uri in rating_data:
                rating_entry = rating_data[spotify_uri]
                found_rating = True
                matched_rating_key = spotify_uri
                print(f"Direct URI match found: {spotify_uri}")

            # Second attempt: Match with just the ID
            elif embedded_id in rating_data:
                rating_entry = rating_data[embedded_id]
                found_rating = True
                matched_rating_key = spotify_uri
                print(f"Direct ID match found: {embedded_id}")

            # Third attempt: Look for partial matches in keys
            if not found_rating:
                # Try multiple possible formats of the track ID
                possible_formats = [
                    spotify_uri,  # Standard format
                    embedded_id,  # Just the ID
                    embedded_id.lower(),  # Lowercase ID
                    embedded_id.upper(),  # Uppercase ID
                ]

                # Check each format against all keys in rating_data
                for format_id in possible_formats:
                    for key in rating_data.keys():
                        # Check both ways - format in key or key in format
                        if format_id in key or key in format_id:
                            rating_entry = rating_data[key]
                            found_rating = True
                            matched_rating_key = spotify_uri
                            print(f"Partial match found: {embedded_id} matched with {key}")
                            break

                    if found_rating:
                        break

            # Fourth attempt: For local tracks, try fuzzy title matching
            if not found_rating and embedded_id and embedded_id.startswith("local_"):
                for spotify_uri, rating_info in rating_data.items():
                    if spotify_uri.startswith("spotify:local:"):
                        parts = spotify_uri.split(":")
                        if len(parts) >= 5:
                            import urllib.parse
                            try:
                                encoded_title = parts[4] if len(parts) > 4 else ""
                                decoded_title = urllib.parse.unquote_plus(encoded_title)

                                # Remove file extension from track_data title if present
                                track_title = track_data['title']
                                if track_title.lower().endswith('.mp3') or track_title.lower().endswith(
                                        '.wav') or track_title.lower().endswith('.aiff'):
                                    track_title = os.path.splitext(track_title)[0]

                                # Normalize both strings for comparison (remove non-alphanumeric chars)
                                decoded_normalized = ''.join(
                                    c.lower() for c in decoded_title if c.isalnum() or c.isspace())
                                track_normalized = ''.join(c.lower() for c in track_title if c.isalnum() or c.isspace())

                                # Print debugging info
                                print(f"Comparing local title: '{decoded_normalized}' with track: '{track_normalized}'")

                                # Check for similarity allowing for some fuzziness
                                similarity = 0
                                if decoded_normalized and track_normalized:
                                    # Use longest common substring as a simple similarity measure
                                    similarity = len(
                                        os.path.commonprefix([decoded_normalized, track_normalized])) / max(
                                        len(decoded_normalized), len(track_normalized))

                                # Simple fuzzy matching - use multiple strategies
                                if similarity > 0.7 or decoded_normalized in track_normalized or track_normalized in decoded_normalized:
                                    rating_entry = rating_info
                                    found_rating = True
                                    matched_rating_key = spotify_uri
                                    print(f"Local file match found for {track_title} (similarity: {similarity:.2f})")
                                    break
                            except Exception as e:
                                print(f"Error in local file matching: {e}")
                                continue

            # Apply the rating and energy if we found a match
            if found_rating and rating_entry:
                try:
                    # Apply star rating if present
                    if 'rating' in rating_entry and rating_entry['rating']:
                        try:
                            raw_rating = float(rating_entry['rating'])
                            floored_rating = math.floor(raw_rating)

                            # Rekordbox uses: 0=0, 1=51, 2=102, 3=153, 4=204, 5=255
                            rating_value = min(int(floored_rating * 51), 255)
                            track_elem.set("Rating", str(rating_value))
                            tracks_with_ratings += 1

                            print(
                                f"Applied rating {raw_rating} -> {floored_rating} -> {rating_value} for track {track_data['title']}")

                            # Mark this rating key as successfully applied
                            rating_keys_with_success.add(matched_rating_key)
                        except Exception as e:
                            print(f"Error applying rating: {e}")
                            # Track the failure
                            rating_keys_with_failure[matched_rating_key] = {
                                'title': track_data['title'],
                                'reason': f"Error applying rating: {str(e)}"
                            }

                    # Apply energy as a comment if present
                    if 'energy' in rating_entry and rating_entry['energy']:
                        try:
                            energy = rating_entry['energy']
                            # Use consistent format "E:X" for energy comments
                            track_elem.set("Comments", f"E:{energy}")
                            print(f"Applied energy {energy} for track {track_data['title']}")
                        except Exception as e:
                            print(f"Error applying energy: {e}")
                            # Only track as failure if we didn't already apply a rating
                            if matched_rating_key not in rating_keys_with_success:
                                rating_keys_with_failure[matched_rating_key] = {
                                    'title': track_data['title'],
                                    'reason': f"Error applying energy: {str(e)}"
                                }
                except Exception as e:
                    print(f"Unexpected error applying ratings: {e}")
                    rating_keys_with_failure[matched_rating_key] = {
                        'title': track_data['title'],
                        'reason': f"Unexpected error: {str(e)}"
                    }

            else:
                # Track failed match
                failed_matches.append({
                    'id': embedded_id,
                    'title': track_data['title'],
                    'error': "No match found in rating data"
                })

        # Set file kind
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.mp3':
            kind = "MP3 File"
        elif file_ext == '.wav':
            kind = "WAV File"
        elif file_ext == '.aiff' or file_ext == '.aif':
            kind = "AIFF File"
        elif file_ext == '.flac':
            kind = "FLAC File"
        else:
            kind = "Audio File"
        track_elem.set("Kind", kind)
        track_elem.set("Size", "0")  # Dummy value

        # Format location properly for Rekordbox
        location = file_path

        # Ensure location has proper URI format with file://localhost/ prefix
        if re.match(r'^[A-Za-z]:', location):  # Windows path with drive letter
            # Convert "C:/path/file.mp3" to "file://localhost/C:/path/file.mp3"
            location = "file://localhost/" + location
        elif not location.startswith("file://"):
            # Add prefix if it doesn't have one
            location = "file://localhost/" + location

        track_elem.set("Location", location)

        # Additional required attributes
        track_elem.set("Remixer", "")
        track_elem.set("Tonality", "")
        track_elem.set("Label", "")
        track_elem.set("Mix", "")

    # Set collection entries count
    collection.set("Entries", str(len(all_tracks)))

    # Create a dictionary to store folder nodes
    folder_nodes = {"": m3u_root_node}  # Empty key now represents the m3u folder
    created_folder_count = 0

    # First create all folder nodes based on the directory structure
    for folder_path, _ in set((path, name) for (path, name) in m3u_files_with_paths.keys()):
        if not folder_path:  # Skip root level
            continue

        # Split the path into individual folder segments
        folder_segments = folder_path.split(os.path.sep)

        # Build the folder path step by step, creating nodes as needed
        current_path = ""
        parent_node = m3u_root_node  # Start from m3u root

        for segment in folder_segments:
            if current_path:
                current_path = os.path.join(current_path, segment)
            else:
                current_path = segment

            # Create this folder level if it doesn't exist
            if current_path not in folder_nodes:
                folder_node = ET.SubElement(parent_node, "NODE")
                folder_node.set("Name", segment)
                folder_node.set("Type", "0")  # 0 = folder
                folder_node.set("Count", "0")  # Will update later
                folder_nodes[current_path] = folder_node
                created_folder_count += 1

            # Update parent for next iteration
            parent_node = folder_nodes[current_path]

    # Now create all playlist nodes within their respective folders
    for (folder_path, playlist_name), m3u_content in m3u_data.items():
        # Get the parent folder node
        parent_node = folder_nodes.get(folder_path, m3u_root_node)  # Default to m3u root

        # Create playlist node
        playlist = ET.SubElement(parent_node, "NODE")
        playlist.set("Name", playlist_name)
        playlist.set("Type", "1")  # 1 = playlist
        playlist.set("KeyType", "0")  # Use TrackID as the key
        created_playlists.append(playlist_name)

        # Track which files are in this playlist
        playlist_track_ids = []

        # Process M3U content to get tracks
        lines = m3u_content.strip().split('\n')
        i = 0

        while i < len(lines):
            line = lines[i].strip()

            if line.startswith('#EXTINF:'):
                i += 1
                if i < len(lines) and not lines[i].startswith('#'):
                    file_path = lines[i].strip().replace('\\', '/')

                    if file_path in all_tracks:
                        track_id = all_tracks[file_path]['id']
                        if track_id not in playlist_track_ids:
                            playlist_track_ids.append(track_id)

                            # Add the track reference to the playlist
                            track_ref = ET.SubElement(playlist, "TRACK")
                            track_ref.set("Key", str(track_id))

            i += 1

        # Set playlist entries count
        playlist.set("Entries", str(len(playlist_track_ids)))

    # Update folder counts (count direct children)
    for path, node in folder_nodes.items():
        children = node.findall("./NODE")
        node.set("Count", str(len(children)))

    # Update "m3u" root folder count
    m3u_children = m3u_root_node.findall("./NODE")
    m3u_root_node.set("Count", str(len(m3u_children)))

    # Update ROOT folder count to include the m3u folder
    root_folder.set("Count", "1")  # Just the m3u folder

    # Write the XML file
    tree = ET.ElementTree(root)

    # Use proper XML declaration
    with open(output_xml_path, 'wb') as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        tree.write(f, encoding='UTF-8')

    if 'validation_results' in locals():
        # Find which rating keys never got applied
        unmatched_rating_keys = set(rating_data.keys()) - rating_keys_with_success

        # Create list of tracks from rating_data that never got matched to a file
        tracks_with_ratings_not_applied = []
        for key in unmatched_rating_keys:
            track_info = {}
            track_id = None

            # Extract the track ID from the key
            if key.startswith("spotify:track:"):
                track_id = key.split("spotify:track:")[1]
                track_info = {'id': track_id, 'key': key}
            elif key.startswith("spotify:local:"):
                # Local track format - keep original ID
                track_id = key
                track_info = {'id': key, 'key': key}
            else:
                # Assume this is already a track ID
                track_id = key
                track_info = {'id': key, 'key': key}

            # Try to get track title from database
            try:
                with UnitOfWork() as uow:
                    track = uow.track_repository.get_by_id(track_id)
                    if track:
                        track_info['title'] = track.title
                        track_info['artists'] = track.artists
                        track_info['db_match'] = True
                    else:
                        track_info['db_match'] = False
                        track_info['title'] = "Unknown (Not found in database)"
            except Exception as e:
                print(f"Error fetching track {track_id} from database: {e}")
                track_info['db_match'] = False
                track_info['title'] = f"Unknown (DB error: {str(e)})"

            # Get rating and energy from the rating data
            entry = rating_data.get(key, {})
            if 'rating' in entry:
                track_info['rating'] = entry['rating']
            if 'energy' in entry:
                track_info['energy'] = entry['energy']

            # Set reason for failure
            if key not in rating_keys_with_failure:
                track_info['reason'] = "No matching file found"
            else:
                # This key had a specific error during application
                failure_info = rating_keys_with_failure[key]
                track_info['file_title'] = failure_info.get('title', 'Unknown')
                track_info['reason'] = failure_info.get('reason', 'Unknown error')

            tracks_with_ratings_not_applied.append(track_info)

        print("\n===== TRACKS WITH RATINGS NOT APPLIED =====")
        print(f"Total tracks with ratings: {len(rating_data)}")
        print(f"Successfully applied ratings to: {len(rating_keys_with_success)} tracks")
        print(f"Failed to apply ratings to: {len(tracks_with_ratings_not_applied)} tracks")

        if tracks_with_ratings_not_applied:
            print("\nDETAILS OF TRACKS WITH UNAPPLIED RATINGS:")
            print("========================================")
            for i, track in enumerate(tracks_with_ratings_not_applied, 1):
                rating_str = f"Rating: {track.get('rating', 'N/A')}" if 'rating' in track else ""
                artist_str = f"{track.get('artists', '')}" if 'artists' in track else ""
                title_str = f"{track.get('title', 'Unknown')}"
                db_status = "In DB" if track.get('db_match', False) else "Not in DB"

                print(
                    f"{i}. {track.get('id', 'Unknown ID')} - {artist_str} - {title_str} - {rating_str} - {db_status} - Reason: {track.get('reason', 'Unknown')}")

            tracks_with_ratings_not_applied.sort(key=lambda x: not x.get('db_match', False))

            # Save detailed report to a file
            unapplied_ratings_file = os.path.join(os.path.dirname(output_xml_path), "unapplied_ratings.json")
            try:
                with open(unapplied_ratings_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'timestamp': datetime.datetime.now().isoformat(),
                        'total_rating_data': len(rating_data),
                        'successful_matches': len(rating_keys_with_success),
                        'failed_matches': len(tracks_with_ratings_not_applied),
                        'tracks_with_ratings_not_applied': tracks_with_ratings_not_applied
                    }, f, indent=2)
                print(f"\nSaved detailed report of unapplied ratings to: {unapplied_ratings_file}")
            except Exception as e:
                print(f"Error saving unapplied ratings report: {e}")

        print("=============================================\n")

        print("\n===== RATING APPLICATION RESULTS =====")
        print(f"Spotify tracks with possible ratings: {validation_results['spotify_with_ratings']}")
        print(f"Local tracks with possible ratings: {validation_results['local_with_ratings']}")
        print(
            f"Total tracks with possible ratings: {validation_results['spotify_with_ratings'] + validation_results['local_with_ratings']}")
        print(f"Tracks with ratings actually applied: {tracks_with_ratings}")
        print(
            f"Coverage percentage: {(tracks_with_ratings / (validation_results['spotify_with_ratings'] + validation_results['local_with_ratings'] or 1)) * 100:.2f}%")
        print("====================================\n")

    print(f"Rekordbox XML file created: {output_xml_path}")
    print(f"Total tracks: {len(all_tracks)}")
    print(f"Total playlists: {len(created_playlists)}")
    print(f"Total folders: {created_folder_count}")
    print(f"Tracks with ratings applied: {tracks_with_ratings}")
    print(f"All playlists are placed under the 'm3u' root folder")

    return len(all_tracks), len(created_playlists)


# Example usage
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python script.py <m3u_folder> <output_xml_path>")
        sys.exit(1)

    m3u_folder = sys.argv[1]
    output_xml = sys.argv[2]

    generate_rekordbox_xml_from_m3us(m3u_folder, output_xml)
