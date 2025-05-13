import datetime
import hashlib
import os
import re
import xml.etree.ElementTree as ET


def generate_rekordbox_xml_from_m3us(m3u_root_folder, output_xml_path):
    """
    Generate a Rekordbox XML file from all M3U playlists in a folder structure.
    The folder structure within m3u_root_folder will be preserved in Rekordbox.
    All playlists will be placed under a root folder called "m3u".

    Args:
        m3u_root_folder: Root directory containing M3U playlists and subfolders
        output_xml_path: Path to write the output XML file
    """
    print(f"Reading M3U playlists from: {m3u_root_folder}")

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

    # First build a complete map of all tracks across all playlists
    for (folder_path, playlist_name), m3u_content in m3u_data.items():
        lines = m3u_content.strip().split('\n')
        i = 0

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

                        all_tracks[file_path] = {
                            'id': track_id_counter,
                            'key': key,
                            'info': track_info,
                            'title': title,
                            'artist': artist,
                            'duration': duration,
                            'path': file_path
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

    print(f"Rekordbox XML file created: {output_xml_path}")
    print(f"Total tracks: {len(all_tracks)}")
    print(f"Total playlists: {len(created_playlists)}")
    print(f"Total folders: {created_folder_count}")
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
