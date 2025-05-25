import datetime
import hashlib
import json
import math
import os
import re
import xml.etree.ElementTree as ET

from mutagen.id3 import ID3, ID3NoHeaderError

from sql.core.unit_of_work import UnitOfWork


class RekordboxXmlGenerator:
    """
    Class to generate Rekordbox XML files from M3U playlists.
    Handles file scanning, track matching, playlist organization, and rating application.
    """

    def __init__(self, m3u_root_folder, master_tracks_dir, rating_data=None):
        """
        Initialize the generator with source folders and optional rating data.

        Args:
            m3u_root_folder: Root directory containing M3U playlists
            master_tracks_dir: Directory containing music files
            rating_data: Optional dict of track ratings and energy values
        """
        self.m3u_root_folder = m3u_root_folder
        self.master_tracks_dir = master_tracks_dir
        self.rating_data = rating_data

        # Initialize tracking variables
        self.file_to_track_id_map = {}
        self.track_id_to_file_map = {}
        self.m3u_files_with_paths = {}
        self.m3u_data = {}
        self.all_tracks = {}
        self.created_playlists = []
        self.tracks_with_ratings = 0
        self.created_folder_count = 0

        # Rating application tracking
        self.validation_results = None
        self.rating_keys_with_success = set()
        self.rating_keys_with_failure = {}

        # XML elements
        self.root = None
        self.collection = None
        self.m3u_root_node = None
        self.root_folder = None
        self.folder_nodes = {}

        print(f"Reading M3U playlists from: {m3u_root_folder}")

    def generate(self, output_xml_path):
        """
        Generate a Rekordbox XML file from M3U playlists.

        Args:
            output_xml_path: Path to write the output XML file

        Returns:
            Tuple of (total_tracks, total_playlists, total_rated)
        """
        # Validate rating data if provided
        if self.rating_data:
            print(f"Received rating data for {len(self.rating_data)} tracks")
            self.validation_results = self._validate_rating_data()
        else:
            print("No rating data provided")

        # Build track ID mappings
        self._build_track_id_mappings()

        # Process M3U files
        self._collect_m3u_files()
        if not self.m3u_files_with_paths:
            print("No M3U files found in the specified folder or subfolders.")
            return 0, 0

        self._read_m3u_content()

        # Create XML structure
        self._create_xml_structure()

        # Process tracks
        self._process_tracks_from_playlists()
        self._add_tracks_to_collection()

        # Set collection entries count
        self.collection.set("Entries", str(len(self.all_tracks)))

        # Create folder and playlist structure
        self._create_folder_structure()
        self._create_playlists()

        # Write the XML file
        self._write_xml_file(output_xml_path)

        # Analyze rating application if applicable
        if self.validation_results and self.rating_data:
            tracks_with_ratings_not_applied = self._analyze_rating_application()

            self._print_rating_analysis(tracks_with_ratings_not_applied)

            if tracks_with_ratings_not_applied:
                self._save_ratings_report(tracks_with_ratings_not_applied, output_xml_path)

            self._print_rating_summary()

        # Print final summary
        print(f"Rekordbox XML file created: {output_xml_path}")
        print(f"Total tracks: {len(self.all_tracks)}")
        print(f"Total playlists: {len(self.created_playlists)}")
        print(f"Total folders: {self.created_folder_count}")
        print(f"Tracks with ratings applied: {self.tracks_with_ratings}")
        print(f"All playlists are placed under the 'm3u' root folder")

        return len(self.all_tracks), len(self.created_playlists), self.tracks_with_ratings

    def _validate_rating_data(self):
        """
        Analyze the rating data and report statistics about available ratings.

        Returns:
            Dict with statistics about rating data
        """
        if not self.rating_data:
            print("No rating data to validate.")
            return

        # Track counts
        spotify_tracks_with_ratings = 0
        spotify_tracks_with_energy = 0
        local_tracks_with_ratings = 0
        local_tracks_with_energy = 0

        # Process each track in rating data
        for track_uri, track_data in self.rating_data.items():
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
        print(f"Total tracks with rating data: {len(self.rating_data)}")
        print(f"Spotify tracks with star ratings: {spotify_tracks_with_ratings}")
        print(f"Spotify tracks with energy ratings: {spotify_tracks_with_energy}")
        print(f"Local tracks with star ratings: {local_tracks_with_ratings}")
        print(f"Local tracks with energy ratings: {local_tracks_with_energy}")
        print("==================================\n")

        return {
            "total": len(self.rating_data),
            "spotify_with_ratings": spotify_tracks_with_ratings,
            "spotify_with_energy": spotify_tracks_with_energy,
            "local_with_ratings": local_tracks_with_ratings,
            "local_with_energy": local_tracks_with_energy
        }

    def _extract_track_id_from_file(self, file_path):
        """
        Extract the TrackId from an MP3 file's metadata.

        Args:
            file_path: Path to the MP3 file

        Returns:
            Track ID string or None if not found
        """
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

    def _build_track_id_mappings(self):
        """
        Build mappings between file paths and track IDs.
        Populates self.file_to_track_id_map and self.track_id_to_file_map
        """
        print(f"Building track ID mapping from directory: {self.master_tracks_dir}")
        total_files = 0
        files_with_id = 0

        # Before processing the tracks, create a set of all track IDs from rating data
        rating_track_ids = set()
        if self.rating_data:
            for uri in self.rating_data.keys():
                # Extract just the ID part from the URI
                if uri.startswith("spotify:track:"):
                    track_id = uri.split("spotify:track:")[1]
                    rating_track_ids.add(track_id)

        for root, _, files in os.walk(self.master_tracks_dir):
            for file in files:
                if file.lower().endswith('.mp3'):
                    total_files += 1
                    file_path = os.path.join(root, file)

                    # Normalize the file path to use forward slashes for consistency
                    normalized_file_path = file_path.replace('\\', '/')

                    # Extract track ID if present
                    track_id = self._extract_track_id_from_file(file_path)
                    if track_id:
                        files_with_id += 1
                        self.file_to_track_id_map[normalized_file_path] = track_id
                        self.track_id_to_file_map[track_id] = normalized_file_path

        print(f"Found {files_with_id} files with embedded TrackIds out of {total_files} total MP3 files")

    def _collect_m3u_files(self):
        """
        Collect all M3U files with their relative folder paths.
        Populates self.m3u_files_with_paths
        """
        total_m3u_count = 0

        # Walk through all directories and collect M3U files
        for root, dirs, files in os.walk(self.m3u_root_folder):
            for file in files:
                if file.lower().endswith('.m3u'):
                    # Calculate the relative path from the root folder
                    rel_path = os.path.relpath(root, self.m3u_root_folder)
                    if rel_path == '.':  # Files in the root directory
                        rel_path = ''

                    # Store the full path and its location in the folder hierarchy
                    full_path = os.path.join(root, file)
                    playlist_name = os.path.splitext(file)[0]

                    self.m3u_files_with_paths[(rel_path, playlist_name)] = full_path
                    total_m3u_count += 1

        print(f"Found {total_m3u_count} M3U playlists in {len(set(p[0] for p in self.m3u_files_with_paths))} folders")

    def _read_m3u_content(self):
        """
        Read content from all M3U files.
        Populates self.m3u_data
        """
        for (folder_path, playlist_name), file_path in self.m3u_files_with_paths.items():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    self.m3u_data[(folder_path, playlist_name)] = content
            except UnicodeDecodeError:
                # Try with a different encoding if UTF-8 fails
                try:
                    with open(file_path, 'r', encoding='latin-1') as f:
                        content = f.read()
                        self.m3u_data[(folder_path, playlist_name)] = content
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    continue

    def _create_xml_structure(self):
        """
        Create the base XML structure for Rekordbox.
        Initializes self.root, self.collection, and self.m3u_root_node
        """
        self.root = ET.Element("DJ_PLAYLISTS")
        self.root.set("Version", "1.0.0")

        product = ET.SubElement(self.root, "PRODUCT")
        product.set("Name", "rekordbox")
        product.set("Version", "6.0.0")
        product.set("Company", "Pioneer DJ")

        self.collection = ET.SubElement(self.root, "COLLECTION")

        playlists = ET.SubElement(self.root, "PLAYLISTS")

        # Create ROOT folder
        self.root_folder = ET.SubElement(playlists, "NODE")  # Store reference to root_folder
        self.root_folder.set("Name", "ROOT")
        self.root_folder.set("Type", "0")

        # Create "m3u" folder inside ROOT
        self.m3u_root_node = ET.SubElement(self.root_folder, "NODE")
        self.m3u_root_node.set("Name", "m3u")
        self.m3u_root_node.set("Type", "0")

    def _process_tracks_from_playlists(self):
        """
        Extract all unique tracks from M3U playlists.
        Populates self.all_tracks
        """
        track_id_counter = 1

        # First build a complete map of all tracks across all playlists
        for (folder_path, playlist_name), m3u_content in self.m3u_data.items():
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
                        if file_path not in self.all_tracks:
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
                            embedded_track_id = self.file_to_track_id_map.get(file_path)

                            self.all_tracks[file_path] = {
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

    def _add_tracks_to_collection(self):
        """
        Add all tracks to the XML collection with ratings if available.
        Updates self.tracks_with_ratings
        """
        for file_path, track_data in self.all_tracks.items():
            track_elem = ET.SubElement(self.collection, "TRACK")

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
            if self.rating_data and track_data.get('embedded_track_id'):
                self._apply_ratings_to_track(track_elem, track_data)

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

    def _apply_ratings_to_track(self, track_elem, track_data):
        """
        Apply ratings and energy values to a track.

        Args:
            track_elem: The XML track element to update
            track_data: The track data dictionary
        """
        embedded_id = track_data['embedded_track_id']

        # Create the primary Spotify URI format
        spotify_uri = f"spotify:track:{embedded_id}"

        found_rating = False
        rating_entry = None
        matched_rating_key = None

        # First attempt: Direct match with spotify URI format
        if spotify_uri in self.rating_data:
            rating_entry = self.rating_data[spotify_uri]
            found_rating = True
            matched_rating_key = spotify_uri
            print(f"Direct URI match found: {spotify_uri}")

        # Second attempt: Match with just the ID
        elif embedded_id in self.rating_data:
            rating_entry = self.rating_data[embedded_id]
            found_rating = True
            matched_rating_key = embedded_id
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
                for key in self.rating_data.keys():
                    # Check both ways - format in key or key in format
                    if format_id in key or key in format_id:
                        rating_entry = self.rating_data[key]
                        found_rating = True
                        matched_rating_key = key
                        print(f"Partial match found: {embedded_id} matched with {key}")
                        break

                if found_rating:
                    break

        # Fourth attempt: For local tracks, try fuzzy title matching
        if not found_rating and embedded_id and embedded_id.startswith("local_"):
            for uri, rating_info in self.rating_data.items():
                if uri.startswith("spotify:local:"):
                    parts = uri.split(":")
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
                                matched_rating_key = uri
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
                        self.tracks_with_ratings += 1

                        print(
                            f"Applied rating {raw_rating} -> {floored_rating} -> {rating_value} for track {track_data['title']}")

                        # Mark this rating key as successfully applied
                        self.rating_keys_with_success.add(matched_rating_key)
                    except Exception as e:
                        print(f"Error applying rating: {e}")
                        # Track the failure
                        self.rating_keys_with_failure[matched_rating_key] = {
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
                        if matched_rating_key not in self.rating_keys_with_success:
                            self.rating_keys_with_failure[matched_rating_key] = {
                                'title': track_data['title'],
                                'reason': f"Error applying energy: {str(e)}"
                            }
            except Exception as e:
                print(f"Unexpected error applying ratings: {e}")
                self.rating_keys_with_failure[matched_rating_key] = {
                    'title': track_data['title'],
                    'reason': f"Unexpected error: {str(e)}"
                }

    def _create_folder_structure(self):
        """
        Create folder nodes based on directory structure, respecting the order from structure file.
        Populates self.folder_nodes and updates self.created_folder_count
        """
        # Initialize folder nodes dict
        self.folder_nodes = {"": self.m3u_root_node}  # Empty key now represents the m3u folder
        self.created_folder_count = 0

        # Load the playlist structure to get the correct folder order
        structure_file = os.path.join(self.m3u_root_folder, '.playlist_structure.json')
        ordered_folders = []

        if os.path.exists(structure_file):
            try:
                with open(structure_file, 'r', encoding='utf-8') as f:
                    structure = json.load(f)

                # Get folders in the exact order they appear in the structure file
                ordered_folders = list(structure.get('folders', {}).keys())
                print(f"Creating folders in structure file order: {len(ordered_folders)} folders")

            except Exception as e:
                print(f"Warning: Could not load structure file for folder ordering: {e}")

        # If no structure file, fall back to discovering folders from M3U files
        if not ordered_folders:
            print("No structure file found, using M3U discovery order")
            discovered_folders = set()
            for folder_path, _ in self.m3u_files_with_paths.keys():
                if folder_path:  # Skip root level
                    # Normalize path separators
                    normalized_path = folder_path.replace('\\', '/')
                    discovered_folders.add(normalized_path)
            ordered_folders = sorted(list(discovered_folders))

        # Create folders in the determined order
        for folder_path in ordered_folders:
            if not folder_path:  # Skip empty paths
                continue

            # Normalize path separators for consistency
            if isinstance(folder_path, str):
                normalized_folder_path = folder_path.replace('\\', '/')
            else:
                normalized_folder_path = str(folder_path).replace('\\', '/')

            # Split the path into individual folder segments
            folder_segments = normalized_folder_path.split('/')

            # Build the folder path step by step, creating nodes as needed
            current_path = ""
            parent_node = self.m3u_root_node  # Start from m3u root

            for segment in folder_segments:
                if current_path:
                    current_path = current_path + '/' + segment
                else:
                    current_path = segment

                # Create this folder level if it doesn't exist
                if current_path not in self.folder_nodes:
                    folder_node = ET.SubElement(parent_node, "NODE")
                    folder_node.set("Name", segment)
                    folder_node.set("Type", "0")  # 0 = folder
                    folder_node.set("Count", "0")  # Will update later
                    self.folder_nodes[current_path] = folder_node
                    self.created_folder_count += 1
                    print(f"Created folder: '{current_path}' (segment: '{segment}')")

                # Update parent for next iteration
                parent_node = self.folder_nodes[current_path]

        # Also need to handle any folders that exist in M3U files but not in structure
        # (in case there are M3U files in folders not listed in the structure)
        for folder_path, _ in self.m3u_files_with_paths.keys():
            if not folder_path:  # Skip root level
                continue

            # Normalize path separators
            normalized_folder_path = folder_path.replace('\\', '/')

            if normalized_folder_path not in self.folder_nodes:
                print(f"Warning: Found M3U folder not in structure: '{normalized_folder_path}'")
                # Create it using the same logic
                folder_segments = normalized_folder_path.split('/')
                current_path = ""
                parent_node = self.m3u_root_node

                for segment in folder_segments:
                    if current_path:
                        current_path = current_path + '/' + segment
                    else:
                        current_path = segment

                    if current_path not in self.folder_nodes:
                        folder_node = ET.SubElement(parent_node, "NODE")
                        folder_node.set("Name", segment)
                        folder_node.set("Type", "0")  # 0 = folder
                        folder_node.set("Count", "0")  # Will update later
                        self.folder_nodes[current_path] = folder_node
                        self.created_folder_count += 1

                    parent_node = self.folder_nodes[current_path]

        print(f"Total folders created: {self.created_folder_count}")

    def _create_playlists(self):
        """
        Create playlist nodes within their folders, respecting the exact order from structure.
        """
        # Load the playlist structure if it exists
        structure_file = os.path.join(self.m3u_root_folder, '.playlist_structure.json')
        playlist_order = {}
        folder_order = {}

        print(f"Looking for structure file at: {structure_file}")
        print(f"Structure file exists: {os.path.exists(structure_file)}")

        if os.path.exists(structure_file):
            try:
                with open(structure_file, 'r', encoding='utf-8') as f:
                    structure = json.load(f)

                # Extract playlist order within each folder
                root_playlists = structure.get('root_playlists', [])
                playlist_order[''] = root_playlists
                playlist_order['root'] = root_playlists

                for folder_path, folder_data in structure.get('folders', {}).items():
                    if isinstance(folder_data, dict):
                        folder_playlists = folder_data.get('playlists', [])
                        playlist_order[folder_path] = folder_playlists

                # Create folder order mapping
                folder_paths = list(structure.get('folders', {}).keys())
                folder_order = {path: idx for idx, path in enumerate(folder_paths)}
                print(f"Folder order mapping: {folder_order}")

            except Exception as e:
                print(f"ERROR loading playlist structure: {e}")
                import traceback
                traceback.print_exc()

        # Create a mapping of playlist names to their M3U content and folder
        playlist_data = {}
        for (folder_path, playlist_name), m3u_content in self.m3u_data.items():
            # NORMALIZE THE PATH SEPARATORS
            normalized_folder_path = folder_path.replace('\\', '/')

            folder_playlists = playlist_order.get(normalized_folder_path, [])
            try:
                if playlist_name in folder_playlists:
                    order_index = folder_playlists.index(playlist_name)
                    has_structure_order = True
                else:
                    order_index = 999
                    has_structure_order = False
            except (ValueError, AttributeError):
                order_index = 999
                has_structure_order = False

            playlist_data[playlist_name] = {
                'content': m3u_content,
                'folder_path': folder_path,  # keep original for folder node lookup
                'normalized_folder_path': normalized_folder_path,
                'order_index': order_index,
                'has_structure_order': has_structure_order
            }

        # Group by folder for analysis
        by_folder = {}
        for name, data in playlist_data.items():
            folder = data['folder_path']
            if folder not in by_folder:
                by_folder[folder] = []
            by_folder[folder].append((name, data['order_index'], data['has_structure_order']))

        # Sort playlists to respect the structure order
        def get_sort_key(playlist_name):
            data = playlist_data[playlist_name]
            folder_path = data['folder_path']

            if folder_path == '' or folder_path == 'root':
                folder_sort_key = -1
            else:
                folder_sort_key = folder_order.get(folder_path, 999)

            playlist_sort_key = data['order_index']
            return folder_sort_key, playlist_sort_key, playlist_name

        sorted_playlists = sorted(playlist_data.keys(), key=get_sort_key)

        # Create playlists in the determined order
        for playlist_name in sorted_playlists:
            data = playlist_data[playlist_name]
            folder_path = data['folder_path']
            m3u_content = data['content']

            # Get the parent folder node
            normalized_folder_path = folder_path.replace('\\', '/')
            parent_node = self.folder_nodes.get(normalized_folder_path, self.m3u_root_node)

            # Create playlist node
            playlist = ET.SubElement(parent_node, "NODE")
            playlist.set("Name", playlist_name)
            playlist.set("Type", "1")  # 1 = playlist
            playlist.set("KeyType", "0")  # Use TrackID as the key
            self.created_playlists.append(playlist_name)

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

                        if file_path in self.all_tracks:
                            track_id = self.all_tracks[file_path]['id']
                            if track_id not in playlist_track_ids:
                                playlist_track_ids.append(track_id)

                                # Add the track reference to the playlist
                                track_ref = ET.SubElement(playlist, "TRACK")
                                track_ref.set("Key", str(track_id))

                i += 1

            # Set playlist entries count
            playlist.set("Entries", str(len(playlist_track_ids)))

        # Update folder counts (count direct children)
        for path, node in self.folder_nodes.items():
            children = node.findall("./NODE")
            node.set("Count", str(len(children)))

        # Update "m3u" root folder count
        m3u_children = self.m3u_root_node.findall("./NODE")
        self.m3u_root_node.set("Count", str(len(m3u_children)))

        # Update ROOT folder count
        self.root_folder.set("Count", "1")  # Just the m3u folder

    def _write_xml_file(self, output_xml_path):
        """
        Write the XML tree to a file.

        Args:
            output_xml_path: Path to write the XML file
        """
        # Write the XML file
        tree = ET.ElementTree(self.root)

        # Use proper XML declaration
        with open(output_xml_path, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            tree.write(f, encoding='UTF-8')

    def _analyze_rating_application(self):
        """
        Analyze which tracks did and didn't get ratings applied.

        Returns:
            List of tracks with ratings that were not applied
        """
        # Find which rating keys never got applied
        unmatched_rating_keys = set(self.rating_data.keys()) - self.rating_keys_with_success

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
            entry = self.rating_data.get(key, {})
            if 'rating' in entry:
                track_info['rating'] = entry['rating']
            if 'energy' in entry:
                track_info['energy'] = entry['energy']

            # Set reason for failure
            if key not in self.rating_keys_with_failure:
                track_info['reason'] = "No matching file found"
            else:
                # This key had a specific error during application
                failure_info = self.rating_keys_with_failure[key]
                track_info['file_title'] = failure_info.get('title', 'Unknown')
                track_info['reason'] = failure_info.get('reason', 'Unknown error')

            tracks_with_ratings_not_applied.append(track_info)

        return tracks_with_ratings_not_applied

    def _print_rating_analysis(self, tracks_with_ratings_not_applied):
        """
        Print analysis of rating application results.

        Args:
            tracks_with_ratings_not_applied: List of tracks with unapplied ratings
        """
        print("\n===== TRACKS WITH RATINGS NOT APPLIED =====")
        print(f"Total tracks with ratings: {len(self.rating_data)}")
        print(f"Successfully applied ratings to: {len(self.rating_keys_with_success)} tracks")
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

    def _save_ratings_report(self, tracks_with_ratings_not_applied, output_xml_path):
        """
        Save detailed report of unapplied ratings.

        Args:
            tracks_with_ratings_not_applied: List of tracks with unapplied ratings
            output_xml_path: Base path to derive the report path
        """
        # Sort unapplied tracks by database match status for easier analysis
        tracks_with_ratings_not_applied.sort(key=lambda x: not x.get('db_match', False))

        # Save detailed report to a file
        unapplied_ratings_file = os.path.join(os.path.dirname(output_xml_path), "unapplied_ratings.json")
        try:
            with open(unapplied_ratings_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.datetime.now().isoformat(),
                    'total_rating_data': len(self.rating_data),
                    'successful_matches': len(self.rating_keys_with_success),
                    'failed_matches': len(tracks_with_ratings_not_applied),
                    'tracks_with_ratings_not_applied': tracks_with_ratings_not_applied
                }, f, indent=2)
            print(f"\nSaved detailed report of unapplied ratings to: {unapplied_ratings_file}")
        except Exception as e:
            print(f"Error saving unapplied ratings report: {e}")

        print("=============================================\n")

    def _print_rating_summary(self):
        """Print summary of rating application results."""
        print("\n===== RATING APPLICATION RESULTS =====")
        print(f"Spotify tracks with possible ratings: {self.validation_results['spotify_with_ratings']}")
        print(f"Local tracks with possible ratings: {self.validation_results['local_with_ratings']}")
        print(
            f"Total tracks with possible ratings: {self.validation_results['spotify_with_ratings'] + self.validation_results['local_with_ratings']}")
        print(f"Tracks with ratings actually applied: {self.tracks_with_ratings}")
        print(
            f"Coverage percentage: {(self.tracks_with_ratings / (self.validation_results['spotify_with_ratings'] + self.validation_results['local_with_ratings'] or 1)) * 100:.2f}%")
        print("====================================\n")

    # Original function as a wrapper for backward compatibility
    def generate_rekordbox_xml_from_m3us(m3u_root_folder, output_xml_path, master_tracks_dir, rating_data=None):
        """
        Generate a Rekordbox XML file from all M3U playlists in a folder structure.
        The folder structure within m3u_root_folder will be preserved in Rekordbox.
        All playlists will be placed under a root folder called "m3u".

        Args:
            m3u_root_folder: Root directory containing M3U playlists and subfolders
            output_xml_path: Path to write the output XML file
            master_tracks_dir: Directory containing music files
            rating_data: Optional dict of track ratings and energy values

        Returns:
            Tuple of (total_tracks, total_playlists)
        """
        generator = RekordboxXmlGenerator(m3u_root_folder, master_tracks_dir, rating_data)
        return generator.generate(output_xml_path)

    # Example usage
    if __name__ == "__main__":
        import sys

        if len(sys.argv) < 3:
            print("Usage: python script.py <m3u_folder> <output_xml_path>")
            sys.exit(1)

        m3u_folder = sys.argv[1]
        output_xml = sys.argv[2]

        generate_rekordbox_xml_from_m3us(m3u_folder, output_xml)
