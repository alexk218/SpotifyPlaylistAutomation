# api/services/track_service.py
import os
import re
import subprocess
from datetime import datetime

import Levenshtein
from mutagen.id3 import ID3, ID3NoHeaderError
from mutagen.mp3 import MP3
from sql.core.unit_of_work import UnitOfWork
from helpers.file_helper import embed_track_id


def search_tracks(master_tracks_dir, query):
    """
    Search for tracks that match the query.

    Args:
        master_tracks_dir: Directory containing master tracks
        query: Search query string

    Returns:
        List of matching track information
    """
    if not query:
        return []

    results = []
    query = query.lower()

    # Scan the files in the master directory
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            file_path = os.path.join(root, file)
            filename_no_ext = os.path.splitext(file)[0].lower()

            # Check if query is in filename
            if query in filename_no_ext:
                # Get TrackId if present
                track_id = None
                embedded_artist_title = "Unknown"
                try:
                    tags = ID3(file_path)
                    if 'TXXX:TRACKID' in tags:
                        track_id = tags['TXXX:TRACKID'].text[0]

                    # Try to get artist and title from ID3 tags
                    artist = ""
                    title = ""
                    if 'TPE1' in tags:  # Artist
                        artist = str(tags['TPE1'])
                    if 'TIT2' in tags:  # Title
                        title = str(tags['TIT2'])

                    if artist and title:
                        embedded_artist_title = f"{artist} - {title}"
                    else:
                        embedded_artist_title = filename_no_ext
                except Exception as e:
                    print(f"Error reading ID3 tags from {file_path}: {e}")

                results.append({
                    'file': file,
                    'track_id': track_id,
                    'embedded_artist_title': embedded_artist_title,
                    'filename': filename_no_ext,
                    'confidence': 1.0 if track_id else 0,
                    'full_path': file_path
                })

    # Sort results by relevance (tracks with IDs first, then by filename match)
    results.sort(key=lambda x: (x['track_id'] is None, x['file'].lower().find(query)))

    return results


def fuzzy_match_track(file_name, current_track_id=None):
    """
    Find potential Spotify track matches for a local file.

    Args:
        file_name: Name of the file to match
        current_track_id: Current track ID if any

    Returns:
        Dictionary with match information
    """
    # Load tracks from database for matching
    with UnitOfWork() as uow:
        try:
            tracks_db = uow.track_repository.get_all()
        except Exception as e:
            print(f"Database error: {e}")
            raise ValueError(f"Database error: {str(e)}")

    # DIRECT MATCH FOR LOCAL FILES - First try an exact file name match
    exact_file_matches = []
    for track in tracks_db:
        if track.is_local:
            # Compare the file name directly
            if track.title == file_name or track.title == os.path.splitext(file_name)[0]:
                # Exact file name match - give this highest priority
                exact_file_matches.append({
                    'track_id': track.track_id,
                    'ratio': 1.0,  # Perfect match
                    'artist': track.artists or "",
                    'title': track.title,
                    'album': track.album or "Local Files",
                    'is_local': True,
                    'match_type': 'exact_file'
                })

    # If we found exact matches, return them immediately
    if exact_file_matches:
        return {
            "file_name": file_name,
            "original_artist": "",
            "original_title": os.path.splitext(file_name)[0],
            "matches": exact_file_matches
        }

    # Extract artist and title from filename
    try:
        name_part = os.path.splitext(file_name)[0]

        # Try standard "Artist - Title" format first
        if " - " in name_part:
            artist, track_title = name_part.split(" - ", 1)
        else:
            # Handle files without the separator
            artist = ""
            track_title = name_part
    except ValueError:
        artist = ""
        track_title = name_part

    # Normalize for comparison
    normalized_artist = artist.lower().replace('&', 'and')
    normalized_title = track_title.lower()

    # Original artist and title for display
    original_artist = artist
    original_title = track_title

    # Handle remix information
    remix_info = ""
    if "remix" in normalized_title.lower():
        remix_parts = normalized_title.lower().split("remix")
        normalized_title = remix_parts[0].strip()
        remix_info = "remix" + remix_parts[1] if len(remix_parts) > 1 else "remix"

    # Calculate matches
    matches = []

    # First, check for matching local files
    local_tracks = [track for track in tracks_db if track.is_local]

    # Try to match against local files first
    for track in local_tracks:
        # For local files, just compare titles since they might not have proper artist-title format
        db_title = track.title.lower()

        # Normalize both titles
        clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
        db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

        # Calculate similarity
        similarity = Levenshtein.ratio(clean_normalized_title, db_title_clean)

        # Add high similarity boost for exact file name match
        if clean_normalized_title == db_title_clean or file_name.lower() == track.title.lower():
            similarity = 1.0

        # Include if reasonable match
        if similarity >= 0.5:
            matches.append({
                'track_id': track.track_id,
                'ratio': similarity,
                'artist': track.artists or "Local File",
                'title': track.title,
                'album': track.album or "Local Files",
                'is_local': True
            })

    # Then process regular Spotify tracks
    for track in tracks_db:
        # Skip local files as we already processed them
        if track.is_local:
            continue

        # Process artists and titles
        db_artists = track.artists.lower().replace('&', 'and')
        db_title = track.title.lower()
        track_id = track.track_id

        # Skip if this is the current track ID
        if current_track_id and track_id == current_track_id:
            continue

        # Split artists and normalize
        db_artist_list = [a.strip() for a in db_artists.split(',')]
        expanded_artists = []
        for db_artist in db_artist_list:
            if ' and ' in db_artist:
                expanded_artists.extend([a.strip() for a in db_artist.split(' and ')])
            else:
                expanded_artists.append(db_artist)

        # Artist match calculation
        artist_ratios = []
        if artist:  # Only do artist matching if we have an artist name
            artist_ratios = [Levenshtein.ratio(normalized_artist, db_artist) for db_artist in expanded_artists]
            artist_ratio = max(artist_ratios) if artist_ratios else 0

            # Perfect match bonus
            if any(normalized_artist == db_artist for db_artist in expanded_artists):
                artist_ratio = 1.0
        else:
            artist_ratio = 0.0

        # Clean titles for better matching
        clean_normalized_title = re.sub(r'[\(\[].*?[\)\]]', '', normalized_title).strip()
        db_title_clean = re.sub(r'[\(\[].*?[\)\]]', '', db_title).strip()

        # Create title variations for matching
        title_variations = [
            db_title,
            db_title_clean,
            db_title.replace(' - ', ' ').replace("'s", "s")
        ]

        # Find best title match
        title_ratios = [Levenshtein.ratio(clean_normalized_title, var) for var in title_variations]
        title_ratio = max(title_ratios)

        # Perfect match bonus
        if clean_normalized_title in [var.lower() for var in title_variations]:
            title_ratio = 1.0

        # Remix bonus
        remix_bonus = 0
        if any(x in track_title.lower() for x in ['remix', 'edit', 'mix', 'version']) and \
                any(x in db_title for x in ['remix', 'edit', 'mix', 'version']):
            remix_bonus = 0.1

            # Extra bonus for same remixer
            remix_pattern = r'\(([^)]+)(remix|edit|version|mix)\)'
            local_remix_match = re.search(remix_pattern, track_title.lower())
            db_remix_match = re.search(remix_pattern, db_title.lower())

            if local_remix_match and db_remix_match and local_remix_match.group(1) == db_remix_match.group(1):
                remix_bonus += 0.1

        # Calculate overall match score
        if artist:
            overall_ratio = (artist_ratio * 0.6 + title_ratio * 0.3 + remix_bonus)
        else:
            overall_ratio = (title_ratio * 0.9 + remix_bonus)

        # Return if over threshold
        if overall_ratio >= 0.45:  # Lower threshold for showing more options
            matches.append({
                'track_id': track_id,
                'ratio': overall_ratio,
                'artist': track.artists,
                'title': track.title,
                'album': track.album,
                'is_local': False
            })

    # Sort by match quality
    matches.sort(key=lambda x: x['ratio'], reverse=True)

    # Limit to top 8 matches
    top_matches = matches[:8]

    # Return the original file info and potential matches
    return {
        "file_name": file_name,
        "original_artist": original_artist,
        "original_title": original_title,
        "matches": top_matches
    }


def update_track_id(file_path, new_track_id):
    """
    Update the track ID in a file's metadata.

    Args:
        file_path: Path to the file
        new_track_id: New track ID to embed

    Returns:
        Dictionary with old and new track IDs
    """
    # Check if new track ID exists in database
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(new_track_id)
        if not track:
            raise ValueError(f"Track ID '{new_track_id}' not found in database")

    # Get existing track ID if any
    old_track_id = None
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            old_track_id = tags['TXXX:TRACKID'].text[0]
    except Exception:
        pass

    success = embed_track_id(file_path, new_track_id)

    if not success:
        raise RuntimeError(f"Failed to update TrackId in file: {file_path}")

    return {
        "old_track_id": old_track_id,
        "new_track_id": new_track_id
    }


def remove_track_id(file_path):
    """
    Remove track ID from a file's metadata.

    Args:
        file_path: Path to the file

    Returns:
        Old track ID if any
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get existing track ID if any for reporting
    old_track_id = None
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            old_track_id = tags['TXXX:TRACKID'].text[0]
            # Remove the TrackId
            tags.delall('TXXX:TRACKID')
            tags.save(file_path)
        else:
            raise ValueError(f"No TrackId found in file: {file_path}")
    except ID3NoHeaderError:
        raise ValueError(f"No ID3 tags found in file: {file_path}")

    return old_track_id


def delete_file(file_path):
    """
    Delete a file from the filesystem.

    Args:
        file_path: Path to the file to delete

    Returns:
        Name of the deleted file
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    filename = os.path.basename(file_path)
    os.remove(file_path)

    return filename


def analyze_embedding_metadata(master_tracks_dir, auto_confirm_threshold=0.75):
    """
    Analyze files that need track ID embedding.

    Args:
        master_tracks_dir: Directory containing master tracks
        auto_confirm_threshold: Threshold for auto-confirming matches

    Returns:
        Dictionary with analysis information
    """
    # Track statistics
    total_files = 0
    files_without_id = []
    auto_matched_files = []

    # Get tracks from database for auto-matching
    with UnitOfWork() as uow:
        tracks_db = uow.track_repository.get_all()

    # Scan local files
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            total_files += 1
            file_path = os.path.join(root, file)

            # Check if file has TrackId
            has_id = False
            try:
                tags = ID3(file_path)
                if 'TXXX:TRACKID' in tags:
                    has_id = True
            except Exception:
                pass

            if not has_id:
                # Try to find high confidence exact matches
                exact_match = None
                for track in tracks_db:
                    if track.is_local and (track.title == file or track.title == os.path.splitext(file)[0]):
                        # Found exact match
                        exact_match = {
                            'fileName': file,
                            'trackId': track.track_id,
                            'confidence': 1.0  # 100% confidence
                        }
                        break

                if exact_match and exact_match['confidence'] >= auto_confirm_threshold:
                    # Auto-match this file - embed the TrackId immediately
                    success = embed_track_id(file_path, exact_match['trackId'])
                    if success:
                        auto_matched_files.append(exact_match)
                    else:
                        # If embedding failed, add to manual process list
                        files_without_id.append(file)
                else:
                    # No high confidence match, add to manual process list
                    files_without_id.append(file)

    return {
        "total_files": total_files,
        "files_without_id": files_without_id,
        "auto_matched_files": auto_matched_files,
        "needs_confirmation": len(files_without_id) > 0,
        "requires_fuzzy_matching": len(files_without_id) > 0
    }


def embed_metadata_batch(master_tracks_dir, user_selections):
    """
    Embed track IDs in multiple files based on user selections.

    Args:
        master_tracks_dir: Directory containing master tracks
        user_selections: List of user-selected file/track ID pairs

    Returns:
        Dictionary with embedding results
    """
    successful_embeds = 0
    failed_embeds = 0
    results = []

    for selection in user_selections:
        file_name = selection.get('fileName')
        track_id = selection.get('trackId')
        confidence = selection.get('confidence')

        # Find the full file path
        file_path = None
        for root, _, files in os.walk(master_tracks_dir):
            if file_name in files:
                file_path = os.path.join(root, file_name)
                break

        if file_path and track_id:
            # Use the embed_track_id function
            success = embed_track_id(file_path, track_id)

            if success:
                successful_embeds += 1
                results.append({
                    "file": file_name,
                    "track_id": track_id,
                    "success": True
                })
            else:
                failed_embeds += 1
                results.append({
                    "file": file_name,
                    "track_id": track_id,
                    "success": False,
                    "reason": "Failed to write to file"
                })
        else:
            failed_embeds += 1
            results.append({
                "file": file_name,
                "track_id": track_id,
                "success": False,
                "reason": "File not found or track ID missing"
            })

    return {
        "successful_embeds": successful_embeds,
        "failed_embeds": failed_embeds,
        "results": results
    }


def direct_tracks_compare(master_tracks_dir, master_playlist_id=None):
    """
    Directly compare Spotify tracks with local tracks from the database.

    Args:
        master_tracks_dir: Directory containing master tracks
        master_playlist_id: Optional ID of the master playlist

    Returns:
        Dictionary with comparison results
    """
    # 1. Get all tracks from the master playlist in the database
    with UnitOfWork() as uow:
        master_tracks = uow.track_repository.get_all()

        # Convert to a list of dicts for JSON serialization
        master_tracks_list = []
        for track in master_tracks:
            master_tracks_list.append({
                'id': track.track_id,
                'name': track.title,
                'artists': track.artists,
                'album': track.album,
                'added_at': track.added_to_master.isoformat() if track.added_to_master else None
            })

        # 2. Get all local tracks (tracks that have paths associated with them)
        # Create a set of track IDs that are verified to exist locally
        local_track_ids = set()
        local_tracks_info = []

        # Scan the master tracks directory to find which files have TrackIds
        # Scan local files to find which ones have TrackIds embedded
        for root, _, files in os.walk(master_tracks_dir):
            for filename in files:
                if not filename.lower().endswith('.mp3'):
                    continue

                file_path = os.path.join(root, filename)

                # Check if this file has a TrackId
                try:
                    try:
                        tags = ID3(file_path)
                        if 'TXXX:TRACKID' in tags:
                            track_id = tags['TXXX:TRACKID'].text[0]
                            local_track_ids.add(track_id)
                            local_tracks_info.append({
                                'path': file_path,
                                'filename': filename,
                                'track_id': track_id,
                                'size': os.path.getsize(file_path),
                                'modified': os.path.getmtime(file_path)
                            })
                    except ID3NoHeaderError:
                        pass
                except Exception as e:
                    print(f"Error reading ID3 tags from {file_path}: {e}")

        # 3. Compare to find missing tracks
        missing_tracks = []
        for track in master_tracks_list:
            # Skip tracks without an ID (shouldn't happen but just in case)
            if not track['id']:
                continue

            # Skip tracks that are local files
            if track['id'].startswith('local_'):
                continue

            # If track ID is not in local tracks, it's missing
            if track['id'] not in local_track_ids:
                missing_tracks.append(track)

        # Sort missing tracks by added_at date, newest first
        missing_tracks.sort(
            key=lambda x: x['added_at'] if x['added_at'] else '0',
            reverse=True
        )

        # 4. Return the results
        return {
            "database_time": datetime.now().isoformat(),
            "master_tracks": master_tracks_list,
            "local_tracks": {
                "count": len(local_track_ids),
                "tracks": local_tracks_info[:100]  # Limit to avoid huge payloads
            },
            "missing_tracks": missing_tracks,
            "music_directory": master_tracks_dir,
            "master_playlist_id": master_playlist_id
        }


def download_and_embed_track(track_id: str, download_dir: str):
    """
    Download a track using spotDL and embed the TrackId metadata.

    Args:
        track_id: Spotify track ID
        download_dir: Directory to download the track to

    Returns:
        Dictionary with download results
    """
    # Get track details from database first
    with UnitOfWork() as uow:
        track = uow.track_repository.get_by_id(track_id)
        if not track:
            raise ValueError(f"Track ID '{track_id}' not found in database")

    # Construct Spotify URL
    spotify_url = f"https://open.spotify.com/track/{track_id}"

    try:
        # Run spotDL command
        cmd = ["spotdl", spotify_url, "--output", download_dir]

        # Execute the command and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            # Parse the output to find the downloaded file
            output_lines = result.stdout.strip().split('\n')
            downloaded_file = None

            # Look for the downloaded file pattern
            for line in output_lines:
                if 'Downloaded' in line and download_dir in line:
                    # Extract filename from spotDL output
                    # Example: Downloaded "Artist - Song": path
                    parts = line.split(': ')
                    if len(parts) >= 2:
                        # Find the actual file that was created
                        for file in os.listdir(download_dir):
                            if file.endswith('.mp3') and track.title in file:
                                downloaded_file = os.path.join(download_dir, file)
                                break
                    break

            if not downloaded_file:
                # Fallback: find the most recently created MP3 file
                mp3_files = [f for f in os.listdir(download_dir) if f.endswith('.mp3')]
                if mp3_files:
                    newest_file = max(mp3_files, key=lambda x: os.path.getctime(os.path.join(download_dir, x)))
                    downloaded_file = os.path.join(download_dir, newest_file)

            if downloaded_file and os.path.exists(downloaded_file):
                # Embed the TrackId using existing helper
                from helpers.file_helper import embed_track_id
                embed_success = embed_track_id(downloaded_file, track_id)

                if embed_success:
                    return {
                        "downloaded_file": downloaded_file,
                        "track_info": f"{track.artists} - {track.title}",
                        "metadata_embedded": True,
                        "spotdl_output": result.stdout
                    }
                else:
                    return {
                        "downloaded_file": downloaded_file,
                        "track_info": f"{track.artists} - {track.title}",
                        "metadata_embedded": False,
                        "warning": "Download successful but metadata embedding failed",
                        "spotdl_output": result.stdout
                    }
            else:
                raise RuntimeError(f"Download appeared successful but could not locate downloaded file")
        else:
            # spotDL command failed
            error_output = result.stderr or result.stdout
            raise RuntimeError(f"spotDL failed: {error_output}")

    except subprocess.TimeoutExpired:
        raise RuntimeError("Download timed out after 5 minutes")
    except Exception as e:
        raise RuntimeError(f"Download failed: {str(e)}")
