import hashlib
import os
from datetime import datetime
from typing import Dict, List, Optional

import Levenshtein
from mutagen.id3 import ID3, ID3NoHeaderError

from helpers.spotify_uri_helper import SpotifyUriHelper
from sql.core.unit_of_work import UnitOfWork
from utils.logger import setup_logger

migration_logger = setup_logger('uri_migration', 'sql', 'uri_migration.log')


def migrate_to_spotify_uris(master_tracks_dir: str, dry_run: bool = False) -> Dict[str, int]:
    """
    Migrate from TrackId-based system to Spotify URI-based system.
    Handles both regular tracks and local files.

    Args:
        master_tracks_dir: Directory containing music files
        dry_run: If True, don't actually create mappings, just report what would be done

    Returns:
        Dictionary with migration statistics
    """
    stats = {
        'files_scanned': 0,
        'regular_tracks_mapped': 0,
        'local_files_mapped': 0,
        'files_unmapped': 0,
        'mappings_created': 0,
        'errors': 0
    }

    migration_logger.info(f"Starting Spotify URI migration from {master_tracks_dir}")
    migration_logger.info(f"Dry run mode: {dry_run}")

    # Step 1: Load all tracks from database and build lookup structures
    tracks_by_id = {}  # TrackId -> Track object
    tracks_by_uri = {}  # SpotifyUri -> Track object
    local_uris = []  # List of local Spotify URIs

    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()

        for track in all_tracks:
            if track.track_id:
                tracks_by_id[track.track_id] = track

            # Build or update Spotify URI
            spotify_uri = build_spotify_uri_for_track(track)
            if spotify_uri:
                tracks_by_uri[spotify_uri] = track
                if SpotifyUriHelper.is_local_uri(spotify_uri):
                    local_uris.append(spotify_uri)

                # Update track in database with Spotify URI if not already set
                if not track.uri and not dry_run:
                    track.uri = spotify_uri
                    uow.track_repository.update(track)

    migration_logger.info(f"Loaded {len(tracks_by_id)} tracks from database")
    migration_logger.info(f"Found {len(local_uris)} local file URIs")

    # Step 2: Scan all music files and create mappings
    unmapped_files = []

    for root, _, files in os.walk(master_tracks_dir):
        for filename in files:
            file_ext = os.path.splitext(filename.lower())[1]
            if file_ext not in ['.mp3', '.wav', '.aiff', '.flac', '.m4a']:
                continue

            stats['files_scanned'] += 1
            file_path = os.path.join(root, filename)
            spotify_uri = None
            mapping_method = None

            try:
                # Method 1: Try to get TrackId from MP3 metadata and convert to URI
                if file_ext == '.mp3':
                    embedded_track_id = get_embedded_track_id(file_path)
                    if embedded_track_id and embedded_track_id in tracks_by_id:
                        track = tracks_by_id[embedded_track_id]
                        spotify_uri = build_spotify_uri_for_track(track)
                        if spotify_uri:
                            stats['regular_tracks_mapped'] += 1
                            mapping_method = 'embedded_track_id'

                # Method 2: Try to match local files by URI
                if not spotify_uri:
                    spotify_uri = match_file_to_local_uri(filename, local_uris)
                    if spotify_uri:
                        stats['local_files_mapped'] += 1
                        mapping_method = 'local_uri_match'

                # Method 3: Try to match by filename patterns against all tracks
                if not spotify_uri:
                    spotify_uri = fuzzy_match_file_to_any_track(filename, tracks_by_uri)
                    if spotify_uri:
                        if SpotifyUriHelper.is_local_uri(spotify_uri):
                            stats['local_files_mapped'] += 1
                        else:
                            stats['regular_tracks_mapped'] += 1
                        mapping_method = 'fuzzy_match'

                if spotify_uri:
                    if not dry_run:
                        # Create the file mapping
                        success = create_file_mapping_with_uri(file_path, spotify_uri)
                        if success:
                            stats['mappings_created'] += 1
                            migration_logger.info(f"Mapped {filename} -> {spotify_uri} ({mapping_method})")
                        else:
                            stats['errors'] += 1
                    else:
                        stats['mappings_created'] += 1
                        migration_logger.info(f"[DRY RUN] Would map {filename} -> {spotify_uri} ({mapping_method})")
                else:
                    unmapped_files.append(filename)
                    stats['files_unmapped'] += 1

            except Exception as e:
                migration_logger.error(f"Error processing {filename}: {e}")
                stats['errors'] += 1

    # Report unmapped files
    if unmapped_files:
        migration_logger.info(f"Unmapped files ({len(unmapped_files)}):")
        for filename in unmapped_files[:20]:  # Show first 20
            migration_logger.info(f"  {filename}")
        if len(unmapped_files) > 20:
            migration_logger.info(f"  ... and {len(unmapped_files) - 20} more")

    # Final statistics
    migration_logger.info("URI migration completed:")
    migration_logger.info(f"  Files scanned: {stats['files_scanned']}")
    migration_logger.info(f"  Regular tracks mapped: {stats['regular_tracks_mapped']}")
    migration_logger.info(f"  Local files mapped: {stats['local_files_mapped']}")
    migration_logger.info(f"  Mappings created: {stats['mappings_created']}")
    migration_logger.info(f"  Files unmapped: {stats['files_unmapped']}")
    migration_logger.info(f"  Errors: {stats['errors']}")

    return stats


def build_spotify_uri_for_track(track) -> Optional[str]:
    """
    Build the appropriate Spotify URI for a track object.

    Args:
        track: Track object from database

    Returns:
        Spotify URI string or None
    """
    # If track already has a Spotify URI, use it
    if hasattr(track, 'spotify_uri') and track.uri:
        return track.uri

    # For regular Spotify tracks, build URI from TrackId
    if track.track_id and not track.track_id.startswith('local_'):
        return SpotifyUriHelper.create_track_uri(track.track_id)

    # For local files, build URI from metadata
    if track.is_local or (track.track_id and track.track_id.startswith('local_')):
        return SpotifyUriHelper.create_local_uri(
            artist=track.artists or '',
            album=track.album or '',
            title=track.title or '',
            duration=None  # We don't have duration in the database
        )

    return None


def get_embedded_track_id(file_path: str) -> Optional[str]:
    """
    Get TrackId from MP3 file's ID3 metadata.

    Args:
        file_path: Path to MP3 file

    Returns:
        TrackId if found, None otherwise
    """
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            return tags['TXXX:TRACKID'].text[0]
    except (ID3NoHeaderError, Exception):
        pass
    return None


def match_file_to_local_uri(filename: str, local_uris: List[str],
                            threshold: float = 0.8) -> Optional[str]:
    """
    Match a filename to a local Spotify URI using fuzzy matching.

    Args:
        filename: Name of the file
        local_uris: List of local Spotify URIs to match against
        threshold: Minimum similarity threshold

    Returns:
        Best matching URI if found, None otherwise
    """
    filename_no_ext = os.path.splitext(filename)[0]
    return SpotifyUriHelper.match_local_file_to_uri(filename_no_ext, local_uris, threshold)


def fuzzy_match_file_to_any_track(filename: str, tracks_by_uri: Dict[str, any],
                                  threshold: float = 0.75) -> Optional[str]:
    """
    Use fuzzy matching to find the best track match for a filename.

    Args:
        filename: Name of the file
        tracks_by_uri: Dictionary of Spotify URI to track objects
        threshold: Minimum similarity threshold

    Returns:
        Spotify URI if good match found, None otherwise
    """
    filename_no_ext = os.path.splitext(filename)[0].lower()
    best_match = None
    best_ratio = 0

    for spotify_uri, track in tracks_by_uri.items():
        # Create expected filename variations
        title = track.title.lower() if track.title else ''
        artists = track.artists.lower() if track.artists else ''

        variations = []
        if artists and title:
            variations.extend([
                f"{artists} - {title}",
                f"{title} - {artists}",
                f"{artists}_{title}",
                f"{title}_{artists}",
            ])
        elif title:
            variations.append(title)

        # For local files, also try parsing URI metadata
        if SpotifyUriHelper.is_local_uri(spotify_uri):
            try:
                uri_info = SpotifyUriHelper.parse_uri(spotify_uri)
                if uri_info.artist and uri_info.title:
                    variations.extend([
                        f"{uri_info.artist.lower()} - {uri_info.title.lower()}",
                        f"{uri_info.title.lower()} - {uri_info.artist.lower()}",
                        uri_info.title.lower()
                    ])
            except Exception:
                pass

        # Check each variation
        for variation in variations:
            if variation:
                ratio = Levenshtein.ratio(filename_no_ext, variation)
                if ratio > best_ratio and ratio >= threshold:
                    best_ratio = ratio
                    best_match = spotify_uri

    if best_match:
        migration_logger.info(f"Fuzzy matched {filename} -> {best_match} (ratio: {best_ratio:.2f})")

    return best_match


def create_file_mapping_with_uri(file_path: str, spotify_uri: str) -> bool:
    """
    Create a file-track mapping in the database using Spotify URI.

    Args:
        file_path: Path to the file
        spotify_uri: Spotify URI to map to

    Returns:
        True if successful, False otherwise
    """
    try:
        # Calculate file info
        file_hash = calculate_file_hash(file_path)
        file_size = os.path.getsize(file_path)
        last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))

        with UnitOfWork() as uow:
            mapping = uow.file_track_mapping_repository.create_mapping(
                file_path=file_path,
                spotify_uri=spotify_uri,
                file_hash=file_hash,
                file_size=file_size,
                last_modified=last_modified
            )
            return mapping is not None

    except Exception as e:
        migration_logger.error(f"Error creating mapping for {file_path}: {e}")
        return False


def calculate_file_hash(file_path: str) -> str:
    """
    Calculate SHA256 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        SHA256 hash string
    """
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def sync_spotify_playlist_with_uris(playlist_id: str, spotify_client) -> Dict[str, int]:
    """
    Sync a Spotify playlist and update database with proper URIs.
    This handles local files which don't have TrackIds.

    Args:
        playlist_id: Spotify playlist ID
        spotify_client: Authenticated Spotify client

    Returns:
        Dictionary with sync statistics
    """
    stats = {
        'tracks_processed': 0,
        'regular_tracks': 0,
        'local_files': 0,
        'tracks_added': 0,
        'errors': 0
    }

    migration_logger.info(f"Syncing playlist {playlist_id} with URI support")

    try:
        # Fetch playlist tracks with full metadata
        tracks = []
        offset = 0
        limit = 100

        while True:
            response = spotify_client.playlist_tracks(
                playlist_id,
                offset=offset,
                limit=limit,
                fields='items(added_at,track(id,name,artists(name),album(name),uri,is_local)),next'
            )

            if not response['items']:
                break

            tracks.extend(response['items'])
            offset += limit

            if not response.get('next'):
                break

        stats['tracks_processed'] = len(tracks)

        with UnitOfWork() as uow:
            for track_item in tracks:
                try:
                    track_data = track_item['track']
                    if not track_data:
                        continue

                    spotify_uri = track_data.get('uri')
                    if not spotify_uri:
                        continue

                    is_local = track_data.get('is_local', False)
                    added_at = datetime.strptime(track_item['added_at'], '%Y-%m-%dT%H:%M:%SZ')

                    # Extract metadata
                    track_name = track_data.get('name', '')
                    artists = ', '.join([artist['name'] for artist in track_data.get('artists', [])])
                    album = track_data.get('album', {}).get('name', '') if track_data.get('album') else ''

                    if is_local:
                        stats['local_files'] += 1
                        migration_logger.info(f"Processing local file: {artists} - {track_name}")

                        # For local files, the URI contains the metadata
                        # Create or update track record
                        existing_track = uow.track_repository.get_by_spotify_uri(spotify_uri)
                        if not existing_track:
                            from sql.models.track import Track
                            track = Track(
                                track_id=None,  # No TrackId for local files
                                title=track_name,
                                artists=artists,
                                album=album,
                                added_to_master=added_at,
                                is_local=True
                            )
                            track.spotify_uri = spotify_uri
                            uow.track_repository.insert(track)
                            stats['tracks_added'] += 1
                    else:
                        stats['regular_tracks'] += 1
                        track_id = track_data.get('id')

                        # For regular tracks, ensure we have the Spotify URI
                        existing_track = uow.track_repository.get_by_id(track_id)
                        if existing_track and not existing_track.uri:
                            existing_track.uri = spotify_uri
                            uow.track_repository.update(existing_track)

                except Exception as e:
                    stats['errors'] += 1
                    migration_logger.error(f"Error processing track: {e}")
                    continue

    except Exception as e:
        migration_logger.error(f"Error syncing playlist {playlist_id}: {e}")
        stats['errors'] += 1

    migration_logger.info(f"Playlist sync completed: {stats}")
    return stats


def verify_uri_migration(master_tracks_dir: str) -> Dict[str, int]:
    """
    Verify the URI migration by checking mappings against actual files.

    Args:
        master_tracks_dir: Directory containing music files

    Returns:
        Dictionary with verification statistics
    """
    stats = {
        'mappings_in_db': 0,
        'files_found': 0,
        'files_missing': 0,
        'local_file_mappings': 0,
        'track_mappings': 0,
        'verification_errors': 0
    }

    migration_logger.info("Starting URI migration verification")

    with UnitOfWork() as uow:
        mappings = uow.file_track_mapping_repository.get_all_active_mappings()
        stats['mappings_in_db'] = len(mappings)

        for mapping in mappings:
            try:
                if SpotifyUriHelper.is_local_uri(mapping.uri):
                    stats['local_file_mappings'] += 1
                else:
                    stats['track_mappings'] += 1

                if os.path.exists(mapping.file_path):
                    stats['files_found'] += 1
                else:
                    stats['files_missing'] += 1
                    migration_logger.warning(f"File not found: {mapping.file_path}")

            except Exception as e:
                stats['verification_errors'] += 1
                migration_logger.error(f"Error verifying {mapping.file_path}: {e}")

    migration_logger.info("URI verification completed:")
    migration_logger.info(f"  Mappings in DB: {stats['mappings_in_db']}")
    migration_logger.info(f"  Local file mappings: {stats['local_file_mappings']}")
    migration_logger.info(f"  Track mappings: {stats['track_mappings']}")
    migration_logger.info(f"  Files found: {stats['files_found']}")
    migration_logger.info(f"  Files missing: {stats['files_missing']}")
    migration_logger.info(f"  Verification errors: {stats['verification_errors']}")

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Migrate to Spotify URI-based mapping system')
    parser.add_argument('--master-tracks-dir', required=True, help='Path to master tracks directory')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')
    parser.add_argument('--verify', action='store_true', help='Verify existing URI mappings')

    args = parser.parse_args()

    if args.verify:
        verify_uri_migration(args.master_tracks_dir)
    else:
        migrate_to_spotify_uris(args.master_tracks_dir, args.dry_run)
