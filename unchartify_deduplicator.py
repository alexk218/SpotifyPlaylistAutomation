#!/usr/bin/env python3
"""
Unchartify Playlist Deduplicator

This script identifies duplicate tracks in Spotify playlists that follow the
"UNCHARTIFY: {artist_name}" format and keeps only the longest/best version
of each track while removing duplicates, radio edits, etc.
"""

import os
import sys
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Set, Optional

from dotenv import load_dotenv
from drivers.spotify_client import authenticate_spotify

from helpers.deduplication_helper import (
    is_unchartify_playlist,
    deduplicate_playlist,
    remove_tracks_from_playlist,
    get_track_display_info,
)
from utils.logger import setup_logger

# Set up logging
dedup_logger = setup_logger('unchartify_deduplicator', 'logs/unchartify_dedup.log')

# Load environment variables
load_dotenv()


def get_unchartify_playlists(spotify_client) -> List[Dict]:
    """
    Get all Unchartify playlists for the current user.

    Args:
        spotify_client: Authenticated Spotify client

    Returns:
        List of playlist information dictionaries
    """
    unchartify_playlists = []
    offset = 0
    limit = 50  # Spotify API limit

    dedup_logger.info("Fetching user's playlists to find Unchartify playlists")
    print("Fetching playlists...")

    while True:
        playlists = spotify_client.current_user_playlists(limit=limit, offset=offset)

        if not playlists['items']:
            break

        # Filter for Unchartify playlists
        for playlist in playlists['items']:
            if is_unchartify_playlist(playlist['name']):
                unchartify_playlists.append({
                    'id': playlist['id'],
                    'name': playlist['name'],
                    'tracks': playlist['tracks']['total']
                })
                dedup_logger.info(
                    f"Found Unchartify playlist: {playlist['name']} with {playlist['tracks']['total']} tracks")

        offset += limit

        if offset >= playlists['total']:
            break

    dedup_logger.info(f"Found {len(unchartify_playlists)} Unchartify playlists")
    return unchartify_playlists


def deduplicate_unchartify_playlists(spotify_client, dry_run: bool = False,
                                     playlist_ids: Optional[List[str]] = None,
                                     playlist_names: Optional[List[str]] = None) -> Dict:
    """
    Deduplicate all Unchartify playlists or specific ones if provided.

    Args:
        spotify_client: Authenticated Spotify client
        dry_run: If True, don't actually remove tracks, just show what would be removed
        playlist_ids: Optional list of playlist IDs to process
        playlist_names: Optional list of playlist names to process

    Returns:
        Dictionary with results statistics
    """
    playlists = []

    # If specific playlist IDs were provided, fetch their details first
    if playlist_ids:
        for playlist_id in playlist_ids:
            try:
                # Clean up the playlist ID (remove URL parameters if present)
                clean_id = playlist_id.split('?')[0]
                playlist = spotify_client.playlist(clean_id, fields="id,name,tracks.total")
                playlists.append({
                    'id': playlist['id'],
                    'name': playlist['name'],
                    'tracks': playlist['tracks']['total']
                })
                dedup_logger.info(
                    f"Added specific playlist by ID: {playlist['name']} with {playlist['tracks']['total']} tracks")
            except Exception as e:
                dedup_logger.error(f"Error fetching playlist {playlist_id}: {e}")
                print(f"Error fetching playlist ID {playlist_id}: {e}")

    # If specific playlist names were provided, find them
    if playlist_names:
        # We need to get all user playlists first
        user_playlists = []
        offset = 0
        limit = 50  # Spotify API limit

        print("Fetching all your playlists to find matches by name...")

        while True:
            results = spotify_client.current_user_playlists(limit=limit, offset=offset)

            if not results['items']:
                break

            user_playlists.extend(results['items'])
            offset += limit

            if offset >= results['total']:
                break

        # Find matches for each requested name
        for name_to_find in playlist_names:
            found = False
            for playlist in user_playlists:
                if name_to_find.lower() in playlist['name'].lower():
                    playlists.append({
                        'id': playlist['id'],
                        'name': playlist['name'],
                        'tracks': playlist['tracks']['total']
                    })
                    found = True
                    dedup_logger.info(
                        f"Added specific playlist by name: {playlist['name']} with {playlist['tracks']['total']} tracks")
                    print(f"Found playlist: {playlist['name']}")

            if not found:
                print(f"Warning: No playlist found matching '{name_to_find}'")
                dedup_logger.warning(f"No playlist found matching name: {name_to_find}")

    # If no specific playlists provided, get all Unchartify playlists
    if not playlist_ids and not playlist_names:
        playlists = get_unchartify_playlists(spotify_client)

        if not playlists:
            dedup_logger.warning("No Unchartify playlists found!")
            print("No Unchartify playlists found in your account.")
            return {'playlists_processed': 0, 'total_tracks_removed': 0}

    # Print summary of playlists to process
    print(f"\nFound {len(playlists)} Unchartify playlists:")
    for i, playlist in enumerate(playlists, 1):
        print(f"{i}. {playlist['name']} ({playlist['tracks']} tracks)")

    # Statistics
    stats = {
        'playlists_processed': 0,
        'total_tracks_removed': 0,
        'details': {}
    }

    # Create logs directory if it doesn't exist
    logs_dir = Path('logs/deduplication_reports')
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Generate report file path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = logs_dir / f'deduplication_report_{timestamp}.txt'

    with open(report_path, 'w', encoding='utf-8') as report_file:
        report_file.write(f"Unchartify Playlist Deduplication Report\n")
        report_file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report_file.write(f"{'Dry run: ' if dry_run else ''}Showing changes{' only' if dry_run else ' to be made'}\n\n")

        # Process each playlist
        for playlist in playlists:
            playlist_id = playlist['id']
            playlist_name = playlist['name']

            print(f"\nProcessing: {playlist_name}")
            dedup_logger.info(f"Processing playlist: {playlist_name} (ID: {playlist_id})")

            report_file.write(f"\n{'=' * 80}\n")
            report_file.write(f"PLAYLIST: {playlist_name}\n")
            report_file.write(f"{'=' * 80}\n\n")

            # Identify duplicates
            try:
                tracks_to_keep, tracks_to_remove = deduplicate_playlist(spotify_client, playlist_id)

                # Write to report file
                if tracks_to_remove:
                    report_file.write(f"Found {len(tracks_to_remove)} duplicate tracks to remove:\n\n")

                    # Group by primary track
                    duplicates_by_primary = {}
                    for track in tracks_to_remove:
                        # Find which kept track this is a duplicate of
                        for keep_track in tracks_to_keep:
                            # We'll use a simple check on track name/artist for the report
                            if track.get('artists', [{}])[0].get('name') == keep_track.get('artists', [{}])[0].get(
                                    'name'):
                                # Check if names are similar enough (basic check)
                                base_name_kept = keep_track.get('name', '').lower().split('[')[0].split('(')[0].strip()
                                base_name_remove = track.get('name', '').lower().split('[')[0].split('(')[0].strip()

                                if base_name_kept == base_name_remove:
                                    keep_id = keep_track.get('id')
                                    if keep_id not in duplicates_by_primary:
                                        duplicates_by_primary[keep_id] = {
                                            'keep': keep_track,
                                            'duplicates': []
                                        }
                                    duplicates_by_primary[keep_id]['duplicates'].append(track)
                                    break

                    # Write grouped duplicates to report
                    for primary_id, group in duplicates_by_primary.items():
                        keep_track = group['keep']
                        duplicates = group['duplicates']

                        report_file.write(f"Keeping: {get_track_display_info(keep_track)}\n")
                        report_file.write(f"Removing these duplicates:\n")

                        for dupe in duplicates:
                            report_file.write(f"- {get_track_display_info(dupe)}\n")

                        report_file.write("\n")

                    # Print summary to console
                    print(f"Found {len(tracks_to_remove)} duplicates to remove")

                    # Log some sample duplicates
                    sample_limit = min(3, len(tracks_to_remove))
                    if sample_limit > 0:
                        print("Sample duplicates to be removed:")
                        for i in range(sample_limit):
                            print(f"  - {get_track_display_info(tracks_to_remove[i])}")

                        if len(tracks_to_remove) > sample_limit:
                            print(f"  - ...and {len(tracks_to_remove) - sample_limit} more")
                else:
                    report_file.write("No duplicates found in this playlist.\n")
                    print("No duplicates found in this playlist.")

                # Remove tracks if not a dry run
                if tracks_to_remove and not dry_run:
                    # Group tracks by what they're duplicates of
                    duplicates_by_primary = {}

                    # Build mapping of track IDs to make lookups easier
                    kept_tracks_map = {track['id']: track for track in tracks_to_keep if 'id' in track}

                    # Make a copy of tracks to remove that we'll work from
                    unmatched_tracks = list(tracks_to_remove)
                    matched_tracks = []

                    # First try to match by using our original grouping logic (which should be available from deduplicate_playlist)
                    # Get all tracks in the playlist
                    all_playlist_tracks = []
                    offset = 0
                    limit = 100  # Spotify API limit

                    while True:
                        results = spotify_client.playlist_items(
                            playlist_id,
                            offset=offset,
                            limit=limit,
                            fields='items(track(id,name,artists(name),duration_ms)),total'
                        )

                        if not results['items']:
                            break

                        all_playlist_tracks.extend([item['track'] for item in results['items'] if item['track']])
                        offset += limit

                        if offset >= results['total']:
                            break

                    # Re-run the grouping logic
                    from helpers.deduplication_helper import group_similar_tracks, rank_track_versions
                    track_groups = group_similar_tracks(all_playlist_tracks)

                    # Use the groups to build our mapping for display
                    for group in track_groups:
                        if len(group) > 1:  # Only process groups with duplicates
                            # Rank the versions
                            ranked_tracks = rank_track_versions(group)
                            best_track = ranked_tracks[0]
                            other_tracks = ranked_tracks[1:]

                            # Check which tracks are in our remove list
                            remove_ids = [t['id'] for t in tracks_to_remove if 'id' in t]
                            matches = [t for t in other_tracks if 'id' in t and t['id'] in remove_ids]

                            if matches:
                                # Add to our display mapping
                                if best_track['id'] not in duplicates_by_primary:
                                    duplicates_by_primary[best_track['id']] = {
                                        'keep': best_track,
                                        'duplicates': []
                                    }

                                for match in matches:
                                    duplicates_by_primary[best_track['id']]['duplicates'].append(match)
                                    # Track which ones we've matched successfully
                                    if match in unmatched_tracks:
                                        unmatched_tracks.remove(match)
                                        matched_tracks.append(match)

                    # If there are still unmatched tracks, something went wrong in our grouping logic
                    # This should not happen with the current implementation, but we'll leave this check
                    # as a safety measure and log warnings
                    if unmatched_tracks:
                        dedup_logger.warning(
                            f"Found {len(unmatched_tracks)} tracks that couldn't be matched to kept tracks")
                        for track in unmatched_tracks:
                            dedup_logger.warning(
                                f"Unmatched track: {track.get('name', 'Unknown')} by {track.get('artists', [{}])[0].get('name', 'Unknown')}")

                        # We'll exclude these from removal
                        tracks_to_remove = matched_tracks
                        dedup_logger.info(
                            f"Will only remove {len(tracks_to_remove)} matched tracks instead of original {len(tracks_to_remove) + len(unmatched_tracks)}")

                    # If we have no tracks to remove after filtering, skip
                    if not tracks_to_remove:
                        print("No duplicate tracks to remove after validation checks.")
                        continue

                    # Ask for confirmation showing the kept tracks vs removed tracks
                    print(f"\nThe following changes will be made to playlist '{playlist_name}':")
                    print("=" * 80)

                    group_count = 1
                    for primary_id, group in duplicates_by_primary.items():
                        keep_track = group['keep']
                        duplicates = group['duplicates']

                        if duplicates:  # Only show groups that have duplicates to remove
                            print(f"\nGroup {group_count}: KEEPING:")
                            print(f"  ✅ {get_track_display_info(keep_track)}")
                            print(f"  REMOVING:")
                            for i, dupe in enumerate(duplicates, 1):
                                print(f"  ❌ {i}. {get_track_display_info(dupe)}")
                            group_count += 1

                    print("\n" + "=" * 80)
                    confirmation = input(
                        f"\nRemove all {len(tracks_to_remove)} tracks shown above from '{playlist_name}'? (y/n): ")

                    if confirmation.lower() != 'y':
                        print("Skipping removal for this playlist.")
                        report_file.write(f"\nUser skipped removal of {len(tracks_to_remove)} tracks.\n")
                        continue

                    print(f"Removing {len(tracks_to_remove)} duplicate tracks...")
                    success = remove_tracks_from_playlist(spotify_client, playlist_id, tracks_to_remove)

                    if success:
                        print(f"Successfully removed {len(tracks_to_remove)} duplicate tracks.")
                        report_file.write(f"\nSuccessfully removed {len(tracks_to_remove)} duplicate tracks.\n")

                        stats['total_tracks_removed'] += len(tracks_to_remove)
                    else:
                        print(f"Error removing some duplicate tracks. Check the logs for details.")
                        report_file.write(f"\nError removing some duplicate tracks. Please check the logs.\n")

                # Update stats
                stats['playlists_processed'] += 1
                stats['details'][playlist_id] = {
                    'name': playlist_name,
                    'tracks_kept': len(tracks_to_keep),
                    'tracks_removed': len(tracks_to_remove)
                }

            except Exception as e:
                dedup_logger.error(f"Error processing playlist {playlist_name}: {e}", exc_info=True)
                print(f"Error processing playlist: {e}")
                report_file.write(f"Error processing playlist: {e}\n")

            # Add spacing in report
            report_file.write("\n\n")

            # Slight delay to avoid API rate limits
            time.sleep(1)

        # Write summary
        report_file.write(f"\n{'=' * 80}\n")
        report_file.write(f"SUMMARY\n")
        report_file.write(f"{'=' * 80}\n\n")
        report_file.write(f"Playlists processed: {stats['playlists_processed']}\n")
        report_file.write(f"Total duplicate tracks removed: {stats['total_tracks_removed']}\n")

        # Add playlist details
        report_file.write("\nBy playlist:\n")
        for playlist_id, detail in stats['details'].items():
            report_file.write(
                f"- {detail['name']}: removed {detail['tracks_removed']} of {detail['tracks_kept'] + detail['tracks_removed']} tracks\n")

    # Print overall summary
    print(f"\nDeduplication complete!")
    print(f"Processed {stats['playlists_processed']} playlists")

    if dry_run:
        print(f"Would remove {stats['total_tracks_removed']} duplicate tracks (dry run)")
    else:
        print(f"Removed {stats['total_tracks_removed']} duplicate tracks")

    print(f"\nSee detailed report at: {report_path}")

    return stats


def main():
    """Main function to run the deduplicator."""
    parser = argparse.ArgumentParser(description='Deduplicate Spotify playlists.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be removed without actually removing tracks')
    parser.add_argument('--playlist-id', action='append', dest='playlist_ids',
                        help='Specific playlist ID to process (can be used multiple times)')
    parser.add_argument('--playlist-name', action='append', dest='playlist_names',
                        help='Specific playlist name to process - will search for partial matches (can be used multiple times)')
    parser.add_argument('--unchartify-only', action='store_true',
                        help='Only process playlists that start with "UNCHARTIFY:" (ignored when --playlist-id or --playlist-name is used)')

    args = parser.parse_args()

    try:
        # Authenticate with Spotify
        print("Connecting to Spotify...")
        spotify_client = authenticate_spotify()

        # Run deduplication
        print("Starting deduplication process...")

        # Handle specific playlists case
        if args.playlist_ids:
            print(f"Processing {len(args.playlist_ids)} specific playlist(s) by ID...")
        elif args.playlist_names:
            print(f"Searching for {len(args.playlist_names)} specific playlist(s) by name...")
        elif args.unchartify_only:
            print("Finding all Unchartify playlists...")
        else:
            print("Finding all playlists...")

        stats = deduplicate_unchartify_playlists(
            spotify_client,
            dry_run=args.dry_run,
            playlist_ids=args.playlist_ids,
            playlist_names=args.playlist_names
        )

        # Exit with appropriate code
        if stats['playlists_processed'] == 0:
            print("No playlists were processed. Please check your input.")
            sys.exit(1)

        sys.exit(0)

    except Exception as e:
        dedup_logger.error(f"Unhandled error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
