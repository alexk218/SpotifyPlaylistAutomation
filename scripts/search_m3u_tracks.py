import argparse
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from helpers.m3u_helper import search_tracks_in_m3u_files, search_tracks_by_title_in_m3u_files


def search_by_title(m3u_directory, search_term, case_sensitive=False):
    """
    Search for tracks by title with flexible matching
    """
    print(f"Searching for tracks containing: '{search_term}'")
    print(f"M3U Directory: {m3u_directory}")
    print(f"Case sensitive: {case_sensitive}")
    print("=" * 60)

    # Find all M3U files and search through them
    matches_found = 0

    for root, dirs, files in os.walk(m3u_directory):
        for filename in files:
            if not filename.lower().endswith('.m3u'):
                continue

            m3u_path = os.path.join(root, filename)
            relative_m3u_path = os.path.relpath(m3u_path, m3u_directory)

            try:
                with open(m3u_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                    for i, line in enumerate(lines):
                        # Look for EXTINF lines which contain track info
                        if line.startswith('#EXTINF:'):
                            try:
                                info_part = line[8:].split(',', 1)
                                if len(info_part) > 1:
                                    track_info = info_part[1].strip()

                                    # Get the file path from the next line
                                    file_path = ""
                                    if i + 1 < len(lines) and not lines[i + 1].startswith('#'):
                                        file_path = lines[i + 1].strip()

                                    # Check if search term is in track info
                                    search_in = track_info if case_sensitive else track_info.lower()
                                    term_to_find = search_term if case_sensitive else search_term.lower()

                                    if term_to_find in search_in:
                                        matches_found += 1
                                        print(f"\n[{matches_found}] Found in: {relative_m3u_path}")
                                        print(f"    Track: {track_info}")
                                        print(f"    File:  {file_path}")

                            except Exception as e:
                                print(f"Error parsing line in {relative_m3u_path}: {e}")
                                continue

            except Exception as e:
                print(f"Error reading M3U file {relative_m3u_path}: {e}")
                continue

    print("=" * 60)
    print(f"Search complete. Found {matches_found} matches.")


def search_by_exact_titles(m3u_directory, titles):
    """
    Search for exact track titles
    """
    print(f"Searching for exact titles: {titles}")
    print(f"M3U Directory: {m3u_directory}")
    print("=" * 60)

    results = search_tracks_by_title_in_m3u_files(m3u_directory, titles)

    total_matches = 0
    for title, matches in results.items():
        print(f"\nSearching for: '{title}'")
        if matches:
            print(f"Found {len(matches)} match(es):")
            for i, match in enumerate(matches, 1):
                total_matches += 1
                print(f"  [{i}] Playlist: {match['playlist']}")
                print(f"      Track: {match['track_info']}")
                print(f"      File:  {match['file_path']}")
        else:
            print("  No matches found")

    print("=" * 60)
    print(f"Search complete. Found {total_matches} total matches.")


def main():
    parser = argparse.ArgumentParser(
        description='Search for tracks in M3U playlists',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python scripts/search_m3u_tracks.py "sundaland"
  python scripts/search_m3u_tracks.py "plommon" --case-sensitive
  python scripts/search_m3u_tracks.py --exact "Sundaland (Extended Mix)"
  python scripts/search_m3u_tracks.py --exact "Track 1" "Track 2" "Track 3"
  python scripts/search_m3u_tracks.py "artist name" --dir "D:\\my_playlists"
        '''
    )

    # Positional argument for search term(s)
    parser.add_argument(
        'search_terms',
        nargs='+',
        help='Track title(s) to search for'
    )

    # Optional arguments
    parser.add_argument(
        '--dir',
        type=str,
        default=r"K:\m3u_playlists",
        help='M3U playlists directory (default: K:\\m3u_playlists)'
    )

    parser.add_argument(
        '--exact',
        action='store_true',
        help='Search for exact title matches instead of partial matches'
    )

    parser.add_argument(
        '--case-sensitive',
        action='store_true',
        help='Perform case-sensitive search'
    )

    args = parser.parse_args()

    # Validate directory exists
    if not os.path.exists(args.dir):
        print(f"Error: Directory '{args.dir}' does not exist!")
        return 1

    # Perform the search based on mode
    if args.exact:
        # Exact title matching for multiple titles
        search_by_exact_titles(args.dir, args.search_terms)
    else:
        # Partial matching - combine all search terms into one search
        search_term = ' '.join(args.search_terms)
        search_by_title(args.dir, search_term, args.case_sensitive)

    return 0


if __name__ == "__main__":
    sys.exit(main())
