import os
import argparse
from mutagen.id3 import ID3, ID3NoHeaderError
from pathlib import Path


def remove_track_id_from_file(file_path):
    """Remove TrackId tag from a single MP3 file."""
    try:
        tags = ID3(file_path)
        if 'TXXX:TRACKID' in tags:
            track_id = tags['TXXX:TRACKID'].text[0]
            tags.delall('TXXX:TRACKID')
            tags.save(file_path)
            return True, track_id
        return False, None
    except ID3NoHeaderError:
        return False, None
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False, None


def find_file_in_directory(filename, search_dir):
    """Find a file by name in the given directory (recursive search)."""
    for root, _, files in os.walk(search_dir):
        if filename in files:
            return os.path.join(root, filename)
    return None


def remove_track_ids_from_list(file_list, search_dir, dry_run=False):
    """Remove TrackIds from a list of specific files."""
    success_count = 0
    skip_count = 0
    not_found_count = 0

    for filename in file_list:
        # First try direct path
        if os.path.exists(filename):
            file_path = filename
        else:
            # Try to find the file in the search directory
            file_path = find_file_in_directory(filename, search_dir)

        if not file_path:
            print(f"File not found: {filename}")
            not_found_count += 1
            continue

        if dry_run:
            print(f"Would remove TrackId from: {file_path}")
            success_count += 1
        else:
            success, track_id = remove_track_id_from_file(file_path)
            if success:
                print(f"Removed TrackId '{track_id}' from: {file_path}")
                success_count += 1
            else:
                print(f"No TrackId found in: {file_path}")
                skip_count += 1

    return success_count, skip_count, not_found_count


def main():
    parser = argparse.ArgumentParser(description='Remove TrackId metadata from MP3 files.')
    parser.add_argument('--files', nargs='+', help='List of specific MP3 files to process')
    parser.add_argument('--file-list', type=str, help='Text file containing list of MP3 files, one per line')
    parser.add_argument('--directory', type=str, required=True, help='Directory to search for the files')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')

    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"Error: Directory not found: {args.directory}")
        return

    file_list = []

    # Process file list from command line
    if args.files:
        file_list.extend(args.files)

    # Process file list from text file
    if args.file_list:
        try:
            with open(args.file_list, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    # Clean the line
                    line = line.strip()
                    if line:
                        file_list.append(line)
        except Exception as e:
            print(f"Error reading file list: {e}")
            return

    if not file_list:
        print("No files specified. Use --files or --file-list.")
        return

    print(f"Found {len(file_list)} files to process")
    print(f"Searching in directory: {args.directory}")
    if args.dry_run:
        print("DRY RUN - No changes will be made")

    success_count, skip_count, not_found_count = remove_track_ids_from_list(
        file_list, args.directory, args.dry_run)

    print(f"\nSummary:")
    print(f"Total files in list: {len(file_list)}")
    print(f"Files found and processed: {success_count + skip_count}")
    print(f"TrackIds removed: {success_count}")
    print(f"Files with no TrackId: {skip_count}")
    print(f"Files not found: {not_found_count}")


if __name__ == "__main__":
    main()
