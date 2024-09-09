from collections import defaultdict
import logging
import os
from typing import List, Tuple

# Dictionary to store tracks and the playlists they appear in
track_occurrences = defaultdict(list)

def write_playlist_to_markdown(playlist_name: str, tracks: List[Tuple[str, str, str]]):
    logging.info(f"Writing playlist to markdown: {playlist_name}")

    directory = 'playlists_markdown'

    # Create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Create a valid filename by removing any forbidden characters
    valid_filename = ''.join(c for c in playlist_name if c.isalnum() or c in (' ', '_')).rstrip()

    file_path = os.path.join(directory, f'{valid_filename}.md')

   # Write the markdown file to the specified directory
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write('## Tags:\n')
        f.write(f'- [[6 - Tags/Music/Playlists/{playlist_name}|{playlist_name}]]\n\n')
        f.write('## Tracks:\n')

        for track_name, artist_name, album_name in tracks:
            # Add the current playlist to the track_occurrences dictionary
            track_key = f'{track_name} | {artist_name}'
            track_occurrences[track_key].append(playlist_name)

            # Write the track name and artist
            f.write(f'- {track_name} | {artist_name}\n')

            # Check if the track is in multiple playlists
            other_playlists = [p for p in track_occurrences[track_key] if p != playlist_name]
            if other_playlists:
                # Add an indicator with links to other playlists, indented with a tab and on a new line
                other_playlists_links = ', '.join([f'[[{p}]]' for p in other_playlists])
                f.write(f'\t- Also in: {other_playlists_links}\n')