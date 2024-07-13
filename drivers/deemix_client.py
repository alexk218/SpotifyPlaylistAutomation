import subprocess
import os

from dotenv import load_dotenv

load_dotenv()

DEEMIX_PATH = os.getenv('DEEMIX_PATH')
MASTER_TRACKS_DOWNLOAD_DIRECTORY = os.getenv('MASTER_TRACKS_DOWNLOAD_DIRECTORY')

def download_track(track_title, artist):
    # Construct the deemix command
    command = [
        DEEMIX_PATH,
        '--bitrate',
        'MP3_320',
        '--output',
        MASTER_TRACKS_DOWNLOAD_DIRECTORY,
        '--search',
        f'{track_title} {artist}'
    ]

    # Run the deemix command
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Failed to download {track_title} by {artist}. Error: {result.stderr}")
    else:
        print(f"Successfully downloaded {track_title} by {artist}.")
