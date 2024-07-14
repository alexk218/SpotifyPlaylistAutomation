import logging
import shutil
import subprocess
import os

import requests
from dotenv import load_dotenv

from helpers.file_helper import track_exists, get_expected_filename

load_dotenv()

DEEMIX_PATH = os.getenv('DEEMIX_PATH')
MASTER_TRACKS_DOWNLOAD_DIRECTORY = os.getenv('MASTER_TRACKS_DOWNLOAD_DIRECTORY')


def download_track(track_title, artist_name, deezer_link):
    if track_exists(track_title, artist_name, MASTER_TRACKS_DOWNLOAD_DIRECTORY):
        logging.info(f"Track already exists: {track_title} by {artist_name}. Skipping download.")
        return

    # Construct the deemix command
    command = [
        DEEMIX_PATH,
        '-b', 'mp3_320',
        '-p', MASTER_TRACKS_DOWNLOAD_DIRECTORY,
        deezer_link
    ]

    # Run the deemix command
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Failed to download {track_title} by {artist_name}. Error: {result.stderr}")
    else:
        print(f"Successfully downloaded {track_title} by {artist_name}.")

        # Rename the downloaded file to match the expected filename
        downloaded_files = os.listdir(MASTER_TRACKS_DOWNLOAD_DIRECTORY)
        for file in downloaded_files:
            if file.endswith(".mp3"):
                original_file_path = os.path.join(MASTER_TRACKS_DOWNLOAD_DIRECTORY, file)
                expected_file_path = os.path.join(MASTER_TRACKS_DOWNLOAD_DIRECTORY,
                                                  get_expected_filename(track_title, artist_name))
                if not os.path.isfile(expected_file_path):
                    shutil.move(original_file_path, expected_file_path)
                    logging.info(f"Renamed {file} to {get_expected_filename(track_title, artist_name)}")
                break

def get_deezer_link(track_title, artist_name):
    query = f"{track_title} {artist_name}"
    url = f"https://api.deezer.com/search?q={query}"
    response = requests.get(url)
    data = response.json()

    if data['data']:
        return data['data'][0]['link']
    else:
        return None


if __name__ == "__main__":
    track = "Optimus"
    artist = "Nail"
    link = get_deezer_link(track, artist)
    if link:
        print(f"Found Deezer link: {link}")
        download_track(track, artist, link)
    else:
        print("No Deezer link found.")
