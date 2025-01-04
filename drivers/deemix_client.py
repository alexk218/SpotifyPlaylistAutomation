import logging
import shutil
import subprocess
import os

import requests
from dotenv import load_dotenv

from helpers.file_helper import track_exists, get_normalized_filename
from sql.helpers.db_helper import fetch_master_tracks_db

load_dotenv()

DEEMIX_PATH = os.getenv('DEEMIX_PATH')
MASTER_TRACKS_DIRECTORY = os.getenv('MASTER_TRACKS_DIRECTORY')

with open('deemix.log', 'w'):
    pass

logging.basicConfig(filename='deemix.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def download_track(track_title, artist_title, album_title, deezer_link):
    command = [
        DEEMIX_PATH,
        '-b', 'mp3_320',
        '-p', MASTER_TRACKS_DIRECTORY,
        deezer_link
    ]

    # Run the deemix command
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Failed to download {artist_title} - {track_title}. Error: {result.stderr}")
        logging.info(f"Failed to download {artist_title} - {track_title}. Error: {result.stderr}")
    else:
        print(f"Successfully downloaded {artist_title} - {track_title}.")
        logging.info(f"Successfully downloaded {artist_title} - {track_title}.")

        # Rename the downloaded file to match the expected filename
        # downloaded_files = os.listdir(MASTER_TRACKS_DIRECTORY)
        # for file in downloaded_files:
        #     if file.endswith(".mp3"):
        #         original_file_path = os.path.join(MASTER_TRACKS_DIRECTORY, file)
        #         expected_file_path = os.path.join(MASTER_TRACKS_DIRECTORY,
        #                                           get_expected_filename(track_title, artist_name, album_name))
        #         if not os.path.isfile(expected_file_path):
        #             shutil.move(original_file_path, expected_file_path)
        #             logging.info(f"Renamed {file} to {get_expected_filename(track_title, artist_name, album_name)}")
        #         break


def get_deezer_link(track_title, artist_title, album_title):
    query = f"{track_title} {artist_title} {album_title}"
    url = f"https://api.deezer.com/search?q={query}"
    response = requests.get(url)
    data = response.json()

    if data['data']:
        for track in data['data']:
            if track['album']['title'].lower() == album_title.lower():
                return track['link']
    return None


if __name__ == "__main__":
    tracks = fetch_master_tracks_db()
    logging.info(f"Found {len(tracks)} tracks in the database.")
    for track in tracks:
        track_name, artist_name, album_name = track

        # Extract the first artist's name
        first_artist_name = artist_name.split(',')[0].strip()

        if track_exists(track_name, first_artist_name, MASTER_TRACKS_DIRECTORY):
            logging.info(
                f"Track already exists: {track_name} by {first_artist_name}. Skipping download.")
            continue

        link = get_deezer_link(track_name, first_artist_name, album_name)
        if link:
            logging.info(f"Found Deezer link: {link}")
            download_track(track_name, first_artist_name, album_name, link)
        else:
            logging.info(f"No Deezer link found for {track_name}, {first_artist_name}, {album_name}.")

    # track = "Optimus"
    # artist = "Nail"
    # album = "Live at Robert Johnson Vol.5"
    # link = get_deezer_link(track, artist, album)
    # if link:
    #     print(f"Found Deezer link: {link}")
    #     download_track(track, artist, album, link)
    # else:
    #     print("No Deezer link found.")
