import logging
import shutil
import subprocess
import os

from dotenv import load_dotenv

from helpers.file_helper import track_exists, get_expected_filename

load_dotenv()

DEEMIX_PATH = os.getenv('DEEMIX_PATH')
MASTER_TRACKS_DOWNLOAD_DIRECTORY = os.getenv('MASTER_TRACKS_DOWNLOAD_DIRECTORY')


def download_track(track_title, artist):
    if track_exists(track_title, artist, MASTER_TRACKS_DOWNLOAD_DIRECTORY):
        logging.info(f"Track already exists: {track_title} by {artist}. Skipping download.")
        return

    # Construct the deemix command
    command = [
        DEEMIX_PATH,
        '-b', 'MP3_320',
        '-p', MASTER_TRACKS_DOWNLOAD_DIRECTORY,
        '--search', f'{track_title} {artist}'
    ]

    # Run the deemix command
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Failed to download {track_title} by {artist}. Error: {result.stderr}")
    else:
        print(f"Successfully downloaded {track_title} by {artist}.")

        # Rename the downloaded file to match the expected filename
        downloaded_files = os.listdir(MASTER_TRACKS_DOWNLOAD_DIRECTORY)
        for file in downloaded_files:
            if file.endswith(".mp3"):
                original_file_path = os.path.join(MASTER_TRACKS_DOWNLOAD_DIRECTORY, file)
                expected_file_path = os.path.join(MASTER_TRACKS_DOWNLOAD_DIRECTORY,
                                                  get_expected_filename(track_title, artist))
                if not os.path.isfile(expected_file_path):
                    shutil.move(original_file_path, expected_file_path)
                    logging.info(f"Renamed {file} to {get_expected_filename(track_title, artist)}")
                break


if __name__ == "__main__":
    download_track("Happier", "Marshmello")
