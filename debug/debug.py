from dotenv import load_dotenv
from utils import logger
from scripts.action_steps import *
from drivers.spotify_client import authenticate_spotify, fetch_my_playlists, fetch_master_tracks

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')


class Debug:
    def __init__(self):
        logging.info("Debug instance created.")
        self.spotify_client = authenticate_spotify()

    def debug_fetch_my_playlists(self):
        my_playlists = fetch_my_playlists(self.spotify_client)
        print("My Playlists:")
        for name, description, playlist_id in my_playlists:
            print(f"Name: {name}, File path: {description}, ID: {playlist_id}")

    def debug_fetch_master_tracks(self):
        master_tracks = fetch_master_tracks(self.spotify_client)
        print("Master Tracks:")
        for track in master_tracks:
            print(track)


if __name__ == "__main__":
    debugger = Debug()
    debugger.debug_fetch_my_playlists()
    # debugger.debug_fetch_master_tracks()
