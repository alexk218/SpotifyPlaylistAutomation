from dotenv import load_dotenv

from controllers.action_steps import *
from drivers.spotify_client import authenticate_spotify, fetch_my_playlists

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

logging.basicConfig(filename='debug.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Debug:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.spotify_client = authenticate_spotify()

    def debug_fetch_my_playlists(self):
        my_playlists = fetch_my_playlists(self.spotify_client)
        print("My Playlists:", my_playlists)


if __name__ == "__main__":
    debugger = Debug()
    debugger.debug_fetch_my_playlists()