from dotenv import load_dotenv
from utils import logger
from scripts.action_steps import *
from drivers.spotify_client import authenticate_spotify, fetch_playlists, fetch_all_unique_tracks, fetch_master_tracks, \
    find_playlists_for_master_tracks

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')

# Clear the existing log file
with open('db.log', 'w'):
    pass

class Debug:
    def __init__(self):
        logging.info("Debug instance created.")
        self.spotify_client = authenticate_spotify()

    def debug_fetch_my_playlists(self):
        my_playlists = fetch_playlists(self.spotify_client)
        print("My Playlists:")
        for playlist in my_playlists:
            print(playlist)

    def debug_fetch_master_tracks(self):
        master_tracks = fetch_master_tracks(self.spotify_client, MASTER_PLAYLIST_ID)
        print("Master Tracks:")
        for track in master_tracks:
            print(track)

    def debug_find_playlists_for_master_tracks(self):
        master_tracks = fetch_master_tracks(self.spotify_client, MASTER_PLAYLIST_ID)
        playlists = find_playlists_for_master_tracks(self.spotify_client, master_tracks, MASTER_PLAYLIST_ID)
        print("Playlists:")
        for playlist in playlists:
            print(playlist)

    def debug_fetch_all_unique_tracks(self):
        my_playlists = fetch_playlists(self.spotify_client)
        all_unique_tracks = fetch_all_unique_tracks(self.spotify_client, my_playlists)
        print("All Tracks:")
        for track in all_unique_tracks:
            print(track)


if __name__ == "__main__":
    debugger = Debug()
    # debugger.debug_fetch_my_playlists()
    # debugger.debug_fetch_master_tracks()
    debugger.debug_find_playlists_for_master_tracks()
    # debugger.debug_fetch_all_unique_tracks()
