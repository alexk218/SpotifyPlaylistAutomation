from dotenv import load_dotenv
from scripts.action_steps import *
from helpers.playlist_helper import *
from drivers.spotify_client import *

load_dotenv()


def main():
    spotify_client = authenticate_spotify()
    all_playlists = fetch_my_playlists(spotify_client)
    for playlist_name, playlist_id in all_playlists:
        current_song_count = fetch_playlist_song_count(spotify_client, playlist_id)
        stored_song_count = load_stored_playlist_song_count(playlist_name)

        if current_song_count != stored_song_count:
            # action.send_notification(playlist_name, current_song_count - stored_song_count)
            store_playlist_song_count(playlist_name, current_song_count)
        else:
            logging.info(f"No change in the number of tracks for {playlist_name}")


if __name__ == "__main__":
    main()
