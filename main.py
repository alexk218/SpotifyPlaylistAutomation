import logging
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import smtplib
import os
from dotenv import load_dotenv
import drivers.action_steps as action
import helpers.playlist_helper as pl

load_dotenv()  # loads environment variables

def main():
    spotify_client = action.authenticate_spotify()
    all_playlists = action.fetch_my_playlists(spotify_client)
    for playlist_name, playlist_id in all_playlists:
        current_song_count = pl.fetch_playlist_song_count(spotify_client, playlist_id)
        stored_song_count = action.load_stored_playlist_song_count(playlist_name)

        if current_song_count != stored_song_count:
            # action.send_notification(playlist_name, current_song_count - stored_song_count)
            action.store_playlist_song_count(playlist_name, current_song_count)
        else:
            logging.info(f"No change in the number of tracks for {playlist_name}")


if __name__ == "__main__":
    main()
