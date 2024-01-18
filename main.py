import logging
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import smtplib
import os
from dotenv import load_dotenv
load_dotenv()  # loads the variables from .env

# Setup logging
logging.basicConfig(filename='spotify_script.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

# Authentication with Spotify
def authenticate_spotify():
    logging.info("Authenticating with Spotify")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID,
                                                   client_secret=SPOTIFY_CLIENT_SECRET,
                                                   redirect_uri="http://localhost:8888/callback",
                                                   scope="playlist-read-private"))
    return sp

# Fetch all private playlists
def fetch_all_playlists(spotify_client):
    logging.info("Fetching all playlists")
    playlists = spotify_client.current_user_playlists()
    return [(playlist['name'], playlist['id']) for playlist in playlists['items']]

# Fetch tracks from a playlist
def fetch_playlist_tracks(spotify_client, playlist_id):
    logging.info(f"Fetching tracks for playlist ID {playlist_id}")
    tracks = spotify_client.playlist_tracks(playlist_id)
    return [(" - ".join([track['track']['name'], ", ".join([artist['name'] for artist in track['track']['artists']])])) for track in tracks['items']]

# Compare with stored data and find new tracks
def find_new_tracks(current_tracks, stored_tracks):
    new_tracks = list(set(current_tracks) - set(stored_tracks))
    logging.info(f"New tracks identified: {new_tracks}")
    return new_tracks


# Send notification
def send_notification(playlist_name, change_in_count):
    logging.info(f"Sending notification for new tracks in {playlist_name}")
    sender_email = SENDER_EMAIL
    receiver_email = SENDER_EMAIL  # assuming you're sending the email to yourself
    password = EMAIL_PASSWORD
    subject = f"New Tracks in {playlist_name}"
    body = f"Change in number of tracks for {playlist_name}: {change_in_count}"
    message = f"Subject: {subject}\n\n{body}"
    message = message.encode('utf-8')

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)

def load_stored_tracks(playlist_name):
    logging.info(f"Loading stored tracks for {playlist_name}")
    try:
        with open(f"{playlist_name}_tracks.txt", "r", encoding='utf-8') as file:
            stored_tracks = [tuple(line.strip().split(' - ')) for line in file]
        return stored_tracks
    except FileNotFoundError:
        return []
    except UnicodeDecodeError:
        logging.error(f"Error decoding file for {playlist_name}")
        return []

def store_tracks(playlist_name, tracks):
    logging.info(f"Storing tracks for {playlist_name}")
    with open(f"{playlist_name}_tracks.txt", "w", encoding='utf-8') as file:
        for track in tracks:
            file.write(f"{track[0]} - {track[1]}\n")

def store_playlist_song_count(playlist_name, song_count):
    with open(f"{playlist_name}_count.txt", "w", encoding='utf-8') as file:
        file.write(str(song_count))

def load_stored_playlist_song_count(playlist_name):
    try:
        with open(f"{playlist_name}_count.txt", "r", encoding='utf-8') as file:
            return int(file.read().strip())
    except (FileNotFoundError, ValueError):
        return 0

def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']

# Main function
def main():
    spotify_client = authenticate_spotify()
    all_playlists = fetch_all_playlists(spotify_client)
    for playlist_name, playlist_id in all_playlists:
        current_song_count = fetch_playlist_song_count(spotify_client, playlist_id)
        stored_song_count = load_stored_playlist_song_count(playlist_name)

        if current_song_count != stored_song_count:
            send_notification(playlist_name, current_song_count - stored_song_count)
            store_playlist_song_count(playlist_name, current_song_count)
        else:
            logging.info(f"No change in the number of tracks for {playlist_name}")


if __name__ == "__main__":
    main()
