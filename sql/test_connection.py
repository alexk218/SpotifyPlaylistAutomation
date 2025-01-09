import os
import pyodbc
from dotenv import load_dotenv
import requests
from drivers.spotify_client import authenticate_spotify, fetch_master_tracks
from utils.logger import setup_logger

load_dotenv()

SERVER_CONNECTION_STRING = os.getenv('SERVER_CONNECTION_STRING')
DATABASE_NAME = os.getenv('DATABASE_NAME')
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')
spotify_logger = setup_logger('spotify_logger', 'sql/spotify.log')


def get_db_connection():
    try:
        connection = pyodbc.connect(
            r'DRIVER={SQL Server};'
            fr'SERVER={SERVER_CONNECTION_STRING};'
            fr'DATABASE={DATABASE_NAME};'
            r'Trusted_Connection=yes;'
        )
        print(f"Connected to SERVER: {SERVER_CONNECTION_STRING}, DATABASE: {DATABASE_NAME}")
        return connection
    except pyodbc.Error as e:
        print(f"Database connection failed: {e}")
        return None


def test_fetch_playlists():
    connection = get_db_connection()
    if not connection:
        return
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT TOP 5 PlaylistName FROM Playlists")
        playlists = cursor.fetchall()
        print("Sample Playlists:")
        for pl in playlists:
            print(pl.PlaylistName)
    except pyodbc.Error as e:
        print("Error fetching playlists:", e)
    finally:
        cursor.close()
        connection.close()


def test_spotify_api_health(spotify_client):
    try:
        # Try a simple API call
        spotify_client.current_user()
        spotify_logger.info("Spotify API is responding")
        print("Spotify API is responding")
        # fetch_master_tracks(spotify_client, "3lhMvQtcjKg4vSn8YIBk2W")
        fetch_playlists(spotify_client)
        spotify_logger.info(f"Playlists fetched successfully")
        # spotify_logger.info(f"Master playlist tracks fetched successfully")
        return True
    except Exception as e:
        print(f"Spotify API health check failed: {e}")
        spotify_logger.error(f"Spotify API health check failed: {e}")
        return False


def fetch_playlists(spotify_client, total_limit=500, debug_limit=None):
    try:
        # Fetch playlists using the Spotipy internal `_get` method
        response = spotify_client._get("me/playlists", limit=50, offset=0)

        # Log the raw HTTP response status code and headers
        print(f"Response status code: {response.status_code}")
        spotify_logger.info(f"Response status code: {response.status_code}")

        # Check if rate limit is hit
        if response.status_code == 429:  # Rate limit
            retry_after = response.headers.get('Retry-After', 'unknown')
            print(f"Rate limit hit. Retry after {retry_after} seconds.")
            spotify_logger.warning(f"Rate limit hit. Retry after {retry_after} seconds.")
            return []

        # Log the response body if not rate limited
        print(f"Response headers: {response.headers}")
        spotify_logger.debug(f"Response headers: {response.headers}")

        response_body = response.json()
        print(f"Response body: {response_body}")
        spotify_logger.debug(f"Response body: {response_body}")

        return response_body.get('items', [])  # Return fetched playlists
    except requests.exceptions.RequestException as e:
        # Log any request exceptions
        print(f"Error during API call: {e}")
        spotify_logger.error(f"Error during API call: {e}")
        return []
    except Exception as e:
        # Catch other exceptions
        print(f"Unexpected error: {e}")
        spotify_logger.error(f"Unexpected error: {e}")
        return []

def main():
    print(os.getenv("SPOTIPY_DEBUG"))
    client = authenticate_spotify()
    test_spotify_api_health(client)


if __name__ == "__main__":
    main()
    # test_fetch_playlists()
    # client = authenticate_spotify()
    # test_spotify_api_health(client)
