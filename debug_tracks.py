# save as debug_tracks.py
import os
from dotenv import load_dotenv
from drivers.spotify_client import authenticate_spotify, fetch_master_tracks

load_dotenv()
MASTER_PLAYLIST_ID = os.getenv('MASTER_PLAYLIST_ID')


def find_problematic_tracks():
    spotify_client = authenticate_spotify()
    tracks = fetch_master_tracks(spotify_client, MASTER_PLAYLIST_ID)

    print(f"Total tracks: {len(tracks)}")

    # Check for tracks with NULL IDs
    null_id_tracks = [t for t in tracks if t[0] is None]
    if null_id_tracks:
        print("\nTracks with NULL IDs:")
        for track in null_id_tracks:
            print(f"- {track[1]} by {track[2]} from {track[3]}")
    else:
        print("\nNo tracks with NULL IDs found.")

    # Also print details about the track being updated
    track_id = "02ZRmZCu9nhck2MPq1ONaY"
    matching_tracks = [t for t in tracks if t[0] == track_id]
    if matching_tracks:
        print(f"\nDetails for track {track_id}:")
        for track in matching_tracks:
            print(f"- Title: {track[1]}")
            print(f"- Artists: {track[2]}")
            print(f"- Album: {track[3]}")
            print(f"- Added at: {track[4]}")
    else:
        print(f"\nTrack {track_id} not found in current master tracks.")


if __name__ == "__main__":
    find_problematic_tracks()
