
def fetch_playlist_song_count(spotify_client, playlist_id):
    response = spotify_client.playlist_tracks(playlist_id, fields='total')
    return response['total']
