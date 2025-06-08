import json
import pytest
from unittest.mock import patch, MagicMock, mock_open


def test_validate_playlists_m3u(client, mock_unit_of_work):
    """Test the validate-playlists endpoint."""

    # Create a serializable class for our track and playlist
    class SerializableObject:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def to_dict(self):
            return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    # Create serializable objects
    mock_playlist = SerializableObject(
        name='Test Playlist',
        playlist_id='playlist123'
    )

    mock_track = SerializableObject(
        track_id='track1',
        title='Test Track',
        artists='Test Artist',
        album='Test Album',
        is_local=False
    )

    # Setup mock methods on unit_of_work
    mock_unit_of_work.playlist_repository.get_all.return_value = [mock_playlist]
    # mock_unit_of_work.track_playlist_repository.get_track_ids_for_playlist.return_value = ['track1']

    # Set up get_by_id to return our serializable track
    def get_by_id_side_effect(track_id):
        if track_id == 'track1':
            return mock_track
        return None

    mock_unit_of_work.track_repository.get_by_id.side_effect = get_by_id_side_effect

    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'playlistsDir': '/test/playlists'
    }

    # Create sample M3U content
    sample_m3u_content = """#EXTM3U
#EXTINF:180,Test Artist - Test Track
/test/dir/track1.mp3
"""

    # We need to patch all the critical functions in the validation process
    with patch('tagify_integration.build_track_id_mapping') as mock_build_map, \
            patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.get_m3u_track_ids') as mock_get_ids, \
            patch('tagify_integration.os.path.exists') as mock_exists, \
            patch('tagify_integration.find_local_file_path_with_extensions') as mock_find_local, \
            patch('builtins.open', mock_open(read_data=sample_m3u_content)), \
            patch('tagify_integration.os.path.join', lambda *args: '/'.join(arg.replace('\\', '/') for arg in args)), \
            patch('tagify_integration.jsonify', side_effect=lambda x: json.dumps(x, default=lambda
                    o: o.to_dict() if hasattr(o, 'to_dict') else str(o))):

        # Configure mocks
        mock_build_map.return_value = {'track1': '/test/dir/track1.mp3'}
        # Return M3U file that matches our playlist name exactly
        mock_walk.return_value = [
            ('/test/playlists', [], ['Test Playlist.m3u'])
        ]
        mock_get_ids.return_value = ['track1']
        mock_exists.return_value = True
        mock_find_local.return_value = None

        # Make request
        response = client.post('/api/validate-playlists',
                               json=request_data,
                               content_type='application/json')

        # Check response status first
        assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.data.decode('utf-8')}"

        # Now parse response data
        data = json.loads(response.data)
        assert data['success'] == True
        assert 'summary' in data
        assert 'playlist_analysis' in data
        assert len(data['playlist_analysis']) == 1
        assert data['playlist_analysis'][0]['name'] == 'Test Playlist'
        assert data['playlist_analysis'][0]['has_m3u'] == True
