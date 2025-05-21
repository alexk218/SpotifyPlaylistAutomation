import json
import pytest
from unittest.mock import patch, MagicMock


def test_validate_playlists_m3u(client, mock_unit_of_work):
    """Test the validate-playlists endpoint."""
    # Setup mock database
    mock_playlist = MagicMock()
    mock_playlist.name = 'Test Playlist'
    mock_playlist.playlist_id = 'playlist123'
    mock_unit_of_work.playlist_repository.get_all.return_value = [mock_playlist]
    mock_unit_of_work.track_playlist_repository.get_track_ids_for_playlist.return_value = ['track1', 'track2']

    mock_track = MagicMock()
    mock_track.track_id = 'track1'
    mock_track.title = 'Test Track'
    mock_track.artists = 'Test Artist'
    mock_track.album = 'Test Album'
    mock_track.is_local = False
    mock_unit_of_work.track_repository.get_by_id.return_value = mock_track

    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'playlistsDir': '/test/playlists'
    }

    # Mock functions
    with patch('tagify_integration.build_track_id_mapping') as mock_build_map, \
            patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.get_m3u_track_ids') as mock_get_ids, \
            patch('tagify_integration.os.path.exists') as mock_exists:
        # Configure mocks
        mock_build_map.return_value = {'track1': '/test/dir/track1.mp3'}
        mock_walk.return_value = [
            ('/test/playlists', [], ['test playlist.m3u'])
        ]
        mock_get_ids.return_value = ['track1']
        mock_exists.return_value = True

        # Make request
        response = client.post('/api/validate-playlists',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert 'summary' in data
        assert 'playlist_analysis' in data
        assert len(data['playlist_analysis']) == 1
        assert data['playlist_analysis'][0]['name'] == 'Test Playlist'
        assert data['playlist_analysis'][0]['has_m3u'] == True
