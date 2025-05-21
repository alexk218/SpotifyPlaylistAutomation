import json
import pytest
from unittest.mock import patch, MagicMock


def test_generate_m3u_analysis(client, mock_unit_of_work):
    """Test the generate-m3u endpoint analysis phase."""
    # Setup mock database
    mock_playlist = MagicMock()
    mock_playlist.name = 'Test Playlist'
    mock_playlist.playlist_id = 'playlist123'
    mock_unit_of_work.playlist_repository.get_all.return_value = [mock_playlist]
    mock_unit_of_work.track_playlist_repository.get_track_ids_for_playlist.return_value = ['track1', 'track2']

    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'playlistsDir': '/test/playlists',
        'extended': True,
        'overwrite': True,
        'confirmed': False
    }

    # Make request
    response = client.post('/api/generate-m3u',
                           json=request_data,
                           content_type='application/json')

    # Verify response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['success'] == True
    assert data['needs_confirmation'] == True
    assert len(data['details']['playlists']) == 1
    assert data['details']['playlists'][0]['name'] == 'Test Playlist'
    assert data['details']['playlists'][0]['track_count'] == 2


def test_generate_m3u_execution(client, mock_unit_of_work):
    """Test the generate-m3u endpoint execution phase."""
    # Setup mock database
    mock_playlist = MagicMock()
    mock_playlist.name = 'Test Playlist'
    mock_playlist.playlist_id = 'playlist123'
    mock_unit_of_work.playlist_repository.get_by_id.return_value = mock_playlist

    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'playlistsDir': '/test/playlists',
        'extended': True,
        'overwrite': True,
        'confirmed': True,
        'playlists_to_update': ['playlist123']
    }

    # Mock functions
    with patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.build_track_id_mapping') as mock_build_map, \
            patch('tagify_integration.os.makedirs') as mock_makedirs, \
            patch('tagify_integration.os.path.exists') as mock_exists, \
            patch('tagify_integration.generate_m3u_playlist') as mock_generate:
        # Configure mocks
        mock_walk.return_value = [
            ('/test/playlists', [], [])
        ]
        mock_build_map.return_value = {'track1': '/test/dir/track1.mp3'}
        mock_exists.return_value = True
        mock_generate.return_value = (2, 2)  # tracks_found, tracks_added

        # Make request
        response = client.post('/api/generate-m3u',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['stats']['playlists_updated'] == 1
        assert mock_generate.called


def test_regenerate_playlist(client, mock_unit_of_work):
    """Test the regenerate-playlist endpoint."""
    # Setup mock database
    mock_playlist = MagicMock()
    mock_playlist.name = 'Test Playlist'
    mock_playlist.playlist_id = 'playlist123'
    mock_unit_of_work.playlist_repository.get_by_id.return_value = mock_playlist

    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'playlistsDir': '/test/playlists',
        'playlist_id': 'playlist123',
        'extended': True,
        'force': True
    }

    # Mock functions
    with patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.build_track_id_mapping') as mock_build_map, \
            patch('tagify_integration.os.makedirs') as mock_makedirs, \
            patch('tagify_integration.os.path.exists') as mock_exists, \
            patch('tagify_integration.generate_m3u_playlist') as mock_generate, \
            patch('tagify_integration.os.path.getsize') as mock_getsize:
        # Configure mocks
        mock_walk.return_value = [
            ('/test/playlists', [], [])
        ]
        mock_build_map.return_value = {'track1': '/test/dir/track1.mp3'}
        mock_exists.return_value = True
        mock_generate.return_value = (2, 2)  # tracks_found, tracks_added
        mock_getsize.return_value = 1000

        # Make request
        response = client.post('/api/regenerate-playlist',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert 'Test Playlist' in data['message']
        assert mock_generate.called
