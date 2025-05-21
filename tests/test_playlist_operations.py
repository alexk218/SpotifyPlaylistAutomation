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


def test_validate_playlists_m3u_with_missing(client, mock_unit_of_work, mock_track_id_mapping):
    """Test the validate-playlists endpoint with missing M3U files"""

    # Create a serializable mock track
    class SerializableTrack:
        def __init__(self, track_id, title, artists, album):
            self.track_id = track_id
            self.title = title
            self.artists = artists
            self.album = album
            self.is_local = False

        def __dict__(self):
            return {
                'track_id': self.track_id,
                'title': self.title,
                'artists': self.artists,
                'album': self.album,
                'is_local': self.is_local
            }

    # Create a track
    mock_track = SerializableTrack('track1', 'Test Track', 'Test Artist', 'Test Album')

    # Setup mock playlist and behavior
    mock_playlist = MagicMock()
    mock_playlist.name = 'Missing Playlist'
    mock_playlist.playlist_id = 'playlist_missing'
    mock_unit_of_work.playlist_repository.get_all.return_value = [mock_playlist]
    mock_unit_of_work.track_playlist_repository.get_track_ids_for_playlist.return_value = ['track1']

    # Make the get_by_id function return dictionaries instead of MagicMock objects
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

    # Mock functions
    with patch('tagify_integration.build_track_id_mapping') as mock_build_map, \
            patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.find_local_file_path_with_extensions') as mock_find_local:
        # Configure mocks
        mock_build_map.return_value = {'track1': '/test/dir/track1.mp3'}
        mock_walk.return_value = [('/test/playlists', [], [])]  # No M3U files
        mock_find_local.return_value = None  # No local files found

        # Patch jsonify to handle MagicMock objects
        with patch('tagify_integration.jsonify', side_effect=lambda x: json.dumps(x, default=str)):
            # Make request
            response = client.post('/api/validate-playlists',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['summary']['missing_m3u_files'] > 0


def test_generate_rekordbox_xml(client):
    """Test the generate-rekordbox-xml endpoint"""
    # Create test request data
    request_data = {
        'playlistsDir': '/test/playlists',
        'rekordboxXmlPath': '/test/output.xml',
        'masterTracksDir': '/test/dir',
        'ratingData': {
            'spotify:track:123': {'rating': 4, 'energy': 8}
        }
    }

    # Mock the RekordboxXmlGenerator
    with patch('tagify_integration.RekordboxXmlGenerator') as mock_generator:
        # Configure mock
        mock_instance = MagicMock()
        mock_instance.generate.return_value = (100, 10, 5)  # tracks, playlists, rated
        mock_generator.return_value = mock_instance

        # Make request
        response = client.post('/api/generate-rekordbox-xml',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert '100 tracks' in data['message']
        assert '10 playlists' in data['message']
        assert mock_instance.generate.called


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
