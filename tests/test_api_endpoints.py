import json
from datetime import datetime

import pytest
from unittest.mock import patch, MagicMock


def test_status_endpoint(client):
    """Test the /status endpoint returns expected data."""
    response = client.get('/status')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['status'] == 'running'
    assert 'env_vars' in data


def test_validate_track_metadata_endpoint(client, mock_unit_of_work):
    """Test the validate-track-metadata endpoint."""
    # Setup mock data
    mock_unit_of_work.track_repository.get_all.return_value = []

    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'confidence_threshold': 0.75
    }

    # Mock filesystem functions
    with patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.ID3') as mock_id3, \
            patch('tagify_integration.MP3') as mock_mp3:

        # Configure mocks
        mock_walk.return_value = [
            ('/test/dir', [], ['track1.mp3', 'track2.mp3'])
        ]

        # Mock ID3 behavior for track with ID
        def side_effect(path):
            mock_tags = MagicMock()
            if 'track1' in path:
                # Mock a track with TrackId
                mock_tags.__contains__.side_effect = lambda x: x == 'TXXX:TRACKID'
                mock_tags.__getitem__.return_value.text = ['spotify:track:123']
            else:
                # Mock a track without TrackId
                mock_tags.__contains__.side_effect = lambda x: False
            return mock_tags

        mock_id3.side_effect = side_effect

        # Mock MP3 for duration
        mock_mp3.return_value.info.length = 240

        # Make request
        response = client.post('/api/validate-track-metadata',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert 'summary' in data
        assert data['summary']['total_files'] == 2
        assert data['summary']['files_with_track_id'] == 1
        assert data['summary']['files_without_track_id'] == 1


def test_embed_metadata_analysis(client):
    """Test the embed-metadata endpoint analysis phase."""
    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'confirmed': False
    }

    # Mock filesystem functions
    with patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.ID3') as mock_id3:

        # Configure mocks
        mock_walk.return_value = [
            ('/test/dir', [], ['track1.mp3', 'track2.mp3'])
        ]

        # Mock ID3 behavior
        def side_effect(path):
            mock_tags = MagicMock()
            if 'track1' in path:
                # Track with ID
                mock_tags.__contains__.side_effect = lambda x: x == 'TXXX:TRACKID'
            else:
                # Track without ID
                mock_tags.__contains__.side_effect = lambda x: False
            return mock_tags

        mock_id3.side_effect = side_effect

        # Make request
        response = client.post('/api/embed-metadata',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['needs_confirmation'] == True
        assert data['requires_fuzzy_matching'] == True
        assert len(data['details']['files_to_process']) == 1


def test_embed_metadata_execution(client):
    """Test the embed-metadata endpoint execution phase."""
    # Create test request data with user selections
    request_data = {
        'masterTracksDir': '/test/dir',
        'confirmed': True,
        'userSelections': [
            {
                'fileName': 'track1.mp3',
                'trackId': 'spotify:track:123',
                'confidence': 0.9
            }
        ]
    }

    # Mock functions
    with patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.embed_track_id') as mock_embed:
        # Configure mocks
        mock_walk.return_value = [
            ('/test/dir', [], ['track1.mp3'])
        ]
        mock_embed.return_value = True

        # Make request
        response = client.post('/api/embed-metadata',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['successful_embeds'] == 1
        assert mock_embed.called


def test_direct_tracks_compare(client, mock_unit_of_work):
    """Test the direct-tracks-compare endpoint"""
    # Import datetime at the top of the file
    from datetime import datetime

    # Create a real-looking Track object instead of MagicMock
    class MockTrack:
        def __init__(self, track_id, title, artists, album, added_to_master=None, is_local=False):
            self.track_id = track_id
            self.title = title
            self.artists = artists
            self.album = album
            self.added_to_master = added_to_master
            self.is_local = is_local

        def isoformat(self):
            if self.added_to_master:
                return self.added_to_master.isoformat()
            return None

        def __dict__(self):
            return {
                'track_id': self.track_id,
                'title': self.title,
                'artists': self.artists,
                'album': self.album,
                'added_to_master': self.added_to_master,
                'is_local': self.is_local
            }

    # Create mock track with proper datetime
    mock_track = MockTrack(
        track_id='spotify:track:123',
        title='Test Track',
        artists='Test Artist',
        album='Test Album',
        added_to_master=datetime.now(),
        is_local=False
    )

    # Setup mock track repository's get_all method
    mock_unit_of_work.track_repository.get_all.return_value = [mock_track]

    # Mock ID3 functions for the local files scan
    with patch('tagify_integration.ID3') as mock_id3, \
            patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.os.path.getsize') as mock_getsize, \
            patch('tagify_integration.os.path.exists') as mock_exists:
        # Configure mocks for the walk
        mock_walk.return_value = [('/test/dir', [], ['track1.mp3'])]
        mock_getsize.return_value = 1000
        mock_exists.return_value = True

        # Mock ID3 tags
        mock_tags = MagicMock()
        mock_tags.__contains__.side_effect = lambda x: x == 'TXXX:TRACKID'
        mock_tags.__getitem__.return_value.text = ['spotify:track:123']
        mock_id3.return_value = mock_tags

        # Make request with needed parameters
        response = client.get('/api/direct-tracks-compare?master_tracks_dir=/test/dir')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert len(data['master_tracks']) == 1  # We should have our one track
        assert data['master_tracks'][0]['id'] == 'spotify:track:123'


def test_error_handling(client):
    """Test error handling in API endpoints"""
    # Test 400 Bad Request
    response = client.post('/api/validate-playlists',
                           json={},  # Missing required parameters
                           content_type='application/json')
    assert response.status_code == 400

    # Test exception handling - need to use a function that's actually used
    # and will trigger an exception in the app
    with patch('tagify_integration.os.makedirs') as mock_makedirs:
        # Set up the mock to raise an exception
        mock_makedirs.side_effect = Exception("Test exception")

        # Use an endpoint that calls makedirs
        response = client.post('/api/generate-m3u',
                               json={'masterTracksDir': '/test', 'playlistsDir': '/test'},
                               content_type='application/json')

        # This should now properly trigger a 500 error
        assert response.status_code == 500
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'exception' in data['message'].lower()
