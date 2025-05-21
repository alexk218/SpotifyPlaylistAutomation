import json
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
