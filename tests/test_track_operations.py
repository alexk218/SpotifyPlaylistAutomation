import json
import os

import pytest
from unittest.mock import patch, MagicMock


def test_fuzzy_match_track(client, mock_unit_of_work):
    """Test the fuzzy-match-track endpoint."""
    # Setup mock track data
    mock_track = MagicMock()
    mock_track.track_id = 'spotify:track:123'
    mock_track.title = 'Test Track'
    mock_track.artists = 'Test Artist'
    mock_track.album = 'Test Album'
    mock_track.is_local = False

    mock_unit_of_work.track_repository.get_all.return_value = [mock_track]

    # Create test request data
    request_data = {
        'fileName': 'test track.mp3',
        'masterTracksDir': '/test/dir',
        'currentTrackId': None
    }

    # Make request
    response = client.post('/api/fuzzy-match-track',
                           json=request_data,
                           content_type='application/json')

    # Verify response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['success'] == True
    assert 'matches' in data


def test_correct_track_id(client, mock_unit_of_work):
    """Test the correct-track-id endpoint."""
    # Setup mock database
    mock_track = MagicMock()
    mock_track.track_id = 'spotify:track:123'
    mock_unit_of_work.track_repository.get_by_id.return_value = mock_track

    # Create test request data
    request_data = {
        'file_path': '/test/dir/track1.mp3',
        'new_track_id': 'spotify:track:123'
    }

    # Mock ID3 functions
    with patch('tagify_integration.ID3') as mock_id3, \
            patch('tagify_integration.embed_track_id') as mock_embed:
        mock_tags = MagicMock()
        mock_tags.__contains__.side_effect = lambda x: x == 'TXXX:TRACKID'
        mock_tags.__getitem__.return_value.text = ['spotify:track:oldid']
        mock_id3.return_value = mock_tags

        mock_embed.return_value = True

        # Make request
        response = client.post('/api/correct-track-id',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['old_track_id'] == 'spotify:track:oldid'
        assert data['new_track_id'] == 'spotify:track:123'
        assert mock_embed.called


def test_remove_track_id(client):
    """Test the remove-track-id endpoint."""
    # Create test request data
    request_data = {
        'file_path': '/test/dir/track1.mp3'
    }

    # Mock functions
    with patch('tagify_integration.os.path.exists') as mock_exists, \
            patch('tagify_integration.ID3') as mock_id3:
        mock_exists.return_value = True

        mock_tags = MagicMock()
        mock_tags.__contains__.side_effect = lambda x: x == 'TXXX:TRACKID'
        mock_tags.__getitem__.return_value.text = ['spotify:track:123']
        mock_id3.return_value = mock_tags

        # Make request
        response = client.post('/api/remove-track-id',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['old_track_id'] == 'spotify:track:123'
        assert mock_tags.delall.called
        assert mock_tags.save.called


def test_search_tracks(client):
    """Test the search-tracks endpoint."""
    # Create test request data
    request_data = {
        'masterTracksDir': '/test/dir',
        'query': 'test'
    }

    # Mock filesystem functions
    with patch('tagify_integration.os.walk') as mock_walk, \
            patch('tagify_integration.ID3') as mock_id3:
        # Configure mocks
        mock_walk.return_value = [
            ('/test/dir', [], ['test_track.mp3', 'other.mp3'])
        ]

        # Mock ID3 behavior
        mock_tags = MagicMock()
        mock_tags.__contains__.side_effect = lambda x: x == 'TXXX:TRACKID'
        mock_tags.__getitem__.return_value.text = ['spotify:track:123']
        mock_id3.return_value = mock_tags

        # Make request
        response = client.post('/api/search-tracks',
                               json=request_data,
                               content_type='application/json')

        # Verify response
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert len(data['results']) == 1
        assert data['results'][0]['file'] == 'test_track.mp3'


def test_file_operations(client, temp_dir):
    """Test file-related operations"""
    # Create a test file in the temp directory
    test_file = os.path.join(temp_dir, "test.txt")
    with open(test_file, "w") as f:
        f.write("Test content")

    # Test file deletion
    request_data = {
        'file_path': test_file
    }

    response = client.post('/api/delete-file',
                           json=request_data,
                           content_type='application/json')

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['success'] == True
    assert not os.path.exists(test_file)
