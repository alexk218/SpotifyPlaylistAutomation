import os
import sys
import pytest
from pathlib import Path
import tempfile
import json
from unittest.mock import patch, MagicMock

# Add project root to path so imports work
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Import the Flask app
from tagify_integration import app


# Mock environment variables
@pytest.fixture(scope="session", autouse=True)
def mock_env_variables():
    with patch.dict(os.environ, {
        "MASTER_TRACKS_DIRECTORY_SSD": "/mock/tracks/dir",
        "MASTER_PLAYLIST_ID": "mock_playlist_id",
    }):
        yield


# Flask test client
@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_spotify_client():
    with patch('tagify_integration.authenticate_spotify') as mock:
        mock_client = MagicMock()
        mock.return_value = mock_client
        yield mock_client


# Mock unit of work
@pytest.fixture
def mock_unit_of_work():
    with patch('tagify_integration.UnitOfWork') as mock:
        # Configure the mock to return usable objects
        mock_instance = MagicMock()
        mock.return_value.__enter__.return_value = mock_instance

        # Set up repositories
        mock_instance.track_repository = MagicMock()
        mock_instance.playlist_repository = MagicMock()
        mock_instance.track_playlist_repository = MagicMock()

        yield mock_instance


# Create a temporary directory for test files
@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_track_id_mapping():
    """Mock the track ID mapping function"""
    with patch('tagify_integration.build_track_id_mapping') as mock:
        # Create a sample mapping
        sample_mapping = {
            'spotify:track:123': '/test/dir/track1.mp3',
            'spotify:track:456': '/test/dir/track2.mp3',
            'local_abc123': '/test/dir/local_track.mp3'
        }
        mock.return_value = sample_mapping
        yield sample_mapping


@pytest.fixture
def sample_m3u_content():
    """Sample M3U playlist content"""
    return """#EXTM3U
#EXTINF:180,Test Artist - Test Track
/test/dir/track1.mp3
#EXTINF:240,Another Artist - Another Track
/test/dir/track2.mp3
"""


# Sample track data
@pytest.fixture
def sample_track_data():
    return {
        'id': 'mock_track_id',
        'title': 'Test Track',
        'artists': 'Test Artist',
        'album': 'Test Album',
        'is_local': False
    }


# Sample playlist data
@pytest.fixture
def sample_playlist_data():
    return {
        'id': 'mock_playlist_id',
        'name': 'Test Playlist',
        'snapshot_id': 'mock_snapshot_id'
    }
