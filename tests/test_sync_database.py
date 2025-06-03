import json
from unittest.mock import patch

import pytest

from tests.helpers.database_fixture_loader import DatabaseFixtureLoader


class TestSyncDatabase:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.fixture_loader = DatabaseFixtureLoader()

    def test_sync_database_tracks_real_data_operations(self):
        """Test sync_database with real API data and database operations using fixtures."""

        # Setup initial database state from fixture
        self.fixture_loader.setup_initial_database_state()

        # Get mock Spotify API response from fixture
        spotify_api_response = self.fixture_loader.get_spotify_api_mock_data()

        # Execute the sync operation
        with patch('drivers.spotify_client.fetch_master_tracks') as mock_fetch:
            mock_fetch.return_value = spotify_api_response

            # Make the actual API call to sync-database
            request_data = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': True,
                'master_playlist_id': 'test_master_playlist'
            }

            response = self.client.post('/api/sync-database',
                                        json=request_data,
                                        content_type='application/json')

            # Verify the API response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

            # Validate sync statistics from fixture
            expected = self.fixture_loader.get_expected_results()
            expected_stats = expected['expected_stats']

            assert data['stats']['added'] == expected_stats['added']
            assert data['stats']['updated'] == expected_stats['updated']
            assert data['stats']['unchanged'] == expected_stats['unchanged']
            assert data['stats']['deleted'] == expected_stats['deleted']

            # Validate final database state matches fixture expectations
            self.fixture_loader.validate_database_state()

    def test_sync_database_tracks(self, client):
        """Test the sync-database endpoint for tracks."""
        # Create test request data
        request_data = {
            'action': 'tracks',
            'force_refresh': False,
            'confirmed': True,
            'master_playlist_id': 'master123',
            'precomputed_changes_from_analysis': {
                'tracks_to_add': [
                    {'id': 'track1', 'artists': 'Artist', 'title': 'Title', 'album': 'Album', 'is_local': False}],
                'tracks_to_update': [],
                'unchanged_tracks': 5
            }
        }

        # Mock functions
        with patch('tagify_integration.sync_tracks_to_db') as mock_sync:
            # Configure mocks
            mock_sync.return_value = (1, 0, 5, 0)  # added, updated, unchanged, deleted

            # Make request
            response = client.post('/api/sync-database',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stats']['added'] == 1
            assert mock_sync.called
            mock_sync.assert_called_with(
                'master123',
                force_full_refresh=False,
                auto_confirm=True,
                precomputed_changes=request_data['precomputed_changes_from_analysis']
            )

    def test_sync_database_playlists(self, client):
        """Test the sync-database endpoint for playlists."""
        # Create test request data
        request_data = {
            'action': 'playlists',
            'force_refresh': False,
            'confirmed': True,
            'playlistSettings': {
                'excludedKeywords': ['Discover Weekly'],
                'excludedPlaylistIds': [],
                'excludeByDescription': ['ignore']
            },
            'precomputed_changes_from_analysis': {
                'to_add': [{'id': 'new1', 'name': 'New Playlist', 'snapshot_id': 'snap1'}],
                'to_update': [],
                'to_delete': []
            }
        }

        # Mock functions
        with patch('tagify_integration.sync_playlists_to_db') as mock_sync:
            # Configure mocks
            mock_sync.return_value = (1, 0, 5, 0)  # added, updated, unchanged, deleted

            # Make request
            response = client.post('/api/sync-database',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stats']['added'] == 1
            assert mock_sync.called
            mock_sync.assert_called_with(
                force_full_refresh=False,
                auto_confirm=True,
                precomputed_changes=request_data['precomputed_changes_from_analysis'],
                exclusion_config=pytest.approx({'forbidden_playlists': [], 'forbidden_words': ['Discover Weekly'],
                                                'description_keywords': ['ignore'], 'forbidden_playlist_ids': []})
            )

    def test_sync_database_associations(self, client):
        """Test the sync-database endpoint for associations."""
        # Create test request data
        request_data = {
            'action': 'associations',
            'force_refresh': False,
            'confirmed': True,
            'master_playlist_id': 'master123',
            'precomputed_changes_from_analysis': {
                'tracks_with_changes': [
                    {'track_id': 'track1', 'track_info': 'Artist - Title', 'add_to': ['Playlist1'], 'remove_from': []}
                ]
            }
        }

        # Mock functions
        with patch('tagify_integration.sync_track_playlist_associations_to_db') as mock_sync:
            # Configure mocks
            mock_sync.return_value = {
                'associations_added': 1,
                'associations_removed': 0,
                'tracks_with_changes': 1
            }

            # Make request
            response = client.post('/api/sync-database',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stats']['associations_added'] == 1
            assert mock_sync.called

    def test_analyze_playlists(self, client):
        """Test the analyze-playlists endpoint"""
        # Create test request data
        request_data = {}

        # Mock analyze_playlists_changes function
        with patch('tagify_integration.analyze_playlists_changes') as mock_analyze:
            mock_analyze.return_value = (2, 1, 5, 0, {
                'to_add': [{'id': 'pl1', 'name': 'New Playlist'}],
                'to_update': [{'id': 'pl2', 'name': 'Updated Playlist', 'old_name': 'Old Name'}],
                'to_delete': []
            })

            # Make request
            response = client.post('/api/analyze-playlists',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stats']['added'] == 2
            assert data['stats']['updated'] == 1
            assert data['needs_confirmation'] == True

    def test_get_exclusion_config(self):
        """Test the exclusion config handling"""
        from tagify_integration import get_exclusion_config

        # Test with client-provided settings
        request_json = {
            'playlistSettings': {
                'excludedKeywords': ['Daily Mix', 'Discover Weekly'],
                'excludedPlaylistIds': ['playlist123'],
                'excludeByDescription': ['Made for you']
            }
        }

        config = get_exclusion_config(request_json)
        assert 'Daily Mix' in config['forbidden_words']
        assert 'playlist123' in config['forbidden_playlist_ids']
        assert 'Made for you' in config['description_keywords']

        # Test with no settings by patching json.load instead
        mock_config = {"forbidden_playlists": ["SKIPPED"]}
        with patch('json.load', return_value=mock_config):
            # This will now use our mocked json.load
            config = get_exclusion_config()
            assert 'SKIPPED' in config['forbidden_playlists']
