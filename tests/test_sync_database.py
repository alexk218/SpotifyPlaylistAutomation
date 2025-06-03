import json
from unittest.mock import patch, MagicMock

import pytest

from tests.helpers.database_fixture_loader import DatabaseFixtureLoader


class TestSyncDatabase:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.fixture_loader = DatabaseFixtureLoader()

    def test_sync_database_tracks_full_end_to_end_flow(self, client):
        """Test the complete sync flow: analysis -> execution using real database operations."""

        # Setup initial database state from fixture
        self.fixture_loader.setup_initial_database_state()

        # Get mock Spotify API response from fixture
        spotify_api_response = self.fixture_loader.get_spotify_api_mock_data()

        with patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            # Configure mocks
            mock_fetch.return_value = spotify_api_response
            mock_auth.return_value = MagicMock()

            # STEP 1: First call - Analysis mode (no precomputed changes)
            analysis_request = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': False,  # Analysis mode
                'master_playlist_id': 'test_master_playlist'
            }

            analysis_response = client.post('/api/sync/database',
                                            json=analysis_request,
                                            content_type='application/json')

            # Verify analysis response
            assert analysis_response.status_code == 200
            analysis_data = json.loads(analysis_response.data)
            assert analysis_data['success'] == True
            assert analysis_data['stage'] == 'analysis'
            assert analysis_data['needs_confirmation'] == True

            # Get the expected results to validate analysis
            expected = self.fixture_loader.get_expected_results()
            expected_stats = expected['expected_stats']

            assert analysis_data['stats']['added'] == expected_stats['added']
            assert analysis_data['stats']['updated'] == expected_stats['updated']
            assert analysis_data['stats']['unchanged'] == expected_stats['unchanged']
            assert analysis_data['stats']['deleted'] == expected_stats['deleted']

            # STEP 2: Second call - Execution with precomputed changes from analysis
            execution_request = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': True,  # Execute the changes
                'master_playlist_id': 'test_master_playlist',
                'precomputed_changes_from_analysis': analysis_data  # Use analysis results
            }

            execution_response = client.post('/api/sync/database',
                                             json=execution_request,
                                             content_type='application/json')

            # Verify execution response
            assert execution_response.status_code == 200
            execution_data = json.loads(execution_response.data)
            assert execution_data['success'] == True
            assert execution_data['stage'] == 'sync_complete'

            # Verify the same stats as analysis
            assert execution_data['stats']['added'] == expected_stats['added']
            assert execution_data['stats']['updated'] == expected_stats['updated']
            assert execution_data['stats']['unchanged'] == expected_stats['unchanged']
            assert execution_data['stats']['deleted'] == expected_stats['deleted']

            # STEP 3: Validate final database state
            self.fixture_loader.validate_database_state()

    def test_sync_database_tracks_analysis_mode(self, client):
        """Test the sync-database endpoint for tracks in analysis mode (not confirmed)."""
        # Setup initial database state from fixture
        self.fixture_loader.setup_initial_database_state()

        # Get mock Spotify API response from fixture
        spotify_api_response = self.fixture_loader.get_spotify_api_mock_data()

        # Patch where the function is imported and used, not the original module
        with patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            # Configure mocks
            mock_fetch.return_value = spotify_api_response
            mock_auth.return_value = MagicMock()  # Mock spotify client

            # Create test request data for analysis mode
            request_data = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': False,  # Analysis mode
                'master_playlist_id': 'test_master_playlist'
            }

            # Make request
            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stage'] == 'analysis'
            assert data['needs_confirmation'] == True

            # Verify the analysis results match our expected changes
            expected = self.fixture_loader.get_expected_results()
            expected_stats = expected['expected_stats']

            assert data['stats']['added'] == expected_stats['added']
            assert data['stats']['updated'] == expected_stats['updated']
            assert data['stats']['unchanged'] == expected_stats['unchanged']
            assert data['stats']['deleted'] == expected_stats['deleted']

            # Verify details are provided
            assert 'details' in data
            assert 'to_add' in data['details']
            assert 'to_update' in data['details']
            assert 'to_delete' in data['details']

    def test_sync_database_tracks_with_precomputed_changes(self, client):
        """Test the sync-database endpoint for tracks with precomputed changes."""
        # Setup initial database state from fixture
        self.fixture_loader.setup_initial_database_state()

        # Create test request data with precomputed changes
        request_data = {
            'action': 'tracks',
            'force_refresh': False,
            'confirmed': True,
            'master_playlist_id': 'test_master_playlist',
            'precomputed_changes_from_analysis': {
                'details': {
                    'all_items_to_add': [
                        {
                            'id': '6O5y18vxALUmmBRpG9iKZ9',
                            'artists': 'Chris Micali',
                            'title': 'Your Life Better',
                            'album': 'So Easy / Your Life Better',
                            'is_local': False
                        }
                    ],
                    'all_items_to_update': [],
                    'all_items_to_delete': [
                        {'id': '7z5dZ9mvhklOsfWtZYOQ9J'},
                        {'id': '7zLm8NfYfn4IRd3GF35N3O'},
                        {'id': '7zNX28H2cf2OKdQn5G5HbP'},
                        {'id': 'local_018e875efa0d7fd2'}
                    ]
                },
                'stats': {
                    'added': 1,
                    'updated': 0,
                    'unchanged': 1,
                    'deleted': 4
                }
            }
        }

        # Mock functions
        with patch('api.services.sync_service.sync_tracks_to_db') as mock_sync:
            # Configure mocks
            mock_sync.return_value = (1, 0, 1, 4)  # added, updated, unchanged, deleted

            # Make request
            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            # Verify response
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stats']['added'] == 1
            assert data['stats']['updated'] == 0
            assert data['stats']['unchanged'] == 1
            assert data['stats']['deleted'] == 4
            assert mock_sync.called

    def test_get_exclusion_config(self):
        """Test the exclusion config handling"""
        from api.services.sync_service import get_exclusion_config

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
            config = get_exclusion_config()
            assert 'SKIPPED' in config['forbidden_playlists']

    def test_sync_database_playlists(self, client):
        pass

    def test_sync_database_associations(self, client):
        pass
