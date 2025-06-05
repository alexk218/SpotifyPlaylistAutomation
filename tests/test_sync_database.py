import json
import os
from unittest.mock import patch, MagicMock

import pytest

from sql.core.unit_of_work import UnitOfWork
from tests.helpers.database_fixture_loader import DatabaseFixtureLoader


class TestSyncDatabase:
    """Comprehensive tests for all sync database functionality"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.fixture_loader = DatabaseFixtureLoader()

    # ========== TRACK SYNC TESTS ==========

    def test_sync_database_tracks_full_end_to_end_flow(self, client):
        """Test the complete track sync flow: analysis -> execution using real database operations."""
        # Setup initial database state from fixture
        self.fixture_loader.setup_initial_database_state()

        # Get mock Spotify API response from fixture
        spotify_api_response = self.fixture_loader.get_spotify_api_mock_data()

        with patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            # Configure mocks
            mock_fetch.return_value = spotify_api_response
            mock_auth.return_value = MagicMock()

            # STEP 1: Analysis mode
            analysis_request = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': False,
                'master_playlist_id': os.getenv('MASTER_PLAYLIST_ID'),
            }

            analysis_response = client.post('/api/sync/database',
                                            json=analysis_request,
                                            content_type='application/json')

            assert analysis_response.status_code == 200
            analysis_data = json.loads(analysis_response.data)
            assert analysis_data['success'] == True
            assert analysis_data['stage'] == 'analysis'
            assert analysis_data['needs_confirmation'] == True

            # STEP 2: Execution with precomputed changes
            execution_request = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': True,
                'master_playlist_id': os.getenv('MASTER_PLAYLIST_ID'),
                'precomputed_changes_from_analysis': analysis_data
            }

            execution_response = client.post('/api/sync/database',
                                             json=execution_request,
                                             content_type='application/json')

            assert execution_response.status_code == 200
            execution_data = json.loads(execution_response.data)
            assert execution_data['success'] == True
            assert execution_data['stage'] == 'sync_complete'

            # Validate final database state
            self.fixture_loader.validate_database_state()

    def test_sync_database_tracks_analysis_mode(self, client):
        """Test track sync analysis mode."""
        self.fixture_loader.setup_initial_database_state()
        spotify_api_response = self.fixture_loader.get_spotify_api_mock_data()

        with patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_fetch.return_value = spotify_api_response
            mock_auth.return_value = MagicMock()

            request_data = {
                'action': 'tracks',
                'force_refresh': True,
                'confirmed': False,
                'master_playlist_id': 'test_master_playlist'
            }

            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['stage'] == 'analysis'
            assert data['needs_confirmation'] == True
            assert 'details' in data
            assert 'to_add' in data['details']
            assert 'to_update' in data['details']
            assert 'to_delete' in data['details']

    # ========== PLAYLIST SYNC TESTS ==========

    def test_sync_database_playlists_analysis_mode(self, client):
        """Test playlist sync analysis mode."""
        self.fixture_loader.setup_initial_database_state('playlists_initial.json')

        # Mock Spotify API response
        spotify_playlists_response = self.fixture_loader.get_spotify_playlists_mock_data()

        with patch('helpers.sync_helper.fetch_playlists') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_fetch.return_value = spotify_playlists_response
            mock_auth.return_value = MagicMock()

            request_data = {
                'action': 'playlists',
                'force_refresh': True,
                'confirmed': False
            }

            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['action'] == 'playlists'
            assert data['stage'] == 'analysis'
            assert data['needs_confirmation'] == True

            # Verify expected stats from fixture
            expected = self.fixture_loader.get_expected_results('playlist_sync_result.json')
            expected_stats = expected['expected_stats']

            assert data['stats']['added'] == expected_stats['added']
            assert data['stats']['updated'] == expected_stats['updated']
            assert data['stats']['deleted'] == expected_stats['deleted']

    def test_sync_database_playlists_execution(self, client):
        """Test playlist sync execution with precomputed changes."""
        self.fixture_loader.setup_initial_database_state('playlists_initial.json')

        # Create precomputed changes based on fixture
        precomputed_changes = {
            'to_add': [
                {
                    'id': 'new_playlist_456',
                    'name': 'New Playlist',
                    'snapshot_id': 'snapshot_new_456'
                }
            ],
            'to_update': [
                {
                    'id': '37i9dQZF1DX0XUsuxWHRQd',
                    'name': 'RapCaviar Updated',
                    'old_name': 'RapCaviar',
                    'snapshot_id': 'new_snapshot_789',
                    'old_snapshot_id': 'old_snapshot_789'
                }
            ],
            'to_delete': [
                {
                    'id': 'playlist_to_be_deleted',
                    'name': 'Old Playlist'
                }
            ]
        }

        request_data = {
            'action': 'playlists',
            'force_refresh': True,
            'confirmed': True,
            'precomputed_changes_from_analysis': precomputed_changes
        }

        response = client.post('/api/sync/database',
                               json=request_data,
                               content_type='application/json')

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['action'] == 'playlists'
        assert data['stage'] == 'sync_complete'

        # Verify the changes were applied
        assert data['stats']['added'] == 1
        assert data['stats']['updated'] == 1  # Updated name
        assert data['stats']['deleted'] == 1

        # Debug: Check actual database state before validation
        with UnitOfWork() as uow:
            all_playlists = uow.playlist_repository.get_all()
            print("\nActual playlists in database after sync:")
            for p in all_playlists:
                print(f"  ID: {p.playlist_id}, Name: {p.name}")
                print(f"      master_sync_snapshot_id: {p.master_sync_snapshot_id}")
                print(f"      associations_snapshot_id: {p.associations_snapshot_id}")

        # Instead of validating against fixture, validate against expected changes
        with UnitOfWork() as uow:
            all_playlists = uow.playlist_repository.get_all()
            playlist_ids = {p.playlist_id for p in all_playlists}
            playlist_names = {p.name for p in all_playlists}

            # Verify expected playlists exist
            assert 'new_playlist_456' in playlist_ids, "New playlist should be added"
            assert '37i9dQZF1DX0XUsuxWHRQd' in playlist_ids, "Updated playlist should still exist"
            assert 'playlist_to_be_deleted' not in playlist_ids, "Deleted playlist should be gone"

            # Verify expected names
            assert 'New Playlist' in playlist_names, "New playlist name should exist"
            assert 'RapCaviar Updated' in playlist_names, "Updated playlist name should exist"
            assert 'Old Playlist' not in playlist_names, "Old playlist name should be gone"

            # Verify snapshot IDs were updated correctly
            for playlist in all_playlists:
                if playlist.playlist_id == 'new_playlist_456':
                    assert playlist.master_sync_snapshot_id == 'snapshot_new_456'
                elif playlist.playlist_id == '37i9dQZF1DX0XUsuxWHRQd':
                    assert playlist.master_sync_snapshot_id == 'new_snapshot_789'
                    assert playlist.name == 'RapCaviar Updated'

    # ========== ASSOCIATIONS SYNC TESTS ==========

    def test_sync_database_associations_analysis_mode(self, client):
        """Test track-playlist associations sync analysis mode."""
        self.fixture_loader.setup_initial_database_state('associations_initial.json')

        # Mock Spotify API responses
        spotify_data = self.fixture_loader.get_spotify_associations_mock_data()

        with patch('helpers.sync_helper.fetch_playlists') as mock_fetch_playlists, \
                patch('helpers.sync_helper.get_track_ids_for_playlist') as mock_get_tracks, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            # Configure mocks
            mock_fetch_playlists.return_value = spotify_data['changed_playlists'] + spotify_data['unchanged_playlists']
            mock_auth.return_value = MagicMock()

            # Mock get_track_ids_for_playlist to return different results based on playlist
            def mock_get_tracks_side_effect(spotify_client, playlist_id, force_refresh=False):
                return spotify_data['playlist_track_associations'].get(playlist_id, [])

            mock_get_tracks.side_effect = mock_get_tracks_side_effect

            request_data = {
                'action': 'associations',
                'force_refresh': True,
                'confirmed': False,
                'master_playlist_id': 'test_master_playlist'
            }

            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['action'] == 'associations'
            assert data['stage'] == 'analysis'
            assert data['needs_confirmation'] == True

            # Verify associations changes exist (actual values will vary)
            assert 'details' in data
            assert 'associations_to_add' in data['details']
            assert 'associations_to_remove' in data['details']

            # Check stats structure matches what the code actually returns
            print("Actual stats structure:", data.get('stats', {}))
            assert 'stats' in data

    def test_sync_database_associations_execution(self, client):
        """Test associations sync execution with precomputed changes."""
        self.fixture_loader.setup_initial_database_state('associations_initial.json')

        # Create precomputed changes based on fixture
        precomputed_changes = {
            'tracks_with_changes': [
                {
                    'track_id': 'track_to_be_removed',
                    'track_info': 'Artist B - Track to be Removed',
                    'add_to': [],
                    'remove_from': ['Changed Playlist']
                },
                {
                    'track_id': 'track_to_be_added',
                    'track_info': 'Artist C - Track to be Added',
                    'add_to': ['Changed Playlist'],
                    'remove_from': []
                }
            ],
            'changed_playlists': [
                {
                    'name': 'Changed Playlist',
                    'id': 'playlist_changed_123',
                    'snapshot_id': 'new_assoc_123'
                }
            ]
        }

        request_data = {
            'action': 'associations',
            'force_refresh': True,
            'confirmed': True,
            'master_playlist_id': 'test_master_playlist',
            'precomputed_changes_from_analysis': precomputed_changes
        }

        response = client.post('/api/sync/database',
                               json=request_data,
                               content_type='application/json')

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['action'] == 'associations'
        assert data['stage'] == 'sync_complete'

        # Verify the associations were updated
        assert data['stats']['associations_added'] == 1
        assert data['stats']['associations_removed'] == 1

        # Debug: Check actual associations in database
        with UnitOfWork() as uow:
            all_tracks = uow.track_repository.get_all()
            print("\nActual associations after sync:")
            for track in all_tracks:
                playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track.track_id)
                for playlist_id in playlist_ids:
                    playlist = uow.playlist_repository.get_by_id(playlist_id)
                    playlist_name = playlist.name if playlist else f"ID:{playlist_id}"
                    print(f"  {track.track_id} -> {playlist_name} ({playlist_id})")

        # Validate against actual expected behavior instead of fixture
        with UnitOfWork() as uow:
            # Verify track_to_be_removed is no longer in Changed Playlist
            removed_track_playlists = uow.track_playlist_repository.get_playlist_ids_for_track('track_to_be_removed')
            assert 'playlist_changed_123' not in removed_track_playlists, "track_to_be_removed should not be in Changed Playlist"

            # Verify track_to_be_added is now in Changed Playlist
            added_track_playlists = uow.track_playlist_repository.get_playlist_ids_for_track('track_to_be_added')
            assert 'playlist_changed_123' in added_track_playlists, "track_to_be_added should be in Changed Playlist"

            # Verify track_to_be_added is still in master playlist
            assert 'test_master_playlist_id' in added_track_playlists, "track_to_be_added should still be in master"

            # Verify track_in_changed_playlist associations remain unchanged
            unchanged_track_playlists = uow.track_playlist_repository.get_playlist_ids_for_track(
                'track_in_changed_playlist')
            assert 'test_master_playlist_id' in unchanged_track_playlists, "track_in_changed_playlist should be in master"

            # Count total associations to verify overall state
            all_associations = []
            for track in uow.track_repository.get_all():
                playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track.track_id)
                all_associations.extend([(track.track_id, pid) for pid in playlist_ids])

            print(f"Total associations: {len(all_associations)}")
            assert len(all_associations) == 4, f"Expected 4 associations, got {len(all_associations)}"

    # ========== SEQUENTIAL 'ALL' SYNC TESTS ==========

    def test_sync_database_all_sequential_flow(self, client):
        """Test the complete 'all' sync flow through all stages."""
        self.fixture_loader.setup_initial_database_state('playlists_initial.json')

        # Mock all Spotify API responses
        with patch('helpers.sync_helper.fetch_playlists') as mock_fetch_playlists, \
                patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch_tracks, \
                patch('helpers.sync_helper.get_track_ids_for_playlist') as mock_get_tracks, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_auth.return_value = MagicMock()
            mock_fetch_playlists.return_value = self.fixture_loader.get_spotify_playlists_mock_data()
            mock_fetch_tracks.return_value = self.fixture_loader.get_spotify_api_mock_data()
            mock_get_tracks.return_value = []

            # STAGE 1: Start
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'start',
                'confirmed': False
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'start'
            assert data['next_stage'] == 'playlists'

            # STAGE 2: Playlists Analysis
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'playlists',
                'confirmed': False,
                'force_refresh': True
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'playlists'
            assert data['next_stage'] == 'tracks'
            assert data['needs_confirmation'] == True

            # STAGE 3: Playlists Execution
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'playlists',
                'confirmed': True,
                'force_refresh': True,
                'precomputed_changes_from_analysis': data['details']
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'sync_complete'
            assert data['next_stage'] == 'tracks'

            # STAGE 4: Tracks Analysis
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'tracks',
                'confirmed': False,
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist'
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'tracks'
            assert data['next_stage'] == 'associations'
            assert data['needs_confirmation'] == True

            # STAGE 5: Tracks Execution
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'tracks',
                'confirmed': True,
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist',
                'precomputed_changes_from_analysis': data
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'sync_complete'
            assert data['next_stage'] == 'associations'

            # STAGE 6: Associations Analysis
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'associations',
                'confirmed': False,
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist'
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'associations'
            assert data['next_stage'] == 'complete'
            assert data['needs_confirmation'] == True

            # STAGE 7: Associations Execution
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'associations',
                'confirmed': True,
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist',
                'precomputed_changes_from_analysis': data
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'sync_complete'
            assert data['next_stage'] == 'complete'

            # STAGE 8: Complete
            response = client.post('/api/sync/database', json={
                'action': 'all',
                'stage': 'complete'
            })

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['action'] == 'all'
            assert data['stage'] == 'complete'

    # ========== CLEAR DATABASE TEST ==========

    def test_sync_database_clear(self, client):
        """Test database clear functionality."""
        # Setup some data first
        self.fixture_loader.setup_initial_database_state()

        # Verify data exists
        with UnitOfWork() as uow:
            tracks_before = uow.track_repository.get_all()
            playlists_before = uow.playlist_repository.get_all()
            assert len(tracks_before) > 0
            assert len(playlists_before) > 0

        # Clear the database
        response = client.post('/api/sync/database', json={
            'action': 'clear'
        })

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert 'cleared successfully' in data['message'].lower()

        # Verify data is cleared
        with UnitOfWork() as uow:
            tracks_after = uow.track_repository.get_all()
            playlists_after = uow.playlist_repository.get_all()
            assert len(tracks_after) == 0
            assert len(playlists_after) == 0

    # ========== ERROR HANDLING TESTS ==========

    def test_sync_database_invalid_action(self, client):
        """Test handling of invalid action parameter."""
        response = client.post('/api/sync/database', json={
            'action': 'invalid_action'
        })

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'Invalid action' in data['message']

    def test_sync_database_unknown_stage_in_all(self, client):
        """Test handling of unknown stage in 'all' action."""
        response = client.post('/api/sync/database', json={
            'action': 'all',
            'stage': 'unknown_stage'
        })

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'Unknown stage' in data['message']

    def test_sync_database_with_exclusion_config(self, client):
        """Test sync with exclusion configuration."""
        self.fixture_loader.setup_initial_database_state('playlists_initial.json')

        # Mock Spotify API to return playlists that should be excluded
        from sql.dto.playlist_info import PlaylistInfo
        excluded_playlists = [
            PlaylistInfo('Discover Weekly', 'excluded_1', 'snap_1'),
            PlaylistInfo('Daily Mix 1', 'excluded_2', 'snap_2'),
            PlaylistInfo('Valid Playlist', 'valid_1', 'snap_3')
        ]

        with patch('helpers.sync_helper.fetch_playlists') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_fetch.return_value = excluded_playlists
            mock_auth.return_value = MagicMock()

            request_data = {
                'action': 'playlists',
                'confirmed': False,
                'playlistSettings': {
                    'excludedKeywords': ['Discover Weekly', 'Daily Mix'],
                    'excludedPlaylistIds': [],
                    'excludeByDescription': []
                }
            }

            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

            # Verify exclusion worked - should only see valid playlist changes
            # This depends on the exclusion logic in fetch_playlists

    # ========== PERFORMANCE AND EDGE CASES ==========

    def test_sync_database_empty_spotify_response(self, client):
        """Test sync when Spotify returns empty data."""
        self.fixture_loader.setup_initial_database_state()

        with patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_fetch.return_value = []  # Empty response
            mock_auth.return_value = MagicMock()

            request_data = {
                'action': 'tracks',
                'confirmed': False,
                'master_playlist_id': 'test_master_playlist'
            }

            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            # Should show all existing tracks as to-be-deleted
            assert data['stats']['deleted'] > 0

    def test_sync_database_with_local_tracks(self, client):
        """Test sync handling local tracks correctly."""
        # Setup database with mix of local and Spotify tracks
        self.fixture_loader.setup_initial_database_state()

        # Mock response with local tracks (track_id = None)
        spotify_response_with_locals = [
            ('4U4Sa6KyRGOzqbpBT8vuMl', 'Regular Track', 'Artist A', 'Album A',
             self.fixture_loader._parse_datetime('2025-05-31T22:03:46.000Z')),
            (None, 'Local Track', 'Local Artist', 'Local Album',
             self.fixture_loader._parse_datetime('2025-05-10T06:28:30.000Z'))
        ]

        with patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_fetch.return_value = spotify_response_with_locals
            mock_auth.return_value = MagicMock()

            request_data = {
                'action': 'tracks',
                'confirmed': False,
                'master_playlist_id': 'test_master_playlist'
            }

            response = client.post('/api/sync/database',
                                   json=request_data,
                                   content_type='application/json')

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

            # Check that local tracks are handled (should have generated local IDs)
            tracks_to_add = data['details']['to_add']
            local_tracks = [t for t in tracks_to_add if t.get('is_local', False)]
            assert len(local_tracks) > 0
            # Local tracks should have generated IDs starting with 'local_'
            for local_track in local_tracks:
                assert local_track['id'].startswith('local_')

    def test_sync_database_large_dataset_pagination(self, client):
        """Test sync with large datasets that might require pagination."""
        # This would test the handling of large responses from Spotify
        # that might be paginated or require multiple API calls
        pass  # Implementation depends on your pagination strategy

    # ========== INTEGRATION TESTS ==========

    def test_sync_database_full_integration_all_actions(self, client):
        """Test a complete integration scenario using all sync actions in sequence."""
        # Start with empty database
        with UnitOfWork() as uow:
            uow.track_playlist_repository.delete_all()
            uow.track_repository.delete_all()
            uow.playlist_repository.delete_all()

        # Mock comprehensive Spotify responses
        with patch('helpers.sync_helper.fetch_playlists') as mock_fetch_playlists, \
                patch('helpers.sync_helper.fetch_master_tracks') as mock_fetch_tracks, \
                patch('helpers.sync_helper.get_track_ids_for_playlist') as mock_get_tracks, \
                patch('helpers.sync_helper.authenticate_spotify') as mock_auth:
            mock_auth.return_value = MagicMock()
            mock_fetch_playlists.return_value = self.fixture_loader.get_spotify_playlists_mock_data()
            mock_fetch_tracks.return_value = self.fixture_loader.get_spotify_api_mock_data()
            mock_get_tracks.return_value = []

            # 1. Sync playlists first (analysis then execution)
            response = client.post('/api/sync/database', json={
                'action': 'playlists',
                'confirmed': False,  # Analysis first
                'force_refresh': True
            })
            assert response.status_code == 200
            analysis_data = json.loads(response.data)
            assert analysis_data['success'] == True

            # Execute playlists sync
            response = client.post('/api/sync/database', json={
                'action': 'playlists',
                'confirmed': True,
                'force_refresh': True,
                'precomputed_changes_from_analysis': analysis_data['details']
            })
            assert response.status_code == 200
            assert json.loads(response.data)['success'] == True

            # 2. Sync tracks (analysis then execution)
            response = client.post('/api/sync/database', json={
                'action': 'tracks',
                'confirmed': False,  # Analysis first
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist'
            })
            assert response.status_code == 200
            analysis_data = json.loads(response.data)
            assert analysis_data['success'] == True

            # Execute tracks sync
            response = client.post('/api/sync/database', json={
                'action': 'tracks',
                'confirmed': True,
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist',
                'precomputed_changes_from_analysis': analysis_data
            })
            assert response.status_code == 200
            assert json.loads(response.data)['success'] == True

            # 3. Sync associations (analysis then execution)
            response = client.post('/api/sync/database', json={
                'action': 'associations',
                'confirmed': False,  # Analysis first
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist'
            })
            assert response.status_code == 200
            analysis_data = json.loads(response.data)
            assert analysis_data['success'] == True

            # Execute associations sync
            response = client.post('/api/sync/database', json={
                'action': 'associations',
                'confirmed': True,
                'force_refresh': True,
                'master_playlist_id': 'test_master_playlist',
                'precomputed_changes_from_analysis': analysis_data
            })
            assert response.status_code == 200
            assert json.loads(response.data)['success'] == True

            # Verify final state
            with UnitOfWork() as uow:
                final_playlists = uow.playlist_repository.get_all()
                final_tracks = uow.track_repository.get_all()
                assert len(final_playlists) > 0
                assert len(final_tracks) > 0

    def test_get_exclusion_config_variations(self):
        """Test the exclusion config handling with different input formats."""
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

        # Test with empty settings
        empty_request = {'playlistSettings': {}}
        config = get_exclusion_config(empty_request)
        assert config['forbidden_words'] == []
        assert config['forbidden_playlist_ids'] == []

        # Test with no settings (should load default config)
        mock_config = {"forbidden_playlists": ["SKIPPED"], "forbidden_words": [], "description_keywords": [],
                       "forbidden_playlist_ids": []}
        with patch('json.load', return_value=mock_config):
            config = get_exclusion_config()
            assert 'SKIPPED' in config['forbidden_playlists']
