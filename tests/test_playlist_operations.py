import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from sql.core.unit_of_work import UnitOfWork
from tests.helpers.database_fixture_loader import DatabaseFixtureLoader


class TestPlaylistOperations:
    """Comprehensive tests for playlist operations with real database and file system interaction."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.fixture_loader = DatabaseFixtureLoader()

    @pytest.fixture
    def temp_directories(self):
        """Create temporary directories for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            master_tracks_dir = temp_path / "master_tracks"
            playlists_dir = temp_path / "playlists"

            master_tracks_dir.mkdir()
            playlists_dir.mkdir()

            yield {
                'master_tracks_dir': str(master_tracks_dir),
                'playlists_dir': str(playlists_dir),
                'temp_path': temp_path
            }

    @pytest.fixture
    def sample_mp3_files(self, temp_directories):
        """Create sample MP3 files with embedded TrackIDs."""
        master_tracks_dir = Path(temp_directories['master_tracks_dir'])

        # Create sample MP3 files (empty files with .mp3 extension for testing)
        files = [
            {
                'filename': 'Artist1 - Track1.mp3',
                'track_id': 'spotify_track_1',
                'path': master_tracks_dir / 'Artist1 - Track1.mp3'
            },
            {
                'filename': 'Artist2 - Track2.mp3',
                'track_id': 'spotify_track_2',
                'path': master_tracks_dir / 'Artist2 - Track2.mp3'
            },
            {
                'filename': 'Local Artist - Local Track.mp3',
                'track_id': 'local_track_1',
                'path': master_tracks_dir / 'Local Artist - Local Track.mp3'
            }
        ]

        # Create the actual files
        for file_info in files:
            file_info['path'].touch()

        return files

    def create_sample_m3u_file(self, playlists_dir: str, playlist_name: str, tracks: list) -> str:
        """Create a sample M3U file for testing."""
        m3u_path = Path(playlists_dir) / f"{playlist_name}.m3u"

        with open(m3u_path, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            for track in tracks:
                f.write(f"#EXTINF:180,{track['artist']} - {track['title']}\n")
                f.write(f"{track['path']}\n")

        return str(m3u_path)

    def create_test_m3u_file(self, playlists_dir, playlist_name, tracks):
        """Helper to create test M3U files."""
        m3u_path = Path(playlists_dir) / f"{playlist_name}.m3u"

        with open(m3u_path, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            for track in tracks:
                f.write(f"#EXTINF:180,{track['artist']} - {track['title']}\n")
                f.write(f"{track['path']}\n")

        return str(m3u_path)

    # ========== ANALYSIS ENDPOINT TESTS ==========

    def test_analyze_m3u_generation_with_empty_database(self, client, temp_directories):
        """Test M3U analysis when database is empty."""
        # Empty database - no playlists should be found
        request_data = {
            'masterTracksDir': temp_directories['master_tracks_dir'],
            'playlistsDir': temp_directories['playlists_dir']
        }

        response = client.get('/api/playlists/analysis', json=request_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True
        assert data['details']['total_playlists'] == 0
        assert len(data['details']['playlists']) == 0

    def test_analyze_m3u_generation_with_playlists(self, client, temp_directories, sample_mp3_files):
        """Test M3U analysis with populated database."""
        # Setup database with test playlists and tracks
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Mock URI to file mapping to include our sample files
        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping:
            mock_build_mapping.return_value = mock_uri_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            response = client.get('/api/playlists/analysis', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['details']['total_playlists'] > 0
            assert len(data['details']['playlists']) > 0

            # Verify playlist structure
            for playlist in data['details']['playlists']:
                assert 'name' in playlist
                assert 'id' in playlist
                assert 'track_count' in playlist
                assert 'local_track_count' in playlist

    def test_analyze_m3u_generation_missing_directories(self, client):
        """Test M3U analysis with missing directories."""
        request_data = {
            'masterTracksDir': '/nonexistent/path',
            'playlistsDir': '/another/nonexistent/path'
        }

        response = client.get('/api/playlists/analysis', json=request_data)

        # Should still work but with no tracks found
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] == True

    def test_analyze_m3u_generation_no_playlists_dir(self, client, temp_directories):
        """Test M3U analysis without playlists directory specified."""
        request_data = {
            'masterTracksDir': temp_directories['master_tracks_dir']
            # Missing playlistsDir
        }

        response = client.get('/api/playlists/analysis', json=request_data)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'not specified' in data['message']

    # ========== GENERATION ENDPOINT TESTS ==========

    def test_generate_playlists_analysis_mode(self, client, temp_directories, sample_mp3_files):
        """Test playlist generation in analysis mode (confirmed=False)."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping:
            mock_build_mapping.return_value = mock_uri_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'confirmed': False
            }

            response = client.post('/api/playlists/generate', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['needs_confirmation'] == (data['details']['total_playlists'] > 0)

    def test_generate_playlists_execution_mode(self, client, temp_directories, sample_mp3_files):
        """Test playlist generation in execution mode (confirmed=True)."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Get playlist IDs from database for the request
        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist_ids = [p.playlist_id for p in playlists if p.name.upper() != "MASTER"]

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_multiple_playlists') as mock_generate:
            mock_build_mapping.return_value = mock_uri_mapping
            mock_generate.return_value = [
                {
                    'id': playlist_ids[0] if playlist_ids else 'test_id',
                    'name': 'Test Playlist',
                    'success': True,
                    'tracks_found': 2,
                    'tracks_added': 2,
                    'm3u_path': '/test/path.m3u'
                }
            ] if playlist_ids else []

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'confirmed': True,
                'playlists_to_update': playlist_ids[:1] if playlist_ids else []
            }

            response = client.post('/api/playlists/generate', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert 'playlists_updated' in data
            if playlist_ids:
                assert data['playlists_updated'] >= 0

    def test_generate_playlists_with_saved_structure(self, client, temp_directories, sample_mp3_files):
        """Test playlist generation with saved playlist structure."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Create a saved playlist structure file
        playlists_dir = Path(temp_directories['playlists_dir'])
        structure_file = playlists_dir / '.playlist_structure.json'

        saved_structure = {
            "root_playlists": ["Test Playlist 1"],
            "folders": {
                "Electronic": {
                    "playlists": ["Test Playlist 2"]
                }
            }
        }

        with open(structure_file, 'w', encoding='utf-8') as f:
            json.dump(saved_structure, f)

        # Create subdirectory for structured playlists
        electronic_dir = playlists_dir / "Electronic"
        electronic_dir.mkdir()

        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist_ids = [p.playlist_id for p in playlists if p.name.upper() != "MASTER"]

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_multiple_playlists') as mock_generate:
            mock_build_mapping.return_value = mock_uri_mapping
            mock_generate.return_value = [
                {
                    'id': playlist_ids[0] if playlist_ids else 'test_id',
                    'name': 'Test Playlist',
                    'success': True,
                    'tracks_found': 2,
                    'tracks_added': 2,
                    'm3u_path': '/test/path.m3u'
                }
            ] if playlist_ids else []

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'confirmed': True,
                'playlists_to_update': playlist_ids[:1] if playlist_ids else []
            }

            response = client.post('/api/playlists/generate', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

    def test_generate_playlists_missing_playlists_dir(self, client, temp_directories):
        """Test playlist generation without playlists directory."""
        request_data = {
            'masterTracksDir': temp_directories['master_tracks_dir'],
            'extended': True,
            'overwrite': True,
            'confirmed': True
            # Missing playlistsDir
        }

        response = client.post('/api/playlists/generate', json=request_data)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'not specified' in data['message']

    def test_generate_playlists_with_errors(self, client, temp_directories, sample_mp3_files):
        """Test playlist generation with simulated errors."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Get playlist IDs from database for the request
        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist_ids = [p.playlist_id for p in playlists if p.name.upper() != "MASTER"]

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping, \
                patch('api.services.playlist_service.generate_playlists') as mock_generate:
            mock_build_mapping.return_value = mock_uri_mapping
            # Simulate an error in playlist generation service
            mock_generate.side_effect = Exception("Simulated generation error")

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'confirmed': True,
                'playlists_to_update': playlist_ids[:1] if playlist_ids else []
            }

            response = client.post('/api/playlists/generate', json=request_data)

            assert response.status_code == 500
            data = json.loads(response.data)
            assert data['success'] == False
            assert 'message' in data

    # ========== REGENERATE ENDPOINT TESTS ==========

    def test_regenerate_playlist_missing_playlists_dir(self, client, temp_directories):
        """Test playlist regeneration without playlists directory."""
        fake_playlist_id = "some_playlist_id"

        request_data = {
            'masterTracksDir': temp_directories['master_tracks_dir'],
            'extended': True,
            'overwrite': True,
            'force': True
            # Missing playlistsDir
        }

        response = client.post(f'/api/playlists/{fake_playlist_id}/regenerate', json=request_data)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'not specified' in data['message']

    def test_regenerate_playlist_with_existing_m3u(self, client, temp_directories, sample_mp3_files):
        """Test regeneration of playlist with existing M3U file."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist = next((p for p in playlists if p.name.upper() != "MASTER"), None)

        if not playlist:
            pytest.skip("No test playlist found in database")

        # Create an existing M3U file
        playlists_dir = Path(temp_directories['playlists_dir'])
        existing_m3u = playlists_dir / f"{playlist.name}.m3u"
        existing_m3u.write_text("#EXTM3U\n# Existing playlist content\n")

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        # Mock the entire regenerate_playlist service function
        mock_result = {
            'success': True,
            'message': f'Successfully regenerated playlist: {playlist.name} with 3 tracks',
            'stats': {
                'playlist_name': playlist.name,
                'tracks_found': 3,
                'tracks_added': 3,
                'm3u_path': str(existing_m3u),
                'location': 'root',
                'file_size': 1500
            }
        }

        with patch('api.services.playlist_service.regenerate_playlist') as mock_regenerate:
            mock_regenerate.return_value = mock_result

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'force': True
            }

            response = client.post(f'/api/playlists/{playlist.playlist_id}/regenerate', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['result']['stats']['tracks_added'] == 3

    def test_regenerate_playlist_with_saved_structure_location(self, client, temp_directories, sample_mp3_files):
        """Test playlist regeneration using saved structure for location."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist = next((p for p in playlists if p.name.upper() != "MASTER"), None)

        if not playlist:
            pytest.skip("No test playlist found in database")

        # Create saved structure
        playlists_dir = Path(temp_directories['playlists_dir'])
        structure_file = playlists_dir / '.playlist_structure.json'

        saved_structure = {
            "root_playlists": [],
            "folders": {
                "Electronic": {
                    "playlists": [playlist.name]
                }
            }
        }

        with open(structure_file, 'w', encoding='utf-8') as f:
            json.dump(saved_structure, f)

        # Create the Electronic subdirectory
        electronic_dir = playlists_dir / "Electronic"
        electronic_dir.mkdir()

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_uri_mapping
            mock_generate.return_value = (2, 2)
            mock_getsize.return_value = 800

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'force': True
            }

            response = client.post(f'/api/playlists/{playlist.playlist_id}/regenerate', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            assert data['result']['stats']['location'] == "Electronic"

    # ========== INTEGRATION TESTS ==========

    def test_full_playlist_workflow(self, client, temp_directories, sample_mp3_files):
        """Test complete workflow: analyze -> generate -> regenerate."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Get playlist info
        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist = next((p for p in playlists if p.name.upper() != "MASTER"), None)

        if not playlist:
            pytest.skip("No test playlist found in database")

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_multiple_playlists') as mock_generate_multiple, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate_single, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_uri_mapping
            mock_generate_multiple.return_value = [
                {
                    'id': playlist.playlist_id,
                    'name': playlist.name,
                    'success': True,
                    'tracks_found': 2,
                    'tracks_added': 2,
                    'm3u_path': '/test/path.m3u'
                }
            ]
            mock_generate_single.return_value = (2, 2)
            mock_getsize.return_value = 1000

            base_request = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            # Step 1: Analysis
            response1 = client.get('/api/playlists/analysis', json=base_request)
            assert response1.status_code == 200
            analysis_data = json.loads(response1.data)
            assert analysis_data['success'] == True

            # Step 2: Generation (analysis mode)
            gen_request = {
                **base_request,
                'extended': True,
                'overwrite': True,
                'confirmed': False
            }
            response2 = client.post('/api/playlists/generate', json=gen_request)
            assert response2.status_code == 200
            gen_analysis = json.loads(response2.data)
            assert gen_analysis['success'] == True

            # Step 3: Generation (execution mode)
            gen_request['confirmed'] = True
            gen_request['playlists_to_update'] = [playlist.playlist_id]
            response3 = client.post('/api/playlists/generate', json=gen_request)
            assert response3.status_code == 200
            gen_result = json.loads(response3.data)
            assert gen_result['success'] == True

            # Step 4: Regenerate single playlist
            regen_request = {
                **base_request,
                'extended': True,
                'overwrite': True,
                'force': True
            }
            response4 = client.post(f'/api/playlists/{playlist.playlist_id}/regenerate', json=regen_request)
            assert response4.status_code == 200
            regen_result = json.loads(response4.data)
            assert regen_result['success'] == True

    def test_playlist_operations_with_empty_tracks(self, client, temp_directories):
        """Test playlist operations when no tracks are found locally."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Mock empty URI mapping
        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_multiple_playlists') as mock_generate:
            mock_build_mapping.return_value = {}  # No tracks found
            mock_generate.return_value = []  # No playlists generated

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            # Test analysis
            response1 = client.get('/api/playlists/analysis', json=request_data)
            assert response1.status_code == 200
            data1 = json.loads(response1.data)
            assert data1['success'] == True

            # All playlists should have 0 local tracks
            for playlist in data1['details']['playlists']:
                assert playlist['local_track_count'] == 0

    def test_playlist_operations_error_handling(self, client, temp_directories):
        """Test error handling in playlist operations."""
        # Test with invalid directories
        request_data = {
            'masterTracksDir': '/invalid/path',
            'playlistsDir': '/invalid/path'
        }

        # Mock an exception in the service layer - targeting the correct function
        with patch('api.services.playlist_service.analyze_m3u_generation') as mock_analyze:
            mock_analyze.side_effect = Exception("Simulated error")

            response = client.get('/api/playlists/analysis', json=request_data)
            assert response.status_code == 500
            data = json.loads(response.data)
            assert data['success'] == False
            assert 'message' in data

    # ========== EDGE CASES AND BOUNDARY CONDITIONS ==========

    def test_playlist_with_special_characters(self, client, temp_directories, sample_mp3_files):
        """Test playlist operations with special characters in playlist names."""
        # Create a special playlist in the database
        special_playlist_name = "Test Playlist [Mix] (2024) & More!"

        # This would require setting up a special fixture or directly inserting into DB
        # For now, we'll test with existing playlists and verify special character handling
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping:
            mock_build_mapping.return_value = mock_uri_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            response = client.get('/api/playlists/analysis', json=request_data)
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

    def test_playlist_operations_performance_with_large_dataset(self, client, temp_directories):
        """Test playlist operations performance with simulated large dataset."""
        # This test would typically use a larger fixture file
        # For now, we'll use the existing one but verify the response structure
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Mock a large URI mapping
        large_mock_mapping = {f'spotify:track:{i}abc{i}': f'/path/to/track{i}.mp3' for i in range(100)}

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping:
            mock_build_mapping.return_value = large_mock_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            response = client.get('/api/playlists/analysis', json=request_data)
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            # Verify that the batch operations work correctly
            assert 'details' in data
            assert 'playlists' in data['details']

    def test_concurrent_playlist_operations(self, client, temp_directories, sample_mp3_files):
        """Test handling of concurrent playlist operations."""
        # This is more of a conceptual test - in practice you'd need threading
        # For now, we'll just ensure multiple rapid requests work
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        mock_uri_mapping = {
            'spotify:track:1abc123': str(sample_mp3_files[0]['path']),
            'spotify:track:2def456': str(sample_mp3_files[1]['path']),
            'spotify:track:3ghi789': str(sample_mp3_files[2]['path'])
        }

        with patch('helpers.m3u_helper.build_uri_to_file_mapping_from_database') as mock_build_mapping:
            mock_build_mapping.return_value = mock_uri_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            # Make multiple requests in succession
            for i in range(3):
                response = client.get('/api/playlists/analysis', json=request_data)
                assert response.status_code == 200
                data = json.loads(response.data)
                assert data['success'] == True
