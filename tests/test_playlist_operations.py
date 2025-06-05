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

        # Mock track ID mapping to include our sample files
        mock_track_mapping = {
            'spotify_track_1': sample_mp3_files[0]['path'],
            'spotify_track_2': sample_mp3_files[1]['path'],
            'local_track_1': sample_mp3_files[2]['path']
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping:
            mock_build_mapping.return_value = mock_track_mapping

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

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping:
            mock_build_mapping.return_value = mock_track_mapping

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

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (2, 2)  # tracks_found, tracks_added

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
            assert 'stats' in data
            if playlist_ids:
                assert data['stats']['playlists_updated'] >= 0

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

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (2, 2)

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

        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist_ids = [p.playlist_id for p in playlists if p.name.upper() != "MASTER"]

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate:
            mock_build_mapping.return_value = {}
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

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True
            # Should show failed playlists
            if playlist_ids:
                assert data['stats']['playlists_failed'] >= 0

    # ========== REGENERATE SINGLE PLAYLIST TESTS ==========

    def test_regenerate_playlist_success(self, client, temp_directories, sample_mp3_files):
        """Test successful regeneration of a single playlist."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        # Get a playlist ID from the database
        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist = next((p for p in playlists if p.name.upper() != "MASTER"), None)

        if not playlist:
            pytest.skip("No test playlist found in database")

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (2, 2)  # tracks_found, tracks_added
            mock_getsize.return_value = 1000  # Mock file size

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
            assert playlist.name in data['message']
            assert 'result' in data
            assert data['result']['success'] == True

    def test_regenerate_playlist_not_found(self, client, temp_directories):
        """Test regeneration of a non-existent playlist."""
        fake_playlist_id = "non_existent_playlist_id"

        request_data = {
            'masterTracksDir': temp_directories['master_tracks_dir'],
            'playlistsDir': temp_directories['playlists_dir'],
            'extended': True,
            'overwrite': True,
            'force': True
        }

        response = client.post(f'/api/playlists/{fake_playlist_id}/regenerate', json=request_data)

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data['success'] == False
        assert 'not found' in data['message'] or 'Error' in data['message']

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

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (3, 3)
            mock_getsize.return_value = 1500

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

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_track_mapping
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

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (2, 2)
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

        # Mock empty track mapping
        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate:
            mock_build_mapping.return_value = {}  # No tracks found
            mock_generate.return_value = (0, 0)  # No tracks generated

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

        # Mock an exception in the service layer
        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping:
            mock_build_mapping.side_effect = Exception("Simulated error")

            response = client.get('/api/playlists/analysis', json=request_data)
            assert response.status_code == 500
            data = json.loads(response.data)
            assert data['success'] == False
            assert 'Error' in data['message'] or 'error' in data['message']

    # ========== EDGE CASES AND BOUNDARY CONDITIONS ==========

    def test_playlist_with_special_characters(self, client, temp_directories, sample_mp3_files):
        """Test playlist operations with special characters in playlist names."""
        # Setup database with a playlist containing special characters
        from sql.models.playlist import Playlist
        from sql.models.track import Track

        with UnitOfWork() as uow:
            # Clear existing data
            uow.track_playlist_repository.delete_all()
            uow.track_repository.delete_all()
            uow.playlist_repository.delete_all()

            # Create a playlist with special characters
            special_playlist = Playlist(
                playlist_id="special_chars_playlist",
                name="Test Playlist: Electronic/Dance (2024) [Mix]",
                master_sync_snapshot_id="snapshot_123",
                associations_snapshot_id="assoc_123"
            )
            uow.playlist_repository.insert(special_playlist)

            # Add a track
            track = Track(
                track_id="spotify_track_1",
                title="Test Track",
                artists="Test Artist",
                album="Test Album",
                is_local=False
            )
            uow.track_repository.insert(track)

            # Add association
            uow.track_playlist_repository.insert("spotify_track_1", "special_chars_playlist")

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate, \
                patch('os.path.getsize') as mock_getsize:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (1, 1)
            mock_getsize.return_value = 500

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir'],
                'extended': True,
                'overwrite': True,
                'confirmed': True,
                'playlists_to_update': ["special_chars_playlist"]
            }

            response = client.post('/api/playlists/generate', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

    def test_concurrent_playlist_operations(self, client, temp_directories, sample_mp3_files):
        """Test behavior when multiple playlist operations might run concurrently."""
        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        with UnitOfWork() as uow:
            playlists = uow.playlist_repository.get_all()
            playlist_ids = [p.playlist_id for p in playlists if p.name.upper() != "MASTER"]

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        # Simulate concurrent operations by running analysis and generation quickly
        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping, \
                patch('helpers.m3u_helper.generate_m3u_playlist') as mock_generate:
            mock_build_mapping.return_value = mock_track_mapping
            mock_generate.return_value = (2, 2)

            base_request = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            # Run analysis
            response1 = client.get('/api/playlists/analysis', json=base_request)
            assert response1.status_code == 200

            # Immediately run generation
            gen_request = {
                **base_request,
                'extended': True,
                'overwrite': True,
                'confirmed': True,
                'playlists_to_update': playlist_ids[:1] if playlist_ids else []
            }
            response2 = client.post('/api/playlists/generate', json=gen_request)
            assert response2.status_code == 200

            # Both should succeed
            data1 = json.loads(response1.data)
            data2 = json.loads(response2.data)
            assert data1['success'] == True
            assert data2['success'] == True

    def test_large_playlist_handling(self, client, temp_directories):
        """Test handling of playlists with many tracks."""
        # Create a playlist with many tracks in the database
        from sql.models.playlist import Playlist
        from sql.models.track import Track

        with UnitOfWork() as uow:
            # Clear existing data
            uow.track_playlist_repository.delete_all()
            uow.track_repository.delete_all()
            uow.playlist_repository.delete_all()

            # Create a large playlist
            large_playlist = Playlist(
                playlist_id="large_playlist",
                name="Large Test Playlist",
                master_sync_snapshot_id="large_snapshot",
                associations_snapshot_id="large_assoc"
            )
            uow.playlist_repository.insert(large_playlist)

            # Add many tracks (simulate a large playlist)
            track_mapping = {}
            for i in range(100):  # 100 tracks
                track_id = f"track_{i:03d}"
                track = Track(
                    track_id=track_id,
                    title=f"Track {i}",
                    artists=f"Artist {i}",
                    album=f"Album {i}",
                    is_local=False
                )
                uow.track_repository.insert(track)
                uow.track_playlist_repository.insert(track_id, "large_playlist")

                # Mock file path
                track_mapping[track_id] = f"/fake/path/track_{i}.mp3"

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping:
            mock_build_mapping.return_value = track_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            response = client.get('/api/playlists/analysis', json=request_data)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['success'] == True

            # Find our large playlist
            large_playlist_data = next(
                (p for p in data['details']['playlists'] if p['name'] == "Large Test Playlist"),
                None
            )
            assert large_playlist_data is not None
            assert large_playlist_data['track_count'] == 100
            assert large_playlist_data['local_track_count'] == 100  # All tracks have mock files

    # ========== PERFORMANCE TESTS ==========

    def test_playlist_operations_performance_monitoring(self, client, temp_directories, sample_mp3_files):
        """Test that playlist operations complete within reasonable time limits."""
        import time

        self.fixture_loader.setup_initial_database_state('playlist_operations_initial.json')

        mock_track_mapping = {
            'spotify_track_1': str(sample_mp3_files[0]['path']),
            'spotify_track_2': str(sample_mp3_files[1]['path']),
            'local_track_1': str(sample_mp3_files[2]['path'])
        }

        with patch('api.services.playlist_service.build_track_id_mapping') as mock_build_mapping:
            mock_build_mapping.return_value = mock_track_mapping

            request_data = {
                'masterTracksDir': temp_directories['master_tracks_dir'],
                'playlistsDir': temp_directories['playlists_dir']
            }

            # Time the analysis operation
            start_time = time.time()
            response = client.get('/api/playlists/analysis', json=request_data)
            end_time = time.time()

            operation_time = end_time - start_time

            assert response.status_code == 200
            assert operation_time < 10.0  # Should complete within 10 seconds

            data = json.loads(response.data)
            assert data['success'] == True


    ## PREVIOUS TESTS
    def test_generate_m3u_analysis(self, client, mock_unit_of_work):
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


    def test_generate_m3u_execution(self, client, mock_unit_of_work):
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


    def test_validate_playlists_m3u_with_missing(self, client, mock_unit_of_work, mock_track_id_mapping):
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

    def test_generate_rekordbox_xml(self, client):
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


    def test_regenerate_playlist(self, client, mock_unit_of_work):
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
