import json
import os
from datetime import datetime
from pathlib import Path
from sql.core.unit_of_work import UnitOfWork
from sql.models.track import Track
from sql.models.playlist import Playlist
from sql.dto.playlist_info import PlaylistInfo


class DatabaseFixtureLoader:
    """Helper class to load and setup database fixtures from JSON files."""

    def __init__(self):
        self.fixtures_path = Path(__file__).parent.parent / 'fixtures'

    def load_fixture(self, category, filename):
        """Load a JSON fixture file."""
        file_path = self.fixtures_path / category / filename
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def setup_initial_database_state(self, fixture_name='tracks_initial.json'):
        """Setup database with initial state from fixture."""
        fixture_data = self.load_fixture('database_states', fixture_name)

        with UnitOfWork() as uow:
            # Clear existing data
            uow.track_playlist_repository.delete_all()
            uow.track_repository.delete_all()
            uow.playlist_repository.delete_all()

            # Add master playlist if it exists in fixture
            if 'master_playlist' in fixture_data:
                playlist_data = fixture_data['master_playlist']
                master_playlist = Playlist(
                    playlist_id=playlist_data['playlist_id'],
                    name=playlist_data['name'],
                    master_sync_snapshot_id=playlist_data['master_sync_snapshot_id'],
                    associations_snapshot_id=playlist_data['associations_snapshot_id']
                )
                uow.playlist_repository.insert(master_playlist)

            # Add other playlists if they exist
            if 'playlists' in fixture_data:
                for playlist_data in fixture_data['playlists']:
                    playlist = Playlist(
                        playlist_id=playlist_data['playlist_id'],
                        name=playlist_data['name'],
                        master_sync_snapshot_id=playlist_data['master_sync_snapshot_id'],
                        associations_snapshot_id=playlist_data['associations_snapshot_id']
                    )
                    uow.playlist_repository.insert(playlist)

            # Add tracks
            if 'tracks' in fixture_data:
                for track_data in fixture_data['tracks']:
                    track = Track(
                        track_id=track_data['track_id'],
                        title=track_data['title'],
                        artists=track_data['artists'],
                        album=track_data['album'],
                        added_to_master=self._parse_datetime(track_data['added_to_master']),
                        is_local=track_data['is_local']
                    )
                    uow.track_repository.insert(track)

            # Add track-playlist associations
            if 'track_playlist_associations' in fixture_data:
                for assoc in fixture_data['track_playlist_associations']:
                    uow.track_playlist_repository.insert(
                        assoc['track_id'],
                        assoc['playlist_id']
                    )
            elif 'master_playlist' in fixture_data and 'tracks' in fixture_data:
                # For backward compatibility, associate all tracks with master playlist
                master_id = fixture_data['master_playlist']['playlist_id']
                for track_data in fixture_data['tracks']:
                    uow.track_playlist_repository.insert(
                        track_data['track_id'],
                        master_id
                    )

    def get_spotify_api_mock_data(self, fixture_name='master_playlist_api_response.json'):
        """Get mock Spotify API data in the format expected by fetch_master_tracks."""
        fixture_data = self.load_fixture('spotify_responses', fixture_name)

        # Return data in the format that fetch_master_tracks expects:
        # List of tuples: (track_id, track_title, artists, album, added_at)
        return [
            (
                track['track_id'],  # This will be None for local tracks
                track['title'],
                track['artists'],
                track['album'],
                self._parse_datetime(track['added_at'])
            )
            for track in fixture_data['api_tracks']
        ]

    def get_spotify_playlists_mock_data(self, fixture_name='spotify_playlists_api_response.json'):
        """Get mock Spotify playlists data in the format expected by fetch_playlists."""
        fixture_data = self.load_fixture('spotify_responses', fixture_name)

        return [
            PlaylistInfo(
                name=playlist['name'],
                playlist_id=playlist['playlist_id'],
                snapshot_id=playlist['snapshot_id']
            )
            for playlist in fixture_data['playlists']
        ]

    def get_spotify_associations_mock_data(self, fixture_name='spotify_associations_api_response.json'):
        """Get mock Spotify associations data."""
        fixture_data = self.load_fixture('spotify_responses', fixture_name)

        # Convert to PlaylistInfo objects
        changed_playlists = [
            PlaylistInfo(
                name=playlist['name'],
                playlist_id=playlist['playlist_id'],
                snapshot_id=playlist['snapshot_id']
            )
            for playlist in fixture_data['changed_playlists']
        ]

        unchanged_playlists = [
            PlaylistInfo(
                name=playlist['name'],
                playlist_id=playlist['playlist_id'],
                snapshot_id=playlist['snapshot_id']
            )
            for playlist in fixture_data['unchanged_playlists']
        ]

        return {
            'changed_playlists': changed_playlists,
            'unchanged_playlists': unchanged_playlists,
            'playlist_track_associations': fixture_data['playlist_track_associations']
        }

    def get_expected_results(self, fixture_name='sync_tracks_result.json'):
        """Get expected test results from fixture."""
        return self.load_fixture('expected_results', fixture_name)

    def validate_database_state(self, expected_fixture='sync_tracks_result.json'):
        """Validate database state matches expected results."""
        expected = self.get_expected_results(expected_fixture)

        with UnitOfWork() as uow:
            if 'expected_final_tracks' in expected:
                self._validate_tracks_state(uow, expected)

            if 'expected_final_playlists' in expected:
                self._validate_playlists_state(uow, expected)

            if 'expected_final_associations' in expected:
                self._validate_associations_state(uow, expected)

    def _validate_tracks_state(self, uow, expected):
        """Validate track data in database."""
        all_tracks = uow.track_repository.get_all()
        actual_track_ids = {track.track_id for track in all_tracks}
        expected_track_ids = {track['track_id'] for track in expected['expected_final_tracks']}

        assert len(all_tracks) == len(expected['expected_final_tracks']), \
            f"Expected {len(expected['expected_final_tracks'])} tracks, got {len(all_tracks)}"

        # Handle local vs Spotify tracks separately
        actual_local_tracks = {tid for tid in actual_track_ids if tid.startswith('local_')}
        expected_local_tracks = {tid for tid in expected_track_ids if tid.startswith('local_')}
        actual_spotify_tracks = {tid for tid in actual_track_ids if not tid.startswith('local_')}
        expected_spotify_tracks = {tid for tid in expected_track_ids if not tid.startswith('local_')}

        # Validate Spotify tracks match exactly
        assert actual_spotify_tracks == expected_spotify_tracks, \
            f"Spotify Track IDs don't match. Expected: {expected_spotify_tracks}, Got: {actual_spotify_tracks}"

        # Validate local track count matches (IDs will be different due to hashing)
        assert len(actual_local_tracks) == len(expected_local_tracks), \
            f"Local track count doesn't match. Expected: {len(expected_local_tracks)}, Got: {len(actual_local_tracks)}"

        # Validate deleted tracks are gone
        if 'expected_deleted_tracks' in expected:
            for deleted_id in expected['expected_deleted_tracks']:
                assert deleted_id not in actual_track_ids, \
                    f"Track {deleted_id} should have been deleted"

        # Validate track details
        expected_by_type = {}
        for expected_track in expected['expected_final_tracks']:
            if expected_track['track_id'].startswith('local_'):
                key = f"{expected_track['artists']}|{expected_track['title']}"
                expected_by_type[key] = expected_track
            else:
                expected_by_type[expected_track['track_id']] = expected_track

        for track in all_tracks:
            if track.track_id.startswith('local_'):
                key = f"{track.artists}|{track.title}"
                assert key in expected_by_type, f"Unexpected local track: {track.artists} - {track.title}"
                expected_track = expected_by_type[key]
            else:
                assert track.track_id in expected_by_type, f"Unexpected Spotify track: {track.track_id}"
                expected_track = expected_by_type[track.track_id]

            assert track.title == expected_track['title']
            assert track.artists == expected_track['artists']
            assert track.album == expected_track['album']
            assert track.is_local == expected_track['is_local']

    def _validate_playlists_state(self, uow, expected):
        """Validate playlist data in database."""
        all_playlists = uow.playlist_repository.get_all()
        actual_playlist_ids = {p.playlist_id for p in all_playlists}
        expected_playlist_ids = {p['playlist_id'] for p in expected['expected_final_playlists']}

        assert len(all_playlists) == len(expected['expected_final_playlists']), \
            f"Expected {len(expected['expected_final_playlists'])} playlists, got {len(all_playlists)}"

        assert actual_playlist_ids == expected_playlist_ids, \
            f"Playlist IDs don't match. Expected: {expected_playlist_ids}, Got: {actual_playlist_ids}"

        # Validate deleted playlists are gone
        if 'expected_deleted_playlists' in expected:
            for deleted_id in expected['expected_deleted_playlists']:
                assert deleted_id not in actual_playlist_ids, \
                    f"Playlist {deleted_id} should have been deleted"

        # Validate playlist details
        expected_by_id = {p['playlist_id']: p for p in expected['expected_final_playlists']}

        for playlist in all_playlists:
            expected_playlist = expected_by_id[playlist.playlist_id]
            assert playlist.name == expected_playlist['name']
            assert playlist.master_sync_snapshot_id == expected_playlist['master_sync_snapshot_id']
            if 'associations_snapshot_id' in expected_playlist:
                assert playlist.associations_snapshot_id == expected_playlist['associations_snapshot_id']

    def _validate_associations_state(self, uow, expected):
        """Validate track-playlist associations in database."""
        # Get all current associations
        all_associations = []
        all_tracks = uow.track_repository.get_all()

        for track in all_tracks:
            playlist_ids = uow.track_playlist_repository.get_playlist_ids_for_track(track.track_id)
            for playlist_id in playlist_ids:
                all_associations.append({
                    'track_id': track.track_id,
                    'playlist_id': playlist_id
                })

        expected_associations = expected['expected_final_associations']

        assert len(all_associations) == len(expected_associations), \
            f"Expected {len(expected_associations)} associations, got {len(all_associations)}"

        # Convert to sets for comparison
        actual_set = {(a['track_id'], a['playlist_id']) for a in all_associations}
        expected_set = {(a['track_id'], a['playlist_id']) for a in expected_associations}

        assert actual_set == expected_set, \
            f"Associations don't match. Expected: {expected_set}, Got: {actual_set}"

    def _parse_datetime(self, date_string):
        """Parse datetime string from fixture."""
        if not date_string:
            return None
        # Handle both ISO formats
        if date_string.endswith('Z'):
            date_string = date_string.replace('Z', '+00:00')
        return datetime.fromisoformat(date_string)
