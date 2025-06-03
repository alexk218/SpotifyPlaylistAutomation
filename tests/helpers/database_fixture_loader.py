import json
import os
from datetime import datetime
from pathlib import Path
from sql.core.unit_of_work import UnitOfWork
from sql.models.track import Track
from sql.models.playlist import Playlist


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

            # Add master playlist
            playlist_data = fixture_data['master_playlist']
            master_playlist = Playlist(
                playlist_id=playlist_data['playlist_id'],
                name=playlist_data['name'],
                master_sync_snapshot_id=playlist_data['master_sync_snapshot_id'],
                associations_snapshot_id=playlist_data['associations_snapshot_id']
            )
            uow.playlist_repository.insert(master_playlist)

            # Add tracks
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

                # Associate with master playlist
                uow.track_playlist_repository.insert(
                    track_data['track_id'],
                    playlist_data['playlist_id']
                )

    def get_spotify_api_mock_data(self, fixture_name='master_playlist_response.json'):
        """Get mock Spotify API data in the format expected by fetch_master_tracks."""
        fixture_data = self.load_fixture('spotify_responses', fixture_name)

        return [
            (
                track['track_id'],
                track['title'],
                track['artists'],
                track['album'],
                self._parse_datetime(track['added_at'])
            )
            for track in fixture_data['api_tracks']
        ]

    def get_expected_results(self, fixture_name='sync_tracks_result.json'):
        """Get expected test results from fixture."""
        return self.load_fixture('expected_results', fixture_name)

    def validate_database_state(self, expected_fixture='sync_tracks_result.json'):
        """Validate database state matches expected results."""
        expected = self.get_expected_results(expected_fixture)

        with UnitOfWork() as uow:
            # Validate track count and IDs
            all_tracks = uow.track_repository.get_all()
            actual_track_ids = {track.track_id for track in all_tracks}
            expected_track_ids = {track['track_id'] for track in expected['expected_final_tracks']}

            assert len(all_tracks) == len(expected['expected_final_tracks']), \
                f"Expected {len(expected['expected_final_tracks'])} tracks, got {len(all_tracks)}"

            assert actual_track_ids == expected_track_ids, \
                f"Track IDs don't match. Expected: {expected_track_ids}, Got: {actual_track_ids}"

            # Validate deleted tracks are gone
            for deleted_id in expected['expected_deleted_tracks']:
                assert deleted_id not in actual_track_ids, \
                    f"Track {deleted_id} should have been deleted"

            # Validate track details
            for expected_track in expected['expected_final_tracks']:
                track = uow.track_repository.get_by_id(expected_track['track_id'])
                assert track is not None, f"Track {expected_track['track_id']} should exist"
                assert track.title == expected_track['title']
                assert track.artists == expected_track['artists']
                assert track.album == expected_track['album']
                assert track.is_local == expected_track['is_local']

    def _parse_datetime(self, date_string):
        """Parse datetime string from fixture."""
        if not date_string:
            return None
        # Handle both ISO formats
        if date_string.endswith('Z'):
            date_string = date_string.replace('Z', '+00:00')
        return datetime.fromisoformat(date_string)
