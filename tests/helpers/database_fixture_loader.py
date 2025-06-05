import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, List

from sql.core.unit_of_work import UnitOfWork
from sql.dto.playlist_info import PlaylistInfo
from sql.models.playlist import Playlist
from sql.models.track import Track


class DatabaseFixtureLoader:
    """Helper class to load and setup database fixtures from JSON files."""

    def __init__(self):
        self.fixtures_path = Path(__file__).parent.parent / 'fixtures'
        self._initial_state = None

    def load_fixture(self, category, filename):
        """Load a JSON fixture file."""
        file_path = self.fixtures_path / category / filename
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def setup_initial_database_state(self, fixture_files: Optional[Union[str, List[str]]] = None):
        """
        Setup database with initial state from fixture file(s).

        Args:
            fixture_files: Can be:
                - None: Load all JSON files in database_states directory
                - str: Load single specific file (e.g., 'tracks_initial.json')
                - List[str]: Load multiple specific files (e.g., ['tracks.json', 'playlists.json'])
        """
        # Determine which files to load
        if fixture_files is None:
            # Load all JSON files in database_states directory
            db_states_dir = self.fixtures_path / 'database_states'
            json_files = [f for f in os.listdir(db_states_dir) if f.endswith('.json')]
            fixture_files = json_files
        elif isinstance(fixture_files, str):
            # Single file
            fixture_files = [fixture_files]
        # else: it's already a list

        # Load and combine all fixture data
        combined_data = {
            'tracks': [],
            'playlists': [],
            'track_playlist_associations': [],
            'master_playlist': None
        }

        for filename in fixture_files:
            print(f"Loading fixture: {filename}")
            fixture_data = self.load_fixture('database_states', filename)

            # Combine tracks (avoid duplicates by track_id)
            if 'tracks' in fixture_data:
                existing_track_ids = {track['track_id'] for track in combined_data['tracks']}
                for track in fixture_data['tracks']:
                    if track['track_id'] not in existing_track_ids:
                        combined_data['tracks'].append(track)
                        existing_track_ids.add(track['track_id'])
                    else:
                        print(f"Skipping duplicate track: {track['track_id']}")

            # Combine playlists (avoid duplicates by playlist_id)
            if 'playlists' in fixture_data:
                existing_playlist_ids = {playlist['playlist_id'] for playlist in combined_data['playlists']}
                for playlist in fixture_data['playlists']:
                    if playlist['playlist_id'] not in existing_playlist_ids:
                        combined_data['playlists'].append(playlist)
                        existing_playlist_ids.add(playlist['playlist_id'])
                    else:
                        print(f"Skipping duplicate playlist: {playlist['playlist_id']}")

            # Handle master playlist (only one allowed, last one wins)
            if 'master_playlist' in fixture_data:
                if combined_data['master_playlist'] is not None:
                    print(f"Overriding master playlist from {filename}")
                combined_data['master_playlist'] = fixture_data['master_playlist']

            # Combine associations (avoid duplicates)
            if 'track_playlist_associations' in fixture_data:
                existing_associations = {(assoc['track_id'], assoc['playlist_id'])
                                         for assoc in combined_data['track_playlist_associations']}
                for assoc in fixture_data['track_playlist_associations']:
                    key = (assoc['track_id'], assoc['playlist_id'])
                    if key not in existing_associations:
                        combined_data['track_playlist_associations'].append(assoc)
                        existing_associations.add(key)
                    else:
                        print(f"Skipping duplicate association: {assoc['track_id']} -> {assoc['playlist_id']}")

        # Store the combined data for validation later
        self._initial_state = combined_data

        # Set up the database
        self._setup_database(combined_data)

        # Print summary
        print(f"Database setup complete:")
        print(f"  - {len(combined_data['tracks'])} tracks loaded")
        print(f"  - {len(combined_data['playlists'])} playlists loaded")
        if combined_data['master_playlist']:
            print(f"  - Master playlist: {combined_data['master_playlist']['name']}")
        print(f"  - {len(combined_data['track_playlist_associations'])} associations loaded")

    def _setup_database(self, combined_data):
        """Set up database with the combined fixture data."""
        with UnitOfWork() as uow:
            # Clear existing data in correct order (foreign keys first)
            uow.track_playlist_repository.delete_all()
            uow.track_repository.delete_all()
            uow.playlist_repository.delete_all()

            # Add master playlist first if it exists
            if combined_data['master_playlist']:
                playlist_data = combined_data['master_playlist']
                master_playlist = Playlist(
                    playlist_id=playlist_data['playlist_id'],
                    name=playlist_data['name'],
                    master_sync_snapshot_id=playlist_data.get('master_sync_snapshot_id', ''),
                    associations_snapshot_id=playlist_data.get('associations_snapshot_id', '')
                )
                uow.playlist_repository.insert(master_playlist)

            # Add other playlists
            for playlist_data in combined_data['playlists']:
                playlist = Playlist(
                    playlist_id=playlist_data['playlist_id'],
                    name=playlist_data['name'],
                    master_sync_snapshot_id=playlist_data.get('master_sync_snapshot_id', ''),
                    associations_snapshot_id=playlist_data.get('associations_snapshot_id', '')
                )
                uow.playlist_repository.insert(playlist)

            # Add tracks
            for track_data in combined_data['tracks']:
                track = Track(
                    track_id=track_data['track_id'],
                    title=track_data['title'],
                    artists=track_data['artists'],
                    album=track_data['album'],
                    added_to_master=self._parse_datetime(track_data.get('added_to_master')),
                    is_local=track_data.get('is_local', False)
                )
                uow.track_repository.insert(track)

            # Add track-playlist associations
            for assoc in combined_data['track_playlist_associations']:
                uow.track_playlist_repository.insert(
                    assoc['track_id'],
                    assoc['playlist_id']
                )

    def get_spotify_api_mock_data(self, fixture_name='master_playlist_api_response.json'):
        """Get mock Spotify API data in the format expected by fetch_master_tracks."""
        fixture_data = self.load_fixture('spotify_responses', fixture_name)

        # List of tuples: (track_id, track_title, artists, album, added_at)
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
                self._validate_tracks_state(uow, expected, self._initial_state)

            if 'expected_final_playlists' in expected:
                self._validate_playlists_state(uow, expected, self._initial_state)

            if 'expected_final_associations' in expected:
                self._validate_associations_state(uow, expected, self._initial_state)

    @staticmethod
    def _validate_tracks_state(uow, expected, initial_state):
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

        # MANDATORY validation of deleted tracks - fail if key is missing
        assert 'expected_deleted_tracks' in expected, \
            "Test fixture must include 'expected_deleted_tracks' key for proper deletion validation"

        # Get initial track IDs to determine what should have been deleted
        assert initial_state is not None, \
            "Initial state must be provided to validate deletions properly"

        initial_track_ids = {track['track_id'] for track in initial_state['tracks']}

        # Calculate which tracks were actually deleted
        actually_deleted_tracks = initial_track_ids - actual_track_ids
        expected_deleted_tracks_set = set(expected['expected_deleted_tracks'])

        # Validate that expected deletions match actual deletions exactly
        assert actually_deleted_tracks == expected_deleted_tracks_set, \
            f"Deletion mismatch. Actually deleted: {actually_deleted_tracks}, " \
            f"Expected to be deleted: {expected_deleted_tracks_set}, " \
            f"Missing from expected: {actually_deleted_tracks - expected_deleted_tracks_set}, " \
            f"Extra in expected: {expected_deleted_tracks_set - actually_deleted_tracks}"

        # Validate deleted tracks are gone (redundant but explicit)
        for deleted_id in expected['expected_deleted_tracks']:
            assert deleted_id not in actual_track_ids, \
                f"Track {deleted_id} should have been deleted but was found in database"

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

    @staticmethod
    def _validate_playlists_state(uow, expected, initial_state):
        """Validate playlist data in database."""
        all_playlists = uow.playlist_repository.get_all()
        actual_playlist_ids = {p.playlist_id for p in all_playlists}
        expected_playlist_ids = {p['playlist_id'] for p in expected['expected_final_playlists']}

        assert len(all_playlists) == len(expected['expected_final_playlists']), \
            f"Expected {len(expected['expected_final_playlists'])} playlists, got {len(all_playlists)}"

        assert actual_playlist_ids == expected_playlist_ids, \
            f"Playlist IDs don't match. Expected: {expected_playlist_ids}, Got: {actual_playlist_ids}"

        # MANDATORY validation of deleted playlists using initial state
        if initial_state is not None:
            # Get initial playlist IDs from combined initial state
            initial_playlist_ids = set()
            if initial_state.get('master_playlist'):
                initial_playlist_ids.add(initial_state['master_playlist']['playlist_id'])
            if initial_state.get('playlists'):
                initial_playlist_ids.update(p['playlist_id'] for p in initial_state['playlists'])

            # Calculate which playlists were actually deleted
            actually_deleted_playlists = initial_playlist_ids - actual_playlist_ids

            # Validate deletions if expected_deleted_playlists is provided
            if 'expected_deleted_playlists' in expected:
                expected_deleted_playlists_set = set(expected['expected_deleted_playlists'])

                assert actually_deleted_playlists == expected_deleted_playlists_set, \
                    f"Playlist deletion mismatch. Actually deleted: {actually_deleted_playlists}, " \
                    f"Expected to be deleted: {expected_deleted_playlists_set}, " \
                    f"Missing from expected: {actually_deleted_playlists - expected_deleted_playlists_set}, " \
                    f"Extra in expected: {expected_deleted_playlists_set - actually_deleted_playlists}"

                # Validate deleted playlists are gone (redundant but explicit)
                for deleted_id in expected['expected_deleted_playlists']:
                    assert deleted_id not in actual_playlist_ids, \
                        f"Playlist {deleted_id} should have been deleted but was found in database"

        # Validate playlist details
        expected_by_id = {p['playlist_id']: p for p in expected['expected_final_playlists']}

        for playlist in all_playlists:
            expected_playlist = expected_by_id[playlist.playlist_id]
            assert playlist.name == expected_playlist['name']
            assert playlist.master_sync_snapshot_id == expected_playlist['master_sync_snapshot_id']
            if 'associations_snapshot_id' in expected_playlist:
                assert playlist.associations_snapshot_id == expected_playlist['associations_snapshot_id']

    @staticmethod
    def _validate_associations_state(uow, expected, initial_state):
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

        # ENHANCED validation using initial state
        if initial_state is not None:
            # Get initial associations
            initial_associations = set()
            for assoc in initial_state.get('track_playlist_associations', []):
                initial_associations.add((assoc['track_id'], assoc['playlist_id']))

            # Calculate what changed
            actually_added_associations = actual_set - initial_associations
            actually_removed_associations = initial_associations - actual_set

            # Validate association changes if provided in expected results
            if 'expected_added_associations' in expected:
                expected_added_set = {(a['track_id'], a['playlist_id']) for a in
                                      expected['expected_added_associations']}
                assert actually_added_associations == expected_added_set, \
                    f"Association additions mismatch. Actually added: {actually_added_associations}, " \
                    f"Expected to be added: {expected_added_set}"

            if 'expected_removed_associations' in expected:
                expected_removed_set = {(a['track_id'], a['playlist_id']) for a in
                                        expected['expected_removed_associations']}
                assert actually_removed_associations == expected_removed_set, \
                    f"Association removals mismatch. Actually removed: {actually_removed_associations}, " \
                    f"Expected to be removed: {expected_removed_set}"

            # Log changes for debugging (if any)
            if actually_added_associations or actually_removed_associations:
                print(f"Association changes detected:")
                print(f"  Added: {actually_added_associations}")
                print(f"  Removed: {actually_removed_associations}")

    @staticmethod
    def _parse_datetime(date_string):
        """Parse datetime string from fixture."""
        if not date_string:
            return None
        # Handle both ISO formats
        if date_string.endswith('Z'):
            date_string = date_string.replace('Z', '+00:00')
        return datetime.fromisoformat(date_string)
