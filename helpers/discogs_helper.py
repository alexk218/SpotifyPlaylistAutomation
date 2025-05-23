import os

import requests
import time
from typing import List, Dict, Optional, Tuple
from utils.logger import setup_logger

from dotenv import load_dotenv

load_dotenv()
discogs_logger = setup_logger('discogs_helper', 'sql', 'discogs_helper.log')

DISCOGS_API_TOKEN = os.environ['DISCOGS_API_TOKEN']


class DiscogsClient:
    def __init__(self,
                 user_agent="SpotifyPlaylistAutomation/1.0 +https://github.com/alexk218/SpotifyPlaylistAutomation",
                 api_token=None):
        self.base_url = "https://api.discogs.com"
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'application/vnd.discogs.v2.plain+json'
        }
        if api_token:
            self.headers['Authorization'] = f'Discogs token={api_token}'
            discogs_logger.info("Using Discogs API token for authentication")
        else:
            discogs_logger.warning("No API token provided - using unauthenticated requests")

        self.rate_limit_delay = 1.0 if api_token else 1.5  # Faster with token

    def search_releases(self, artist: str, title: str) -> List[Dict]:
        """
        Search Discogs for releases by artist and title.

        Args:
            artist: Artist name
            title: Track title

        Returns:
            List of release data with track information
        """
        # Clean search terms
        clean_artist = self._clean_search_term(artist)
        clean_title = self._clean_search_term(title)

        search_query = f"{clean_artist} {clean_title}"

        try:
            discogs_logger.info(f"Searching Discogs for: {search_query}")

            url = f"{self.base_url}/database/search"
            params = {
                'q': search_query,
                'type': 'release',
                'per_page': 25  # Reduce to 25 to be more conservative
            }

            discogs_logger.info(f"Making request to: {url} with params: {params}")
            discogs_logger.info(f"Headers: {self.headers}")

            response = requests.get(url, headers=self.headers, params=params, timeout=30)

            # Log the response details for debugging
            discogs_logger.info(f"Response status: {response.status_code}")
            discogs_logger.info(f"Response headers: {dict(response.headers)}")

            if response.status_code == 401:
                discogs_logger.error("401 Unauthorized - Check User-Agent or consider using API token")
                discogs_logger.error(f"Response text: {response.text}")
                return []

            if response.status_code == 429:
                discogs_logger.warning("Rate limited - waiting longer before retry")
                time.sleep(5)  # Wait 5 seconds for rate limit
                return []

            response.raise_for_status()

            # Rate limiting
            time.sleep(self.rate_limit_delay)

            search_results = response.json()
            releases_with_tracks = []

            # Get detailed track info for each release
            for result in search_results.get('results', []):
                if self._is_relevant_result(result, clean_artist, clean_title):
                    release_details = self._get_release_details(result['id'])
                    if release_details:
                        track_matches = self._find_matching_tracks(
                            release_details, clean_artist, clean_title
                        )
                        if track_matches:
                            releases_with_tracks.extend(track_matches)

            discogs_logger.info(f"Found {len(releases_with_tracks)} relevant tracks")
            return releases_with_tracks

        except Exception as e:
            discogs_logger.error(f"Error searching Discogs for '{search_query}': {e}")
            return []

    def _clean_search_term(self, term: str) -> str:
        """Clean search terms for better matching."""
        import re

        # Remove common patterns that interfere with search
        term = re.sub(r'\(feat\..*?\)', '', term, flags=re.IGNORECASE)
        term = re.sub(r'\(ft\..*?\)', '', term, flags=re.IGNORECASE)
        term = re.sub(r'\(featuring.*?\)', '', term, flags=re.IGNORECASE)
        term = re.sub(r'\(original mix\)', '', term, flags=re.IGNORECASE)
        term = re.sub(r'\(radio edit\)', '', term, flags=re.IGNORECASE)
        term = re.sub(r'\(edit\)', '', term, flags=re.IGNORECASE)
        term = re.sub(r'\[.*?\]', '', term)

        # Clean up extra spaces
        term = ' '.join(term.split())

        return term.strip()

    def _is_relevant_result(self, result: Dict, artist: str, title: str) -> bool:
        """Check if search result is relevant to our query."""
        result_title = result.get('title', '').lower()

        # Basic relevance check
        artist_words = artist.lower().split()
        title_words = title.lower().split()

        artist_match = any(word in result_title for word in artist_words if len(word) > 2)
        title_match = any(word in result_title for word in title_words if len(word) > 2)

        return artist_match and title_match

    def _get_release_details(self, release_id: int) -> Optional[Dict]:
        """Get detailed information about a specific release."""
        try:
            url = f"{self.base_url}/releases/{release_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            time.sleep(self.rate_limit_delay)

            return response.json()

        except Exception as e:
            discogs_logger.error(f"Error getting release details for ID {release_id}: {e}")
            return None

    def _find_matching_tracks(self, release: Dict, artist: str, title: str) -> List[Dict]:
        """Find tracks in a release that match our search criteria."""
        matches = []

        tracklist = release.get('tracklist', [])
        for track in tracklist:
            track_title = track.get('title', '')
            track_duration = track.get('duration', '')

            # Check if this track matches our search
            if self._tracks_match(track_title, title):
                duration_seconds = self._parse_duration(track_duration)

                match_info = {
                    'artist': artist,  # Use original artist name
                    'title': track_title,
                    'duration_seconds': duration_seconds,
                    'duration_formatted': track_duration,
                    'release_title': release.get('title', ''),
                    'release_year': release.get('year'),
                    'formats': [f.get('name') for f in release.get('formats', [])],
                    'labels': [l.get('name') for l in release.get('labels', [])],
                    'discogs_url': release.get('uri', ''),
                    'mix_type': self._identify_mix_type(track_title)
                }

                matches.append(match_info)

        return matches

    def _tracks_match(self, discogs_title: str, search_title: str) -> bool:
        """Check if Discogs track title matches our search title."""
        import difflib

        # Normalize both titles
        norm_discogs = self._clean_search_term(discogs_title.lower())
        norm_search = self._clean_search_term(search_title.lower())

        # Use similarity ratio
        similarity = difflib.SequenceMatcher(None, norm_discogs, norm_search).ratio()

        return similarity > 0.7

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string (MM:SS) to seconds."""
        if not duration_str:
            return 0

        try:
            parts = duration_str.split(':')
            if len(parts) == 2:
                minutes, seconds = map(int, parts)
                return minutes * 60 + seconds
            elif len(parts) == 3:
                hours, minutes, seconds = map(int, parts)
                return hours * 3600 + minutes * 60 + seconds
        except (ValueError, IndexError):
            pass

        return 0

    def _identify_mix_type(self, title: str) -> str:
        """Identify the type of mix from the title."""
        title_lower = title.lower()

        if 'extended' in title_lower:
            return 'Extended Mix'
        elif 'club mix' in title_lower:
            return 'Club Mix'
        elif 'original mix' in title_lower:
            return 'Original Mix'
        elif 'radio edit' in title_lower or 'edit' in title_lower:
            return 'Radio Edit'
        elif 'remix' in title_lower:
            return 'Remix'
        elif 'vocal' in title_lower:
            return 'Vocal Mix'
        elif 'instrumental' in title_lower:
            return 'Instrumental'
        else:
            return 'Unknown'


def find_extended_versions(artist: str, title: str, current_duration: int, api_token: str = None) -> Tuple[
    List[Dict], bool]:
    """
    Find extended versions of a track using Discogs API.
    """

    client = DiscogsClient(api_token=DISCOGS_API_TOKEN)
    all_versions = client.search_releases(artist, title)

    # Filter for versions longer than current
    extended_versions = []
    for version in all_versions:
        if version['duration_seconds'] > current_duration + 30:  # At least 30 seconds longer
            extended_versions.append(version)

    # Sort by duration (longest first)
    extended_versions.sort(key=lambda x: x['duration_seconds'], reverse=True)

    has_longer = len(extended_versions) > 0

    return extended_versions, has_longer
