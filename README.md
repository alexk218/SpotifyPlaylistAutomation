# Spotify Playlist Automation

A comprehensive Python-based toolkit for managing Spotify playlists, syncing metadata to a local database, and organizing music files for DJ workflows.

## Overview

Spotify Playlist Automation is designed to streamline the management of your music collection across Spotify and local storage. It enables DJs and music enthusiasts to:

1. Keep a local database in sync with your Spotify playlists
2. Organize local music files based on Spotify playlists
3. Generate M3U playlists for DJ software like Rekordbox
4. Validate and manage local audio files with metadata from Spotify

The application can be used via command line or through a modern web interface built with Vue.js and Vuetify.

## Features

### Spotify Sync
- **Playlist Synchronization**: Keep your local database in sync with your Spotify playlists
- **Track Synchronization**: Sync track metadata from your Spotify library
- **Association Synchronization**: Maintain accurate track-to-playlist relationships
- **Master Playlist**: Sync all tracks from your playlists to a designated MASTER playlist
- **Unplaylisted Management**: Find and sync Liked Songs that aren't in any playlist to an UNSORTED playlist

### File Management
- **M3U Generation**: Create M3U playlists that reference your local music files for DJ software
- **Metadata Embedding**: Embed Spotify Track IDs into your local MP3 files
- **Track Validation**: Validate local tracks against Spotify data
- **File Cleanup**: Identify and move unwanted files to a quarantine directory

### Analysis & Validation
- **Track Validation**: Compare local files against your Spotify library
- **Song Length Validation**: Identify songs shorter than a specified duration
- **Track Count**: Statistics on tracks with embedded metadata
- **Duplicate Management**: Tools for managing duplicate tracks in your playlists

## Installation

### Prerequisites
- Python 3.8+
- Spotify Developer Account (for API access)
- SQL Server (for database storage)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/SpotifyPlaylistAutomation.git
cd SpotifyPlaylistAutomation
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with the following:
```
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SERVER_CONNECTION_STRING=your_sql_server
DATABASE_NAME=your_database_name
MASTER_PLAYLIST_ID=your_master_playlist_id
UNSORTED_PLAYLIST_ID=your_unsorted_playlist_id
MASTER_TRACKS_DIRECTORY=path/to/your/music/library
PLAYLISTS_DIRECTORY=path/to/playlists
QUARANTINE_DIRECTORY=path/to/quarantine
M3U_PLAYLISTS_DIRECTORY=path/to/m3u/playlists
```

4. Register your application on the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/) to get your Client ID and Secret.

## Usage

### Command-Line Interface

The application provides a comprehensive CLI for all operations:

```bash
# Database Operations
python -m sql.program --sync-playlists  # Sync playlists
python -m sql.program --sync-tracks     # Sync master tracks
python -m sql.program --sync-associations  # Sync track-playlist associations
python -m sql.program --sync-all        # Sync playlists, tracks, and associations
python -m sql.program --clear-db        # Clear all database tables

# Spotify Operations
python -m sql.program --sync-master     # Sync all tracks to MASTER playlist
python -m sql.program --sync-unplaylisted  # Sync unplaylisted tracks to UNSORTED

# File Operations
python -m sql.program --generate-m3u    # Generate M3U playlists
python -m sql.program --embed-metadata  # Embed TrackId metadata
python -m sql.program --remove-track-ids  # Remove TrackId metadata
python -m sql.program --cleanup-tracks  # Move unwanted files to quarantine

# Validation Operations
python -m sql.program --validate-tracks    # Validate local tracks
python -m sql.program --validate-lengths   # Check song lengths
python -m sql.program --count-track-ids    # Count files with TrackId
python -m sql.program --validate-all       # Run all validations

# Cache Management
python -m sql.program --clear-cache    # Clear Spotify API cache
```

### Web Interface (IN PROGRESS)

The application will also provide a modern web interface built with Vue.js and Vuetify.
Will be available soon...

## Architecture

- **Drivers**: API clients for Spotify, Deezer, and notification services
- **Helpers**: Utility modules for various operations
- **SQL**: Database models, repositories, and core database functionality
- **Cache Manager**: Smart caching of Spotify API responses
- **API**: Flask web server providing RESTful endpoints
- **Frontend**: Vue.js web application with Vuetify components

## Database Schema

The application uses a SQL Server database with the following core tables:

- **Playlists**: Stores playlist metadata (ID, name, description)
- **Tracks**: Stores track metadata (ID, title, artists, album, added date)
- **TrackPlaylists**: Junction table for track-playlist associations
