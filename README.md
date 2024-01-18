# SpotifyPlaylistAutomation

**Overview**
SpotifyPlaylistAutomation is a Python-based tool designed to streamline and automate the process of managing and downloading tracks from Spotify playlists. It uses the Spotify API to authenticate, fetch playlist details, track changes in playlists, and notify users of updates by email. This tool is intended for music enthusiasts/DJs who regularly update and manage Spotify playlists, allowing users to keep track of the changes made to their playlists.

**Features**
Authentication with Spotify API
Fetches all private playlists
Tracks changes in playlists (new additions)
Sends email notifications on playlist updates
Automated and scheduled checks for playlist changes

**Future Features**
**Automatic Detection of Unsorted Liked Songs:** Will sift through your Liked Songs on Spotify and detect tracks that haven't been added to any of your playlists yet, ensuring that every no songs are left unsorted.
**Automated Download of New Playlist Additions:** Newly added songs will be automatically downloaded using Deemix, and sorted according to your Spotify playlists.
**Integration with RekordBox:** Newly downloaded songs from your Spotify playlists will be directly dragged into RekordBox.

# Getting Started
**Prerequisites**
spotipy (A lightweight Python library for the Spotify Web API)
Access to Spotify API (Client ID and Client Secret)
A mail server setup for sending notifications (e.g., Gmail)

**Setup**
Register your application on the Spotify Developer Dashboard to get your Client ID and Client Secret.
Set up environment variables for Spotify credentials and email configuration. Create a .env file in the root directory with the following contents:
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SENDER_EMAIL=your_email_address
EMAIL_PASSWORD=your_email_password
