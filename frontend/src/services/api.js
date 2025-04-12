import axios from 'axios';

const API_URL = process.env.NODE_ENV === 'production'
  ? 'http://localhost:5000/api'  // For production
  : 'http://localhost:5000/api';  // For development

const apiClient = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 10000
});

export default {
  // Status and Connection
  getStatus() {
    return apiClient.get('/status');
  },
  connectSpotify() {
    return apiClient.post('/spotify/connect');
  },

  // Database operations
  clearDatabase() {
    return apiClient.post('/db/clear');
  },

  // Sync operations
  syncPlaylists(forceRefresh = false, confirm = false) {
    return apiClient.post('/sync/playlists', {
      force_refresh: forceRefresh,
      confirm: confirm
    });
  },
  syncTracks(forceRefresh = false, confirm = false) {
    return apiClient.post('/sync/tracks', {
      force_refresh: forceRefresh,
      confirm: confirm
    });
  },
  syncAll(forceRefresh = false, confirm = false) {
    return apiClient.post('/sync/all', {
      force_refresh: forceRefresh,
      confirm: confirm
    });
  },
  syncToMaster() {
    return apiClient.post('/spotify/sync-to-master');
  },
  syncUnplaylisted() {
    return apiClient.post('/spotify/sync-unplaylisted');
  },

  // Cache operations
  clearCache() {
    return apiClient.post('/cache/clear');
  },

  // File operations
  generateM3u(options) {
    return apiClient.post('/files/generate-m3u', options);
  },
  embedMetadata(options) {
    return apiClient.post('/files/embed-metadata', options);
  },
  countTrackIds() {
    return apiClient.get('/files/count-track-ids');
  },
  removeTrackIds() {
    return apiClient.post('/files/remove-track-ids');
  },
  cleanupTracks() {
    return apiClient.post('/files/cleanup-tracks');
  },

  // Validation
  validateTracks() {
    return apiClient.post('/validation/tracks');
  },
  validateSongLengths(minLength = 5) {
    return apiClient.post('/validation/song-lengths', { min_length: minLength });
  },

  // Logs
  getLogFiles() {
    return apiClient.get('/logs/files');
  },
  getLogContent(filename) {
    return apiClient.get(`/logs/content/${filename}`);
  }
};
