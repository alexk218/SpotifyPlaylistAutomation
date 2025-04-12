<template>
  <div>
    <h1 class="text-h4 mb-4">File Operations</h1>
    
    <v-row>
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Directory Information</v-card-title>
          <v-card-text v-if="directories">
            <v-list-item v-for="(dir, key) in directories" :key="key">
              <template v-slot:prepend>
                <v-icon icon="mdi-folder"></v-icon>
              </template>
              <v-list-item-title>{{ formatDirName(key) }}</v-list-item-title>
              <v-list-item-subtitle class="text-truncate">{{ dir }}</v-list-item-subtitle>
            </v-list-item>
          </v-card-text>
          <v-card-text v-else>
            <v-progress-circular indeterminate></v-progress-circular>
            <span class="ml-2">Loading directory information...</span>
          </v-card-text>
        </v-card>
        
        <v-card class="mt-4">
          <v-card-title>M3U Playlist Generation</v-card-title>
          <v-card-text>
            <v-checkbox v-model="m3uOptions.extended" label="Use Extended M3U Format"></v-checkbox>
            <v-checkbox v-model="m3uOptions.overwrite" label="Overwrite Existing Files"></v-checkbox>
            <v-checkbox v-model="m3uOptions.allPlaylists" label="Process All Playlists"></v-checkbox>
            
            <v-btn color="primary" block @click="generateM3u" :loading="loading.generateM3u">
              Generate M3U Playlists
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
      
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Metadata Operations</v-card-title>
          <v-card-text>
            <v-checkbox v-model="metadataOptions.interactive" label="Interactive Fuzzy Matching"></v-checkbox>
            
            <v-btn color="primary" block class="mb-2" @click="embedMetadata" :loading="loading.embedMetadata">
              Embed TrackId Metadata
            </v-btn>
            
            <v-btn color="info" block class="mb-2" @click="countTrackIds" :loading="loading.countTrackIds">
              Count Files With TrackId
            </v-btn>
            
            <v-btn color="error" block @click="confirmRemoveTrackIds" :loading="loading.removeTrackIds">
              Remove All TrackIds
            </v-btn>
          </v-card-text>
        </v-card>
        
        <v-card class="mt-4">
          <v-card-title>File Cleanup</v-card-title>
          <v-card-text>
            <p class="text-caption mb-2">
              This will move unwanted files from the master tracks directory to the quarantine directory.
            </p>
            
            <v-btn color="warning" block @click="confirmCleanupTracks" :loading="loading.cleanupTracks">
              Clean Up Unwanted Files
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
    
    <v-row v-if="lastOperation.type">
      <v-col cols="12">
        <v-card :color="lastOperation.success ? 'success' : 'error'" dark>
          <v-card-title>Last Operation: {{ lastOperation.type }}</v-card-title>
          <v-card-text>
            <div v-if="lastOperation.success">
              <div v-if="lastOperation.stats">
                <div v-for="(value, key) in lastOperation.stats" :key="key">
                  {{ formatStatName(key) }}: {{ value }}
                </div>
              </div>
              <p v-else>Operation completed successfully.</p>
            </div>
            <div v-else>
              Error: {{ lastOperation.error }}
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
    
    <v-dialog v-model="dialog.show" max-width="500">
      <v-card>
        <v-card-title>{{ dialog.title }}</v-card-title>
        <v-card-text>{{ dialog.text }}</v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn color="grey darken-1" text @click="dialog.show = false">Cancel</v-btn>
          <v-btn :color="dialog.color || 'error'" @click="confirmAction">Confirm</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import api from '../services/api';

export default {
  name: 'FileOperationsView',
  setup() {
    const directories = ref(null);
    const m3uOptions = ref({
      extended: true,
      overwrite: true,
      allPlaylists: false
    });
    
    const metadataOptions = ref({
      interactive: false
    });
    
    const loading = ref({
      generateM3u: false,
      embedMetadata: false,
      countTrackIds: false,
      removeTrackIds: false,
      cleanupTracks: false
    });
    
    const lastOperation = ref({
      type: '',
      success: false,
      stats: null,
      error: ''
    });
    
    const dialog = ref({
      show: false,
      title: '',
      text: '',
      color: '',
      action: null
    });
    
    const showSnackbar = (text, color) => {
      const app = document.querySelector('#app').__vue_app__;
      if (app.config.globalProperties.$showSnackbar) {
        app.config.globalProperties.$showSnackbar(text, color);
      }
    };
    
    const getStatus = async () => {
      try {
        const response = await api.getStatus();
        if (response.data.success) {
          directories.value = response.data.directories;
        }
      } catch (error) {
        showSnackbar('Error fetching directory information', 'error');
      }
    };
    
    const formatDirName = (key) => {
      return key
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
    };
    
    const formatStatName = (key) => {
      return key
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
    };
    
    const generateM3u = async () => {
      loading.value.generateM3u = true;
      try {
        await api.generateM3u({
          extended: m3uOptions.value.extended,
          overwrite: m3uOptions.value.overwrite,
          allPlaylists: m3uOptions.value.allPlaylists
        });
        lastOperation.value = {
          type: 'Generate M3U Playlists',
          success: true
        };
        showSnackbar('M3U playlists generated successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Generate M3U Playlists',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to generate M3U playlists', 'error');
      } finally {
        loading.value.generateM3u = false;
      }
    };
    
    const embedMetadata = async () => {
      loading.value.embedMetadata = true;
      try {
        const response = await api.embedMetadata({
          interactive: metadataOptions.value.interactive
        });
        lastOperation.value = {
          type: 'Embed TrackId Metadata',
          success: true,
          stats: response.data.stats
        };
        showSnackbar('Metadata embedded successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Embed TrackId Metadata',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to embed metadata', 'error');
      } finally {
        loading.value.embedMetadata = false;
      }
    };
    
    const countTrackIds = async () => {
      loading.value.countTrackIds = true;
      try {
        const response = await api.countTrackIds();
        lastOperation.value = {
          type: 'Count TrackIds',
          success: true,
          stats: response.data.stats
        };
        showSnackbar('Track IDs counted successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Count TrackIds',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to count track IDs', 'error');
      } finally {
        loading.value.countTrackIds = false;
      }
    };
    
    const confirmRemoveTrackIds = () => {
      dialog.value = {
        show: true,
        title: 'Remove All TrackIds',
        text: 'Are you sure you want to remove TrackIds from all MP3 files? This action cannot be undone.',
        color: 'error',
        action: removeTrackIds
      };
    };
    
    const removeTrackIds = async () => {
      dialog.value.show = false;
      loading.value.removeTrackIds = true;
      try {
        const response = await api.removeTrackIds();
        lastOperation.value = {
          type: 'Remove TrackIds',
          success: true,
          stats: response.data.stats
        };
        showSnackbar('TrackIds removed successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Remove TrackIds',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to remove TrackIds', 'error');
      } finally {
        loading.value.removeTrackIds = false;
      }
    };
    
    const confirmCleanupTracks = () => {
      dialog.value = {
        show: true,
        title: 'Clean Up Unwanted Files',
        text: 'This will move unwanted files from the master tracks directory to the quarantine directory. Continue?',
        color: 'warning',
        action: cleanupTracks
      };
    };
    
    const cleanupTracks = async () => {
      dialog.value.show = false;
      loading.value.cleanupTracks = true;
      try {
        await api.cleanupTracks();
        lastOperation.value = {
          type: 'Cleanup Tracks',
          success: true
        };
        showSnackbar('Files cleaned up successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Cleanup Tracks',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to clean up files', 'error');
      } finally {
        loading.value.cleanupTracks = false;
      }
    };
    
    const confirmAction = () => {
      if (dialog.value.action) {
        dialog.value.action();
      }
    };
    
    onMounted(() => {
      getStatus();
    });
    
    return {
      directories,
      m3uOptions,
      metadataOptions,
      loading,
      lastOperation,
      dialog,
      formatDirName,
      formatStatName,
      generateM3u,
      embedMetadata,
      countTrackIds,
      confirmRemoveTrackIds,
      confirmCleanupTracks,
      confirmAction
    };
  }
}
</script>