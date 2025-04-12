<template>
  <div>
    <h1 class="text-h4 mb-4">Sync Operations</h1>

    <v-row>
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Database Operations</v-card-title>
          <v-card-text>
            <v-checkbox v-model="forceRefresh" label="Force Full Refresh"></v-checkbox>

            <v-btn color="primary" block class="mb-2" @click="syncPlaylists" :loading="loading.syncPlaylists">
              Sync Playlists
            </v-btn>

            <v-btn color="primary" block class="mb-2" @click="syncMasterTracks" :loading="loading.syncTracks">
              Sync Master Tracks
            </v-btn>

            <v-btn color="primary" block class="mb-2" @click="syncAll" :loading="loading.syncAll">
              Sync All
            </v-btn>

            <v-divider class="my-4"></v-divider>

            <v-btn color="error" block @click="confirmClearDatabase" :loading="loading.clearDb">
              Clear Database
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Spotify Sync Operations</v-card-title>
          <v-card-text>
            <p class="text-caption mb-4">
              These operations interact directly with your Spotify account and can make changes to your playlists.
            </p>

            <v-btn color="success" block class="mb-2" @click="syncToMaster" :loading="loading.syncMaster">
              Sync All Tracks to MASTER Playlist
            </v-btn>

            <v-btn color="success" block class="mb-2" @click="syncUnplaylisted" :loading="loading.syncUnplaylisted">
              Sync Unplaylisted Tracks to UNSORTED Playlist
            </v-btn>

            <v-divider class="my-4"></v-divider>

            <v-btn color="warning" block @click="clearCache" :loading="loading.clearCache">
              Clear Spotify API Cache
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
                <div v-if="lastOperation.stats.playlists">
                  <h3>Playlists:</h3>
                  <div>Added: {{ lastOperation.stats.playlists.added }}</div>
                  <div>Updated: {{ lastOperation.stats.playlists.updated }}</div>
                  <div>Unchanged: {{ lastOperation.stats.playlists.unchanged }}</div>
                  <v-divider class="my-2"></v-divider>
                </div>

                <div v-if="lastOperation.stats.tracks">
                  <h3>Tracks:</h3>
                  <div>Added: {{ lastOperation.stats.tracks.added }}</div>
                  <div>Updated: {{ lastOperation.stats.tracks.updated }}</div>
                  <div>Unchanged: {{ lastOperation.stats.tracks.unchanged }}</div>
                </div>

                <div v-if="!lastOperation.stats.playlists && !lastOperation.stats.tracks">
                  <div>Added: {{ lastOperation.stats.added }}</div>
                  <div>Updated: {{ lastOperation.stats.updated }}</div>
                  <div>Unchanged: {{ lastOperation.stats.unchanged }}</div>
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
        <v-card-text>
          {{ dialog.text }}

          <div v-if="dialog.details && dialog.details.to_add && dialog.details.to_add.length > 0" class="mt-3">
            <v-expansion-panels>
              <v-expansion-panel>
                <v-expansion-panel-title>
                  Playlists to Add ({{ dialog.details.to_add.length }})
                </v-expansion-panel-title>
                <v-expansion-panel-text>
                  <v-list dense>
                    <v-list-item v-for="playlist in dialog.details.to_add" :key="playlist.id">
                      <v-list-item-title>{{ playlist.name }}</v-list-item-title>
                    </v-list-item>
                  </v-list>
                </v-expansion-panel-text>
              </v-expansion-panel>
            </v-expansion-panels>
          </div>

          <div v-if="dialog.details && dialog.details.to_update && dialog.details.to_update.length > 0" class="mt-3">
            <v-expansion-panels>
              <v-expansion-panel>
                <v-expansion-panel-title>
                  Playlists to Update ({{ dialog.details.to_update.length }})
                </v-expansion-panel-title>
                <v-expansion-panel-text>
                  <v-list dense>
                    <v-list-item v-for="playlist in dialog.details.to_update" :key="playlist.id">
                      <v-list-item-title>{{ playlist.old_name }} → {{ playlist.name }}</v-list-item-title>
                    </v-list-item>
                  </v-list>
                </v-expansion-panel-text>
              </v-expansion-panel>
            </v-expansion-panels>
          </div>
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn color="grey darken-1" text
            @click="dialog.cancel ? dialog.cancel() : dialog.show = false">Cancel</v-btn>
          <v-btn color="primary" @click="dialog.action">Confirm</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script>
import api from '../services/api';
import { ref, getCurrentInstance } from 'vue'

export default {
  name: 'SyncView',
  setup() {
    const forceRefresh = ref(false);
    const loading = ref({
      syncPlaylists: false,
      syncTracks: false,
      syncAll: false,
      clearDb: false,
      syncMaster: false,
      syncUnplaylisted: false,
      clearCache: false
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
      action: null
    });

    const showSnackbar = (text, color) => {
      const { proxy } = getCurrentInstance()
      if (proxy && proxy.$showSnackbar) {
        proxy.$showSnackbar(text, color)
      } else {
        console.error('Snackbar function not available')
      }
    }

    const syncPlaylists = async () => {
      loading.value.syncPlaylists = true;
      try {
        // First call without confirm to get analysis
        const response = await api.syncPlaylists(forceRefresh.value, false);

        if (response.data.needs_confirmation) {
          // Show confirmation dialog with analysis
          const analysis = response.data.analysis;

          // Show confirmation dialog with details
          const confirmResult = await confirmDialog(
            'Confirm Playlist Sync',
            `Found ${analysis.added} playlists to add and ${analysis.updated} to update.`,
            analysis
          );

          if (!confirmResult) {
            loading.value.syncPlaylists = false;
            return;
          }

          // User confirmed, proceed with sync
          const syncResponse = await api.syncPlaylists(forceRefresh.value, true);
          lastOperation.value = {
            type: 'Sync Playlists',
            success: true,
            stats: syncResponse.data.stats
          };
          showSnackbar('Playlists synced successfully', 'success');
        } else {
          // No confirmation needed (or already handled by backend)
          lastOperation.value = {
            type: 'Sync Playlists',
            success: true,
            stats: response.data.stats
          };
          showSnackbar('Playlists synced successfully', 'success');
        }
      } catch (error) {
        // Handle error...
      } finally {
        loading.value.syncPlaylists = false;
      }
    };

    // Add a confirmation dialog helper
    const confirmDialog = (title, message, details) => {
      return new Promise((resolve) => {
        dialog.value = {
          show: true,
          title: title,
          text: message,
          details: details,
          action: () => {
            dialog.value.show = false;
            resolve(true);
          },
          cancel: () => {
            dialog.value.show = false;
            resolve(false);
          }
        };
      });
    };

    const syncMasterTracks = async () => {
      loading.value.syncTracks = true;
      try {
        // First call without confirm to get analysis
        const response = await api.syncTracks(forceRefresh.value, false);

        if (response.data.needs_confirmation) {
          // Show confirmation dialog with analysis
          const analysis = response.data.analysis;

          // Format tracks for display
          const addedTracks = analysis.added.map(track =>
            `${track.artists} - ${track.title} (${track.album})`
          );

          const confirmMessage = `Found ${addedTracks.length} tracks to add. Would you like to proceed?`;

          // Show confirmation dialog with details
          const confirmResult = await confirmDialog(
            'Confirm Track Sync',
            confirmMessage,
            { added_tracks: addedTracks }
          );

          if (!confirmResult) {
            loading.value.syncTracks = false;
            return;
          }

          // User confirmed, proceed with sync
          const syncResponse = await api.syncTracks(forceRefresh.value, true);
          lastOperation.value = {
            type: 'Sync Master Tracks',
            success: true,
            stats: syncResponse.data.stats
          };
          showSnackbar('Master tracks synced successfully', 'success');
        } else {
          // No confirmation needed (or already handled by backend)
          lastOperation.value = {
            type: 'Sync Master Tracks',
            success: true,
            stats: response.data.stats
          };
          showSnackbar('Master tracks synced successfully', 'success');
        }
      } catch (error) {
        // Error handling
      } finally {
        loading.value.syncTracks = false;
      }
    };

    const syncAll = async () => {
      loading.value.syncAll = true;
      try {
        // First call without confirm to get analysis
        const response = await api.syncAll(forceRefresh.value, false);

        if (response.data.needs_confirmation) {
          // Show confirmation dialog with analysis
          const analysis = response.data.analysis;

          // Format tracks and playlists for display
          const addedTracks = analysis.tracks.added.map(track =>
            `${track.artists} - ${track.title} (${track.album})`
          );

          const confirmMessage = `Found ${analysis.playlists.added} playlists to add, 
        ${analysis.playlists.updated} to update, and 
        ${addedTracks.length} tracks to add. Would you like to proceed?`;

          // Show confirmation dialog with details
          const confirmResult = await confirmDialog(
            'Confirm Full Sync',
            confirmMessage,
            {
              playlists: analysis.playlists,
              tracks: { added_tracks: addedTracks }
            }
          );

          if (!confirmResult) {
            loading.value.syncAll = false;
            return;
          }

          // User confirmed, proceed with sync
          const syncResponse = await api.syncAll(forceRefresh.value, true);
          lastOperation.value = {
            type: 'Sync All',
            success: true,
            stats: syncResponse.data.stats
          };
          showSnackbar('All sync operations completed successfully', 'success');
        } else {
          // No confirmation needed (or already handled by backend)
          lastOperation.value = {
            type: 'Sync All',
            success: true,
            stats: response.data.stats
          };
          showSnackbar('All sync operations completed successfully', 'success');
        }
      } catch (error) {
        // Error handling remains the same
      } finally {
        loading.value.syncAll = false;
      }
    };

    const confirmClearDatabase = () => {
      dialog.value = {
        show: true,
        title: 'Clear Database',
        text: 'Are you sure you want to clear all database tables? This action cannot be undone.',
        action: clearDatabase
      };
    };

    const clearDatabase = async () => {
      dialog.value.show = false;
      loading.value.clearDb = true;
      try {
        await api.clearDatabase();
        lastOperation.value = {
          type: 'Clear Database',
          success: true
        };
        showSnackbar('Database cleared successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Clear Database',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to clear database', 'error');
      } finally {
        loading.value.clearDb = false;
      }
    };

    const syncToMaster = async () => {
      loading.value.syncMaster = true;
      try {
        await api.syncToMaster();
        lastOperation.value = {
          type: 'Sync to MASTER Playlist',
          success: true
        };
        showSnackbar('Tracks synced to MASTER playlist successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Sync to MASTER Playlist',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to sync to MASTER playlist', 'error');
      } finally {
        loading.value.syncMaster = false;
      }
    };

    const syncUnplaylisted = async () => {
      loading.value.syncUnplaylisted = true;
      try {
        await api.syncUnplaylisted();
        lastOperation.value = {
          type: 'Sync Unplaylisted Tracks',
          success: true
        };
        showSnackbar('Unplaylisted tracks synced successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Sync Unplaylisted Tracks',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to sync unplaylisted tracks', 'error');
      } finally {
        loading.value.syncUnplaylisted = false;
      }
    };

    const clearCache = async () => {
      loading.value.clearCache = true;
      try {
        await api.clearCache();
        lastOperation.value = {
          type: 'Clear Cache',
          success: true
        };
        showSnackbar('Spotify API cache cleared successfully', 'success');
      } catch (error) {
        lastOperation.value = {
          type: 'Clear Cache',
          success: false,
          error: error.response?.data?.error || error.message
        };
        showSnackbar('Failed to clear cache', 'error');
      } finally {
        loading.value.clearCache = false;
      }
    };

    const confirmAction = () => {
      if (dialog.value.action) {
        dialog.value.action();
      }
    };

    return {
      forceRefresh,
      loading,
      lastOperation,
      dialog,
      syncPlaylists,
      syncMasterTracks,
      syncAll,
      confirmClearDatabase,
      syncToMaster,
      syncUnplaylisted,
      clearCache,
      confirmAction
    };
  }
}
</script>