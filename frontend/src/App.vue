<template>
  <v-app>
    <v-navigation-drawer permanent>
      <v-list>
        <v-list-item title="Spotify Automation" class="text-h6">
          <template v-slot:prepend>
            <v-avatar color="green" size="32">
              <v-icon icon="mdi-spotify"></v-icon>
            </v-avatar>
          </template>
        </v-list-item>
        
        <v-divider class="my-2"></v-divider>
        
        <v-list-item
          v-for="(item, i) in menuItems"
          :key="i"
          :value="item"
          @click="currentSection = item.value"
          :active="currentSection === item.value"
          color="primary"
        >
          <template v-slot:prepend>
            <v-icon :icon="item.icon"></v-icon>
          </template>
          
          <v-list-item-title v-text="item.title"></v-list-item-title>
        </v-list-item>
      </v-list>
      
      <template v-slot:append>
        <div class="pa-2">
          <v-btn block color="primary" @click="connectToSpotify" :disabled="isSpotifyConnected" :loading="connecting">
            {{ isSpotifyConnected ? 'Connected' : 'Connect to Spotify' }}
          </v-btn>
          <div v-if="isSpotifyConnected && spotifyUser" class="text-caption text-center mt-1">
            {{ spotifyUser.display_name }}
          </div>
        </div>
      </template>
    </v-navigation-drawer>

    <v-main>
      <v-container fluid>
        <component :is="currentComponent"></component>
      </v-container>
    </v-main>
    
    <v-snackbar
      v-model="snackbar.show"
      :color="snackbar.color"
      :timeout="snackbar.timeout"
    >
      {{ snackbar.text }}
      
      <template v-slot:actions>
        <v-btn
          variant="text"
          @click="snackbar.show = false"
        >
          Close
        </v-btn>
      </template>
    </v-snackbar>
  </v-app>
</template>

<script>
import { ref, computed, onMounted, defineAsyncComponent } from 'vue';
import api from './services/api';
import { getCurrentInstance } from 'vue'

// Import components
const SyncView = defineAsyncComponent(() => import('./components/SyncView.vue'));
const FileOperationsView = defineAsyncComponent(() => import('./components/FileOperationsView.vue'));
const ValidationView = defineAsyncComponent(() => import('./components/ValidationView.vue'));
const LogsView = defineAsyncComponent(() => import('./components/LogsView.vue'));

export default {
  name: 'App',
  
  setup() {
    const isSpotifyConnected = ref(false);
    const spotifyUser = ref(null);
    const connecting = ref(false);
    const currentSection = ref('sync');
    const app = getCurrentInstance();
    app.appContext.config.globalProperties.$showSnackbar = (text, color = 'info') => {
      snackbar.value.text = text
      snackbar.value.color = color
      snackbar.value.show = true
    }
    
    const snackbar = ref({
      show: false,
      text: '',
      color: 'info',
      timeout: 3000
    });
    
    const menuItems = [
      { title: 'Sync', icon: 'mdi-sync', value: 'sync' },
      { title: 'File Operations', icon: 'mdi-file-music', value: 'files' },
      { title: 'Validation', icon: 'mdi-check-circle', value: 'validation' },
      { title: 'Logs', icon: 'mdi-text-box', value: 'logs' }
    ];
    
    const currentComponent = computed(() => {
      switch (currentSection.value) {
        case 'sync': return SyncView;
        case 'files': return FileOperationsView;
        case 'validation': return ValidationView;
        case 'logs': return LogsView;
        default: return SyncView;
      }
    });
    
    const showSnackbar = (text, color = 'info') => {
      snackbar.value.text = text;
      snackbar.value.color = color;
      snackbar.value.show = true;
    };
    
    const checkStatus = async () => {
      try {
        const response = await api.getStatus();
        if (response.data.success) {
          isSpotifyConnected.value = response.data.spotify_connected;
          spotifyUser.value = response.data.spotify_user;
        }
      } catch (error) {
        console.error('Error checking status:', error);
      }
    };
    
    const connectToSpotify = async () => {
      if (isSpotifyConnected.value) return;
      
      connecting.value = true;
      try {
        const response = await api.connectSpotify();
        if (response.data.success) {
          isSpotifyConnected.value = true;
          spotifyUser.value = response.data.user;
          showSnackbar(`Connected to Spotify as ${response.data.user.display_name}`, 'success');
        }
      } catch (error) {
        console.error('Error connecting to Spotify:', error);
        showSnackbar('Failed to connect to Spotify', 'error');
      } finally {
        connecting.value = false;
      }
    };
    
    onMounted(() => {
      checkStatus();
      
      // Create a global event bus
      const app = document.querySelector('#app').__vue_app__;
      app.config.globalProperties.$showSnackbar = showSnackbar;
    });
    
    return {
      isSpotifyConnected,
      spotifyUser,
      connecting,
      currentSection,
      menuItems,
      currentComponent,
      snackbar,
      connectToSpotify
    };
  }
}
</script>