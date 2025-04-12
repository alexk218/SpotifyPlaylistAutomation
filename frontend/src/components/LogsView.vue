<template>
    <div>
      <h1 class="text-h4 mb-4">Logs</h1>
      
      <v-card>
        <v-card-title class="d-flex align-center">
          <div>Log Viewer</div>
          <v-spacer></v-spacer>
          <v-select
            v-model="selectedLogFile"
            :items="logFiles"
            label="Select Log File"
            density="compact"
            variant="outlined"
            hide-details
            class="log-select"
            @update:model-value="fetchLogContent"
          ></v-select>
          <v-btn icon class="ml-2" @click="refreshLogs" :loading="loading.refresh">
            <v-icon>mdi-refresh</v-icon>
          </v-btn>
        </v-card-title>
        
        <v-card-text>
          <div v-if="!logContent && !loading.content" class="text-center pa-4">
            <p>Select a log file to view its contents</p>
          </div>
          
          <div v-if="loading.content" class="text-center pa-4">
            <v-progress-circular indeterminate></v-progress-circular>
            <p class="mt-2">Loading log content...</p>
          </div>
          
          <pre v-if="logContent" class="log-content">{{ logContent }}</pre>
        </v-card-text>
        
        <v-card-actions v-if="logContent">
          <v-spacer></v-spacer>
          <v-btn prepend-icon="mdi-download" @click="downloadLog">
            Download Log
          </v-btn>
        </v-card-actions>
      </v-card>
    </div>
  </template>
  
  <script>
  import { ref, onMounted } from 'vue';
  import api from '../services/api';
  
  export default {
    name: 'LogsView',
    setup() {
      const logFiles = ref([]);
      const selectedLogFile = ref(null);
      const logContent = ref(null);
      const loading = ref({
        files: false,
        content: false,
        refresh: false
      });
      
      const showSnackbar = (text, color) => {
        const app = document.querySelector('#app').__vue_app__;
        if (app.config.globalProperties.$showSnackbar) {
          app.config.globalProperties.$showSnackbar(text, color);
        }
      };
      
      const getLogFiles = async () => {
        loading.value.files = true;
        try {
          const response = await api.getLogFiles();
          logFiles.value = response.data.files;
          if (logFiles.value.length > 0 && !selectedLogFile.value) {
            selectedLogFile.value = logFiles.value[0];
            fetchLogContent();
          }
        } catch (error) {
          showSnackbar('Error fetching log files', 'error');
        } finally {
          loading.value.files = false;
        }
      };
      
      const fetchLogContent = async () => {
        if (!selectedLogFile.value) return;
        
        loading.value.content = true;
        logContent.value = null;
        
        try {
          const response = await api.getLogContent(selectedLogFile.value);
          logContent.value = response.data.content;
        } catch (error) {
          showSnackbar('Error fetching log content', 'error');
        } finally {
          loading.value.content = false;
        }
      };
      
      const refreshLogs = async () => {
        loading.value.refresh = true;
        await getLogFiles();
        if (selectedLogFile.value) {
          await fetchLogContent();
        }
        loading.value.refresh = false;
      };
      
      const downloadLog = () => {
        if (!logContent.value || !selectedLogFile.value) return;
        
        const element = document.createElement('a');
        const file = new Blob([logContent.value], { type: 'text/plain' });
        element.href = URL.createObjectURL(file);
        element.download = selectedLogFile.value;
        document.body.appendChild(element);
        element.click();
        document.body.removeChild(element);
      };
      
      onMounted(() => {
        getLogFiles();
      });
      
      return {
        logFiles,
        selectedLogFile,
        logContent,
        loading,
        fetchLogContent,
        refreshLogs,
        downloadLog
      };
    }
  }
  </script>
  
  <style scoped>
  .log-select {
    max-width: 300px;
  }
  
  .log-content {
    white-space: pre-wrap;
    font-family: monospace;
    font-size: 0.85rem;
    background-color: #f5f5f5;
    padding: 1rem;
    border-radius: 4px;
    max-height: 600px;
    overflow-y: auto;
  }
  </style>