<template>
  <div>
    <h1 class="text-h4 mb-4">Validation</h1>
    
    <v-row>
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Track Validation</v-card-title>
          <v-card-text>
            <v-btn color="primary" block class="mb-2" @click="validateTracks" :loading="loading.validateTracks">
              Validate Local Tracks Against Database
            </v-btn>
            
            <v-btn color="primary" block class="mb-2" @click="validateSongLengths" :loading="loading.validateLengths">
              Validate Song Lengths
            </v-btn>
            
            <v-divider class="my-4"></v-divider>
            
            <div class="text-caption">
              Minimum song length to validate against (in minutes):
            </div>
            <v-slider
              v-model="minLengthMinutes"
              min="2"
              max="10"
              step="1"
              thumb-label
              ticks
            ></v-slider>
          </v-card-text>
        </v-card>
      </v-col>
      
      <v-col cols="12" md="6">
        <v-card v-if="validationResults">
          <v-card-title>Validation Results</v-card-title>
          <v-card-text>
            <v-list density="compact">
              <v-list-item v-for="(value, key) in validationResults" :key="key">
                <v-list-item-title>{{ formatLabel(key) }}</v-list-item-title>
                <v-list-item-subtitle>{{ value }}</v-list-item-subtitle>
              </v-list-item>
              
              <v-list-item v-if="validationResults.total_files && validationResults.short_songs !== undefined">
                <v-list-item-title>Short Songs Percentage</v-list-item-title>
                <v-list-item-subtitle>
                  {{ calculatePercentage(validationResults.short_songs, validationResults.total_files) }}%
                </v-list-item-subtitle>
              </v-list-item>
              
              <v-list-item v-if="validationResults.files_with_valid_trackid && validationResults.total_local_files">
                <v-list-item-title>Files With TrackId Percentage</v-list-item-title>
                <v-list-item-subtitle>
                  {{ calculatePercentage(validationResults.files_with_valid_trackid, validationResults.total_local_files) }}%
                </v-list-item-subtitle>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
    
    <v-row v-if="validationResults">
      <v-col cols="12">
        <v-card class="mt-4">
          <v-card-title class="d-flex align-center">
            Validation Summary
            <v-spacer></v-spacer>
            <v-btn icon @click="validateAll" :loading="loading.validateAll">
              <v-icon>mdi-refresh</v-icon>
            </v-btn>
          </v-card-title>
          <v-card-text>
            <v-chip
              class="ma-1"
              :color="getMissingDownloadsColor()"
              label
            >
              Missing Downloads: {{ validationResults.missing_downloads || '0' }}
            </v-chip>
            
            <v-chip
              class="ma-1"
              :color="getFilesWithoutTrackIdColor()"
              label
            >
              Files Without TrackId: {{ validationResults.files_without_trackid || '0' }}
            </v-chip>
            
            <v-chip
              class="ma-1"
              :color="getUnmatchedFilesColor()"
              label
            >
              Unmatched Files: {{ validationResults.unmatched_files || '0' }}
            </v-chip>
            
            <v-chip
              v-if="validationResults.short_songs !== undefined"
              class="ma-1"
              :color="getShortSongsColor()"
              label
            >
              Short Songs: {{ validationResults.short_songs || '0' }}
            </v-chip>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script>
import { ref, computed } from 'vue';
import api from '../services/api';

export default {
  name: 'ValidationView',
  setup() {
    const loading = ref({
      validateTracks: false,
      validateLengths: false,
      validateAll: false
    });
    
    const validationResults = ref(null);
    const minLengthMinutes = ref(5);
    
    const showSnackbar = (text, color) => {
      const app = document.querySelector('#app').__vue_app__;
      if (app.config.globalProperties.$showSnackbar) {
        app.config.globalProperties.$showSnackbar(text, color);
      }
    };
    
    const formatLabel = (key) => {
      return key
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
    };
    
    const calculatePercentage = (part, total) => {
      if (!total) return 0;
      return Math.round((part / total) * 100);
    };
    
    const validateTracks = async () => {
      loading.value.validateTracks = true;
      try {
        const response = await api.validateTracks();
        validationResults.value = response.data.results;
        showSnackbar('Track validation completed', 'success');
      } catch (error) {
        showSnackbar('Failed to validate tracks', 'error');
      } finally {
        loading.value.validateTracks = false;
      }
    };
    
    const validateSongLengths = async () => {
      loading.value.validateLengths = true;
      try {
        const response = await api.validateSongLengths(minLengthMinutes.value);
        
        if (validationResults.value) {
          // Merge with existing results
          validationResults.value = {
            ...validationResults.value,
            ...response.data.results
          };
        } else {
          validationResults.value = response.data.results;
        }
        
        showSnackbar('Song length validation completed', 'success');
      } catch (error) {
        showSnackbar('Failed to validate song lengths', 'error');
      } finally {
        loading.value.validateLengths = false;
      }
    };

    const validateAll = async () => {
      loading.value.validateAll = true;
      try {
        // Run both validations
        const tracksResponse = await api.validateTracks();
        const lengthsResponse = await api.validateSongLengths(minLengthMinutes.value);
        
        // Merge results
        validationResults.value = {
          ...tracksResponse.data.results,
          ...lengthsResponse.data.results
        };
        
        showSnackbar('All validations completed', 'success');
      } catch (error) {
        showSnackbar('Failed to run all validations', 'error');
      } finally {
        loading.value.validateAll = false;
      }
    };
    
    const getMissingDownloadsColor = () => {
      if (!validationResults.value || !validationResults.value.missing_downloads) return 'success';
      return validationResults.value.missing_downloads > 0 ? 'warning' : 'success';
    };
    
    const getFilesWithoutTrackIdColor = () => {
      if (!validationResults.value || !validationResults.value.files_without_trackid) return 'success';
      return validationResults.value.files_without_trackid > 10 ? 'error' : 
             validationResults.value.files_without_trackid > 0 ? 'warning' : 'success';
    };
    
    const getUnmatchedFilesColor = () => {
      if (!validationResults.value || !validationResults.value.unmatched_files) return 'success';
      return validationResults.value.unmatched_files > 0 ? 'error' : 'success';
    };
    
    const getShortSongsColor = () => {
      if (!validationResults.value || validationResults.value.short_songs === undefined) return 'success';
      return validationResults.value.short_songs > 10 ? 'error' : 
             validationResults.value.short_songs > 0 ? 'warning' : 'success';
    };
    
    return {
      loading,
      validationResults,
      minLengthMinutes,
      formatLabel,
      calculatePercentage,
      validateTracks,
      validateSongLengths,
      validateAll,
      getMissingDownloadsColor,
      getFilesWithoutTrackIdColor,
      getUnmatchedFilesColor,
      getShortSongsColor
    };
  }
}
</script>