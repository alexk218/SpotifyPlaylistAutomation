const { defineConfig } = require('@vue/cli-service')
module.exports = defineConfig({
  transpileDependencies: true,
  pluginOptions: {
    electronBuilder: {
      preload: 'src/preload.js',
      builderOptions: {
        appId: 'com.spotify.automation',
        productName: 'Spotify Playlist Automation',
        extraResources: [
          {
            from: '../api',
            to: 'api',
            filter: ['**/*', '!__pycache__/**/*']
          },
          {
            from: '../drivers',
            to: 'drivers',
            filter: ['**/*', '!__pycache__/**/*']
          },
          {
            from: '../helpers',
            to: 'helpers',
            filter: ['**/*', '!__pycache__/**/*']
          },
          {
            from: '../sql',
            to: 'sql',
            filter: ['**/*', '!__pycache__/**/*']
          },
          {
            from: '../utils',
            to: 'utils',
            filter: ['**/*', '!__pycache__/**/*']
          },
          {
            from: '../.env',
            to: '.env'
          },
          {
            from: '../exclusion_config.json',
            to: 'exclusion_config.json'
          }
        ],
        win: {
          target: ['nsis'],
          icon: 'public/icon.ico'
        },
        nsis: {
          oneClick: false,
          allowToChangeInstallationDirectory: true
        }
      }
    }
  }
})