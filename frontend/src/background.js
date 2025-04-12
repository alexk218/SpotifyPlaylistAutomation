'use strict'

import { app, protocol, BrowserWindow, dialog } from 'electron'
import { createProtocol } from 'vue-cli-plugin-electron-builder/lib'
import installExtension, { VUEJS3_DEVTOOLS } from 'electron-devtools-installer'
import path from 'path'
import { spawn } from 'child_process'
import { fileURLToPath } from 'url'
import fs from 'fs'

const isDevelopment = process.env.NODE_ENV !== 'production'

// Keep a global reference of the window object, if you don't, the window will
// be closed automatically when the JavaScript object is garbage collected.
let win
let pythonProcess = null

// Path to Python interpreter and API script
let pythonExecutable
let apiScript

// Define paths for production and development
if (isDevelopment) {
  // In development, we assume Python and the API script are in their normal locations
  pythonExecutable = 'python' // or 'python3' depending on your system
  apiScript = '../api/server.py' // Relative to current file
} else {
  // In production, the Python executable and API script are packaged with the app
  const __dirname = path.dirname(fileURLToPath(import.meta.url))
  pythonExecutable = path.join(process.resourcesPath, 'python', 'python.exe') // Windows
  apiScript = path.join(process.resourcesPath, 'api', 'server.py')
}

// Scheme must be registered before the app is ready
protocol.registerSchemesAsPrivileged([
  { scheme: 'app', privileges: { secure: true, standard: true } }
])

async function startPythonApi() {
  return new Promise((resolve, reject) => {
    // Set environment variable for production mode
    const env = { ...process.env, PROD_MODE: '1' }
    
    // Start the Python process
    pythonProcess = spawn(pythonExecutable, [apiScript], { env })
    
    // Handle output
    pythonProcess.stdout.on('data', (data) => {
      console.log(`Python API: ${data}`)
      // When we see the server has started, resolve the promise
      if (data.toString().includes('Running on http://127.0.0.1:5000')) {
        resolve()
      }
    })
    
    pythonProcess.stderr.on('data', (data) => {
      console.error(`Python API Error: ${data}`)
    })
    
    pythonProcess.on('close', (code) => {
      console.log(`Python API process exited with code ${code}`)
      pythonProcess = null
      
      if (code !== 0 && !isDevelopment) {
        dialog.showErrorBox(
          'API Error',
          `The Python API process exited unexpectedly with code ${code}. The application may not function correctly.`
        )
      }
    })
    
    // Set a timeout in case the API doesn't start
    setTimeout(() => {
      if (pythonProcess) {
        resolve() // Assume it's running even if we didn't see the confirmation message
      } else {
        reject(new Error('Timeout waiting for Python API to start'))
      }
    }, 5000)
  })
}

async function createWindow() {
  try {
    // Start the Python API
    if (!isDevelopment) {
      await startPythonApi()
    }
    
    // Create the browser window.
    win = new BrowserWindow({
      width: 1200,
      height: 800,
      webPreferences: {
        // Required for Electron 12+
        contextIsolation: !process.env.ELECTRON_NODE_INTEGRATION,
        nodeIntegration: process.env.ELECTRON_NODE_INTEGRATION,
        preload: path.join(__dirname, 'preload.js')
      }
    })
    
    win.setMenuBarVisibility(false)

    if (process.env.WEBPACK_DEV_SERVER_URL) {
      // Load the url of the dev server if in development mode
      await win.loadURL(process.env.WEBPACK_DEV_SERVER_URL)
      if (!process.env.IS_TEST) win.webContents.openDevTools()
    } else {
      createProtocol('app')
      // Load the index.html when not in development
      win.loadURL('app://./index.html')
    }

    win.on('closed', () => {
      win = null
    })
  } catch (error) {
    console.error('Error creating window:', error)
    dialog.showErrorBox(
      'Error Starting Application',
      `An error occurred while starting the application: ${error.message}`
    )
  }
}

// Quit when all windows are closed.
app.on('window-all-closed', () => {
  // On macOS it is common for applications and their menu bar
  // to stay active until the user quits explicitly with Cmd + Q
  if (process.platform !== 'darwin') {
    app.quit()
  }
})

app.on('activate', () => {
  // On macOS it's common to re-create a window in the app when the
  // dock icon is clicked and there are no other windows open.
  if (win === null) {
    createWindow()
  }
})

// This method will be called when Electron has finished
// initialization and is ready to create browser windows.
// Some APIs can only be used after this event occurs.
app.on('ready', async () => {
  if (isDevelopment && !process.env.IS_TEST) {
    // Install Vue Devtools
    try {
      await installExtension(VUEJS3_DEVTOOLS)
    } catch (e) {
      console.error('Vue Devtools failed to install:', e.toString())
    }
  }
  createWindow()
})

// Clean up the Python process when the app is closing
app.on('will-quit', () => {
  if (pythonProcess) {
    // On Windows, we need to kill the process tree
    if (process.platform === 'win32') {
      spawn('taskkill', ['/pid', pythonProcess.pid, '/f', '/t'])
    } else {
      pythonProcess.kill()
    }
  }
})

// Exit cleanly on request from parent process in development mode.
if (isDevelopment) {
  if (process.platform === 'win32') {
    process.on('message', (data) => {
      if (data === 'graceful-exit') {
        app.quit()
      }
    })
  } else {
    process.on('SIGTERM', () => {
      app.quit()
    })
  }
}