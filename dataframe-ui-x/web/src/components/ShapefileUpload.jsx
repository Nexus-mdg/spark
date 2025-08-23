import React, { useState, useCallback } from 'react'
import { uploadShapefile, listShapefiles, deleteShapefile, buildShapefileDownloadUrl } from '../api.js'

const ShapefileUpload = () => {
  const [isDragOver, setIsDragOver] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [shapefiles, setShapefiles] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [uploadForm, setUploadForm] = useState({
    name: '',
    file: null
  })

  // Load shapefiles on component mount
  React.useEffect(() => {
    loadShapefiles()
  }, [])

  const loadShapefiles = async () => {
    try {
      setLoading(true)
      const response = await listShapefiles()
      if (response.success) {
        setShapefiles(response.shapefiles)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleDragOver = useCallback((e) => {
    e.preventDefault()
    setIsDragOver(true)
  }, [])

  const handleDragLeave = useCallback((e) => {
    e.preventDefault()
    setIsDragOver(false)
  }, [])

  const handleDrop = useCallback((e) => {
    e.preventDefault()
    setIsDragOver(false)
    
    const files = Array.from(e.dataTransfer.files)
    if (files.length > 0) {
      const file = files[0]
      handleFileSelect(file)
    }
  }, [])

  const handleFileSelect = (file) => {
    if (!file) return

    // Validate file type
    const validExtensions = ['.zip', '.shp', '.geojson', '.json']
    const fileExtension = file.name.toLowerCase().split('.').pop()
    
    if (!validExtensions.some(ext => ext.includes(fileExtension))) {
      setError('Please select a valid file: ZIP, SHP, GeoJSON, or JSON')
      return
    }

    // Auto-generate name from filename
    const baseName = file.name.replace(/\.[^/.]+$/, '')
    
    setUploadForm({
      file: file,
      name: baseName.replace(/[^a-zA-Z0-9-_]/g, '_')
    })
    setError('')
  }

  const handleFileInputChange = (e) => {
    const file = e.target.files[0]
    handleFileSelect(file)
  }

  const handleUpload = async () => {
    if (!uploadForm.file || !uploadForm.name.trim()) {
      setError('Please select a file and enter a name')
      return
    }

    try {
      setUploading(true)
      setError('')
      const response = await uploadShapefile(uploadForm.file, uploadForm.name.trim())
      if (response.success) {
        setUploadForm({ file: null, name: '' })
        await loadShapefiles()
        // Reset file input
        const fileInput = document.getElementById('shapefile-input')
        if (fileInput) fileInput.value = ''
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setUploading(false)
    }
  }

  const handleDelete = async (name) => {
    if (!confirm(`Are you sure you want to delete shapefile "${name}"?`)) {
      return
    }

    try {
      setLoading(true)
      await deleteShapefile(name)
      await loadShapefiles()
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  return (
    <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
      <h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100 flex items-center">
        <svg className="w-5 h-5 mr-2" fill="currentColor" viewBox="0 0 20 20">
          <path fillRule="evenodd" d="M3 17a1 1 0 011-1h12a1 1 0 011 1v1a1 1 0 01-1 1H4a1 1 0 01-1-1v-1zM3.293 7.707A1 1 0 014 7h12a1 1 0 01.707.293l2 2A1 1 0 0119 10v5a1 1 0 01-1 1v-1a1 1 0 00-1-1H3a1 1 0 00-1 1v1a1 1 0 01-1-1v-5a1 1 0 01.293-.707l2-2z" clipRule="evenodd" />
        </svg>
        Shapefile Management
      </h2>

      {error && (
        <div className="mb-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {/* Upload Section */}
      <div className="mb-6">
        <h3 className="text-md font-medium mb-3 text-gray-900 dark:text-gray-100">
          Upload Shapefile
        </h3>
        
        {/* Drag and Drop Area */}
        <div
          className={`relative border-2 border-dashed rounded-lg p-6 transition-colors ${
            isDragOver
              ? 'border-blue-400 bg-blue-50 dark:bg-blue-900/20'
              : 'border-gray-300 dark:border-gray-600'
          }`}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
        >
          <div className="text-center">
            <svg
              className="mx-auto h-12 w-12 text-gray-400"
              stroke="currentColor"
              fill="none"
              viewBox="0 0 48 48"
            >
              <path
                d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02"
                strokeWidth={2}
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            <div className="mt-4">
              <label htmlFor="shapefile-input" className="cursor-pointer">
                <span className="mt-2 block text-sm font-medium text-gray-900 dark:text-gray-100">
                  {uploadForm.file ? uploadForm.file.name : 'Drop files here or click to upload'}
                </span>
                <span className="mt-1 block text-xs text-gray-500 dark:text-gray-400">
                  Supports ZIP, SHP, GeoJSON, JSON files
                </span>
              </label>
              <input
                id="shapefile-input"
                name="shapefile-input"
                type="file"
                className="sr-only"
                accept=".zip,.shp,.geojson,.json"
                onChange={handleFileInputChange}
              />
            </div>
          </div>
        </div>

        {/* Upload Form */}
        {uploadForm.file && (
          <div className="mt-4 space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Shapefile Name
              </label>
              <input
                type="text"
                value={uploadForm.name}
                onChange={(e) => setUploadForm({ ...uploadForm, name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                placeholder="Enter a name for this shapefile"
              />
            </div>
            
            <div className="flex space-x-3">
              <button
                onClick={handleUpload}
                disabled={uploading || !uploadForm.name.trim()}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md font-medium disabled:opacity-50 flex items-center"
              >
                {uploading ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                    Uploading...
                  </>
                ) : (
                  'Upload Shapefile'
                )}
              </button>
              <button
                onClick={() => {
                  setUploadForm({ file: null, name: '' })
                  const fileInput = document.getElementById('shapefile-input')
                  if (fileInput) fileInput.value = ''
                }}
                className="px-4 py-2 bg-gray-500 hover:bg-gray-600 text-white rounded-md font-medium"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Shapefiles List */}
      <div>
        <h3 className="text-md font-medium mb-3 text-gray-900 dark:text-gray-100">
          Uploaded Shapefiles
        </h3>
        
        {loading && !uploading && (
          <div className="text-center py-4">
            <div className="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
            <span className="ml-2">Loading...</span>
          </div>
        )}

        {shapefiles.length === 0 && !loading ? (
          <div className="text-center py-8 text-gray-500 dark:text-gray-400">
            <svg className="w-12 h-12 mx-auto mb-4 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
            </svg>
            <p>No shapefiles uploaded yet.</p>
            <p className="text-sm">Upload a shapefile to get started!</p>
          </div>
        ) : (
          <div className="space-y-3">
            {shapefiles.map((shapefile) => (
              <div
                key={shapefile.name}
                className="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">
                      {shapefile.name}
                    </h4>
                    <div className="text-sm text-gray-500 dark:text-gray-400 mt-1 space-y-1">
                      <p>Features: {shapefile.feature_count} | Size: {formatFileSize(shapefile.file_size)}</p>
                      <p>CRS: {shapefile.crs}</p>
                      <p>Uploaded: {new Date(shapefile.uploaded_at).toLocaleDateString()}</p>
                    </div>
                  </div>
                  <div className="flex space-x-2 ml-4">
                    <a
                      href={buildShapefileDownloadUrl(shapefile.name)}
                      download
                      className="text-green-600 hover:text-green-800 text-sm font-medium"
                    >
                      Download
                    </a>
                    <button
                      onClick={() => handleDelete(shapefile.name)}
                      className="text-red-600 hover:text-red-800 text-sm font-medium"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  )
}

export default ShapefileUpload