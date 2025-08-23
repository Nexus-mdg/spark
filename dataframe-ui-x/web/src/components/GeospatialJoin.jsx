import React, { useState, useEffect } from 'react'
import { listShapefiles, listJoins, createGeospatialJoin, buildJoinDownloadUrl } from '../api.js'

const GeospatialJoin = ({ dataframeName, dataframeColumns = [] }) => {
  const [shapefiles, setShapefiles] = useState([])
  const [joins, setJoins] = useState([])
  const [loading, setLoading] = useState(false)
  const [joining, setJoining] = useState(false)
  const [error, setError] = useState('')
  const [joinForm, setJoinForm] = useState({
    shapefile_name: '',
    join_type: 'inner',
    output_name: '',
    lat_column: '',
    lon_column: '',
    buffer_distance: 0
  })

  useEffect(() => {
    loadData()
  }, [])

  useEffect(() => {
    // Auto-generate output name when shapefile is selected
    if (joinForm.shapefile_name && dataframeName) {
      setJoinForm(prev => ({
        ...prev,
        output_name: `${dataframeName}_${prev.shapefile_name}_join`
      }))
    }
  }, [joinForm.shapefile_name, dataframeName])

  const loadData = async () => {
    try {
      setLoading(true)
      const [shapefilesRes, joinsRes] = await Promise.all([
        listShapefiles(),
        listJoins()
      ])
      
      if (shapefilesRes.success) {
        setShapefiles(shapefilesRes.shapefiles)
      }
      
      if (joinsRes.success) {
        setJoins(joinsRes.joins)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleJoin = async () => {
    if (!joinForm.shapefile_name || !joinForm.lat_column || !joinForm.lon_column) {
      setError('Please select a shapefile and coordinate columns')
      return
    }

    try {
      setJoining(true)
      setError('')
      
      const payload = {
        dataframe_name: dataframeName,
        shapefile_name: joinForm.shapefile_name,
        join_type: joinForm.join_type,
        output_name: joinForm.output_name,
        dataframe_coords: {
          lat: joinForm.lat_column,
          lon: joinForm.lon_column
        },
        buffer_distance: parseFloat(joinForm.buffer_distance) || 0
      }

      const response = await createGeospatialJoin(payload)
      if (response.success) {
        // Reset form
        setJoinForm({
          shapefile_name: '',
          join_type: 'inner',
          output_name: '',
          lat_column: '',
          lon_column: '',
          buffer_distance: 0
        })
        await loadData()
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setJoining(false)
    }
  }

  // Try to auto-detect coordinate columns
  const getCoordinateColumns = () => {
    const latCandidates = dataframeColumns.filter(col => 
      /lat|latitude|y/i.test(col) && !/long|longitude|lng/i.test(col)
    )
    const lonCandidates = dataframeColumns.filter(col => 
      /lon|lng|long|longitude|x/i.test(col) && !/lat|latitude/i.test(col)
    )
    return { latCandidates, lonCandidates }
  }

  const { latCandidates, lonCandidates } = getCoordinateColumns()

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
          <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM4.332 8.027a6.012 6.012 0 011.912-2.706C6.512 5.73 6.974 6 7.5 6A1.5 1.5 0 019 7.5V8a2 2 0 004 0 2 2 0 011.523-1.943A5.977 5.977 0 0116 10c0 .34-.028.675-.083 1H15a2 2 0 00-2 2v2.197A5.973 5.973 0 0110 16v-2a2 2 0 00-2-2 2 2 0 01-2-2 2 2 0 00-1.668-1.973z" clipRule="evenodd" />
        </svg>
        Geospatial Joins
      </h2>

      {error && (
        <div className="mb-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {/* Join Form */}
      <div className="mb-6">
        <h3 className="text-md font-medium mb-3 text-gray-900 dark:text-gray-100">
          Create Geospatial Join
        </h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Shapefile Selection */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Shapefile
            </label>
            <select
              value={joinForm.shapefile_name}
              onChange={(e) => setJoinForm({ ...joinForm, shapefile_name: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            >
              <option value="">Select a shapefile...</option>
              {shapefiles.map((shapefile) => (
                <option key={shapefile.name} value={shapefile.name}>
                  {shapefile.name} ({shapefile.feature_count} features)
                </option>
              ))}
            </select>
          </div>

          {/* Join Type */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Join Type
            </label>
            <select
              value={joinForm.join_type}
              onChange={(e) => setJoinForm({ ...joinForm, join_type: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            >
              <option value="inner">Inner Join</option>
              <option value="left">Left Join</option>
              <option value="right">Right Join</option>
              <option value="outer">Outer Join</option>
            </select>
          </div>

          {/* Latitude Column */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Latitude Column
            </label>
            <select
              value={joinForm.lat_column}
              onChange={(e) => setJoinForm({ ...joinForm, lat_column: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            >
              <option value="">Select latitude column...</option>
              {latCandidates.length > 0 && (
                <optgroup label="Suggested">
                  {latCandidates.map((col) => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </optgroup>
              )}
              <optgroup label="All Columns">
                {dataframeColumns.map((col) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </optgroup>
            </select>
          </div>

          {/* Longitude Column */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Longitude Column
            </label>
            <select
              value={joinForm.lon_column}
              onChange={(e) => setJoinForm({ ...joinForm, lon_column: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            >
              <option value="">Select longitude column...</option>
              {lonCandidates.length > 0 && (
                <optgroup label="Suggested">
                  {lonCandidates.map((col) => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </optgroup>
              )}
              <optgroup label="All Columns">
                {dataframeColumns.map((col) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </optgroup>
            </select>
          </div>

          {/* Output Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Output Name
            </label>
            <input
              type="text"
              value={joinForm.output_name}
              onChange={(e) => setJoinForm({ ...joinForm, output_name: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
              placeholder="Output name for the joined data"
            />
          </div>

          {/* Buffer Distance */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Buffer Distance (degrees)
            </label>
            <input
              type="number"
              step="0.001"
              min="0"
              value={joinForm.buffer_distance}
              onChange={(e) => setJoinForm({ ...joinForm, buffer_distance: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
              placeholder="0"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Optional buffer around points for spatial matching
            </p>
          </div>
        </div>

        <div className="mt-4">
          <button
            onClick={handleJoin}
            disabled={joining || !joinForm.shapefile_name || !joinForm.lat_column || !joinForm.lon_column}
            className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-md font-medium disabled:opacity-50 flex items-center"
          >
            {joining ? (
              <>
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Creating Join...
              </>
            ) : (
              'Create Geospatial Join'
            )}
          </button>
        </div>
      </div>

      {/* Previous Joins */}
      <div>
        <h3 className="text-md font-medium mb-3 text-gray-900 dark:text-gray-100">
          Previous Joins
        </h3>
        
        {loading && !joining && (
          <div className="text-center py-4">
            <div className="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
            <span className="ml-2">Loading...</span>
          </div>
        )}

        {joins.length === 0 && !loading ? (
          <div className="text-center py-8 text-gray-500 dark:text-gray-400">
            <svg className="w-12 h-12 mx-auto mb-4 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            <p>No geospatial joins created yet.</p>
            <p className="text-sm">Create your first join above!</p>
          </div>
        ) : (
          <div className="space-y-3">
            {joins.map((join) => (
              <div
                key={join.name}
                className="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">
                      {join.name}
                    </h4>
                    <div className="text-sm text-gray-500 dark:text-gray-400 mt-1 space-y-1">
                      <p>
                        {join.join_parameters?.dataframe_name} + {join.join_parameters?.shapefile_name} 
                        ({join.join_parameters?.join_type} join)
                      </p>
                      <p>Features: {join.feature_count} | Size: {formatFileSize(join.file_size)}</p>
                      <p>Created: {new Date(join.created_at).toLocaleDateString()}</p>
                    </div>
                  </div>
                  <div className="flex space-x-2 ml-4">
                    <a
                      href={buildJoinDownloadUrl(join.name)}
                      download
                      className="text-green-600 hover:text-green-800 text-sm font-medium"
                    >
                      Download
                    </a>
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

export default GeospatialJoin