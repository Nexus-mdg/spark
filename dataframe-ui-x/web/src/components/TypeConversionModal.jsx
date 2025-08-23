import React, { useState } from 'react'
import { StaticIcon, EphemeralIcon, TemporaryIcon } from './DataFrameTypeIcons.jsx'

function TypeConversionModal({ 
  open, 
  dataframe, 
  onConvert, 
  onCancel, 
  converting = false 
}) {
  const [selectedType, setSelectedType] = useState(dataframe?.type || 'static')
  const [autoDeleteHours, setAutoDeleteHours] = useState(dataframe?.auto_delete_hours || 10)
  const [showConfirmation, setShowConfirmation] = useState(false)

  if (!open || !dataframe) return null

  const formatTimeRemaining = (expiresAt) => {
    if (!expiresAt) return null
    const now = new Date()
    const expiry = new Date(expiresAt)
    const diffMs = expiry.getTime() - now.getTime()
    
    if (diffMs <= 0) return 'Expired'
    
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60))
    const diffMinutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60))
    
    if (diffHours > 0) {
      return `${diffHours}h ${diffMinutes}m`
    } else {
      return `${diffMinutes}m`
    }
  }

  const getNewExpiration = (type, hours) => {
    const now = new Date()
    switch (type) {
      case 'static':
        return null
      case 'temporary':
        return new Date(now.getTime() + 3600000) // 1 hour
      case 'ephemeral':
        return new Date(now.getTime() + hours * 3600000)
      default:
        return null
    }
  }

  const isDestructiveChange = () => {
    const current = dataframe.type || 'static'
    if (current === 'static') return false // Static to anything is not destructive
    
    if (current === 'ephemeral' && selectedType === 'temporary') {
      // Check if current expiration is more than 1 hour away
      if (dataframe.expires_at) {
        const currentExpiry = new Date(dataframe.expires_at)
        const oneHourFromNow = new Date(Date.now() + 3600000)
        return currentExpiry > oneHourFromNow
      }
    }
    
    if (current === 'ephemeral' && selectedType === 'ephemeral') {
      // Check if new hours is less than current remaining time
      const currentHours = dataframe.auto_delete_hours || 10
      return autoDeleteHours < currentHours
    }
    
    return false
  }

  const handleConvert = () => {
    if (isDestructiveChange() && !showConfirmation) {
      setShowConfirmation(true)
      return
    }

    const payload = { type: selectedType }
    if (selectedType === 'ephemeral') {
      payload.auto_delete_hours = autoDeleteHours
    }
    
    onConvert(dataframe.name, payload)
  }

  const handleCancel = () => {
    setShowConfirmation(false)
    onCancel()
  }

  const currentExpiry = dataframe.expires_at ? new Date(dataframe.expires_at) : null
  const newExpiry = getNewExpiration(selectedType, autoDeleteHours)

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50" onClick={handleCancel}>
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-lg" onClick={(e) => e.stopPropagation()}>
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Convert DataFrame Type
          </h4>
          <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
            Change the lifecycle policy for "{dataframe.name}"
          </p>
        </div>

        <div className="px-6 py-4 space-y-4">
          {/* Current Type */}
          <div className="p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
            <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Current Type</h5>
            <div className="flex items-center gap-3">
              <span className="text-lg">
                {dataframe.type === 'static' && <StaticIcon />}
                {dataframe.type === 'ephemeral' && <EphemeralIcon />}
                {dataframe.type === 'temporary' && <TemporaryIcon />}
              </span>
              <div>
                <span className="font-medium capitalize">{dataframe.type || 'static'}</span>
                {currentExpiry && (
                  <div className="text-xs text-gray-600 dark:text-gray-400">
                    Expires in {formatTimeRemaining(dataframe.expires_at)} ({currentExpiry.toLocaleString()})
                  </div>
                )}
                {!currentExpiry && dataframe.type === 'static' && (
                  <div className="text-xs text-gray-600 dark:text-gray-400">Never expires</div>
                )}
              </div>
            </div>
          </div>

          {/* Type Selection */}
          <div>
            <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">Convert To</h5>
            <div className="space-y-3">
              {['static', 'ephemeral', 'temporary'].map((type) => (
                <label key={type} className="flex items-start gap-3 p-3 border border-gray-200 dark:border-gray-600 rounded-lg cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700">
                  <input
                    type="radio"
                    name="type"
                    value={type}
                    checked={selectedType === type}
                    onChange={(e) => setSelectedType(e.target.value)}
                    className="mt-1"
                  />
                  <div className="flex items-center gap-2">
                    <span className="text-lg">
                      {type === 'static' && <StaticIcon />}
                      {type === 'ephemeral' && <EphemeralIcon />}
                      {type === 'temporary' && <TemporaryIcon />}
                    </span>
                    <div>
                      <div className="font-medium capitalize">{type}</div>
                      <div className="text-xs text-gray-600 dark:text-gray-400">
                        {type === 'static' && 'Never expires, manually deleted only'}
                        {type === 'temporary' && 'Auto-deletes after 1 hour'}
                        {type === 'ephemeral' && 'Auto-deletes after specified hours'}
                      </div>
                    </div>
                  </div>
                </label>
              ))}
            </div>
          </div>

          {/* Ephemeral Hours Input */}
          {selectedType === 'ephemeral' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Auto-delete after (hours)
              </label>
              <input
                type="number"
                min="1"
                max="168"
                value={autoDeleteHours}
                onChange={(e) => setAutoDeleteHours(Math.max(1, parseInt(e.target.value) || 1))}
                className="block w-full rounded-md border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 dark:focus:border-indigo-400 dark:focus:ring-indigo-400 text-sm px-3 py-2"
              />
            </div>
          )}

          {/* New Expiration Preview */}
          {selectedType !== dataframe.type && (
            <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
              <h5 className="text-sm font-medium text-blue-800 dark:text-blue-200 mb-1">After Conversion</h5>
              <div className="text-sm text-blue-700 dark:text-blue-300">
                {newExpiry ? (
                  <>Expires: {newExpiry.toLocaleString()}</>
                ) : (
                  <>Never expires</>
                )}
              </div>
            </div>
          )}

          {/* Destructive Change Warning */}
          {isDestructiveChange() && !showConfirmation && (
            <div className="p-3 bg-orange-50 dark:bg-orange-900/20 rounded-lg border border-orange-200 dark:border-orange-800">
              <div className="flex">
                <svg className="h-5 w-5 text-orange-400 dark:text-orange-300 mr-2 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                </svg>
                <div>
                  <h6 className="text-sm font-medium text-orange-800 dark:text-orange-200">Shorter expiration time</h6>
                  <p className="text-xs text-orange-700 dark:text-orange-300 mt-1">
                    This conversion will reduce the time before the DataFrame expires and is automatically deleted.
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Confirmation Warning */}
          {showConfirmation && (
            <div className="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
              <div className="flex">
                <svg className="h-5 w-5 text-red-400 dark:text-red-300 mr-2 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                </svg>
                <div>
                  <h6 className="text-sm font-medium text-red-800 dark:text-red-200">Confirm type conversion</h6>
                  <p className="text-xs text-red-700 dark:text-red-300 mt-1">
                    Are you sure you want to shorten the expiration time? This action cannot be undone.
                  </p>
                </div>
              </div>
            </div>
          )}
        </div>

        <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-600 flex items-center justify-end gap-3">
          <button
            onClick={handleCancel}
            disabled={converting}
            className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Cancel
          </button>
          <button
            onClick={handleConvert}
            disabled={converting || selectedType === dataframe.type}
            className="px-4 py-2 text-sm font-medium text-white bg-indigo-600 border border-transparent rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {converting ? 'Converting...' : showConfirmation ? 'Confirm Convert' : 'Convert Type'}
          </button>
        </div>
      </div>
    </div>
  )
}

export default TypeConversionModal