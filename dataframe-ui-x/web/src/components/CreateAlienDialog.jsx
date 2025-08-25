import React, { useState } from 'react'
import { AlienIcon } from './DataFrameTypeIcons.jsx'

export default function CreateAlienDialog({ isOpen, onClose, onSubmit }) {
  const [formData, setFormData] = useState({
    name: '',
    description: '',
    serverUrl: '',
    projectId: '',
    formId: '',
    username: '',
    password: '',
    syncFrequency: 60 // Default 60 minutes
  })
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')

  const handleInputChange = (e) => {
    const { name, value } = e.target
    setFormData(prev => ({
      ...prev,
      [name]: value
    }))
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setIsSubmitting(true)
    setError('')

    try {
      const odkConfig = {
        server_url: formData.serverUrl,
        project_id: formData.projectId,
        form_id: formData.formId,
        username: formData.username,
        password: formData.password
      }

      await onSubmit({
        name: formData.name,
        description: formData.description,
        odk_config: odkConfig,
        sync_frequency: parseInt(formData.syncFrequency)
      })

      // Reset form
      setFormData({
        name: '',
        description: '',
        serverUrl: '',
        projectId: '',
        formId: '',
        username: '',
        password: '',
        syncFrequency: 60
      })
      onClose()
    } catch (err) {
      setError(err.message || 'Failed to create alien DataFrame')
    } finally {
      setIsSubmitting(false)
    }
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-800 rounded-lg p-5 w-full max-w-md mx-4 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center gap-3 mb-3">
          <AlienIcon className="w-6 h-6" />
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Create Alien DataFrame</h2>
        </div>

        {error && (
          <div className="mb-3 p-2 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
            <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              DataFrame Name *
            </label>
            <input
              type="text"
              name="name"
              value={formData.name}
              onChange={handleInputChange}
              required
              className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
              placeholder="my_alien_dataframe"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Description
            </label>
            <textarea
              name="description"
              value={formData.description}
              onChange={handleInputChange}
              rows={1}
              className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
              placeholder="Data from ODK Central survey..."
            />
          </div>

          <div className="border-t dark:border-gray-600 pt-3">
            <h3 className="font-medium text-gray-900 dark:text-white mb-2">ODK Central Configuration</h3>
            
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Server URL *
              </label>
              <input
                type="url"
                name="serverUrl"
                value={formData.serverUrl}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
                placeholder="https://central.example.com"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Project ID *
              </label>
              <input
                type="text"
                name="projectId"
                value={formData.projectId}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
                placeholder="1"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Form ID *
              </label>
              <input
                type="text"
                name="formId"
                value={formData.formId}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
                placeholder="survey_form"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Username *
              </label>
              <input
                type="text"
                name="username"
                value={formData.username}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
                placeholder="ODK Central username"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Password *
              </label>
              <input
                type="password"
                name="password"
                value={formData.password}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
                placeholder="ODK Central password"
              />
            </div>
          </div>

          <div className="border-t dark:border-gray-600 pt-3">
            <h3 className="font-medium text-gray-900 dark:text-white mb-2">Sync Settings</h3>
            
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Sync Frequency (minutes)
              </label>
              <select
                name="syncFrequency"
                value={formData.syncFrequency}
                onChange={handleInputChange}
                className="w-full px-3 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
              >
                <option value={15}>Every 15 minutes</option>
                <option value={60}>Every hour</option>
                <option value={360}>Every 6 hours</option>
                <option value={1440}>Every 24 hours</option>
              </select>
            </div>
          </div>

          <div className="flex gap-3 pt-3">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
              disabled={isSubmitting}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="flex-1 px-4 py-2 bg-blue-600 dark:bg-blue-500 text-white rounded-md hover:bg-blue-700 dark:hover:bg-blue-600 disabled:opacity-50 transition-colors"
            >
              {isSubmitting ? 'Creating...' : 'Create DataFrame'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}