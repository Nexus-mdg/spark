import React, { useState, useEffect } from 'react'
import { listStories, getStory, createStory, deleteStory } from '../api.js'

const StorySection = ({ dataframeName }) => {
  const [stories, setStories] = useState([])
  const [currentStory, setCurrentStory] = useState(null)
  const [isEditing, setIsEditing] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [storyForm, setStoryForm] = useState({
    name: '',
    title: '',
    content: '',
    tags: []
  })

  // Load stories on component mount
  useEffect(() => {
    loadStories()
  }, [])

  const loadStories = async () => {
    try {
      setLoading(true)
      const response = await listStories()
      if (response.success) {
        // Filter stories for this dataframe
        const dataframeStories = response.stories.filter(
          story => story.dataframe_name === dataframeName
        )
        setStories(dataframeStories)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleCreateStory = () => {
    setStoryForm({
      name: `${dataframeName}-story-${Date.now()}`,
      title: `Story for ${dataframeName}`,
      content: `# ${dataframeName} Analysis Story

Describe your insights and findings about this dataset here...

## Key Observations
- 

## Methodology
- 

## Conclusions
- `,
      tags: []
    })
    setCurrentStory(null)
    setIsEditing(true)
  }

  const handleEditStory = async (storyName) => {
    try {
      setLoading(true)
      const response = await getStory(storyName)
      if (response.success) {
        setCurrentStory(response.story)
        setStoryForm({
          name: response.story.name,
          title: response.story.title,
          content: response.story.content,
          tags: response.story.tags || []
        })
        setIsEditing(true)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleSaveStory = async () => {
    try {
      setLoading(true)
      const payload = {
        ...storyForm,
        dataframe_name: dataframeName
      }
      const response = await createStory(payload)
      if (response.success) {
        setIsEditing(false)
        setCurrentStory(response.story)
        await loadStories()
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleDeleteStory = async (storyName) => {
    if (!confirm(`Are you sure you want to delete the story "${storyName}"?`)) {
      return
    }
    try {
      setLoading(true)
      await deleteStory(storyName)
      if (currentStory && currentStory.name === storyName) {
        setCurrentStory(null)
      }
      await loadStories()
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleCancel = () => {
    setIsEditing(false)
    setError('')
  }

  return (
    <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex items-center">
          <svg className="w-5 h-5 mr-2" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M3 4a1 1 0 011-1h12a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1V4zm0 4a1 1 0 011-1h12a1 1 0 011 1v6a1 1 0 01-1 1H4a1 1 0 01-1-1V8z" clipRule="evenodd" />
          </svg>
          Story Mode
        </h2>
        <button
          onClick={handleCreateStory}
          disabled={loading}
          className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
        >
          <svg className="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
          </svg>
          New Story
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {loading && (
        <div className="text-center py-4">
          <div className="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
          <span className="ml-2">Loading...</span>
        </div>
      )}

      {/* Story Editor */}
      {isEditing && (
        <div className="mb-6 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
          <h3 className="text-md font-medium mb-3 text-gray-900 dark:text-gray-100">
            {currentStory ? 'Edit Story' : 'Create New Story'}
          </h3>
          
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Story Name
              </label>
              <input
                type="text"
                value={storyForm.name}
                onChange={(e) => setStoryForm({ ...storyForm, name: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                placeholder="Story name"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Title
              </label>
              <input
                type="text"
                value={storyForm.title}
                onChange={(e) => setStoryForm({ ...storyForm, title: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                placeholder="Story title"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Content (Markdown supported)
              </label>
              <textarea
                value={storyForm.content}
                onChange={(e) => setStoryForm({ ...storyForm, content: e.target.value })}
                rows={10}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white font-mono text-sm"
                placeholder="Write your story content here..."
              />
            </div>

            <div className="flex space-x-3">
              <button
                onClick={handleSaveStory}
                disabled={loading || !storyForm.name.trim()}
                className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-md font-medium disabled:opacity-50"
              >
                Save Story
              </button>
              <button
                onClick={handleCancel}
                className="px-4 py-2 bg-gray-500 hover:bg-gray-600 text-white rounded-md font-medium"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Stories List */}
      {!isEditing && (
        <div>
          {stories.length === 0 ? (
            <div className="text-center py-8 text-gray-500 dark:text-gray-400">
              <svg className="w-12 h-12 mx-auto mb-4 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
              <p>No stories yet for this dataframe.</p>
              <p className="text-sm">Click "New Story" to create your first story!</p>
            </div>
          ) : (
            <div className="space-y-3">
              {stories.map((story) => (
                <div
                  key={story.name}
                  className="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <h4 className="font-medium text-gray-900 dark:text-gray-100">
                        {story.title || story.name}
                      </h4>
                      <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        Updated: {new Date(story.updated_at).toLocaleDateString()}
                      </p>
                    </div>
                    <div className="flex space-x-2 ml-4">
                      <button
                        onClick={() => handleEditStory(story.name)}
                        className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleDeleteStory(story.name)}
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
      )}

      {/* Current Story Display */}
      {currentStory && !isEditing && (
        <div className="mt-6 border-t border-gray-200 dark:border-gray-700 pt-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-md font-medium text-gray-900 dark:text-gray-100">
              {currentStory.title}
            </h3>
            <button
              onClick={() => handleEditStory(currentStory.name)}
              className="text-blue-600 hover:text-blue-800 text-sm font-medium"
            >
              Edit
            </button>
          </div>
          <div className="prose dark:prose-invert max-w-none">
            <pre className="whitespace-pre-wrap text-sm bg-gray-50 dark:bg-gray-700 p-4 rounded-md">
              {currentStory.content}
            </pre>
          </div>
        </div>
      )}
    </section>
  )
}

export default StorySection