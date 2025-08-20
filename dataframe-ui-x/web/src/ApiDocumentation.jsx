import React from 'react'
import Header from './Header.jsx'
import Footer from './components/Footer.jsx'

export default function ApiDocumentation() {
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header title="API Documentation" />
      
      <main className="flex-1 max-w-6xl mx-auto px-4 py-8 space-y-6">
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100 mb-6">
            API Documentation
          </h1>
          
          <div className="prose prose-gray dark:prose-invert max-w-none">
            <p className="text-gray-700 dark:text-gray-300 mb-8">
              The Spark Test Visualizer provides a RESTful API for programmatic access to all data processing capabilities.
              All endpoints return JSON responses and require authentication.
            </p>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Base URL</h2>
            <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg font-mono text-sm mb-6">
              http://localhost:8080/api
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Authentication</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-6">
              All API requests require a valid session token. Authenticate first using the login endpoint.
            </p>

            <div className="space-y-8">
              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Authentication</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /auth/login</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Authenticate and get session token</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "username": "your_username",
  "password": "your_password"
}`}</pre>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /auth/logout</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Invalidate current session</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">DataFrames</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">List all cached dataframes</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /dataframes/upload</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Upload a new dataframe</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`Content-Type: multipart/form-data
- file: CSV/Excel/JSON file
- name: DataFrame name (optional)
- description: Description (optional)`}</pre>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get dataframe preview and metadata</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">DELETE /dataframes/{name}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Delete a cached dataframe</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Operations</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /operations/preview</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Preview operation result without applying</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "dataframe_name": "my_data",
  "operation": "filter",
  "params": {
    "filters": [{"column": "age", "op": ">", "value": 18}],
    "combine": "and"
  }
}`}</pre>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /operations/apply</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Apply operation and save result</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Pipelines</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /pipelines</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">List all saved pipelines</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /pipelines</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Save a new pipeline</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "name": "data_cleaning_pipeline",
  "description": "Basic data cleaning workflow",
  "steps": [
    {"op": "load", "params": {"name": "raw_data"}},
    {"op": "filter", "params": {"filters": [...]}},
    {"op": "groupby", "params": {"by": "category", "agg": {...}}}
  ]
}`}</pre>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /pipelines/{name}/run</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Execute a saved pipeline</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">DELETE /pipelines/{name}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Delete a saved pipeline</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Export</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/export/csv</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Download dataframe as CSV</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/export/json</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Download dataframe as JSON</p>
                  </div>
                </div>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4 mt-8">Error Responses</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-4">
              All errors return appropriate HTTP status codes with JSON error details:
            </p>
            <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg text-sm">
              <pre className="text-gray-800 dark:text-gray-200">{`{
  "success": false,
  "error": "Error description",
  "code": "ERROR_CODE"
}`}</pre>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4 mt-8">Rate Limiting</h2>
            <p className="text-gray-700 dark:text-gray-300">
              API requests are limited to 100 requests per minute per authenticated user.
              Rate limit headers are included in all responses.
            </p>
          </div>
        </div>
      </main>
      
      <Footer />
    </div>
  )
}