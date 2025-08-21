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

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Base URLs</h2>
            <div className="space-y-3 mb-6">
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Main Application UI</div>
                <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg font-mono text-sm">
                  http://localhost:5001
                </div>
              </div>
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">DataFrame API</div>
                <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg font-mono text-sm">
                  http://localhost:4999
                </div>
              </div>
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Notifications Service (ntfy)</div>
                <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg font-mono text-sm">
                  https://localhost:8443
                </div>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Authentication</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-6">
              All API requests require a valid session token. Authenticate first using the login endpoint.
            </p>

            <div className="space-y-8">
              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Authentication (UI Service - Port 5001)</h3>
                
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

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /auth/user</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get current user information</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /auth/change-password</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Change user password</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "current_password": "current_password",
  "new_password": "new_password"
}`}</pre>
                    </div>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">DataFrames (DataFrame API - Port 4999)</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">List all cached dataframes with metadata</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /dataframes/upload</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Upload a new dataframe (CSV, Excel, JSON)</p>
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
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/page/{page}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get paginated dataframe data</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/metadata</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get dataframe metadata (columns, types, stats)</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">DELETE /dataframes/{name}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Delete a cached dataframe</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">DELETE /dataframes</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Clear all cached dataframes</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Operations (DataFrame API - Port 4999)</h3>
                
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
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Apply operation and save result</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "dataframe_name": "my_data",
  "operation": "groupby",
  "params": {
    "by": ["category"],
    "agg": {"sales": "sum", "count": "size"}
  },
  "result_name": "grouped_data"
}`}</pre>
                    </div>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /operations/compare</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Compare two dataframes</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "left_df": "dataframe1",
  "right_df": "dataframe2",
  "compare_type": "differences"
}`}</pre>
                    </div>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Pipelines (DataFrame API - Port 4999)</h3>
                
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
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /pipelines/{name}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get pipeline definition</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /pipelines/preview</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Preview pipeline execution</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "steps": [...],
  "preview_rows": 10
}`}</pre>
                    </div>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /pipelines/run</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Execute pipeline steps</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`{
  "steps": [...],
  "result_name": "pipeline_result"
}`}</pre>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /pipelines/{name}/run</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Execute a saved pipeline</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /pipelines/{name}/export</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Export pipeline as YAML</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /pipelines/import</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-2">Import pipeline from YAML</p>
                    <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-sm">
                      <pre className="text-gray-800 dark:text-gray-200">{`Content-Type: multipart/form-data
- file: YAML file containing pipeline definition`}</pre>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">DELETE /pipelines/{name}</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Delete a saved pipeline</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Export (DataFrame API - Port 4999)</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/export/csv</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Download dataframe as CSV</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/export/json</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Download dataframe as JSON</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/export/excel</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Download dataframe as Excel (.xlsx)</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /dataframes/{name}/export/parquet</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Download dataframe as Parquet</p>
                  </div>
                </div>
              </div>

              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-3">Health & System (DataFrame API - Port 4999)</h3>
                
                <div className="space-y-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /health</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">API health check</p>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /version</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get API version information</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">GET /cache/stats</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Get cache statistics and memory usage</p>
                  </div>

                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">POST /cache/clear</h4>
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Clear all cached data</p>
                  </div>
                </div>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4 mt-8">Spark Services</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-4">
              The platform includes Apache Spark for distributed data processing:
            </p>
            <div className="space-y-3 mb-6">
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Spark Master Web UI</div>
                <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg font-mono text-sm">
                  http://localhost:8081
                </div>
              </div>
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Spark Worker Web UI</div>
                <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg font-mono text-sm">
                  http://localhost:8082
                </div>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4 mt-8">Common Response Formats</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-4">
              Successful responses typically include the following structure:
            </p>
            <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg text-sm mb-6">
              <pre className="text-gray-800 dark:text-gray-200">{`{
  "success": true,
  "data": { ... },
  "message": "Operation completed successfully"
}`}</pre>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Error Responses</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-4">
              All errors return appropriate HTTP status codes with JSON error details:
            </p>
            <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded-lg text-sm mb-6">
              <pre className="text-gray-800 dark:text-gray-200">{`{
  "success": false,
  "error": "Error description",
  "code": "ERROR_CODE"
}`}</pre>
            </div>
            
            <div className="space-y-3 mb-6">
              <div className="text-sm">
                <span className="font-medium text-gray-900 dark:text-gray-100">400 Bad Request:</span>
                <span className="text-gray-700 dark:text-gray-300"> Invalid request parameters or malformed data</span>
              </div>
              <div className="text-sm">
                <span className="font-medium text-gray-900 dark:text-gray-100">401 Unauthorized:</span>
                <span className="text-gray-700 dark:text-gray-300"> Authentication required or invalid session</span>
              </div>
              <div className="text-sm">
                <span className="font-medium text-gray-900 dark:text-gray-100">404 Not Found:</span>
                <span className="text-gray-700 dark:text-gray-300"> Requested resource (dataframe, pipeline) not found</span>
              </div>
              <div className="text-sm">
                <span className="font-medium text-gray-900 dark:text-gray-100">422 Unprocessable Entity:</span>
                <span className="text-gray-700 dark:text-gray-300"> Valid request format but invalid data values</span>
              </div>
              <div className="text-sm">
                <span className="font-medium text-gray-900 dark:text-gray-100">500 Internal Server Error:</span>
                <span className="text-gray-700 dark:text-gray-300"> Server-side processing error</span>
              </div>
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