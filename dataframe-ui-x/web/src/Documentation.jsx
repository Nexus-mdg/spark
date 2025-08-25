import React from 'react'
import Header from './Header.jsx'
import Footer from './components/Footer.jsx'

export default function Documentation() {
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header title="Documentation" />
      
      <main className="flex-1 max-w-6xl mx-auto px-4 py-8 space-y-6">
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100 mb-6">
            Spark Test Visualizer Documentation
          </h1>
          
          <div className="prose prose-gray dark:prose-invert max-w-none">
            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Getting Started</h2>
            <p className="text-gray-700 dark:text-gray-300 mb-6">
              Spark Test Visualizer is a powerful web application for analyzing and visualizing Apache Spark DataFrames. 
              This tool provides an intuitive interface for data exploration, transformation, and pipeline creation.
            </p>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Core Features</h2>
            
            <div className="grid md:grid-cols-2 gap-6 mb-8">
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">📊 Data Analysis</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Upload CSV, Excel, or JSON files and instantly get statistical insights, column profiles, and interactive charts.
                </p>
              </div>
              
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">🔧 Operations</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Perform single operations like filtering, grouping, aggregation, and column transformations with real-time previews.
                </p>
              </div>
              
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">🔗 Chained Operations</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Create sequential data transformation pipelines by chaining multiple operations together.
                </p>
              </div>
              
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">🚀 Chained Pipelines</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Build complex data processing workflows with multiple data sources and advanced pipeline management.
                </p>
              </div>
              
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">⚡ Multi-Engine Support</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Choose between Pandas and Spark engines for data processing to optimize performance based on your data size and complexity.
                </p>
              </div>
              
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">📝 Code Export</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Export your data transformation pipelines as R or Python code to reproduce workflows outside the application.
                </p>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Multi-Engine Processing</h2>
            <div className="bg-green-50 dark:bg-green-900/30 border border-green-200 dark:border-green-700 rounded-lg p-4 mb-6">
              <div className="space-y-3">
                <div>
                  <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100">🐼 Pandas Engine</h4>
                  <p className="text-gray-700 dark:text-gray-300">
                    Best for small to medium datasets (&lt; 1GB). Provides fast in-memory processing with familiar Python syntax.
                    Ideal for data exploration, prototyping, and datasets that fit in memory.
                  </p>
                </div>
                <div>
                  <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100">⚡ Spark Engine</h4>
                  <p className="text-gray-700 dark:text-gray-300">
                    Designed for large datasets and distributed computing. Handles datasets larger than memory through 
                    lazy evaluation and automatic optimization. Choose this for production workloads and big data processing.
                  </p>
                </div>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Supported Data Formats</h2>
            <ul className="list-disc list-inside text-gray-700 dark:text-gray-300 space-y-2 mb-6">
              <li><strong>CSV files</strong> - Comma-separated values with automatic delimiter detection</li>
              <li><strong>Excel files</strong> - .xlsx and .xls formats with sheet selection</li>
              <li><strong>JSON files</strong> - Structured JSON data with schema inference</li>
            </ul>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Available Operations</h2>
            <div className="space-y-4 mb-6">
              <div>
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">Data Loading & Management</h3>
                <p className="text-gray-700 dark:text-gray-300">Load dataframes, rename dataframes with descriptions, duplicate, convert types, and manage cached data</p>
              </div>
              
              <div>
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">Filtering & Selection</h3>
                <p className="text-gray-700 dark:text-gray-300">Filter rows based on conditions, select specific columns, sample data</p>
              </div>
              
              <div>
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">Aggregation & Grouping</h3>
                <p className="text-gray-700 dark:text-gray-300">Group by columns, compute aggregations (sum, avg, count, etc.)</p>
              </div>
              
              <div>
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">Joins & Merging</h3>
                <p className="text-gray-700 dark:text-gray-300">Inner, left, right, and outer joins between dataframes</p>
              </div>
              
              <div>
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">Data Transformation</h3>
                <p className="text-gray-700 dark:text-gray-300">Sort, pivot, unpivot, and custom column transformations</p>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Advanced Column Operations</h2>
            <div className="space-y-6 mb-6">
              <div className="bg-yellow-50 dark:bg-yellow-900/30 border border-yellow-200 dark:border-yellow-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">🔧 Mutate - Create/Transform Columns</h3>
                <p className="text-gray-700 dark:text-gray-300 mb-3">
                  Create new columns or transform existing ones using powerful expression syntax. Supports both vector and row-level operations.
                </p>
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">Vector Mode (Column-wise)</h4>
                    <div className="bg-gray-100 dark:bg-gray-700 p-2 rounded text-sm font-mono text-gray-800 dark:text-gray-200">
                      <p>col('price') * col('quantity')</p>
                      <p>np.where(col('age') &gt;= 18, 'adult', 'minor')</p>
                      <p>col('name').str.upper()</p>
                    </div>
                  </div>
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">Row Mode (Row-wise)</h4>
                    <div className="bg-gray-100 dark:bg-gray-700 p-2 rounded text-sm font-mono text-gray-800 dark:text-gray-200">
                      <p>r['price'] * r['quantity']</p>
                      <p>str(r['first']) + ' ' + str(r['last'])</p>
                      <p>'Category: ' + str(r['category'])</p>
                    </div>
                  </div>
                </div>
              </div>

              <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">🏷️ Rename - Column Renaming</h3>
                <p className="text-gray-700 dark:text-gray-300 mb-3">
                  Batch rename multiple columns using a mapping dictionary. Useful for standardizing column names or improving readability.
                </p>
                <div className="bg-gray-100 dark:bg-gray-700 p-2 rounded text-sm font-mono text-gray-800 dark:text-gray-200">
                  <p>{"{ \"id\": \"person_id\", \"name\": \"full_name\", \"dob\": \"birth_date\" }"}</p>
                </div>
              </div>

              <div className="bg-purple-50 dark:bg-purple-900/30 border border-purple-200 dark:border-purple-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">📅 DateTime - Date Processing</h3>
                <p className="text-gray-700 dark:text-gray-300 mb-3">
                  Parse date strings into datetime objects or derive components like year, month, day from existing datetime columns.
                </p>
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">Parse Dates</h4>
                    <p className="text-sm text-gray-600 dark:text-gray-400">Convert string dates to datetime format with automatic or custom format detection.</p>
                  </div>
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-gray-100">Derive Components</h4>
                    <p className="text-sm text-gray-600 dark:text-gray-400">Extract year, month, day, or other date components into separate columns.</p>
                  </div>
                </div>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">DataFrame Management</h2>
            <div className="space-y-4 mb-6">
              <div className="bg-green-50 dark:bg-green-900/30 border border-green-200 dark:border-green-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">📝 Rename DataFrames</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Rename your dataframes and update their descriptions to better organize your work. Useful for creating 
                  meaningful names that describe the data transformation steps or business context.
                </p>
              </div>
              
              <div className="bg-orange-50 dark:bg-orange-900/30 border border-orange-200 dark:border-orange-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">🔄 Type Conversion</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Convert dataframes between different types (e.g., persistent vs ephemeral) with configurable auto-deletion 
                  settings for temporary data management.
                </p>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Code Export & Pipeline Sharing</h2>
            <div className="space-y-4 mb-6">
              <div className="bg-indigo-50 dark:bg-indigo-900/30 border border-indigo-200 dark:border-indigo-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">🐍 Python Code Export</h3>
                <p className="text-gray-700 dark:text-gray-300 mb-3">
                  Export your data transformation pipeline as Python code using pandas. The generated code includes:
                </p>
                <ul className="list-disc list-inside text-sm text-gray-600 dark:text-gray-400 space-y-1">
                  <li>Redis cache integration for data loading</li>
                  <li>Complete pandas operations matching your pipeline</li>
                  <li>Error handling and data validation</li>
                  <li>Ready-to-run Python scripts</li>
                </ul>
              </div>
              
              <div className="bg-cyan-50 dark:bg-cyan-900/30 border border-cyan-200 dark:border-cyan-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">📊 R Code Export</h3>
                <p className="text-gray-700 dark:text-gray-300 mb-3">
                  Export your pipeline as R code using tidyverse/dplyr syntax. The generated code includes:
                </p>
                <ul className="list-disc list-inside text-sm text-gray-600 dark:text-gray-400 space-y-1">
                  <li>dplyr and tidyverse operations</li>
                  <li>Redis integration with R</li>
                  <li>Pipe-based workflow syntax</li>
                  <li>Statistical analysis ready code</li>
                </ul>
              </div>
              
              <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">💾 Pipeline Import/Export</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Save and share complex pipelines using YAML format. Import pipelines created by others or backup 
                  your own workflows for future use.
                </p>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Enhanced Pipeline Features</h2>
            <div className="space-y-4 mb-6">
              <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">🔗 Chained Pipelines</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Connect multiple pipelines together to create complex data processing workflows. Chain the output 
                  of one pipeline as the input to another, enabling modular and reusable data processing components.
                </p>
              </div>
              
              <div className="bg-teal-50 dark:bg-teal-900/30 border border-teal-200 dark:border-teal-700 rounded-lg p-4">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">👁️ Pipeline Preview</h3>
                <p className="text-gray-700 dark:text-gray-300">
                  Preview the results of your pipeline before executing it on the full dataset. See intermediate 
                  results for each step and validate your transformations with a sample of data.
                </p>
              </div>
            </div>

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Tips & Best Practices</h2>
            <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-lg p-4 mb-6">
              <ul className="list-disc list-inside text-gray-700 dark:text-gray-300 space-y-2">
                <li>Use descriptive names for your dataframes to easily identify them in pipelines</li>
                <li>Preview operations before running them to ensure correct transformations</li>
                <li>Choose the appropriate engine (Pandas for smaller data, Spark for large datasets)</li>
                <li>Use vector mode for mutate operations when possible for better performance</li>
                <li>Export complex pipelines as code to reproduce results in your preferred environment</li>
                <li>Save frequently used pipelines for reuse across different datasets</li>
                <li>Use pipeline chaining for modular data processing workflows</li>
                <li>Test datetime parsing with small samples before applying to full datasets</li>
                <li>Use the dark/light mode toggle for comfortable viewing in different environments</li>
                <li>Export results in your preferred format (CSV, JSON) for external analysis</li>
              </ul>
            </div>
            
            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">What's New</h2>
            <div className="bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg p-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-3">Recent Feature Additions</h3>
              <div className="space-y-2 text-gray-700 dark:text-gray-300">
                <div className="flex items-start space-x-2">
                  <span className="text-emerald-500 dark:text-emerald-400">✨</span>
                  <span><strong>Multi-Engine Support:</strong> Choose between Pandas and Spark engines for optimal performance</span>
                </div>
                <div className="flex items-start space-x-2">
                  <span className="text-emerald-500 dark:text-emerald-400">✨</span>
                  <span><strong>Advanced Column Operations:</strong> Mutate, rename, and datetime operations with expression support</span>
                </div>
                <div className="flex items-start space-x-2">
                  <span className="text-emerald-500 dark:text-emerald-400">✨</span>
                  <span><strong>Code Export:</strong> Generate R and Python code from your data transformation pipelines</span>
                </div>
                <div className="flex items-start space-x-2">
                  <span className="text-emerald-500 dark:text-emerald-400">✨</span>
                  <span><strong>Enhanced DataFrame Management:</strong> Rename dataframes and convert between types</span>
                </div>
                <div className="flex items-start space-x-2">
                  <span className="text-emerald-500 dark:text-emerald-400">✨</span>
                  <span><strong>Pipeline Chaining:</strong> Connect multiple pipelines for complex workflows</span>
                </div>
                <div className="flex items-start space-x-2">
                  <span className="text-emerald-500 dark:text-emerald-400">✨</span>
                  <span><strong>Pipeline Preview:</strong> Test pipelines on sample data before full execution</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
      
      <Footer />
    </div>
  )
}