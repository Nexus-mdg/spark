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
                <p className="text-gray-700 dark:text-gray-300">Load dataframes, rename, duplicate, and manage cached data</p>
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

            <h2 className="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Tips & Best Practices</h2>
            <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-lg p-4">
              <ul className="list-disc list-inside text-gray-700 dark:text-gray-300 space-y-2">
                <li>Use descriptive names for your dataframes to easily identify them in pipelines</li>
                <li>Preview operations before running them to ensure correct transformations</li>
                <li>Save frequently used pipelines for reuse across different datasets</li>
                <li>Use the dark/light mode toggle for comfortable viewing in different environments</li>
                <li>Export results in your preferred format (CSV, JSON) for external analysis</li>
              </ul>
            </div>
          </div>
        </div>
      </main>
      
      <Footer />
    </div>
  )
}