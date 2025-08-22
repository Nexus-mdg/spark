import React, { useEffect, useMemo, useState } from 'react'
import { Link, useParams, useNavigate } from 'react-router-dom'
import Header from './Header.jsx'
import Footer from './components/Footer.jsx'
import { getProfile, getDataframe } from './api.js'
import { Bar } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend
} from 'chart.js'

ChartJS.register(BarElement, CategoryScale, LinearScale, Tooltip, Legend)

export default function Analysis() {
  const { name } = useParams()
  const navigate = useNavigate()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [profile, setProfile] = useState(null)
  // New: dataframe preview state
  const [dfLoading, setDfLoading] = useState(false)
  const [dfError, setDfError] = useState('')
  const [dfMeta, setDfMeta] = useState(null)
  const [dfColumns, setDfColumns] = useState([])
  const [dfPreview, setDfPreview] = useState([])

  useEffect(() => {
    let mounted = true
    setLoading(true)
    setError('')
    getProfile(name)
      .then((res) => {
        if (!mounted) return
        if (res.success) setProfile(res)
        else setError(res.error || 'Failed to load profile')
      })
      .catch((e) => setError(e.message || 'Failed'))
      .finally(() => mounted && setLoading(false))
    return () => { mounted = false }
  }, [name])

  // New: load dataframe preview
  useEffect(() => {
    let active = true
    setDfLoading(true)
    setDfError('')
    setDfMeta(null)
    setDfColumns([])
    setDfPreview([])
    getDataframe(name, { preview: true })
      .then((res) => {
        if (!active) return
        if (res.success) {
          setDfMeta(res.metadata)
          setDfColumns(res.columns || [])
          setDfPreview(res.preview || [])
        } else {
          setDfError(res.error || 'Failed to load DataFrame preview')
        }
      })
      .catch((e) => setDfError(e.message || 'Failed to load DataFrame'))
      .finally(() => { if (active) setDfLoading(false) })
    return () => { active = false }
  }, [name])

  const numericCharts = useMemo(() => {
    if (!profile) return []
    return (profile.numeric_distributions || []).slice(0, 4)
  }, [profile])

  const categoricalCharts = useMemo(() => {
    if (!profile) return []
    return (profile.categorical_distributions || []).slice(0, 4)
  }, [profile])

  return (
    <div className="bg-gradient-to-br from-gray-50 via-blue-50 to-indigo-50 dark:from-gray-900 dark:via-gray-800 dark:to-gray-900 min-h-screen text-gray-900 dark:text-gray-100 transition-colors">
      <Header title="Analysis">
        <Link 
          to="/" 
          className="inline-flex items-center px-3 py-1.5 rounded-md bg-slate-800 hover:bg-slate-700 text-sm font-medium text-white transition-colors" 
          title="Back to home"
        >
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" className="h-4 w-4 mr-2" fill="currentColor">
            <path d="M10.828 11H20a1 1 0 110 2h-9.172l3.536 3.536a1 1 0 01-1.415 1.415l-5.243-5.243a1.25 1.25 0 010-1.768l5.243-5.243a1 1 0 111.415 1.414L10.828 11z"/>
          </svg>
          Back to Home
        </Link>
      </Header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        {/* Dataframe name section */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100 break-all">
            📊 {name}
          </h1>
          <p className="text-gray-600 dark:text-gray-400 mt-1">
            Dataframe Analysis Dashboard
          </p>
        </div>
        {loading && <div>Loading analysis…</div>}
        {error && <div className="text-red-600">{error}</div>}

        {profile && (
          <>
            <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
              <h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">Overview</h2>
              <div className="text-sm text-gray-700 dark:text-gray-300 flex flex-wrap gap-x-6 gap-y-2">
                <span><span className="font-medium">Rows:</span> {profile.row_count}</span>
                <span><span className="font-medium">Cols:</span> {profile.col_count}</span>
                {profile.metadata?.description && (<span><span className="font-medium">Description:</span> {profile.metadata.description}</span>)}
              </div>
            </section>

            {/* New: DataFrame preview section */}
            <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
              <h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">DataFrame preview</h2>
              {dfLoading && (<div className="text-sm text-gray-600 dark:text-gray-400">Loading preview…</div>)}
              {dfError && (<div className="text-sm text-red-600 dark:text-red-400">{dfError}</div>)}
              {dfMeta && (
                <div className="text-sm text-gray-700 dark:text-gray-300 mb-3">
                  <div className="flex flex-wrap gap-3">
                    <span><span className="font-medium">Rows:</span> {dfMeta.rows}</span>
                    <span><span className="font-medium">Cols:</span> {dfMeta.cols}</span>
                    <span><span className="font-medium">Size:</span> {dfMeta.size_mb} MB</span>
                    <span><span className="font-medium">Created:</span> {dfMeta.timestamp && !isNaN(new Date(dfMeta.timestamp).getTime()) ? new Date(dfMeta.timestamp).toLocaleString() : '-'}</span>
                  </div>
                  {dfMeta.description && (<div className="text-gray-600 dark:text-gray-400 mt-1">{dfMeta.description}</div>)}
                </div>
              )}
              <div className="overflow-auto border dark:border-gray-600 rounded">
                <table className="min-w-full text-xs">
                  <thead className="sticky top-0 bg-white dark:bg-gray-700 border-b dark:border-gray-600">
                    <tr>
                      {dfColumns.map((c) => (<th key={c} className="px-2 py-1 text-left text-gray-900 dark:text-gray-100">{c}</th>))}
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {dfPreview.map((row, idx) => (
                      <tr key={idx}>
                        {dfColumns.map((c) => (<td key={c} className="px-2 py-1 whitespace-nowrap text-gray-700 dark:text-gray-300">{String(row?.[c] ?? '')}</td>))}
                      </tr>
                    ))}
                    {(!dfLoading && dfPreview.length === 0) && (
                      <tr><td className="px-2 py-2 text-gray-500 dark:text-gray-400" colSpan={Math.max(1, dfColumns.length)}>No preview rows</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
              <h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">Columns</h2>
              <div className="overflow-auto">
                <table className="min-w-full text-xs">
                  <thead className="text-left text-gray-600 dark:text-gray-400 border-b dark:border-gray-600">
                    <tr>
                      <th className="py-2 pr-4">Name</th>
                      <th className="py-2 pr-4">Type</th>
                      <th className="py-2 pr-4">Non-null</th>
                      <th className="py-2 pr-4">Nulls</th>
                      <th className="py-2 pr-4">Unique</th>
                      <th className="py-2 pr-4">Min</th>
                      <th className="py-2 pr-4">P25</th>
                      <th className="py-2 pr-4">Median</th>
                      <th className="py-2 pr-4">P75</th>
                      <th className="py-2 pr-4">Max</th>
                      <th className="py-2 pr-4">Mean</th>
                      <th className="py-2 pr-4">Std</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y dark:divide-gray-600">
                    {profile.summary.map((s) => (
                      <tr key={s.name}>
                        <td className="py-2 pr-4 font-medium">{s.name}</td>
                        <td className="py-2 pr-4">{s.dtype}</td>
                        <td className="py-2 pr-4">{s.nonnull}</td>
                        <td className="py-2 pr-4">{s.nulls}</td>
                        <td className="py-2 pr-4">{s.unique}</td>
                        <td className="py-2 pr-4">{s.min ?? '-'}</td>
                        <td className="py-2 pr-4">{s.p25 ?? '-'}</td>
                        <td className="py-2 pr-4">{s.p50 ?? '-'}</td>
                        <td className="py-2 pr-4">{s.p75 ?? '-'}</td>
                        <td className="py-2 pr-4">{s.max ?? '-'}</td>
                        <td className="py-2 pr-4">{s.mean ?? '-'}</td>
                        <td className="py-2 pr-4">{s.std ?? '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            {numericCharts.length > 0 && (
              <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
                <h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">Numeric distributions</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {numericCharts.map((c) => (
                    <div key={c.name} className="p-3 border dark:border-gray-600 rounded">
                      <div className="text-sm font-medium mb-2 text-gray-900 dark:text-gray-100">{c.name}</div>
                      <Bar
                        data={{
                          labels: c.labels,
                          datasets: [{ label: 'Count', data: c.counts, backgroundColor: 'rgba(99, 102, 241, 0.7)' }]
                        }}
                        options={{
                          responsive: true,
                          plugins: { legend: { display: false } },
                          scales: { x: { ticks: { maxRotation: 45, minRotation: 0 } } }
                        }}
                      />
                    </div>
                  ))}
                </div>
              </section>
            )}

            {categoricalCharts.length > 0 && (
              <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
                <h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">Top categorical values</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {categoricalCharts.map((c) => (
                    <div key={c.name} className="p-3 border dark:border-gray-600 rounded">
                      <div className="text-sm font-medium mb-2 text-gray-900 dark:text-gray-100">{c.name}</div>
                      <Bar
                        data={{
                          labels: c.labels,
                          datasets: [{ label: 'Count', data: c.counts, backgroundColor: 'rgba(16, 185, 129, 0.7)' }]
                        }}
                        options={{
                          responsive: true,
                          plugins: { legend: { display: false } },
                          scales: { x: { ticks: { maxRotation: 45, minRotation: 0 } } }
                        }}
                      />
                    </div>
                  ))}
                </div>
              </section>
            )}
          </>
        )}
      </main>
    </div>
  )
}
