import React, { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { getProfile } from './api.js'
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
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [profile, setProfile] = useState(null)

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

  const numericCharts = useMemo(() => {
    if (!profile) return []
    return (profile.numeric_distributions || []).slice(0, 4)
  }, [profile])

  const categoricalCharts = useMemo(() => {
    if (!profile) return []
    return (profile.categorical_distributions || []).slice(0, 4)
  }, [profile])

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-slate-900 text-white">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link to="/" className="inline-flex items-center px-3 py-1.5 rounded-md bg-slate-800 hover:bg-slate-700" title="Back to home">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" className="h-5 w-5" fill="currentColor"><path d="M10.828 11H20a1 1 0 110 2h-9.172l3.536 3.536a1 1 0 01-1.415 1.415l-5.243-5.243a1.25 1.25 0 010-1.768l5.243-5.243a1 1 0 111.415 1.414L10.828 11z"/></svg>
            </Link>
            <h1 className="text-lg font-semibold">Analysis: {name}</h1>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        {loading && <div>Loading analysis…</div>}
        {error && <div className="text-red-600">{error}</div>}

        {profile && (
          <>
            <section className="bg-white rounded-lg shadow p-5">
              <h2 className="text-base font-semibold mb-3">Overview</h2>
              <div className="text-sm text-gray-700 flex flex-wrap gap-x-6 gap-y-2">
                <span><span className="font-medium">Rows:</span> {profile.row_count}</span>
                <span><span className="font-medium">Cols:</span> {profile.col_count}</span>
                {profile.metadata?.description && (<span><span className="font-medium">Description:</span> {profile.metadata.description}</span>)}
              </div>
            </section>
            <section className="bg-white rounded-lg shadow p-5">
              <h2 className="text-base font-semibold mb-3">Columns</h2>
              <div className="overflow-auto">
                <table className="min-w-full text-xs">
                  <thead className="text-left text-gray-600 border-b">
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
                  <tbody className="divide-y">
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
              <section className="bg-white rounded-lg shadow p-5">
                <h2 className="text-base font-semibold mb-3">Numeric distributions</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {numericCharts.map((c) => (
                    <div key={c.name} className="p-3 border rounded">
                      <div className="text-sm font-medium mb-2">{c.name}</div>
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
              <section className="bg-white rounded-lg shadow p-5">
                <h2 className="text-base font-semibold mb-3">Top categorical values</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {categoricalCharts.map((c) => (
                    <div key={c.name} className="p-3 border rounded">
                      <div className="text-sm font-medium mb-2">{c.name}</div>
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
