import React, { useEffect, useMemo, useRef, useState } from 'react'
import {
  getStats,
  listDataframes,
  uploadDataframe,
  clearCache,
  deleteDataframe,
  getDataframe
} from './api.js'

function useToast() {
  const [msg, setMsg] = useState('')
  const [visible, setVisible] = useState(false)
  const timer = useRef(null)

  const show = (text, ms = 2500) => {
    setMsg(text)
    setVisible(true)
    if (timer.current) clearTimeout(timer.current)
    timer.current = setTimeout(() => setVisible(false), ms)
  }

  return { msg, visible, show }
}

function Modal({ open, title, onClose, children }) {
  if (!open) return null
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50" onClick={onClose}>
      <div className="bg-white rounded-lg shadow-xl w-full max-w-6xl max-h-[85vh] flex flex-col" onClick={(e) => e.stopPropagation()}>
        <div className="px-5 py-3 border-b flex items-center justify-between">
          <h3 className="text-base font-semibold">{title}</h3>
          <button onClick={onClose} className="p-2 rounded hover:bg-gray-100">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd"/></svg>
          </button>
        </div>
        <div className="overflow-auto px-5 py-4">{children}</div>
      </div>
    </div>
  )
}

export default function App() {
  const [stats, setStats] = useState({ dataframe_count: 0, total_size_mb: 0 })
  const [rows, setRows] = useState([])
  const [loadingList, setLoadingList] = useState(false)

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [file, setFile] = useState(null)
  const [uploading, setUploading] = useState(false)

  const [viewerOpen, setViewerOpen] = useState(false)
  const [viewerTitle, setViewerTitle] = useState('DataFrame Viewer')
  const [viewerMeta, setViewerMeta] = useState(null)
  const [viewerColumns, setViewerColumns] = useState([])
  const [viewerPreview, setViewerPreview] = useState([])

  const toast = useToast()

  const refreshStats = async () => {
    const res = await getStats()
    if (res.success) {
      setStats(res.stats)
    }
  }

  const refreshList = async () => {
    setLoadingList(true)
    try {
      const res = await listDataframes()
      if (res.success) {
        setRows((res.dataframes || []).sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || '')))
      }
    } finally {
      setLoadingList(false)
    }
  }

  useEffect(() => {
    refreshStats()
    refreshList()
  }, [])

  const onDrop = (e) => {
    e.preventDefault()
    if (e.dataTransfer?.files?.length) {
      setFile(e.dataTransfer.files[0])
    }
  }

  const onUpload = async (e) => {
    e.preventDefault()
    if (!file) {
      toast.show('Please choose a file to upload')
      return
    }
    setUploading(true)
    try {
      await uploadDataframe({ file, name: name.trim(), description: description.trim() })
      setName('')
      setDescription('')
      setFile(null)
      toast.show('Upload successful')
      await refreshStats()
      await refreshList()
    } catch (err) {
      toast.show(err.message || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const onClearCache = async () => {
    if (!confirm('Clear all cached DataFrames?')) return
    try {
      await clearCache()
      toast.show('Cache cleared')
      await refreshStats()
      await refreshList()
    } catch (err) {
      toast.show(err.message || 'Failed to clear cache')
    }
  }

  const onDelete = async (n) => {
    if (!confirm(`Delete DataFrame ${n}?`)) return
    try {
      await deleteDataframe(n)
      toast.show('Deleted')
      await refreshStats()
      await refreshList()
    } catch (err) {
      toast.show(err.message || 'Delete failed')
    }
  }

  const openViewer = async (n) => {
    setViewerOpen(true)
    setViewerTitle(`DataFrame: ${n}`)
    setViewerMeta(null)
    setViewerColumns([])
    setViewerPreview([])
    try {
      const res = await getDataframe(n, { preview: true })
      if (res.success) {
        setViewerMeta(res.metadata)
        setViewerColumns(res.columns || [])
        setViewerPreview(res.preview || [])
      }
    } catch (err) {
      toast.show(err.message || 'Failed to load dataframe')
    }
  }

  const uploadDisabled = uploading

  return (
    <div className="bg-gray-50 min-h-screen text-gray-900">
      <header className="bg-slate-900 text-white">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
          <h1 className="text-lg font-semibold">Spark test visualizer (Tailwind + React)</h1>
          <div className="text-sm text-slate-300">
            {stats ? (
              <span>
                {stats.dataframe_count} cached • {stats.total_size_mb} MB
              </span>
            ) : 'Loading stats...'}
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        {/* Stats */}
        <section className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="rounded-lg bg-gradient-to-r from-indigo-500 to-purple-600 text-white p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{stats.dataframe_count}</div>
                <div className="text-white/80">DataFrames Cached</div>
              </div>
              <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 text-white/90" viewBox="0 0 20 20" fill="currentColor"><path d="M2 5a2 2 0 012-2h2a2 2 0 012 2v1H2V5zM2 9h6v2H2V9zm0 4h6v1a2 2 0 01-2 2H4a2 2 0 01-2-2v-1zM10 5a2 2 0 012-2h2a2 2 0 012 2v1h-6V5zm0 4h6v2h-6V9zm0 4h6v1a2 2 0 01-2 2h-2a2 2 0 01-2-2v-1z"/></svg>
            </div>
          </div>
          <div className="rounded-lg bg-gradient-to-r from-teal-500 to-emerald-600 text-white p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{stats.total_size_mb} MB</div>
                <div className="text-white/80">Total Cache Size</div>
              </div>
              <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 text-white/90" viewBox="0 0 20 20" fill="currentColor"><path d="M2 5a2 2 0 012-2h12a2 2 0 012 2v3H2V5zm0 5h16v5a2 2 0 01-2 2H4a2 2 0 01-2-2v-5z"/></svg>
            </div>
          </div>
        </section>

        {/* Upload */}
        <section className="bg-white rounded-lg shadow p-5">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-base font-semibold">Upload DataFrame</h2>
          </div>
          <form onSubmit={onUpload} className="space-y-4">
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <label className="block">
                <span className="block text-sm font-medium text-gray-700">DataFrame Name</span>
                <input value={name} onChange={(e) => setName(e.target.value)} type="text" placeholder="Optional" className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500" />
              </label>
              <label className="block">
                <span className="block text-sm font-medium text-gray-700">Description</span>
                <input value={description} onChange={(e) => setDescription(e.target.value)} type="text" placeholder="Optional" className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500" />
              </label>
            </div>

            <div
              onDragOver={(e) => e.preventDefault()}
              onDrop={onDrop}
              onClick={() => document.getElementById('file-input').click()}
              className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center hover:border-indigo-400 transition cursor-pointer"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="mx-auto h-10 w-10 text-gray-400" viewBox="0 0 20 20" fill="currentColor"><path d="M3 3a2 2 0 00-2 2v3h2V5h12v3h2V5a2 2 0 00-2-2H3z"/><path d="M3 9h14v6a2 2 0 01-2 2H5a2 2 0 01-2-2V9zm7 1a1 1 0 00-1 1v2H8l3 3 3-3h-1v-2a1 1 0 00-1-1h-2z"/></svg>
              <div className="mt-2 text-sm text-gray-600">Drop files here or click to browse</div>
              <div className="text-xs text-gray-500">CSV, Excel (.xlsx, .xls), JSON</div>
              {file && (
                <div className="mt-2 text-xs text-gray-700">
                  Selected: <span className="font-medium">{file.name}</span>
                </div>
              )}
            </div>
            <input id="file-input" className="hidden" type="file" accept=".csv,.xlsx,.xls,.json" onChange={(e) => setFile(e.target.files?.[0] || null)} />

            <button disabled={uploadDisabled} className="inline-flex items-center px-4 py-2 rounded-md bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-2" viewBox="0 0 20 20" fill="currentColor"><path d="M3 3a2 2 0 00-2 2v3h2V5h12v3h2V5a2 2 0 00-2-2H3z"/><path d="M3 9h14v6a2 2 0 01-2 2H5a2 2 0 01-2-2V9zm7 1a1 1 0 00-1 1v2H8l3 3 3-3h-1v-2a1 1 0 00-1-1h-2z"/></svg>
              {uploading ? 'Uploading…' : 'Upload DataFrame'}
            </button>
          </form>
        </section>

        {/* Cached DataFrames */}
        <section className="bg-white rounded-lg shadow">
          <div className="p-5 border-b border-gray-200 flex items-center justify-between">
            <h2 className="text-base font-semibold">Cached DataFrames</h2>
            <button onClick={onClearCache} className="inline-flex items-center px-3 py-1.5 rounded-md border border-red-600 text-red-600 hover:bg-red-50">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-1.5" viewBox="0 0 20 20" fill="currentColor"><path d="M6 8a1 1 0 011 1v7a1 1 0 11-2 0V9a1 1 0 011-1zm4 0a1 1 0 011 1v7a1 1 0 11-2 0V9a1 1 0 011-1zm5-3h-3.5l-1-1h-3l-1 1H3v2h14V5z"/><path d="M5 7h10l-1 10a2 2 0 01-2 2H8a2 2 0 01-2-2L5 7z"/></svg>
              Clear All Cache
            </button>
          </div>
          <div className="p-5 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-left text-gray-600 border-b">
                <tr>
                  <th className="py-2 pr-4">Name</th>
                  <th className="py-2 pr-4">Description</th>
                  <th className="py-2 pr-4">Dimensions</th>
                  <th className="py-2 pr-4">Size</th>
                  <th className="py-2 pr-4">Created</th>
                  <th className="py-2">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {loadingList && (
                  <tr><td className="py-3 text-gray-500" colSpan={6}>Loading…</td></tr>
                )}
                {!loadingList && rows.length === 0 && (
                  <tr><td className="py-3 text-gray-500" colSpan={6}>No cached DataFrames</td></tr>
                )}
                {rows.map((r) => (
                  <tr key={r.name}>
                    <td className="py-2 pr-4 font-medium">{r.name}</td>
                    <td className="py-2 pr-4 text-gray-700">{r.description || '-'}</td>
                    <td className="py-2 pr-4">{r.rows} x {r.cols}</td>
                    <td className="py-2 pr-4">{r.size_mb} MB</td>
                    <td className="py-2 pr-4">{new Date(r.timestamp).toLocaleString()}</td>
                    <td className="py-2">
                      <div className="flex items-center gap-2">
                        <button onClick={() => openViewer(r.name)} className="px-2 py-1 rounded bg-slate-800 text-white hover:bg-slate-700">View</button>
                        <button onClick={() => onDelete(r.name)} className="px-2 py-1 rounded border border-red-600 text-red-600 hover:bg-red-50">Delete</button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>

      {/* Viewer Modal */}
      <Modal open={viewerOpen} title={viewerTitle} onClose={() => setViewerOpen(false)}>
        {viewerMeta && (
          <div className="text-sm text-gray-700 mb-3">
            <div className="flex flex-wrap gap-3">
              <span><span className="font-medium">Rows:</span> {viewerMeta.rows}</span>
              <span><span className="font-medium">Cols:</span> {viewerMeta.cols}</span>
              <span><span className="font-medium">Size:</span> {viewerMeta.size_mb} MB</span>
              <span><span className="font-medium">Created:</span> {new Date(viewerMeta.timestamp).toLocaleString()}</span>
            </div>
            {viewerMeta.description && (
              <div className="text-gray-600 mt-1">{viewerMeta.description}</div>
            )}
          </div>
        )}
        <div className="overflow-auto border rounded">
          <table className="min-w-full text-xs">
            <thead className="sticky top-0 bg-white border-b">
              <tr>
                {viewerColumns.map((c) => (
                  <th key={c} className="px-2 py-1 text-left">{c}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {viewerPreview.map((row, idx) => (
                <tr key={idx}>
                  {viewerColumns.map((c) => (
                    <td key={c} className="px-2 py-1 whitespace-nowrap text-gray-700">{String(row?.[c] ?? '')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Modal>

      {/* Toast */}
      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>
    </div>
  )
}

