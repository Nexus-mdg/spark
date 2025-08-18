import React, { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  getStats,
  listDataframes,
  uploadDataframe,
  clearCache,
  deleteDataframe,
  getDataframe,
  buildDownloadCsvUrl,
  buildDownloadJsonUrl
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
          <button onClick={onClose} className="p-2 rounded hover:bg-gray-100" aria-label="Close">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd"/></svg>
          </button>
        </div>
        <div className="overflow-auto px-5 py-4">{children}</div>
      </div>
    </div>
  )
}

function ConfirmDialog({ open, title = 'Confirm action', message = 'Are you sure?', confirmText = 'Confirm', cancelText = 'Cancel', confirming = false, onConfirm, onCancel }) {
  if (!open) return null
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50" onClick={onCancel}>
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md" onClick={(e) => e.stopPropagation()}>
        <div className="px-5 py-4 border-b">
          <h4 className="text-base font-semibold">{title}</h4>
        </div>
        <div className="px-5 py-4 text-sm text-gray-700">{message}</div>
        <div className="px-5 py-3 border-t flex items-center justify-end gap-2">
          <button onClick={onCancel} className="px-3 py-1.5 rounded border border-gray-300 hover:bg-gray-50">{cancelText}</button>
          <button disabled={confirming} onClick={onConfirm} className="px-3 py-1.5 rounded bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">{confirming ? 'Working…' : confirmText}</button>
        </div>
      </div>
    </div>
  )
}

export default function Home() {
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

  const [confirmOpen, setConfirmOpen] = useState(false)
  const [confirmTitle, setConfirmTitle] = useState('Confirm action')
  const [confirmMessage, setConfirmMessage] = useState('Are you sure?')
  const [confirming, setConfirming] = useState(false)
  const confirmActionRef = useRef(null)

  const toast = useToast()
  const navigate = useNavigate()

  const refreshStats = async () => {
    const res = await getStats()
    if (res.success) setStats(res.stats)
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

  useEffect(() => { refreshStats(); refreshList() }, [])

  const onDrop = (e) => {
    e.preventDefault()
    if (e.dataTransfer?.files?.length) setFile(e.dataTransfer.files[0])
  }

  const onUpload = async (e) => {
    e.preventDefault()
    if (!file) return toast.show('Please choose a file to upload')
    setUploading(true)
    try {
      await uploadDataframe({ file, name: name.trim(), description: description.trim() })
      setName(''); setDescription(''); setFile(null)
      toast.show('Upload successful')
      await refreshStats(); await refreshList()
    } catch (err) {
      toast.show(err.message || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const openConfirmDialog = ({ title, message, action }) => {
    setConfirmTitle(title); setConfirmMessage(message); confirmActionRef.current = action; setConfirmOpen(true)
  }

  const onConfirmProceed = async () => {
    if (!confirmActionRef.current) return
    setConfirming(true)
    try { await confirmActionRef.current(); setConfirmOpen(false) } finally { setConfirming(false) }
  }

  const onClearCache = () => openConfirmDialog({
    title: 'Clear all cached DataFrames',
    message: 'This will permanently delete all cached DataFrames. Do you want to continue?',
    action: async () => {
      try { await clearCache(); toast.show('Cache cleared'); await refreshStats(); await refreshList() } catch (err) { toast.show(err.message || 'Failed to clear cache') }
    }
  })

  const onDelete = (n) => openConfirmDialog({
    title: `Delete DataFrame “${n}”`,
    message: 'This action cannot be undone. Do you want to delete this DataFrame?',
    action: async () => {
      try { await deleteDataframe(n); toast.show('Deleted'); await refreshStats(); await refreshList() } catch (err) { toast.show(err.message || 'Delete failed') }
    }
  })

  const openViewer = async (n) => {
    setViewerOpen(true); setViewerTitle(`DataFrame: ${n}`); setViewerMeta(null); setViewerColumns([]); setViewerPreview([])
    try {
      const res = await getDataframe(n, { preview: true })
      if (res.success) { setViewerMeta(res.metadata); setViewerColumns(res.columns || []); setViewerPreview(res.preview || []) }
    } catch (err) { toast.show(err.message || 'Failed to load dataframe') }
  }

  const copyLink = async (url) => {
    try {
      await navigator.clipboard.writeText(url)
      toast.show('Link copied')
    } catch {
      toast.show('Copy failed')
    }
  }

  return (
    <div className="bg-gray-50 min-h-screen text-gray-900">
      <header className="bg-slate-900 text-white">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
          <h1 className="text-lg font-semibold">Spark test visualizer</h1>
          <div className="flex items-center gap-3">
            <button onClick={() => navigate('/operations')} className="px-3 py-1.5 rounded bg-indigo-600 hover:bg-indigo-700">Operations</button>
            <button onClick={() => navigate('/chained-operations')} className="px-3 py-1.5 rounded bg-emerald-600 hover:bg-emerald-700">Chained Ops</button>
            <div className="text-sm text-slate-300">{stats ? (<span>{stats.dataframe_count} cached • {stats.total_size_mb} MB</span>) : 'Loading stats...'}</div>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        <section className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="rounded-lg bg-gradient-to-r from-indigo-500 to-purple-600 text-white p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{stats.dataframe_count}</div>
                <div className="text-white/80">DataFrames Cached</div>
              </div>
              <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 text-white/90" viewBox="0 0 20 20" fill="currentColor"><path d="M2 5a2 2 0 002-2h2a2 2 0 012 2v1H2V5zM2 9h6v2H2V9zm0 4h6v1a2 2 0 01-2 2H4a2 2 0 01-2-2v-1zM10 5a2 2 0 012-2h2a2 2 0 012 2v1h-6V5zm0 4h6v2h-6V9zm0 4h6v1a2 2 0 01-2 2h-2a2 2 0 01-2-2v-1z"/></svg>
            </div>
          </div>
          <div className="rounded-lg bg-gradient-to-r from-teal-500 to-emerald-600 text-white p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{stats.total_size_mb} MB</div>
                <div className="text-white/80">Total Cache Size</div>
              </div>
              <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 text-white/90" viewBox="0 0 20 20" fill="currentColor"><path d="M2 5a2 2 0 002-2h12a2 2 0 002 2v3H2V5zm0 5h16v5a2 2 0 01-2 2H4a2 2 0 01-2-2v-5z"/></svg>
            </div>
          </div>
        </section>

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

            <div onDragOver={(e) => e.preventDefault()} onDrop={onDrop} onClick={() => document.getElementById('file-input').click()} className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center hover:border-indigo-400 transition cursor-pointer">
              <svg xmlns="http://www.w3.org/2000/svg" className="mx-auto h-10 w-10 text-gray-400" viewBox="0 0 20 20" fill="currentColor"><path d="M3 3a2 2 0 00-2 2v3h2V5h12v3h2V5a2 2 0 00-2-2H3z"/><path d="M3 9h14v6a2 2 0 01-2 2H5a2 2 0 01-2-2V9zm7 1a1 1 0 00-1 1v2H8l3 3 3-3h-1v-2a1 1 0 00-1-1h-2z"/></svg>
              <div className="mt-2 text-sm text-gray-600">Drop files here or click to browse</div>
              <div className="text-xs text-gray-500">CSV, Excel (.xlsx, .xls), JSON</div>
              {file && (<div className="mt-2 text-xs text-gray-700">Selected: <span className="font-medium">{file.name}</span></div>)}
            </div>
            <input id="file-input" className="hidden" type="file" accept=".csv,.xlsx,.xls,.json" onChange={(e) => setFile(e.target.files?.[0] || null)} />

            <button disabled={uploading} className="inline-flex items-center px-4 py-2 rounded-md bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-2" viewBox="0 0 20 20" fill="currentColor"><path d="M3 3a2 2 0 00-2 2v3h2V5h12v3h2V5a2 2 0 00-2-2H3z"/><path d="M3 9h14v6a2 2 0 01-2 2H5a2 2 0 01-2-2V9zm7 1a1 1 0 00-1 1v2H8l3 3 3-3h-1v-2a1 1 0 00-1-1h-2z"/></svg>
              {uploading ? 'Uploading…' : 'Upload DataFrame'}
            </button>
          </form>
        </section>

        <section className="bg-white rounded-lg shadow">
          <div className="p-5 border-b border-gray-200 flex items-center justify-between">
            <h2 className="text-base font-semibold">Cached DataFrames</h2>
            <button onClick={onClearCache} className="inline-flex items-center px-3 py-1.5 rounded-md border border-red-600 text-red-600 hover:bg-red-50">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-1.5" viewBox="0 0 20 20" fill="currentColor"><path d="M6 8a1 1 0 011 1v7a1 1 0 11-2 0V9a1 1 0 011-1zm4 0a1 1 0 011 1v7a1 1 0 11-2 0V9a1 1 0 011-1zm5-3h-3.5l-1-1h-3l-1 1H3v2h14V5z"/><path d="M5 7h10l-1 10a2 2 0 01-2 2H8a2 2 0 01-2-2L5 7z"/></svg>
              Clear All Cache
            </button>
          </div>
          <div className="p-5 overflow-x-auto">
            <table className="min-w-full text-sm table-fixed">
              <thead className="text-left text-gray-600 border-b">
                <tr>
                  <th className="py-2 pr-4 w-[28ch]">Name</th>
                  <th className="py-2 pr-4 w-[40ch]">Description</th>
                  <th className="py-2 pr-4">Dimensions</th>
                  <th className="py-2 pr-4">Size</th>
                  <th className="py-2 pr-4">Created</th>
                  <th className="py-2">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {loadingList && (<tr><td className="py-3 text-gray-500" colSpan={6}>Loading…</td></tr>)}
                {!loadingList && rows.length === 0 && (<tr><td className="py-3 text-gray-500" colSpan={6}>No cached DataFrames</td></tr>)}
                {rows.map((r) => (
                  <tr key={r.name}>
                    <td className="py-2 pr-4 font-medium align-top">
                      <div className="max-w-[28ch]">
                        <button
                          className="block max-w-full overflow-hidden text-ellipsis whitespace-nowrap text-indigo-600 hover:underline"
                          onClick={() => navigate(`/analysis/${encodeURIComponent(r.name)}`)}
                          title={r.name}
                          aria-label="Open analysis"
                        >
                          {r.name}
                        </button>
                      </div>
                    </td>
                    <td className="py-2 pr-4 text-gray-700">
                      <div className="max-w-[40ch]">
                        <span
                          className="block max-w-full overflow-hidden text-ellipsis whitespace-nowrap"
                          title={r.description ? r.description : ''}
                        >
                          {r.description || '-'}
                        </span>
                      </div>
                    </td>
                    <td className="py-2 pr-4">{r.rows} x {r.cols}</td>
                    <td className="py-2 pr-4">{r.size_mb} MB</td>
                    <td className="py-2 pr-4">{new Date(r.timestamp).toLocaleString()}</td>
                    <td className="py-2">
                      <div className="flex items-center gap-1">
                        <button onClick={() => openViewer(r.name)} className="p-2 rounded hover:bg-gray-100" title="Preview">
                          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="currentColor"><path d="M12 5c-7.633 0-11 7-11 7s3.367 7 11 7 11-7 11-7-3.367-7-11-7zm0 12a5 5 0 110-10 5 5 0 010 10zm0-2.5a2.5 2.5 0 100-5 2.5 2.5 0 000 5z"/></svg>
                        </button>
                        <a href={buildDownloadCsvUrl(r.name)} className="p-2 rounded hover:bg-gray-100" title="Download CSV">
                          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="currentColor"><path d="M12 3a1 1 0 011 1v9.586l2.293-2.293a1 1 0 111.414 1.414l-4.007 4.007a1.25 1.25 0 01-1.772 0L6.92 12.707a1 1 0 011.414-1.414L10.5 13.46V4a1 1 0 011-1z"/><path d="M5 19a2 2 0 002 2h10a2 2 0 002-2v-2a1 1 0 112 0v2a4 4 0 01-4 4H7a4 4 0 01-4-4v-2a1 1 0 112 0v2z"/></svg>
                        </a>
                        <button onClick={() => copyLink(buildDownloadJsonUrl(r.name))} className="p-2 rounded hover:bg-gray-100" title="Copy JSON link">
                          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="currentColor"><path d="M8 7a3 3 0 013-3h7a3 3 0 013 3v7a3 3 0 01-3 3h-2v-2h2a1 1 0 001-1V7a1 1 0 00-1-1h-7a1 1 0 00-1 1v2H8V7z"/><path d="M3 10a3 3 0 013-3h7a3 3 0 013 3v7a3 3 0 01-3 3H6a3 3 0 01-3-3v-7zm3-1a1 1 0 00-1 1v7a1 1 0 001 1h7a1 1 0 001-1v-7a1 1 0 00-1-1H6z"/></svg>
                        </button>
                        <button onClick={() => onDelete(r.name)} className="p-2 rounded hover:bg-red-50 text-red-600" title="Delete">
                          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="currentColor"><path d="M9 3a1 1 0 00-1 1v1H5.5a1 1 0 100 2H6v12a3 3 0 003 3h6a3 3 0 003-3V7h.5a1 1 0 100-2H16V4a1 1 0 00-1-1H9zm2 4a1 1 0 012 0v10a1 1 0 11-2 0V7zm5 0a1 1 0 10-2 0v10a1 1 0 102 0V7zM10 4h4v1h-4V4z"/></svg>
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>

      <Modal open={viewerOpen} title={viewerTitle} onClose={() => setViewerOpen(false)}>
        {viewerMeta && (
          <div className="text-sm text-gray-700 mb-3">
            <div className="flex flex-wrap gap-3">
              <span><span className="font-medium">Rows:</span> {viewerMeta.rows}</span>
              <span><span className="font-medium">Cols:</span> {viewerMeta.cols}</span>
              <span><span className="font-medium">Size:</span> {viewerMeta.size_mb} MB</span>
              <span><span className="font-medium">Created:</span> {new Date(viewerMeta.timestamp).toLocaleString()}</span>
            </div>
            {viewerMeta.description && (<div className="text-gray-600 mt-1">{viewerMeta.description}</div>)}
          </div>
        )}
        <div className="overflow-auto border rounded">
          <table className="min-w-full text-xs">
            <thead className="sticky top-0 bg-white border-b">
              <tr>
                {viewerColumns.map((c) => (<th key={c} className="px-2 py-1 text-left">{c}</th>))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {viewerPreview.map((row, idx) => (
                <tr key={idx}>
                  {viewerColumns.map((c) => (<td key={c} className="px-2 py-1 whitespace-nowrap text-gray-700">{String(row?.[c] ?? '')}</td>))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Modal>

      <ConfirmDialog open={confirmOpen} title={confirmTitle} message={confirmMessage} confirming={confirming} onConfirm={onConfirmProceed} onCancel={() => !confirming && setConfirmOpen(false)} />

      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>
    </div>
  )
}
