import React, { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Bar, Doughnut, Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  BarElement,
  CategoryScale,
  LinearScale,
  ArcElement,
  LineElement,
  PointElement,
  Tooltip,
  Legend
} from 'chart.js'
import Header from './Header.jsx'
import Pagination from './components/Pagination.jsx'
import {
  getStats,
  listDataframes,
  uploadDataframe,
  clearCache,
  deleteDataframe,
  getDataframe,
  buildDownloadCsvUrl,
  buildDownloadJsonUrl,
  renameDataframe
} from './api.js'

ChartJS.register(BarElement, CategoryScale, LinearScale, ArcElement, LineElement, PointElement, Tooltip, Legend)

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
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(1)
  const itemsPerPage = parseInt(process.env.MAX_ITEMS_PER_PAGE || '15', 10)

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

  // Edit modal state
  const [editOpen, setEditOpen] = useState(false)
  const [editOldName, setEditOldName] = useState('')
  const [editName, setEditName] = useState('')
  const [editDesc, setEditDesc] = useState('')
  const [savingEdit, setSavingEdit] = useState(false)

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
        setCurrentPage(1) // Reset to first page when data changes
      }
    } finally {
      setLoadingList(false)
    }
  }

  // Pagination calculations
  const totalItems = rows.length
  const startIndex = (currentPage - 1) * itemsPerPage
  const endIndex = startIndex + itemsPerPage
  const paginatedRows = rows.slice(startIndex, endIndex)

  const handlePageChange = (page) => {
    setCurrentPage(page)
  }

  // Chart data calculations
  const chartData = React.useMemo(() => {
    if (rows.length === 0) return null

    // Size distribution chart
    const sizeRanges = { 'Small (<1MB)': 0, 'Medium (1-10MB)': 0, 'Large (10-100MB)': 0, 'XLarge (>100MB)': 0 }
    rows.forEach(df => {
      const size = df.size_mb || 0
      if (size < 1) sizeRanges['Small (<1MB)']++
      else if (size < 10) sizeRanges['Medium (1-10MB)']++
      else if (size < 100) sizeRanges['Large (10-100MB)']++
      else sizeRanges['XLarge (>100MB)']++
    })

    // Rows distribution
    const rowRanges = { 'Tiny (<1K)': 0, 'Small (1K-10K)': 0, 'Medium (10K-100K)': 0, 'Large (>100K)': 0 }
    rows.forEach(df => {
      const rows = df.rows || 0
      if (rows < 1000) rowRanges['Tiny (<1K)']++
      else if (rows < 10000) rowRanges['Small (1K-10K)']++
      else if (rows < 100000) rowRanges['Medium (10K-100K)']++
      else rowRanges['Large (>100K)']++
    })

    // Recent activity (last 7 days)
    const today = new Date()
    const last7Days = []
    for (let i = 6; i >= 0; i--) {
      const date = new Date(today)
      date.setDate(date.getDate() - i)
      last7Days.push({
        date: date.toISOString().split('T')[0],
        label: date.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }),
        count: 0
      })
    }

    rows.forEach(df => {
      const dfDate = new Date(df.timestamp).toISOString().split('T')[0]
      const dayEntry = last7Days.find(d => d.date === dfDate)
      if (dayEntry) dayEntry.count++
    })

    return {
      sizeDistribution: {
        labels: Object.keys(sizeRanges),
        datasets: [{
          data: Object.values(sizeRanges),
          backgroundColor: ['#10b981', '#3b82f6', '#f59e0b', '#ef4444'],
          borderColor: ['#059669', '#2563eb', '#d97706', '#dc2626'],
          borderWidth: 2
        }]
      },
      rowsDistribution: {
        labels: Object.keys(rowRanges),
        datasets: [{
          label: 'DataFrames',
          data: Object.values(rowRanges),
          backgroundColor: 'rgba(99, 102, 241, 0.8)',
          borderColor: 'rgb(99, 102, 241)',
          borderWidth: 1
        }]
      },
      recentActivity: {
        labels: last7Days.map(d => d.label),
        datasets: [{
          label: 'DataFrames Created',
          data: last7Days.map(d => d.count),
          borderColor: 'rgb(168, 85, 247)',
          backgroundColor: 'rgba(168, 85, 247, 0.1)',
          tension: 0.4,
          fill: true
        }]
      }
    }
  }, [rows])

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

  // Open edit modal for a given row
  const openEdit = (row) => {
    setEditOldName(row.name)
    setEditName(row.name)
    setEditDesc(row.description || '')
    setEditOpen(true)
  }

  const saveEdit = async () => {
    if (!editOldName) return
    setSavingEdit(true)
    try {
      await renameDataframe(editOldName, { new_name: editName, description: editDesc })
      setEditOpen(false)
      toast.show('Updated successfully')
      await refreshList(); await refreshStats()
    } catch (err) {
      toast.show(err.message || 'Update failed')
    } finally {
      setSavingEdit(false)
    }
  }

  return (
    <div className="bg-gradient-to-br from-gray-50 via-blue-50 to-indigo-50 dark:from-gray-900 dark:via-gray-800 dark:to-gray-900 min-h-screen text-gray-900 dark:text-gray-100 transition-colors">
      <Header title="Spark test visualizer">
        <div className="text-sm text-slate-300">
          {stats ? (<span>{stats.dataframe_count} cached • {stats.total_size_mb} MB</span>) : 'Loading stats...'}
        </div>
      </Header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        {/* Enhanced Stats Section */}
        <section className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="rounded-xl bg-gradient-to-br from-indigo-500 via-purple-500 to-pink-500 text-white p-6 shadow-lg">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{stats.dataframe_count}</div>
                <div className="text-white/80 font-medium">DataFrames</div>
                <div className="text-white/60 text-sm">Cached in memory</div>
              </div>
              <div className="w-12 h-12 bg-white/20 rounded-full flex items-center justify-center">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" viewBox="0 0 20 20" fill="currentColor">
                  <path d="M3 4a1 1 0 011-1h12a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1V4zM3 10a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H4a1 1 0 01-1-1v-6zM14 9a1 1 0 00-1 1v6a1 1 0 001 1h2a1 1 0 001-1v-6a1 1 0 00-1-1h-2z" />
                </svg>
              </div>
            </div>
          </div>
          
          <div className="rounded-xl bg-gradient-to-br from-emerald-500 via-teal-500 to-cyan-500 text-white p-6 shadow-lg">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{stats.total_size_mb}</div>
                <div className="text-white/80 font-medium">MB Total</div>
                <div className="text-white/60 text-sm">Memory usage</div>
              </div>
              <div className="w-12 h-12 bg-white/20 rounded-full flex items-center justify-center">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" viewBox="0 0 20 20" fill="currentColor">
                  <path d="M3 4a1 1 0 000 2h11.586l-2.293 2.293a1 1 0 101.414 1.414l4-4a1 1 0 000-1.414l-4-4a1 1 0 10-1.414 1.414L14.586 4H3zM3 11a1 1 0 100 2h3.586l-2.293 2.293a1 1 0 101.414 1.414l4-4a1 1 0 000-1.414l-4-4a1 1 0 10-1.414 1.414L7.586 11H3z" />
                </svg>
              </div>
            </div>
          </div>
          
          <div className="rounded-xl bg-gradient-to-br from-orange-400 via-red-400 to-pink-400 text-white p-6 shadow-lg">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{rows.reduce((sum, df) => sum + (df.rows || 0), 0).toLocaleString()}</div>
                <div className="text-white/80 font-medium">Total Rows</div>
                <div className="text-white/60 text-sm">Across all DataFrames</div>
              </div>
              <div className="w-12 h-12 bg-white/20 rounded-full flex items-center justify-center">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M3 3a1 1 0 000 2v8a2 2 0 002 2h2.586l-1.293 1.293a1 1 0 101.414 1.414L10 15.414l2.293 2.293a1 1 0 001.414-1.414L12.414 15H15a2 2 0 002-2V5a1 1 0 100-2H3zm11.707 4.707a1 1 0 00-1.414-1.414L10 9.586 8.707 8.293a1 1 0 00-1.414 0l-2 2a1 1 0 101.414 1.414L8 10.414l1.293 1.293a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
              </div>
            </div>
          </div>
          
          <div className="rounded-xl bg-gradient-to-br from-violet-500 via-purple-500 to-indigo-500 text-white p-6 shadow-lg">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-3xl font-bold">{rows.reduce((max, df) => Math.max(max, df.cols || 0), 0)}</div>
                <div className="text-white/80 font-medium">Max Columns</div>
                <div className="text-white/60 text-sm">Widest DataFrame</div>
              </div>
              <div className="w-12 h-12 bg-white/20 rounded-full flex items-center justify-center">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" viewBox="0 0 20 20" fill="currentColor">
                  <path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
            </div>
          </div>
        </section>

        {/* Charts Section */}
        {chartData && (
          <section className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Size Distribution</h3>
              <div className="h-64">
                <Doughnut 
                  data={chartData.sizeDistribution} 
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: {
                        position: 'bottom',
                        labels: { padding: 20, usePointStyle: true }
                      }
                    }
                  }} 
                />
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Row Count Distribution</h3>
              <div className="h-64">
                <Bar 
                  data={chartData.rowsDistribution}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      y: { beginAtZero: true }
                    }
                  }}
                />
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Recent Activity</h3>
              <div className="h-64">
                <Line 
                  data={chartData.recentActivity}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      y: { beginAtZero: true }
                    }
                  }}
                />
              </div>
            </div>
          </section>
        )}

        <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Upload DataFrame</h2>
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

        <section className="bg-white dark:bg-gray-800 rounded-xl shadow-lg">
          <div className="p-6 border-b border-gray-200 dark:border-gray-600 flex items-center justify-between">
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Cached DataFrames</h2>
            <div className="flex items-center gap-2">
              <button onClick={refreshStats} className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors" title="Refresh list">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clipRule="evenodd" />
                </svg>
              </button>
              <button onClick={onClearCache} className="p-2 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20 text-red-600 dark:text-red-400 transition-colors" title="Clear all cache">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path d="M6 8a1 1 0 011 1v7a1 1 0 11-2 0V9a1 1 0 011-1zm4 0a1 1 0 011 1v7a1 1 0 11-2 0V9a1 1 0 011-1zm5-3h-3.5l-1-1h-3l-1 1H3v2h14V5z"/>
                  <path d="M5 7h10l-1 10a2 2 0 01-2 2H8a2 2 0 01-2-2L5 7z"/>
                </svg>
              </button>
            </div>
          </div>
          <div className="p-6 overflow-x-auto">
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
                {paginatedRows.map((r) => (
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
                        <button onClick={() => openEdit(r)} className="p-2 rounded hover:bg-gray-100" title="Edit name/description" aria-label="Edit">
                          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25z"/><path d="M20.71 7.04a1.003 1.003 0 000-1.42l-2.34-2.34a1.003 1.003 0 00-1.42 0l-1.83 1.83 3.75 3.75 1.84-1.82z"/></svg>
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
          
          {/* Pagination */}
          {!loadingList && rows.length > 0 && (
            <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-600">
              <Pagination
                currentPage={currentPage}
                totalItems={totalItems}
                itemsPerPage={itemsPerPage}
                onPageChange={handlePageChange}
              />
            </div>
          )}
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

      {/* Edit DataFrame Modal */}
      <Modal open={editOpen} title={`Edit DataFrame`} onClose={() => !savingEdit && setEditOpen(false)}>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700">Name</label>
            <input value={editName} onChange={(e) => setEditName(e.target.value)} type="text" className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500" />
            <div className="text-xs text-gray-500 mt-1">Renaming will update the DataFrame key; update any references in pipelines if needed.</div>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700">Description</label>
            <textarea value={editDesc} onChange={(e) => setEditDesc(e.target.value)} rows={2} className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"></textarea>
          </div>
          <div className="flex items-center justify-end gap-2 pt-2">
            <button disabled={savingEdit} onClick={() => setEditOpen(false)} className="px-3 py-1.5 rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
            <button disabled={savingEdit} onClick={saveEdit} className="px-3 py-1.5 rounded bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">{savingEdit ? 'Saving…' : 'Save Changes'}</button>
          </div>
        </div>
      </Modal>

      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>
    </div>
  )
}
