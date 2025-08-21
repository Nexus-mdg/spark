import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Header from './Header.jsx'
import Footer from './components/Footer.jsx'
import {
  listDataframes,
  opsCompare,
  opsMerge,
  opsPivot,
  opsFilter,
  opsGroupBy,
  buildDownloadCsvUrl,
  getDataframe,
  opsSelect,
  opsRename,
  opsDatetime,
  opsMutate
} from './api.js'

function Section({ title, children }) {
  return (
    <section className="bg-white dark:bg-gray-800 rounded-lg shadow p-5">
      <h2 className="text-base font-semibold mb-4 text-gray-900 dark:text-gray-100">{title}</h2>
      {children}
    </section>
  )
}

function useToast() {
  const [msg, setMsg] = useState('')
  const [visible, setVisible] = useState(false)
  useEffect(() => {
    if (!msg) return
    setVisible(true)
    const t = setTimeout(() => setVisible(false), 2500)
    return () => clearTimeout(t)
  }, [msg])
  return { show: (m) => setMsg(m), visible, msg }
}

function DataframePreview({ name, columnsFilter, excludeSelected }) {
  const [state, setState] = useState({ loading: false, error: '', columns: [], rows: [], total: null })
  useEffect(() => {
    let alive = true
    if (!name) { setState({ loading: false, error: '', columns: [], rows: [], total: null }); return }
    setState(s => ({ ...s, loading: true, error: '' }))
    getDataframe(name, { preview: true })
      .then(res => {
        if (!alive) return
        const cols = (res.columns || [])
        const rows = (res.preview || [])
        const total = res.total_rows || (res.pagination ? res.pagination.total_rows : null) || null
        // Decide which columns to show
        let useCols = cols
        if (Array.isArray(columnsFilter) && columnsFilter.length > 0) {
          useCols = excludeSelected ? cols.filter(c => !columnsFilter.includes(c)) : cols.filter(c => columnsFilter.includes(c))
        }
        const projRows = rows.map(r => {
          if (!Array.isArray(columnsFilter) || columnsFilter.length === 0) return r
          const obj = {}
          useCols.forEach(c => { obj[c] = r[c] })
          return obj
        })
        const limitedRows = projRows.slice(0, 10)
        setState({ loading: false, error: '', columns: useCols, rows: limitedRows, total })
      })
      .catch(e => { if (alive) setState({ loading: false, error: e.message || 'Failed to load preview', columns: [], rows: [], total: null }) })
    return () => { alive = false }
  }, [name, JSON.stringify(columnsFilter), !!excludeSelected])

  if (!name) return null
  return (
    <div className="mt-3 border rounded bg-slate-50 dark:bg-gray-800 border-gray-200 dark:border-gray-600">
      <div className="px-3 py-2 text-xs text-slate-600 dark:text-gray-300 flex items-center gap-2">
        <span className="font-medium">Preview:</span>
        <span className="">{name}</span>
        {state.total != null && (<span className="ml-auto">showing {state.rows.length} of {state.total}</span>)}
      </div>
      {state.loading ? (
        <div className="px-3 py-3 text-sm text-slate-600 dark:text-gray-300 flex items-center gap-2"><img src="/loader.svg" className="w-5 h-5" alt=""/> Loading…</div>
      ) : state.error ? (
        <div className="px-3 py-3 text-sm text-red-600 dark:text-red-400">{state.error}</div>
      ) : (
        <div className="overflow-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-slate-100 dark:bg-gray-700">
              <tr>
                {state.columns.map(c => (<th key={c} className="text-left px-3 py-2 whitespace-nowrap border-b border-gray-200 dark:border-gray-600 text-gray-900 dark:text-gray-100">{c}</th>))}
              </tr>
            </thead>
            <tbody>
              {state.rows.map((r, i) => (
                <tr key={i} className={i % 2 ? 'bg-white dark:bg-gray-800' : 'bg-gray-50 dark:bg-gray-700'}>
                  {state.columns.map(c => (
                    <td key={c} className="px-3 py-1 align-top border-b border-gray-200 dark:border-gray-600 max-w-[300px] truncate text-gray-900 dark:text-gray-100" title={r[c] !== null && r[c] !== undefined ? String(r[c]) : ''}>
                      {r[c] !== null && r[c] !== undefined ? String(r[c]) : ''}
                    </td>
                  ))}
                </tr>
              ))}
              {state.rows.length === 0 && (
                <tr><td className="px-3 py-2 text-slate-500 dark:text-gray-400" colSpan={state.columns.length || 1}>No rows</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default function Operations() {
  const [dfs, setDfs] = useState([])
  const [loading, setLoading] = useState(false)
  const toast = useToast()
  const navigate = useNavigate()

  const refresh = async () => {
    setLoading(true)
    try {
      const res = await listDataframes()
      if (res.success) setDfs((res.dataframes || []).sort((a, b) => a.name.localeCompare(b.name)))
    } finally { setLoading(false) }
  }

  useEffect(() => { refresh() }, [])

  const dfOptions = dfs.map(d => ({ value: d.name, label: d.name, columns: d.columns || [] }))

  // Compare state
  const [cmp1, setCmp1] = useState('')
  const [cmp2, setCmp2] = useState('')
  const [cmpRes, setCmpRes] = useState(null)
  const [cmpLoading, setCmpLoading] = useState(false)

  const onCompare = async () => {
    if (!cmp1 || !cmp2) return toast.show('Pick two dataframes')
    setCmpLoading(true)
    setCmpRes(null)
    try {
      const res = await opsCompare({ name1: cmp1, name2: cmp2 })
      setCmpRes(res)
      toast.show(res.identical ? 'DataFrames are identical' : `Compared: ${res.result_type}`)
      if (res.created && res.created.length) await refresh()
    } catch (e) { toast.show(e.message || 'Compare failed') }
    finally { setCmpLoading(false) }
  }

  // Merge state
  const [mergeNames, setMergeNames] = useState([])
  const [mergeKeys, setMergeKeys] = useState('')
  const [mergeHow, setMergeHow] = useState('inner')
  const onMerge = async () => {
    const names = mergeNames
    const keys = mergeKeys.split(',').map(s => s.trim()).filter(Boolean)
    if (names.length < 2 || keys.length < 1) return toast.show('Pick 2+ dataframes and at least 1 key')
    try {
      const res = await opsMerge({ names, keys, how: mergeHow })
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Merge failed') }
  }

  // Pivot state
  const [pvMode, setPvMode] = useState('wider')
  const [pvName, setPvName] = useState('')
  const [pvIndex, setPvIndex] = useState('')
  const [pvNamesFrom, setPvNamesFrom] = useState('')
  const [pvValuesFrom, setPvValuesFrom] = useState('')
  const [pvAgg, setPvAgg] = useState('first')
  const [plIdVars, setPlIdVars] = useState('')
  const [plValueVars, setPlValueVars] = useState('')
  const [plVarName, setPlVarName] = useState('variable')
  const [plValueName, setPlValueName] = useState('value')
  const onPivot = async () => {
    if (!pvName) return toast.show('Pick a dataframe')
    try {
      if (pvMode === 'wider') {
        const payload = {
          mode: 'wider',
          name: pvName,
          index: pvIndex.split(',').map(s => s.trim()).filter(Boolean),
          names_from: pvNamesFrom,
          values_from: pvValuesFrom.split(',').map(s => s.trim()).filter(Boolean),
          aggfunc: pvAgg
        }
        const res = await opsPivot(payload)
        toast.show(`Created ${res.name}`)
      } else {
        const payload = {
          mode: 'longer',
          name: pvName,
          id_vars: plIdVars.split(',').map(s => s.trim()).filter(Boolean),
          value_vars: plValueVars.split(',').map(s => s.trim()).filter(Boolean),
          var_name: plVarName,
          value_name: plValueName
        }
        const res = await opsPivot(payload)
        toast.show(`Created ${res.name}`)
      }
      await refresh()
    } catch (e) { toast.show(e.message || 'Pivot failed') }
  }

  // Filter
  const [ftName, setFtName] = useState('')
  const [filters, setFilters] = useState([{ col: '', op: 'eq', value: '' }])
  const [ftCombine, setFtCombine] = useState('and')
  const addFilter = () => setFilters([...filters, { col: '', op: 'eq', value: '' }])
  const removeFilter = (idx) => setFilters(filters.filter((_, i) => i !== idx))
  const updateFilter = (idx, patch) => setFilters(filters.map((f, i) => i === idx ? { ...f, ...patch } : f))
  const onFilter = async () => {
    if (!ftName) return toast.show('Pick a dataframe')
    try {
      const res = await opsFilter({ name: ftName, filters, combine: ftCombine })
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Filter failed') }
  }

  // Group by
  const [gbName, setGbName] = useState('')
  const [gbBy, setGbBy] = useState('')
  const [gbAggs, setGbAggs] = useState('')
  const onGroupBy = async () => {
    if (!gbName) return toast.show('Pick a dataframe')
    let aggsObj = undefined
    if (gbAggs.trim()) {
      try { aggsObj = JSON.parse(gbAggs) } catch { return toast.show('Aggs must be JSON') }
    }
    try {
      const res = await opsGroupBy({ name: gbName, by: gbBy.split(',').map(s => s.trim()).filter(Boolean), aggs: aggsObj })
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'GroupBy failed') }
  }

  // Select (column projection)
  const [selName, setSelName] = useState('')
  const [selCols, setSelCols] = useState([])
  const [selExclude, setSelExclude] = useState(false)
  const selectedDfMeta = dfOptions.find(o => o.value === selName)
  useEffect(() => { setSelCols([]) }, [selName])
  const toggleSelCol = (col) => setSelCols(prev => prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col])
  const onSelectCols = async () => {
    if (!selName) return toast.show('Pick a dataframe')
    if (selCols.length === 0) return toast.show('Pick at least one column')
    try {
      const res = await opsSelect({ name: selName, columns: selCols, exclude: selExclude })
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Select failed') }
  }

  // Rename columns
  const [rnName, setRnName] = useState('')
  const [rnMap, setRnMap] = useState('')
  const onRename = async () => {
    if (!rnName) return toast.show('Pick a dataframe')
    if (!rnMap.trim()) return toast.show('Provide a mapping JSON')
    try {
      const map = JSON.parse(rnMap)
      if (!map || typeof map !== 'object' || Array.isArray(map)) return toast.show('Mapping must be a JSON object')
      const res = await opsRename({ name: rnName, map })
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) {
      if (e instanceof SyntaxError) return toast.show('Invalid JSON mapping')
      toast.show(e.message || 'Rename failed')
    }
  }

  // Date/Time state
  const [dtName, setDtName] = useState('')
  const [dtAction, setDtAction] = useState('parse')
  const [dtSource, setDtSource] = useState('')
  const [dtFormat, setDtFormat] = useState('')
  const [dtTarget, setDtTarget] = useState('')
  const [dtOverwrite, setDtOverwrite] = useState(false)
  const [dtMonthStyle, setDtMonthStyle] = useState('short')
  const [dtOutYear, setDtOutYear] = useState(true)
  const [dtOutMonth, setDtOutMonth] = useState(true)
  const [dtOutDay, setDtOutDay] = useState(true)
  const [dtOutYearMonth, setDtOutYearMonth] = useState(true)

  const selectedDf = dfOptions.find(o => o.value === dtName)
  useEffect(() => { setDtSource('') }, [dtName])

  const onDateTimeRun = async (payload) => {
    try {
      const res = await opsDatetime(payload)
      if (!res.success) throw new Error(res.error || '')
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Datetime op failed') }
  }

  const runDateTime = () => {
    if (!dtName) return
    if (!dtSource) return
    if (dtAction === 'parse') {
      const payload = { name: dtName, action: 'parse', source: dtSource }
      if (dtFormat.trim()) payload.format = dtFormat.trim()
      if (dtTarget.trim()) payload.target = dtTarget.trim()
      if (dtOverwrite) payload.overwrite = true
      onDateTimeRun(payload)
    } else {
      const payload = {
        name: dtName,
        action: 'derive',
        source: dtSource,
        month_style: dtMonthStyle,
        outputs: { year: dtOutYear, month: dtOutMonth, day: dtOutDay, year_month: dtOutYearMonth }
      }
      onDateTimeRun(payload)
    }
  }

  return (
    <div className="bg-gray-50 dark:bg-gray-900 min-h-screen text-gray-900 dark:text-gray-100 transition-colors flex flex-col">
      <Header title="Operations">
        <div className="text-sm text-slate-300">
          {loading ? 'Loading…' : `${dfs.length} dataframes`}
        </div>
      </Header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6 flex-grow">
        {/* Explanatory Text */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8 text-indigo-500" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M11.49 3.17c-.38-1.56-2.6-1.56-2.98 0a1.532 1.532 0 01-2.286.948c-1.372-.836-2.942.734-2.106 2.106.54.886.061 2.042-.947 2.287-1.561.379-1.561 2.6 0 2.978a1.532 1.532 0 01.947 2.287c-.836 1.372.734 2.942 2.106 2.106a1.532 1.532 0 012.287.947c.379 1.561 2.6 1.561 2.978 0a1.533 1.533 0 012.287-.947c1.372.836 2.942-.734 2.106-2.106a1.533 1.533 0 01.947-2.287c1.561-.379 1.561-2.6 0-2.978a1.532 1.532 0 01-.947-2.287c.836-1.372-.734-2.942-2.106-2.106a1.532 1.532 0 01-2.287-.947zM10 13a3 3 0 100-6 3 3 0 000 6z" clipRule="evenodd" />
              </svg>
            </div>
            <div>
              <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">DataFrame Operations</h2>
              <p className="text-gray-600 dark:text-gray-300 mb-3">
                Perform single operations on your DataFrames. Each operation creates a new DataFrame with the results, 
                preserving your original data. Perfect for quick transformations and data exploration.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 text-sm">
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-indigo-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Compare DataFrames</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-indigo-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Merge & Join operations</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-indigo-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Pivot & Reshape data</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-indigo-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Filter & Group operations</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-indigo-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Column selection & renaming</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-indigo-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Date/time & expression handling</span>
                </div>
              </div>
            </div>
          </div>
        </div>
        <Section title="Compare two DataFrames">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Left</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={cmp1} onChange={e => setCmp1(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Right</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={cmp2} onChange={e => setCmp2(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <button disabled={cmpLoading} onClick={onCompare} className={`px-4 py-2 text-white rounded hover:bg-indigo-700 ${cmpLoading ? 'bg-indigo-400 cursor-not-allowed' : 'bg-indigo-600'}`}>{cmpLoading ? 'Comparing…' : 'Compare'}</button>
          </div>
          {/* Loader image while waiting for compare response */}
          {cmpLoading && (
            <div className="mt-3 flex items-center gap-2 text-sm text-slate-600">
              <img src="/loader.svg" alt="loading" className="w-6 h-6" />
              <span>Running comparison…</span>
            </div>
          )}
          {/* Previews below choices */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3">
            <DataframePreview name={cmp1} />
            <DataframePreview name={cmp2} />
          </div>
          {cmpRes && !cmpLoading && (
            <div className="mt-3 text-sm">
              <div>Result: <span className="font-medium">{cmpRes.identical ? 'identical' : cmpRes.result_type}</span></div>
              {(cmpRes.left_unique > 0) && (<div>Left unique rows: {cmpRes.left_unique}</div>)}
              {(cmpRes.right_unique > 0) && (<div>Right unique rows: {cmpRes.right_unique}</div>)}
              {(cmpRes.created || []).length > 0 && (
                <div className="mt-2 flex flex-wrap gap-2">
                  {(cmpRes.created || []).map(n => (
                    <a key={n} href={buildDownloadCsvUrl(n)} className="text-indigo-600 underline">{n}.csv</a>
                  ))}
                </div>
              )}
            </div>
          )}
        </Section>

        <Section title="Merge multiple DataFrames">
          <div className="space-y-3">
            <div>
              <div className="text-sm mb-2 text-gray-900 dark:text-gray-100">Pick 2+ dataframes</div>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                {dfOptions.map(o => (
                  <label key={o.value} className={`px-3 py-2 rounded border cursor-pointer flex items-center gap-2 transition-colors ${mergeNames.includes(o.value) ? 'bg-indigo-50 dark:bg-indigo-900/30 border-indigo-400 dark:border-indigo-500' : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-650'}`}>
                    <input type="checkbox" className="rounded text-indigo-600 focus:ring-indigo-500" checked={mergeNames.includes(o.value)} onChange={e => setMergeNames(e.target.checked ? [...mergeNames, o.value] : mergeNames.filter(n => n !== o.value))} />
                    <span className="text-sm text-gray-900 dark:text-gray-100 truncate">{o.label}</span>
                  </label>
                ))}
              </div>
            </div>
            {/* Previews for selected */}
            {mergeNames.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {mergeNames.map(n => (<DataframePreview key={n} name={n} />))}
              </div>
            )}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
              <label className="block">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Join keys (comma)</span>
                <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={mergeKeys} onChange={e => setMergeKeys(e.target.value)} placeholder="id" />
              </label>
              <label className="block">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Join type</span>
                <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={mergeHow} onChange={e => setMergeHow(e.target.value)}>
                  <option>inner</option>
                  <option>left</option>
                  <option>right</option>
                  <option>outer</option>
                </select>
              </label>
              <button onClick={onMerge} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Merge</button>
            </div>
          </div>
        </Section>

        <Section title="Pivot">
          <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={pvName} onChange={e => setPvName(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Mode</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={pvMode} onChange={e => setPvMode(e.target.value)}>
                <option value="wider">wider</option>
                <option value="longer">longer</option>
              </select>
            </label>
            {pvMode === 'wider' ? (
              <>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Index cols (comma)</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={pvIndex} onChange={e => setPvIndex(e.target.value)} placeholder="id" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">names_from</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={pvNamesFrom} onChange={e => setPvNamesFrom(e.target.value)} placeholder="category" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">values_from (comma)</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={pvValuesFrom} onChange={e => setPvValuesFrom(e.target.value)} placeholder="value" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">aggfunc</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={pvAgg} onChange={e => setPvAgg(e.target.value)} placeholder="first" />
                </label>
              </>
            ) : (
              <>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">id_vars (comma)</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={plIdVars} onChange={e => setPlIdVars(e.target.value)} placeholder="id" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">value_vars (comma)</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={plValueVars} onChange={e => setPlValueVars(e.target.value)} placeholder="col1,col2" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">var_name</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={plVarName} onChange={e => setPlVarName(e.target.value)} />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">value_name</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={plValueName} onChange={e => setPlValueName(e.target.value)} />
                </label>
              </>
            )}
            <div className="md:col-span-6">
              <button onClick={onPivot} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Run Pivot</button>
            </div>
          </div>
          {/* Preview below df choice */}
          {pvName && (<DataframePreview name={pvName} />)}
        </Section>

        <Section title="Filter">
          <div className="space-y-3">
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 max-w-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={ftName} onChange={e => setFtName(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            {/* Preview below df choice */}
            {ftName && (<DataframePreview name={ftName} />)}
            <div className="flex items-center gap-3">
              <span className="text-sm text-gray-900 dark:text-gray-100">Combine</span>
              <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={ftCombine} onChange={e => setFtCombine(e.target.value)}>
                <option>and</option>
                <option>or</option>
              </select>
            </div>
            {filters.map((f, idx) => (
              <div key={idx} className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Column</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={f.col} onChange={e => updateFilter(idx, { col: e.target.value })} placeholder="column name" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Op</span>
                  <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={f.op} onChange={e => updateFilter(idx, { op: e.target.value })}>
                    <option>eq</option><option>ne</option><option>lt</option><option>lte</option><option>gt</option><option>gte</option>
                    <option>in</option><option>nin</option><option>contains</option><option>startswith</option><option>endswith</option>
                    <option>isnull</option><option>notnull</option>
                  </select>
                </label>
                <label className="block md:col-span-3">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Value</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={f.value} onChange={e => updateFilter(idx, { value: e.target.value })} placeholder="value or [v1,v2] for in" />
                </label>
                <button className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={() => removeFilter(idx)}>Remove</button>
              </div>
            ))}
            <div className="flex items-center gap-2">
              <button className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={addFilter}>Add filter</button>
              <button className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700" onClick={onFilter}>Run Filter</button>
            </div>
          </div>
        </Section>

        <Section title="Group by">
          <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={gbName} onChange={e => setGbName(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Group by (comma)</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={gbBy} onChange={e => setGbBy(e.target.value)} placeholder="col1,col2" />
            </label>
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Aggs JSON (optional)</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={gbAggs} onChange={e => setGbAggs(e.target.value)} placeholder='{"col":"sum","col2":["mean","max"]}' />
            </label>
            <button onClick={onGroupBy} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Run GroupBy</button>
          </div>
          {/* Preview below df choice */}
          {gbName && (<DataframePreview name={gbName} />)}
        </Section>

        <Section title="Select columns">
          <div className="space-y-3">
            <label className="block max-w-sm">
              <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={selName} onChange={e => setSelName(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            {/* Columns chooser */}
            {selName && (
              <div>
                <div className="text-sm mb-1">Pick columns</div>
                <div className="flex flex-wrap gap-2">
                  {(selectedDfMeta?.columns || []).map(c => (
                    <label key={c} className={`px-2 py-1 rounded border cursor-pointer text-gray-900 dark:text-gray-100 ${selCols.includes(c) ? 'bg-indigo-50 dark:bg-indigo-900/30 border-indigo-400 dark:border-indigo-500' : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600'}`}>
                      <input type="checkbox" className="mr-1" checked={selCols.includes(c)} onChange={() => toggleSelCol(c)} />
                      {c}
                    </label>
                  ))}
                </div>
                <label className="inline-flex items-center gap-2 mt-2">
                  <input type="checkbox" checked={selExclude} onChange={e => setSelExclude(e.target.checked)} />
                  <span className="text-sm text-gray-900 dark:text-gray-100">Exclude selected</span>
                </label>
              </div>
            )}
            {/* Preview filtered to selected columns (or remaining if exclude) */}
            {selName && (<DataframePreview name={selName} columnsFilter={selCols} excludeSelected={selExclude} />)}
            <div>
              <button onClick={onSelectCols} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Create selection</button>
            </div>
          </div>
        </Section>

        <Section title="Rename columns">
          <div className="space-y-3">
            <label className="block max-w-sm">
              <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={rnName} onChange={e => setRnName(e.target.value)}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            {/* Preview of source */}
            {rnName && (<DataframePreview name={rnName} />)}
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Mapping JSON (old→new)</span>
              <textarea className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono text-xs h-24 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" placeholder='{"old_col":"new_col", "age":"age_years"}' value={rnMap} onChange={e => setRnMap(e.target.value)} />
            </label>
            <div>
              <button onClick={onRename} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Run Rename</button>
            </div>
          </div>
        </Section>

        {/* New: Date / Time */}
        <Section title="Date / Time">
          <DateTimeSection dfOptions={dfOptions} onRun={async (payload) => { try { const res = await opsDatetime(payload); if (!res.success) throw new Error(res.error||''); toast.show(`Created ${res.name}`); await refresh() } catch (e) { toast.show(e.message || 'Datetime op failed') } }} />
        </Section>

        {/* New: Mutate */}
        <Section title="Mutate (create/overwrite column via expression)">
          <MutateSection dfOptions={dfOptions} onRun={async (payload) => { try { const res = await opsMutate(payload); if (!res.success) throw new Error(res.error||''); toast.show(`Created ${res.name}`); await refresh() } catch (e) { toast.show(e.message || 'Mutate failed') } }} />
        </Section>
      </main>

      <Footer />

      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>
    </div>
  )
}

function DateTimeSection({ dfOptions, onRun }) {
  const [name, setName] = useState('')
  const [action, setAction] = useState('parse')
  const [source, setSource] = useState('')
  const [format, setFormat] = useState('')
  const [target, setTarget] = useState('')
  const [overwrite, setOverwrite] = useState(false)
  const [monthStyle, setMonthStyle] = useState('short')
  const [outYear, setOutYear] = useState(true)
  const [outMonth, setOutMonth] = useState(true)
  const [outDay, setOutDay] = useState(true)
  const [outYearMonth, setOutYearMonth] = useState(true)

  const selectedDf = dfOptions.find(o => o.value === name)
  useEffect(() => { setSource('') }, [name])

  const run = () => {
    if (!name) return
    if (!source) return
    if (action === 'parse') {
      const payload = { name, action: 'parse', source }
      if (format.trim()) payload.format = format.trim()
      if (target.trim()) payload.target = target.trim()
      if (overwrite) payload.overwrite = true
      onRun(payload)
    } else {
      const payload = { name, action: 'derive', source, month_style: monthStyle, outputs: { year: outYear, month: outMonth, day: outDay, year_month: outYearMonth } }
      onRun(payload)
    }
  }

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
        <label className="block md:col-span-2">
          <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
          <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={name} onChange={e => setName(e.target.value)}>
            <option value="">Select…</option>
            {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
          </select>
        </label>
        <label className="block">
          <span className="block text-sm text-gray-900 dark:text-gray-100">Action</span>
          <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={action} onChange={e => setAction(e.target.value)}>
            <option value="parse">parse (string → date)</option>
            <option value="derive">derive parts</option>
          </select>
        </label>
        <label className="block md:col-span-2">
          <span className="block text-sm text-gray-900 dark:text-gray-100">Source column</span>
          <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" list="dt-cols" value={source} onChange={e => setSource(e.target.value)} placeholder="date_col" />
          <datalist id="dt-cols">
            {(selectedDf?.columns || []).map(c => (<option key={c} value={c}>{c}</option>))}
          </datalist>
        </label>
      </div>
      {name && (<DataframePreview name={name} />)}
      {action === 'parse' ? (
        <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
          <label className="block">
            <span className="block text-sm text-gray-900 dark:text-gray-100">Format (optional)</span>
            <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={format} onChange={e => setFormat(e.target.value)} placeholder="e.g. %Y-%m-%d" />
          </label>
          <label className="block">
            <span className="block text-sm text-gray-900 dark:text-gray-100">Target column (optional)</span>
            <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={target} onChange={e => setTarget(e.target.value)} placeholder="new_date" />
          </label>
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={overwrite} onChange={e => setOverwrite(e.target.checked)} />
            <span className="text-sm text-gray-900 dark:text-gray-100">Overwrite if exists</span>
          </label>
          <div className="md:col-span-6">
            <button onClick={run} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Run Parse</button>
          </div>
        </div>
      ) : (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Month style</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={monthStyle} onChange={e => setMonthStyle(e.target.value)}>
                <option value="short">Jan</option>
                <option value="short_lower">jan</option>
                <option value="long">January</option>
                <option value="num">1..12</option>
              </select>
            </label>
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={outYear} onChange={e => setOutYear(e.target.checked)} />
              <span className="text-sm text-gray-900 dark:text-gray-100">year</span>
            </label>
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={outMonth} onChange={e => setOutMonth(e.target.checked)} />
              <span className="text-sm text-gray-900 dark:text-gray-100">month</span>
            </label>
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={outDay} onChange={e => setOutDay(e.target.checked)} />
              <span className="text-sm text-gray-900 dark:text-gray-100">day</span>
            </label>
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={outYearMonth} onChange={e => setOutYearMonth(e.target.checked)} />
              <span className="text-sm text-gray-900 dark:text-gray-100">year_month</span>
            </label>
            <div className="md:col-span-6">
              <button onClick={run} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Run Derive</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function MutateSection({ dfOptions, onRun }) {
  const [name, setName] = useState('')
  const [target, setTarget] = useState('')
  const [expr, setExpr] = useState('')
  const [mode, setMode] = useState('vector')
  const [overwrite, setOverwrite] = useState(false)

  const selectedDf = dfOptions.find(o => o.value === name)

  const run = () => {
    if (!name || !target || !expr.trim()) return
    const payload = { name, target: target.trim(), expr: expr.trim(), mode, overwrite }
    onRun(payload)
  }

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
        <label className="block md:col-span-2">
          <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
          <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={name} onChange={e => setName(e.target.value)}>
            <option value="">Select…</option>
            {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
          </select>
        </label>
        <label className="block">
          <span className="block text-sm text-gray-900 dark:text-gray-100">Target column</span>
          <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={target} onChange={e => setTarget(e.target.value)} placeholder="new_col" />
        </label>
        <label className="block">
          <span className="block text-sm text-gray-900 dark:text-gray-100">Mode</span>
          <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={mode} onChange={e => setMode(e.target.value)}>
            <option value="vector">vector (Series/scalar)</option>
            <option value="row">row (use r[\"col\"]) </option>
          </select>
        </label>
        <label className="inline-flex items-center gap-2">
          <input type="checkbox" checked={overwrite} onChange={e => setOverwrite(e.target.checked)} />
          <span className="text-sm text-gray-900 dark:text-gray-100">Overwrite if exists</span>
        </label>
      </div>
      {name && (<DataframePreview name={name} />)}
      <label className="block">
        <span className="block text-sm text-gray-900 dark:text-gray-100">Expression</span>
        <textarea className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono text-xs h-28 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" value={expr} onChange={e => setExpr(e.target.value)} placeholder="Examples:\n- vector: col('a') + col('b')\n- vector: np.where(col('x') > 0, 'pos', 'neg')\n- vector: col('name').astype(str).str[:3] + '_' + col('country')\n- row: r['price'] * r['qty']\n- vector date: pd.to_datetime(col('ts')).dt.year" />
      </label>
      <div>
        <button onClick={run} className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">Run Mutate</button>
      </div>
    </div>
  )
}
