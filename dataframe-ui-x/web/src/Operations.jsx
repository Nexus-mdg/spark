import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Header from './Header.jsx'
import Footer from './components/Footer.jsx'
import { useProcessingEngine } from './contexts/ProcessingEngineContext.jsx'
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
  opsMutate,
  // Spark operations
  sparkOpsMerge,
  sparkOpsPivot,
  sparkOpsFilter,
  sparkOpsGroupBy,
  sparkOpsSelect,
  sparkOpsRename,
  sparkOpsDatetime,
  sparkOpsMutate
} from './api.js'

function Section({ title, children }) {
  return (
    <section className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-5">
      <h3 className="text-base font-semibold mb-4 text-gray-900 dark:text-gray-100">{title}</h3>
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
  const { processingEngine, isSparkMode } = useProcessingEngine()

  // Helper function to route operations based on processing engine preference
  const getOperationFunc = (operationType) => {
    const operationMap = {
      pandas: {
        select: opsSelect,
        filter: opsFilter,
        groupby: opsGroupBy,
        merge: opsMerge,
        pivot: opsPivot,
        rename: opsRename,
        datetime: opsDatetime,
        mutate: opsMutate
      },
      spark: {
        select: sparkOpsSelect,
        filter: sparkOpsFilter,
        groupby: sparkOpsGroupBy,
        merge: sparkOpsMerge,
        pivot: sparkOpsPivot,
        rename: sparkOpsRename,
        datetime: sparkOpsDatetime,
        mutate: sparkOpsMutate
      }
    }
    
    return operationMap[processingEngine]?.[operationType] || operationMap.pandas[operationType]
  }

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
  const [cmpError, setCmpError] = useState('')

  const validateCompare = () => {
    if (!cmp1) return 'Please select the first dataframe'
    if (!cmp2) return 'Please select the second dataframe'
    if (cmp1 === cmp2) return 'Please select two different dataframes'
    return ''
  }

  const onCompare = async () => {
    const error = validateCompare()
    if (error) {
      setCmpError(error)
      return toast.show(error)
    }
    setCmpError('')
    setCmpLoading(true)
    setCmpRes(null)
    try {
      const res = await opsCompare({ name1: cmp1, name2: cmp2 })
      setCmpRes(res)
      toast.show(res.identical ? 'DataFrames are identical' : `Compared: ${res.result_type}`)
      if (res.created && res.created.length) await refresh()
    } catch (e) { 
      const errorMsg = e.message || 'Comparison failed. Please try again.'
      setCmpError(errorMsg)
      toast.show(errorMsg)
    }
    finally { setCmpLoading(false) }
  }

  // Merge state
  const [mergeNames, setMergeNames] = useState([])
  const [mergeKeys, setMergeKeys] = useState('')
  const [mergeSelectedKeys, setMergeSelectedKeys] = useState([])
  const [mergeHow, setMergeHow] = useState('inner')
  const [mergeLoading, setMergeLoading] = useState(false)
  const [mergeError, setMergeError] = useState('')

  // Get common columns between selected dataframes
  const getCommonColumns = () => {
    if (mergeNames.length < 2) return []
    const selectedDfs = mergeNames.map(name => dfOptions.find(df => df.value === name)).filter(Boolean)
    if (selectedDfs.length < 2) return []
    
    let commonCols = selectedDfs[0].columns || []
    for (let i = 1; i < selectedDfs.length; i++) {
      const dfCols = selectedDfs[i].columns || []
      commonCols = commonCols.filter(col => dfCols.includes(col))
    }
    return commonCols
  }

  const validateMerge = () => {
    if (mergeNames.length < 2) return 'Please select at least 2 dataframes to merge'
    if (mergeSelectedKeys.length < 1) return 'Please select at least one join key'
    return ''
  }

  const onMerge = async () => {
    const error = validateMerge()
    if (error) {
      setMergeError(error)
      return toast.show(error)
    }
    setMergeError('')
    setMergeLoading(true)
    
    const names = mergeNames
    const keys = mergeSelectedKeys
    try {
      const res = await getOperationFunc('merge')({ names, keys, how: mergeHow })
      toast.show(`Successfully created merged dataframe: ${res.name}`)
      await refresh()
    } catch (e) { 
      const errorMsg = e.message || 'Merge operation failed. Please check your join keys and try again.'
      setMergeError(errorMsg)
      toast.show(errorMsg)
    }
    finally { setMergeLoading(false) }
  }

  // Pivot state
  const [pvMode, setPvMode] = useState('wider')
  const [pvName, setPvName] = useState('')
  const [pvIndex, setPvIndex] = useState('')
  const [pvSelectedIndex, setPvSelectedIndex] = useState('')
  const [pvNamesFrom, setPvNamesFrom] = useState('')
  const [pvValuesFrom, setPvValuesFrom] = useState('')
  const [pvSelectedValuesFrom, setPvSelectedValuesFrom] = useState('')
  const [pvAgg, setPvAgg] = useState('first')
  const [plIdVars, setPlIdVars] = useState('')
  const [plSelectedIdVars, setPlSelectedIdVars] = useState('')
  const [plValueVars, setPlValueVars] = useState('')
  const [plSelectedValueVars, setPlSelectedValueVars] = useState('')
  const [plVarName, setPlVarName] = useState('variable')
  const [plValueName, setPlValueName] = useState('value')
  const onPivot = async () => {
    if (!pvName) return toast.show('Pick a dataframe')
    try {
      if (pvMode === 'wider') {
        const payload = {
          mode: 'wider',
          name: pvName,
          index: pvSelectedIndex ? [pvSelectedIndex] : [],
          names_from: pvNamesFrom,
          values_from: pvSelectedValuesFrom ? [pvSelectedValuesFrom] : [],
          aggfunc: pvAgg
        }
        const res = await getOperationFunc('pivot')(payload)
        toast.show(`Created ${res.name}`)
      } else {
        const payload = {
          mode: 'longer',
          name: pvName,
          id_vars: plSelectedIdVars ? [plSelectedIdVars] : [],
          value_vars: plSelectedValueVars ? [plSelectedValueVars] : [],
          var_name: plVarName,
          value_name: plValueName
        }
        const res = await getOperationFunc('pivot')(payload)
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
      const res = await getOperationFunc('filter')({ name: ftName, filters, combine: ftCombine })
      toast.show(`Created ${res.name}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Filter failed') }
  }

  // Group by
  const [gbName, setGbName] = useState('')
  const [gbBy, setGbBy] = useState('')
  const [gbSelectedBy, setGbSelectedBy] = useState([])
  const [gbAggs, setGbAggs] = useState('')
  const onGroupBy = async () => {
    if (!gbName) return toast.show('Pick a dataframe')
    let aggsObj = undefined
    if (gbAggs.trim()) {
      try { aggsObj = JSON.parse(gbAggs) } catch { return toast.show('Aggs must be JSON') }
    }
    try {
      const res = await getOperationFunc('groupby')({ name: gbName, by: gbSelectedBy, aggs: aggsObj })
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
      const res = await getOperationFunc('select')({ name: selName, columns: selCols, exclude: selExclude })
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
      const res = await getOperationFunc('rename')({ name: rnName, map })
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
      const res = await getOperationFunc('datetime')(payload)
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
        <div className="text-sm text-slate-300 flex items-center gap-4">
          <div>
            {loading ? 'Loading…' : `${dfs.length} dataframes`}
          </div>
          <div className="flex items-center gap-1">
            <span className="text-xs">Engine:</span>
            <span className={`px-2 py-1 rounded text-xs font-medium ${
              isSparkMode 
                ? 'bg-orange-500/20 text-orange-200 border border-orange-500/30' 
                : 'bg-blue-500/20 text-blue-200 border border-blue-500/30'
            }`}>
              {isSparkMode ? 'Apache Spark' : 'Pandas'}
            </span>
          </div>
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
              <p className="text-gray-600 dark:text-gray-300 mb-4">
                Perform operations on your DataFrames with enhanced validation, accessibility, and user feedback. 
                Each operation creates a new DataFrame, preserving your original data.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-blue-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Data Analysis & Comparison</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-green-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Data Transformation & Reshaping</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-orange-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Data Filtering & Grouping</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-purple-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Column Operations & Expressions</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Data Analysis Section */}
        <div className="space-y-4">
          <div className="flex items-center gap-3 py-2">
            <div className="w-1 h-6 bg-blue-500 rounded-full"></div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Data Analysis & Comparison</h2>
            <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700"></div>
          </div>

        <Section title="Compare two DataFrames">
            <div className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
                <label className="block">
                  <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Left DataFrame</span>
                  <select 
                    className={`mt-1 border rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 ${cmpError && !cmp1 ? 'border-red-300 dark:border-red-500' : 'border-gray-300 dark:border-gray-600'}`}
                    value={cmp1} 
                    onChange={e => { setCmp1(e.target.value); setCmpError(''); }}
                    aria-label="Select left dataframe for comparison"
                    aria-describedby={cmpError && !cmp1 ? "cmp-error" : undefined}
                  >
                    <option value="">Select dataframe…</option>
                    {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
                  </select>
                </label>
                <label className="block">
                  <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Right DataFrame</span>
                  <select 
                    className={`mt-1 border rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 ${cmpError && !cmp2 ? 'border-red-300 dark:border-red-500' : 'border-gray-300 dark:border-gray-600'}`}
                    value={cmp2} 
                    onChange={e => { setCmp2(e.target.value); setCmpError(''); }}
                    aria-label="Select right dataframe for comparison"
                    aria-describedby={cmpError && !cmp2 ? "cmp-error" : undefined}
                  >
                    <option value="">Select dataframe…</option>
                    {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
                  </select>
                </label>
                <button 
                  disabled={cmpLoading || !cmp1 || !cmp2} 
                  onClick={onCompare} 
                  className={`px-6 py-2 text-white rounded-md font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 ${
                    cmpLoading || !cmp1 || !cmp2 
                      ? 'bg-gray-400 cursor-not-allowed' 
                      : 'bg-indigo-600 hover:bg-indigo-700'
                  }`}
                  aria-label="Compare selected dataframes"
                >
                  {cmpLoading ? (
                    <span className="flex items-center gap-2">
                      <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Comparing…
                    </span>
                  ) : 'Compare'}
                </button>
              </div>
              
              {/* Error message */}
              {cmpError && (
                <div id="cmp-error" className="mt-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                  <div className="flex items-center gap-2">
                    <svg className="h-4 w-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <span className="text-sm text-red-800 dark:text-red-200">{cmpError}</span>
                  </div>
                </div>
              )}
            </div>
            
            {/* Loading state */}
            {cmpLoading && (
              <div className="mt-4 p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md">
                <div className="flex items-center gap-3">
                  <img src="/loader.svg" alt="" className="w-6 h-6" role="presentation" />
                  <span className="text-sm text-blue-800 dark:text-blue-200">Running comparison analysis…</span>
                </div>
              </div>
            )}
            
            {/* Previews below choices */}
            {(cmp1 || cmp2) && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
                {cmp1 && <DataframePreview name={cmp1} />}
                {cmp2 && <DataframePreview name={cmp2} />}
              </div>
            )}
            
            {/* Results */}
            {cmpRes && !cmpLoading && (
              <div className="mt-4 p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md">
                <div className="text-sm space-y-2">
                  <div className="font-medium text-green-800 dark:text-green-200">
                    Result: <span className="font-semibold">{cmpRes.identical ? 'Identical' : cmpRes.result_type}</span>
                  </div>
                  {(cmpRes.left_unique > 0) && (
                    <div className="text-green-700 dark:text-green-300">Left unique rows: {cmpRes.left_unique}</div>
                  )}
                  {(cmpRes.right_unique > 0) && (
                    <div className="text-green-700 dark:text-green-300">Right unique rows: {cmpRes.right_unique}</div>
                  )}
                  {(cmpRes.created || []).length > 0 && (
                    <div className="mt-3 flex flex-wrap gap-2">
                      <span className="text-green-700 dark:text-green-300 text-sm">Download results:</span>
                      {(cmpRes.created || []).map(n => (
                        <a key={n} href={buildDownloadCsvUrl(n)} className="text-indigo-600 hover:text-indigo-800 underline text-sm">{n}.csv</a>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}
        </Section>
        </div>

        {/* Data Transformation Section */}
        <div className="space-y-4">
          <div className="flex items-center gap-3 py-2">
            <div className="w-1 h-6 bg-green-500 rounded-full"></div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Data Transformation & Reshaping</h2>
            <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700"></div>
          </div>

        <Section title="Merge multiple DataFrames">
          <div className="space-y-4">
            <div>
              <div className="text-sm font-medium mb-3 text-gray-900 dark:text-gray-100">
                Select DataFrames to merge
                <span className="text-xs text-gray-500 dark:text-gray-400 ml-2">(minimum 2 required)</span>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                {dfOptions.map(o => (
                  <label 
                    key={o.value} 
                    className={`px-3 py-2 rounded-md border cursor-pointer flex items-center gap-2 transition-all duration-200 ${
                      mergeNames.includes(o.value) 
                        ? 'bg-indigo-50 dark:bg-indigo-900/30 border-indigo-400 dark:border-indigo-500 ring-2 ring-indigo-200 dark:ring-indigo-800' 
                        : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-650 hover:border-gray-400 dark:hover:border-gray-500'
                    }`}
                  >
                    <input 
                      type="checkbox" 
                      className="rounded text-indigo-600 focus:ring-indigo-500 focus:ring-offset-2" 
                      checked={mergeNames.includes(o.value)} 
                      onChange={e => {
                        setMergeNames(e.target.checked ? [...mergeNames, o.value] : mergeNames.filter(n => n !== o.value))
                        setMergeError('')
                        setMergeSelectedKeys([]) // Reset join keys when selection changes
                      }}
                      aria-label={`Select ${o.label} for merging`}
                    />
                    <span className="text-sm text-gray-900 dark:text-gray-100 truncate">{o.label}</span>
                  </label>
                ))}
              </div>
            </div>
            
            {/* Error message */}
            {mergeError && (
              <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                <div className="flex items-center gap-2">
                  <svg className="h-4 w-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span className="text-sm text-red-800 dark:text-red-200">{mergeError}</span>
                </div>
              </div>
            )}
            
            {/* Previews for selected */}
            {mergeNames.length > 0 && (
              <div className="space-y-3">
                <div className="text-sm font-medium text-gray-900 dark:text-gray-100">
                  Selected DataFrames ({mergeNames.length})
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {mergeNames.map(n => (<DataframePreview key={n} name={n} />))}
                </div>
              </div>
            )}
            
            <div className="space-y-4">
              {/* Join keys section - full width for better layout */}
              <div>
                <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Join keys</span>
                {getCommonColumns().length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs text-gray-600 dark:text-gray-400">
                      Select common columns ({getCommonColumns().length} available)
                    </div>
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2 max-h-32 overflow-y-auto border border-gray-200 dark:border-gray-700 rounded p-3 bg-gray-50 dark:bg-gray-800">
                      {getCommonColumns().map(col => (
                        <label 
                          key={col} 
                          className={`px-2 py-1 rounded-md border cursor-pointer text-sm transition-all flex items-center ${
                            mergeSelectedKeys.includes(col) 
                              ? 'bg-indigo-50 dark:bg-indigo-900/30 border-indigo-400 dark:border-indigo-500 text-indigo-900 dark:text-indigo-100' 
                              : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-650'
                          }`}
                        >
                          <input 
                            type="checkbox" 
                            className="mr-2 rounded text-indigo-600 focus:ring-indigo-500" 
                            checked={mergeSelectedKeys.includes(col)} 
                            onChange={() => {
                              setMergeSelectedKeys(prev => 
                                prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col]
                              )
                              setMergeError('')
                            }}
                            aria-label={`Select ${col} as join key`}
                          />
                          <span className="truncate">{col}</span>
                        </label>
                      ))}
                    </div>
                    {mergeSelectedKeys.length > 0 && (
                      <div className="text-xs text-gray-600 dark:text-gray-400">
                        Selected: {mergeSelectedKeys.join(', ')}
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="text-sm text-gray-500 dark:text-gray-400 p-3 border border-gray-300 dark:border-gray-600 rounded bg-gray-50 dark:bg-gray-800">
                    {mergeNames.length < 2 ? 'Select at least 2 dataframes to see common columns' : 'No common columns found between selected dataframes'}
                  </div>
                )}
              </div>
              
              {/* Join type section - separate for better layout */}
              <div className="max-w-xs">
                <label className="block">
                  <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Join type</span>
                  <select 
                    className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                    value={mergeHow} 
                    onChange={e => setMergeHow(e.target.value)}
                    aria-label="Select join type"
                  >
                    <option value="inner">Inner join</option>
                    <option value="left">Left join</option>
                    <option value="right">Right join</option>
                    <option value="outer">Outer join</option>
                  </select>
                </label>
              </div>
              <div className="flex justify-end">
                <button 
                  disabled={mergeLoading || mergeNames.length < 2 || mergeSelectedKeys.length === 0} 
                  onClick={onMerge} 
                  className={`px-6 py-2 text-white rounded-md font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 ${
                    mergeLoading || mergeNames.length < 2 || mergeSelectedKeys.length === 0
                      ? 'bg-gray-400 cursor-not-allowed' 
                      : 'bg-indigo-600 hover:bg-indigo-700'
                  }`}
                  aria-label="Merge selected dataframes"
                >
                  {mergeLoading ? (
                    <span className="flex items-center gap-2">
                      <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Merging…
                    </span>
                  ) : 'Merge DataFrames'}
                </button>
              </div>
            </div>
            
            {/* Loading state */}
            {mergeLoading && (
              <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md">
                <div className="flex items-center gap-3">
                  <img src="/loader.svg" alt="" className="w-6 h-6" role="presentation" />
                  <span className="text-sm text-blue-800 dark:text-blue-200">
                    Merging {mergeNames.length} dataframes using {mergeHow} join...
                  </span>
                </div>
              </div>
            )}
          </div>
        </Section>

        <Section title="Pivot">
          <div className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <label className="block">
                <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">DataFrame</span>
                <select 
                  className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                  value={pvName} 
                  onChange={e => {
                    setPvName(e.target.value)
                    // Reset selections when dataframe changes
                    setPvSelectedIndex('')
                    setPvSelectedValuesFrom('')
                    setPlSelectedIdVars('')
                    setPlSelectedValueVars('')
                  }}
                  aria-label="Select dataframe for pivoting"
                >
                  <option value="">Select…</option>
                  {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
                </select>
              </label>
              <label className="block">
                <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Mode</span>
                <select 
                  className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                  value={pvMode} 
                  onChange={e => setPvMode(e.target.value)}
                  aria-label="Select pivot mode"
                >
                  <option value="wider">Wider (pivot table)</option>
                  <option value="longer">Longer (melt)</option>
                </select>
              </label>
            </div>
            
            {pvName && (
              <div className="space-y-4">
                {pvMode === 'wider' ? (
                  <div className="space-y-4">
                    {/* Index column selection */}
                    <div>
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Index column</span>
                        <select 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={pvSelectedIndex} 
                          onChange={e => setPvSelectedIndex(e.target.value)}
                          aria-label="Select index column"
                        >
                          <option value="">Select column…</option>
                          {(dfOptions.find(o => o.value === pvName)?.columns || []).map(c => (
                            <option key={c} value={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                    </div>
                    
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Names from column</span>
                        <select 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={pvNamesFrom} 
                          onChange={e => setPvNamesFrom(e.target.value)}
                          aria-label="Select column for pivot names"
                        >
                          <option value="">Select column…</option>
                          {(dfOptions.find(o => o.value === pvName)?.columns || []).map(c => (
                            <option key={c} value={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                      
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Aggregation function</span>
                        <select 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={pvAgg} 
                          onChange={e => setPvAgg(e.target.value)}
                          aria-label="Select aggregation function"
                        >
                          <option value="first">first</option>
                          <option value="mean">mean</option>
                          <option value="sum">sum</option>
                          <option value="count">count</option>
                          <option value="min">min</option>
                          <option value="max">max</option>
                          <option value="std">std</option>
                          <option value="var">var</option>
                        </select>
                      </label>
                    </div>
                    
                    {/* Values from column selection */}
                    <div>
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Values from column</span>
                        <select 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={pvSelectedValuesFrom} 
                          onChange={e => setPvSelectedValuesFrom(e.target.value)}
                          aria-label="Select values from column"
                        >
                          <option value="">Select column…</option>
                          {(dfOptions.find(o => o.value === pvName)?.columns || []).map(c => (
                            <option key={c} value={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                    </div>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {/* ID variable selection */}
                    <div>
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">ID variable</span>
                        <select 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={plSelectedIdVars} 
                          onChange={e => setPlSelectedIdVars(e.target.value)}
                          aria-label="Select ID variable column"
                        >
                          <option value="">Select column…</option>
                          {(dfOptions.find(o => o.value === pvName)?.columns || []).map(c => (
                            <option key={c} value={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                    </div>
                    
                    {/* Value variable selection */}
                    <div>
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Value variable</span>
                        <select 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={plSelectedValueVars} 
                          onChange={e => setPlSelectedValueVars(e.target.value)}
                          aria-label="Select value variable column"
                        >
                          <option value="">Select column…</option>
                          {(dfOptions.find(o => o.value === pvName)?.columns || []).map(c => (
                            <option key={c} value={c}>{c}</option>
                          ))}
                        </select>
                      </label>
                    </div>
                    
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Variable name</span>
                        <input 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={plVarName} 
                          onChange={e => setPlVarName(e.target.value)}
                          placeholder="variable"
                          aria-label="Enter variable column name"
                        />
                      </label>
                      <label className="block">
                        <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Value name</span>
                        <input 
                          className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                          value={plValueName} 
                          onChange={e => setPlValueName(e.target.value)}
                          placeholder="value"
                          aria-label="Enter value column name"
                        />
                      </label>
                    </div>
                  </div>
                )}
                
                <div className="flex justify-end">
                  <button 
                    onClick={onPivot} 
                    className="px-6 py-2 bg-indigo-600 text-white rounded-md font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
                    disabled={!pvName}
                    aria-label="Run pivot operation"
                  >
                    Run Pivot
                  </button>
                </div>
              </div>
            )}
          </div>
          {/* Preview below df choice */}
          {pvName && (<DataframePreview name={pvName} />)}
        </Section>
        </div>

        {/* Data Filtering & Grouping Section */}
        <div className="space-y-4">
          <div className="flex items-center gap-3 py-2">
            <div className="w-1 h-6 bg-orange-500 rounded-full"></div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Data Filtering & Grouping</h2>
            <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700"></div>
          </div>

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
              <button 
                className="px-6 py-2 bg-indigo-600 text-white rounded-md font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed" 
                onClick={onFilter}
                disabled={!ftName}
              >
                Run Filter
              </button>
            </div>
          </div>
        </Section>

        <Section title="Group by">
          <div className="space-y-4">
            <label className="block max-w-sm">
              <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">DataFrame</span>
              <select 
                className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                value={gbName} 
                onChange={e => {
                  setGbName(e.target.value)
                  setGbSelectedBy([]) // Reset when dataframe changes
                }}
                aria-label="Select dataframe for grouping"
              >
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            
            {/* Column selection for grouping */}
            {gbName && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
                    Group by columns ({gbSelectedBy.length} selected)
                  </span>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setGbSelectedBy(dfOptions.find(o => o.value === gbName)?.columns || [])}
                      className="text-xs px-2 py-1 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
                    >
                      Select All
                    </button>
                    <button
                      onClick={() => setGbSelectedBy([])}
                      className="text-xs px-2 py-1 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
                    >
                      Clear All
                    </button>
                  </div>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2 max-h-40 overflow-y-auto border border-gray-200 dark:border-gray-700 rounded p-3 bg-gray-50 dark:bg-gray-800">
                  {(dfOptions.find(o => o.value === gbName)?.columns || []).map(c => (
                    <label 
                      key={c} 
                      className={`px-2 py-1 rounded-md border cursor-pointer text-sm transition-all ${
                        gbSelectedBy.includes(c) 
                          ? 'bg-indigo-50 dark:bg-indigo-900/30 border-indigo-400 dark:border-indigo-500 text-indigo-900 dark:text-indigo-100' 
                          : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-650'
                      }`}
                    >
                      <input 
                        type="checkbox" 
                        className="mr-2" 
                        checked={gbSelectedBy.includes(c)} 
                        onChange={() => setGbSelectedBy(prev => 
                          prev.includes(c) ? prev.filter(col => col !== c) : [...prev, c]
                        )}
                        aria-label={`Toggle column ${c} for grouping`}
                      />
                      <span className="truncate">{c}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}
            
            <label className="block">
              <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Aggregations JSON (optional)</span>
              <textarea 
                className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono text-xs h-20 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400" 
                value={gbAggs} 
                onChange={e => setGbAggs(e.target.value)} 
                placeholder='{"column_name": "sum", "other_col": ["mean", "max"]}'
                aria-label="Enter aggregation functions as JSON"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Example: {"{"}"col":"sum","col2":["mean","max"]{"}"}
              </p>
            </label>
            
            <div>
              <button 
                onClick={onGroupBy} 
                disabled={!gbName || gbSelectedBy.length === 0}
                className="px-6 py-2 bg-indigo-600 text-white rounded-md font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
                aria-label="Create grouped dataframe"
              >
                Run GroupBy
              </button>
            </div>
          </div>
          {/* Preview below df choice */}
          {gbName && (<DataframePreview name={gbName} />)}
        </Section>
        </div>

        {/* Column Operations Section */}
        <div className="space-y-4">
          <div className="flex items-center gap-3 py-2">
            <div className="w-1 h-6 bg-purple-500 rounded-full"></div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Column Operations</h2>
            <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700"></div>
          </div>

        <Section title="Select columns">
          <div className="space-y-4">
            <label className="block max-w-sm">
              <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">DataFrame</span>
              <select 
                className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                value={selName} 
                onChange={e => setSelName(e.target.value)}
                aria-label="Select dataframe for column selection"
              >
                <option value="">Select dataframe…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            
            {/* Columns chooser */}
            {selName && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
                    Select columns ({selCols.length} selected)
                  </span>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setSelCols(selectedDfMeta?.columns || [])}
                      className="text-xs px-2 py-1 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
                    >
                      Select All
                    </button>
                    <button
                      onClick={() => setSelCols([])}
                      className="text-xs px-2 py-1 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
                    >
                      Clear All
                    </button>
                  </div>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2 max-h-40 overflow-y-auto border border-gray-200 dark:border-gray-700 rounded p-3 bg-gray-50 dark:bg-gray-800">
                  {(selectedDfMeta?.columns || []).map(c => (
                    <label 
                      key={c} 
                      className={`px-2 py-1 rounded-md border cursor-pointer text-sm transition-all ${
                        selCols.includes(c) 
                          ? 'bg-indigo-50 dark:bg-indigo-900/30 border-indigo-400 dark:border-indigo-500 text-indigo-900 dark:text-indigo-100' 
                          : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-650'
                      }`}
                    >
                      <input 
                        type="checkbox" 
                        className="mr-2" 
                        checked={selCols.includes(c)} 
                        onChange={() => toggleSelCol(c)}
                        aria-label={`Toggle column ${c}`}
                      />
                      <span className="truncate">{c}</span>
                    </label>
                  ))}
                </div>
                <label className="inline-flex items-center gap-2">
                  <input 
                    type="checkbox" 
                    checked={selExclude} 
                    onChange={e => setSelExclude(e.target.checked)}
                    className="rounded text-indigo-600 focus:ring-indigo-500"
                  />
                  <span className="text-sm text-gray-900 dark:text-gray-100">
                    Exclude selected columns (keep the rest)
                  </span>
                </label>
              </div>
            )}
            
            {/* Preview filtered to selected columns (or remaining if exclude) */}
            {selName && (<DataframePreview name={selName} columnsFilter={selCols} excludeSelected={selExclude} />)}
            
            <div>
              <button 
                onClick={onSelectCols} 
                disabled={!selName || selCols.length === 0}
                className="px-6 py-2 bg-indigo-600 text-white rounded-md font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
                aria-label="Create new dataframe with selected columns"
              >
                Create Selection
              </button>
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
          <DateTimeSection dfOptions={dfOptions} onRun={async (payload) => { try { const res = await getOperationFunc('datetime')(payload); if (!res.success) throw new Error(res.error||''); toast.show(`Created ${res.name}`); await refresh() } catch (e) { toast.show(e.message || 'Datetime op failed') } }} />
        </Section>

        {/* New: Mutate */}
        <Section title="Mutate (create/overwrite column via expression)">
          <MutateSection dfOptions={dfOptions} onRun={async (payload) => { try { const res = await getOperationFunc('mutate')(payload); if (!res.success) throw new Error(res.error||''); toast.show(`Created ${res.name}`); await refresh() } catch (e) { toast.show(e.message || 'Mutate failed') } }} />
        </Section>
        </div>

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
  const [usePredefined, setUsePredefined] = useState(false)
  const [predefinedOp, setPredefinedOp] = useState('')
  const [sourceCol, setSourceCol] = useState('')

  const selectedDf = dfOptions.find(o => o.value === name)

  // Predefined mutations
  const predefinedMutations = [
    { value: 'uppercase', label: 'Convert to uppercase', expr: "col('{source}').astype(str).str.upper()" },
    { value: 'lowercase', label: 'Convert to lowercase', expr: "col('{source}').astype(str).str.lower()" },
    { value: 'string_length', label: 'String length', expr: "col('{source}').astype(str).str.len()" },
    { value: 'absolute', label: 'Absolute value', expr: "col('{source}').abs()" },
    { value: 'round_2', label: 'Round to 2 decimals', expr: "col('{source}').round(2)" },
    { value: 'is_null', label: 'Check if null', expr: "col('{source}').isnull()" },
    { value: 'fill_zero', label: 'Fill null with 0', expr: "col('{source}').fillna(0)" },
    { value: 'year_from_date', label: 'Extract year from date', expr: "pd.to_datetime(col('{source}')).dt.year" },
    { value: 'month_from_date', label: 'Extract month from date', expr: "pd.to_datetime(col('{source}')).dt.month" },
    { value: 'log', label: 'Natural logarithm', expr: "np.log(col('{source}'))" },
    { value: 'sqrt', label: 'Square root', expr: "np.sqrt(col('{source}'))" },
    { value: 'first_3_chars', label: 'First 3 characters', expr: "col('{source}').astype(str).str[:3]" }
  ]

  // Generate expression from predefined operation
  const generatePredefinedExpr = () => {
    if (!predefinedOp || !sourceCol) return ''
    const mutation = predefinedMutations.find(m => m.value === predefinedOp)
    return mutation ? mutation.expr.replace('{source}', sourceCol) : ''
  }

  // Update expression when predefined selections change
  useEffect(() => {
    if (usePredefined && predefinedOp && sourceCol) {
      setExpr(generatePredefinedExpr())
    }
  }, [usePredefined, predefinedOp, sourceCol])

  const run = () => {
    if (!name || !target || !expr.trim()) return
    const payload = { name, target: target.trim(), expr: expr.trim(), mode, overwrite }
    onRun(payload)
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <label className="block">
          <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">DataFrame</span>
          <select 
            className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
            value={name} 
            onChange={e => {
              setName(e.target.value)
              setSourceCol('') // Reset source column when dataframe changes
            }}
            aria-label="Select dataframe for mutation"
          >
            <option value="">Select…</option>
            {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
          </select>
        </label>
        <label className="block">
          <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Target column</span>
          <input 
            className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
            value={target} 
            onChange={e => setTarget(e.target.value)} 
            placeholder="new_column_name"
            aria-label="Enter target column name"
          />
        </label>
        <label className="block">
          <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Mode</span>
          <select 
            className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
            value={mode} 
            onChange={e => setMode(e.target.value)}
            aria-label="Select mutation mode"
          >
            <option value="vector">Vector (Series/scalar)</option>
            <option value="row">Row (use r["col"])</option>
          </select>
        </label>
      </div>
      
      {name && (<DataframePreview name={name} />)}
      
      {/* Expression input method selector */}
      <div className="space-y-3">
        <div className="flex items-center gap-4">
          <label className="inline-flex items-center gap-2">
            <input 
              type="radio" 
              name="expr-method" 
              checked={!usePredefined} 
              onChange={() => setUsePredefined(false)}
              className="text-indigo-600 focus:ring-indigo-500"
            />
            <span className="text-sm font-medium text-gray-900 dark:text-gray-100">Custom expression</span>
          </label>
          <label className="inline-flex items-center gap-2">
            <input 
              type="radio" 
              name="expr-method" 
              checked={usePredefined} 
              onChange={() => setUsePredefined(true)}
              className="text-indigo-600 focus:ring-indigo-500"
            />
            <span className="text-sm font-medium text-gray-900 dark:text-gray-100">Predefined operations</span>
          </label>
        </div>
        
        {usePredefined ? (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <label className="block">
                <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Source column</span>
                <select 
                  className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                  value={sourceCol} 
                  onChange={e => setSourceCol(e.target.value)}
                  aria-label="Select source column for operation"
                >
                  <option value="">Select column…</option>
                  {(selectedDf?.columns || []).map(c => (<option key={c} value={c}>{c}</option>))}
                </select>
              </label>
              <label className="block">
                <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Operation</span>
                <select 
                  className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                  value={predefinedOp} 
                  onChange={e => setPredefinedOp(e.target.value)}
                  aria-label="Select predefined operation"
                >
                  <option value="">Select operation…</option>
                  {predefinedMutations.map(op => (<option key={op.value} value={op.value}>{op.label}</option>))}
                </select>
              </label>
            </div>
            <div className="p-3 bg-gray-50 dark:bg-gray-800 rounded border border-gray-200 dark:border-gray-700">
              <label className="block">
                <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Generated expression (read-only)</span>
                <textarea 
                  className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono text-xs h-20 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300" 
                  value={generatePredefinedExpr()} 
                  readOnly
                  aria-label="Generated expression preview"
                />
              </label>
            </div>
          </div>
        ) : (
          <div className="space-y-3">
            {/* Expression syntax help */}
            <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md p-4">
              <h4 className="text-sm font-medium text-blue-900 dark:text-blue-100 mb-2">Expression Syntax Guide</h4>
              <div className="text-xs text-blue-800 dark:text-blue-200 space-y-1">
                <div><strong>Vector mode examples:</strong></div>
                <ul className="list-disc list-inside space-y-1 ml-2">
                  <li>Basic operations: <code>col('price') * col('quantity')</code></li>
                  <li>String operations: <code>col('name').astype(str).str.upper()</code></li>
                  <li>Conditional: <code>np.where(col('age') &gt;= 18, 'Adult', 'Minor')</code></li>
                  <li>Date operations: <code>pd.to_datetime(col('date')).dt.year</code></li>
                  <li>Math functions: <code>np.sqrt(col('value'))</code></li>
                </ul>
                <div className="mt-2"><strong>Row mode examples:</strong></div>
                <ul className="list-disc list-inside space-y-1 ml-2">
                  <li>Row operations: <code>r['price'] * r['quantity']</code></li>
                  <li>Complex logic: <code>'High' if r['score'] &gt; 80 else 'Low'</code></li>
                </ul>
              </div>
            </div>
            
            <label className="block">
              <span className="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Expression</span>
              <textarea 
                className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono text-sm h-24 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" 
                value={expr} 
                onChange={e => setExpr(e.target.value)} 
                placeholder="Enter your pandas expression..."
                aria-label="Enter custom expression"
              />
            </label>
          </div>
        )}
      </div>
      
      <div className="flex items-center justify-between">
        <label className="inline-flex items-center gap-2">
          <input 
            type="checkbox" 
            checked={overwrite} 
            onChange={e => setOverwrite(e.target.checked)}
            className="rounded text-indigo-600 focus:ring-indigo-500" 
          />
          <span className="text-sm text-gray-900 dark:text-gray-100">Overwrite if column exists</span>
        </label>
        <button 
          onClick={run} 
          disabled={!name || !target || !expr.trim()}
          className="px-6 py-2 bg-indigo-600 text-white rounded-md font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
          aria-label="Run mutation operation"
        >
          Run Mutate
        </button>
      </div>
    </div>
  )
}
