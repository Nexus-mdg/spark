import React, { useState, useEffect } from 'react'
import { getDataframe } from '../api.js'

// DataframePreview component for showing preview when dataframe is selected
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
        <div className="px-3 py-3 text-sm text-slate-600 dark:text-gray-300 flex items-center gap-2"><span>⏳</span> Loading…</div>
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
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

/**
 * Enhanced Mutate Builder Component
 * 
 * A reusable component for building mutate operations with:
 * - Better layout with grid system
 * - Mode selection (vector/row)
 * - Expression method selector (Custom vs Predefined)
 * - Predefined operations with source column selection
 * - Expression syntax guide
 * - Better user guidance and validation
 * 
 * @param {Object} props
 * @param {Array} props.dfOptions - Available dataframe options
 * @param {string} props.selectedDataframe - Pre-selected dataframe (optional)
 * @param {Function} props.onSubmit - Callback when mutation is submitted
 * @param {string} props.submitLabel - Label for submit button (default: "Run Mutate")
 * @param {boolean} props.showDataframeSelector - Whether to show dataframe selector (default: true)
 * @param {boolean} props.showPreview - Whether to show dataframe preview (default: true)
 * @param {Object} props.initialState - Initial state for the form (optional)
 */
export default function MutateBuilder({
  dfOptions = [],
  selectedDataframe = '',
  onSubmit = () => {},
  submitLabel = 'Run Mutate',
  showDataframeSelector = true,
  showPreview = true,
  initialState = {}
}) {
  const [name, setName] = useState(initialState.name || selectedDataframe)
  const [target, setTarget] = useState(initialState.target || '')
  const [expr, setExpr] = useState(initialState.expr || '')
  const [mode, setMode] = useState(initialState.mode || 'vector')
  const [overwrite, setOverwrite] = useState(initialState.overwrite || false)
  const [usePredefined, setUsePredefined] = useState(initialState.usePredefined || false)
  const [predefinedOp, setPredefinedOp] = useState(initialState.predefinedOp || '')
  const [sourceCol, setSourceCol] = useState(initialState.sourceCol || '')

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

  // Reset source column when dataframe changes
  useEffect(() => {
    if (showDataframeSelector) {
      setSourceCol('')
    }
  }, [name, showDataframeSelector])

  const handleSubmit = () => {
    if (!name && showDataframeSelector) return
    if (!target || !expr.trim()) return
    
    const payload = {
      ...(showDataframeSelector && { name }),
      target: target.trim(),
      expr: expr.trim(),
      mode,
      overwrite
    }
    
    onSubmit(payload)
  }

  return (
    <div className="space-y-4">
      <div className={`grid grid-cols-1 ${showDataframeSelector ? 'md:grid-cols-3' : 'md:grid-cols-2'} gap-4`}>
        {showDataframeSelector && (
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
        )}
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
      
      {showPreview && name && (<DataframePreview name={name} />)}
      
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
          onClick={handleSubmit} 
          disabled={(!name && showDataframeSelector) || !target || !expr.trim()}
          className="px-6 py-2 bg-indigo-600 text-white rounded-md font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
          aria-label="Run mutation operation"
        >
          {submitLabel}
        </button>
      </div>
    </div>
  )
}