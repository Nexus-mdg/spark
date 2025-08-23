import React, { useEffect, useMemo, useState, useContext } from 'react'
import { useNavigate } from 'react-router-dom'
import Header from './Header.jsx'
import Pagination from './components/Pagination.jsx'
import Footer from './components/Footer.jsx'
import ConfirmDialog from './components/ConfirmDialog.jsx'
import { EngineContext, ENGINE_INFO } from './contexts/EngineContext.jsx'
import {
  listDataframes,
  pipelinePreview,
  pipelineRun,
  getDataframe,
  pipelinesList,
  pipelineSave,
  pipelineGet,
  pipelineDelete,
  pipelineRunByName,
  buildPipelineExportUrl,
  pipelineImportYaml
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

function SmallTable({ columns = [], rows = [] }) {
  return (
    <div className="overflow-auto border border-gray-200 dark:border-gray-600 rounded">
      <table className="min-w-full text-xs">
        <thead className="bg-slate-100 dark:bg-gray-700">
          <tr>
            {columns.map(c => (<th key={c} className="text-left px-2 py-1 border-b border-gray-200 dark:border-gray-600 whitespace-nowrap text-gray-900 dark:text-gray-100">{c}</th>))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className={i % 2 ? 'bg-white dark:bg-gray-800' : 'bg-slate-50 dark:bg-gray-700'}>
              {columns.map(c => (
                <td key={c} className="px-2 py-1 border-b border-gray-200 dark:border-gray-600 max-w-[300px] truncate text-gray-900 dark:text-gray-100" title={r[c] !== null && r[c] !== undefined ? String(r[c]) : ''}>
                  {r[c] !== null && r[c] !== undefined ? String(r[c]) : ''}
                </td>
              ))}
            </tr>
          ))}
          {rows.length === 0 && (
            <tr><td className="px-2 py-2 text-slate-500 dark:text-gray-400" colSpan={columns.length || 1}>No rows</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

function ParamInput({ op, dfOptions, onCreate, stepCount = 0 }) {
  const [state, setState] = useState({})
  useEffect(() => { setState({}) }, [op])
  const update = (patch) => setState(s => ({ ...s, ...patch }))
  const pickMany = (key, value, checked) => update({ [key]: checked ? ([...(state[key] || []), value]) : (state[key] || []).filter(v => v !== value) })

  const render = () => {
    switch (op) {
      case 'load':
        return (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.name || ''} onChange={e => update({ name: e.target.value })}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => state.name && onCreate({ op: 'load', params: { name: state.name } })}>Add step</button>
          </div>
        )
      case 'merge':
        // Check if we have a current dataframe context (not the first step)
        const hasCurrentDataframe = stepCount > 0;
        const minDataframesRequired = hasCurrentDataframe ? 1 : 2;
        const selectedCount = (state.names || []).length;
        const isAdvancedJoin = state.advancedJoin || false;
        
        return (
          <div className="space-y-3">
            {hasCurrentDataframe && (
              <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded p-3 text-sm">
                <div className="font-medium text-blue-800 dark:text-blue-200">Current dataframe from previous step will be automatically included</div>
                <div className="text-blue-600 dark:text-blue-300 mt-1">Pick 1+ additional dataframes to merge with the current result</div>
              </div>
            )}
            <div>
              <div className="text-sm mb-1 text-gray-900 dark:text-gray-100">
                {hasCurrentDataframe ? 'Pick 1+ additional dataframes' : 'Pick 2+ dataframes'}
              </div>
              <div className="flex flex-wrap gap-2">
                {dfOptions.map(o => (
                  <label key={o.value} className={`px-2 py-1 rounded border cursor-pointer ${((state.names||[]).includes(o.value)) ? 'bg-indigo-50 dark:bg-indigo-900/50 border-indigo-400 dark:border-indigo-500' : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600'} text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600`}>
                    <input type="checkbox" className="mr-1" checked={(state.names||[]).includes(o.value)} onChange={e => pickMany('names', o.value, e.target.checked)} />
                    {o.label}
                  </label>
                ))}
              </div>
            </div>
            
            {/* Advanced Join Toggle */}
            <div className="flex items-center gap-2">
              <input 
                type="checkbox" 
                id="advancedJoin" 
                checked={isAdvancedJoin} 
                onChange={e => update({ advancedJoin: e.target.checked, keys: '', leftOn: '', rightOn: '' })} 
              />
              <label htmlFor="advancedJoin" className="text-sm text-gray-900 dark:text-gray-100 cursor-pointer">
                Advanced Join (different column names)
              </label>
            </div>

            {/* Join Configuration */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
              {!isAdvancedJoin ? (
                // Simple join with same column names
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Join keys (comma)</span>
                  <input 
                    className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" 
                    value={state.keys || ''} 
                    onChange={e => update({ keys: e.target.value })} 
                    placeholder="id" 
                  />
                </label>
              ) : (
                // Advanced join with column mapping
                <>
                  <label className="block">
                    <span className="block text-sm text-gray-900 dark:text-gray-100">Left columns (comma)</span>
                    <input 
                      className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" 
                      value={state.leftOn || ''} 
                      onChange={e => update({ leftOn: e.target.value })} 
                      placeholder="customer_id" 
                    />
                  </label>
                  <label className="block">
                    <span className="block text-sm text-gray-900 dark:text-gray-100">Right columns (comma)</span>
                    <input 
                      className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" 
                      value={state.rightOn || ''} 
                      onChange={e => update({ rightOn: e.target.value })} 
                      placeholder="cust_id" 
                    />
                  </label>
                </>
              )}
              <label className="block">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Join type</span>
                <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.how || 'inner'} onChange={e => update({ how: e.target.value })}>
                  <option>inner</option>
                  <option>left</option>
                  <option>right</option>
                  <option>outer</option>
                </select>
              </label>
              <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
                const names = state.names || []
                if (names.length < minDataframesRequired) return
                
                if (isAdvancedJoin) {
                  // Advanced join with column mapping
                  const leftOn = String(state.leftOn || '').split(',').map(s=>s.trim()).filter(Boolean)
                  const rightOn = String(state.rightOn || '').split(',').map(s=>s.trim()).filter(Boolean)
                  if (leftOn.length === 0 || rightOn.length === 0 || leftOn.length !== rightOn.length) return
                  onCreate({ op: 'merge', params: { names, left_on: leftOn, right_on: rightOn, how: state.how || 'inner' } })
                } else {
                  // Simple join with same column names
                  const keys = String(state.keys || '').split(',').map(s=>s.trim()).filter(Boolean)
                  if (keys.length === 0) return
                  onCreate({ op: 'merge', params: { names, keys, how: state.how || 'inner' } })
                }
              }}>Add step</button>
            </div>
            
            {isAdvancedJoin && (
              <div className="bg-yellow-50 dark:bg-yellow-900/30 border border-yellow-200 dark:border-yellow-700 rounded p-3 text-sm">
                <div className="font-medium text-yellow-800 dark:text-yellow-200">Column Mapping Info</div>
                <div className="text-yellow-700 dark:text-yellow-300 mt-1">
                  Left columns are from the {hasCurrentDataframe ? 'current dataframe' : 'first selected dataframe'}. 
                  Right columns are from the {hasCurrentDataframe ? 'selected dataframe(s)' : 'second dataframe'}.
                </div>
              </div>
            )}
          </div>
        )
      case 'filter':
        return <FilterBuilder onCreate={(filters, combine) => onCreate({ op: 'filter', params: { filters, combine } })} />
      case 'groupby':
        return (
          <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Group by (comma)</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.by || ''} onChange={e => update({ by: e.target.value })} placeholder="country,year" />
            </label>
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Aggregations (JSON)</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.aggs || ''} onChange={e => update({ aggs: e.target.value })} placeholder='{"sales":"sum"}' />
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
              const by = String(state.by||'').split(',').map(s=>s.trim()).filter(Boolean)
              let aggs = undefined
              if (String(state.aggs||'').trim()) { try { aggs = JSON.parse(state.aggs) } catch { return } }
              if (by.length === 0) return
              onCreate({ op: 'groupby', params: { by, aggs } })
            }}>Add step</button>
          </div>
        )
      case 'select':
        return (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Columns (comma)</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.columns || ''} onChange={e => update({ columns: e.target.value })} placeholder="id,name,value" />
            </label>
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={!!state.exclude} onChange={e => update({ exclude: e.target.checked })} />
              <span className="text-sm text-gray-900 dark:text-gray-100">Exclude selected</span>
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
              const columns = String(state.columns||'').split(',').map(s=>s.trim()).filter(Boolean)
              if (columns.length === 0) return
              const params = { columns }
              if (state.exclude) params.exclude = true
              onCreate({ op: 'select', params })
            }}>Add step</button>
          </div>
        )
      case 'rename':
        return (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Mapping (JSON)</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.map || ''} onChange={e => update({ map: e.target.value })} placeholder='{"old":"new"}' />
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
              try { const m = JSON.parse(state.map || '{}'); if (!m || Array.isArray(m)) return; onCreate({ op: 'rename', params: { map: m } }) } catch { return }
            }}>Add step</button>
          </div>
        )
      case 'pivot':
        return <PivotBuilder dfOptions={dfOptions} onCreate={onCreate} />
      case 'compare':
        return (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Other DataFrame</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.other || ''} onChange={e => update({ other: e.target.value })}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Action</span>
              <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.action || 'mismatch'} onChange={e => update({ action: e.target.value })}>
                <option value="mismatch">mismatch (rows)</option>
                <option value="identical">identical (pass/flag)</option>
              </select>
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => state.other && onCreate({ op: 'compare', params: { name: state.other, action: state.action || 'mismatch' } })}>Add step</button>
          </div>
        )
      case 'datetime':
        return (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
              <label className="block md:col-span-2">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Action</span>
                <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.action || 'parse'} onChange={e => update({ action: e.target.value })}>
                  <option value="parse">parse (string → date)</option>
                  <option value="derive">derive parts</option>
                </select>
              </label>
              <label className="block md:col-span-2">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Source column</span>
                <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.source || ''} onChange={e => update({ source: e.target.value })} placeholder="date_col" />
              </label>
            </div>
            { (state.action || 'parse') === 'parse' ? (
              <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Format (optional)</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.format || ''} onChange={e => update({ format: e.target.value })} placeholder="%Y-%m-%d" />
                </label>
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Target (optional)</span>
                  <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.target || ''} onChange={e => update({ target: e.target.value })} placeholder="new_date" />
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={!!state.overwrite} onChange={e => update({ overwrite: e.target.checked })} />
                  <span className="text-sm text-gray-900 dark:text-gray-100">Overwrite if exists</span>
                </label>
                <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
                  if (!state.source) return
                  const payload = { action: 'parse', source: state.source }
                  if (String(state.format||'').trim()) payload.format = state.format
                  if (String(state.target||'').trim()) payload.target = state.target
                  if (state.overwrite) payload.overwrite = true
                  onCreate({ op: 'datetime', params: payload })
                }}>Add step</button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
                <label className="block">
                  <span className="block text-sm text-gray-900 dark:text-gray-100">Month style</span>
                  <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.month_style || 'short'} onChange={e => update({ month_style: e.target.value })}>
                    <option value="short">Jan</option>
                    <option value="short_lower">jan</option>
                    <option value="long">January</option>
                    <option value="num">1..12</option>
                  </select>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'year' in (state.outputs||{}) ? !!state.outputs.year : true} onChange={e => update({ outputs: { ...(state.outputs||{}), year: e.target.checked } })} />
                  <span className="text-sm text-gray-900 dark:text-gray-100">year</span>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'month' in (state.outputs||{}) ? !!state.outputs.month : true} onChange={e => update({ outputs: { ...(state.outputs||{}), month: e.target.checked } })} />
                  <span className="text-sm text-gray-900 dark:text-gray-100">month</span>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'day' in (state.outputs||{}) ? !!state.outputs.day : true} onChange={e => update({ outputs: { ...(state.outputs||{}), day: e.target.checked } })} />
                  <span className="text-sm text-gray-900 dark:text-gray-100">day</span>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'year_month' in (state.outputs||{}) ? !!state.outputs.year_month : true} onChange={e => update({ outputs: { ...(state.outputs||{}), year_month: e.target.checked } })} />
                  <span className="text-sm text-gray-900 dark:text-gray-100">year_month</span>
                </label>
                <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
                  if (!state.source) return
                  const payload = { action: 'derive', source: state.source, month_style: state.month_style || 'short', outputs: state.outputs || { year: true, month: true, day: true, year_month: true } }
                  onCreate({ op: 'datetime', params: payload })
                }}>Add step</button>
              </div>
            )}
          </div>
        )
      case 'mutate':
        return (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
              <label className="block">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Target column</span>
                <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.target || ''} onChange={e => update({ target: e.target.value })} placeholder="new_col" />
              </label>
              <label className="block">
                <span className="block text-sm text-gray-900 dark:text-gray-100">Mode</span>
                <select className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.mode || 'vector'} onChange={e => update({ mode: e.target.value })}>
                  <option value="vector">vector</option>
                  <option value="row">row</option>
                </select>
              </label>
              <label className="inline-flex items-center gap-2">
                <input type="checkbox" checked={!!state.overwrite} onChange={e => update({ overwrite: e.target.checked })} />
                <span className="text-sm text-gray-900 dark:text-gray-100">Overwrite if exists</span>
              </label>
              <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
                const target = String(state.target||'').trim(); const expr = String(state.expr||'').trim();
                if (!target || !expr) return
                onCreate({ op: 'mutate', params: { target, expr, mode: state.mode || 'vector', overwrite: !!state.overwrite } })
              }}>Add step</button>
            </div>
            <label className="block">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Expression</span>
              <textarea className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 font-mono text-xs h-28 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.expr || ''} onChange={e => update({ expr: e.target.value })} placeholder={"Examples:\n- vector: col('a') + col('b')\n- vector: np.where(col('x') > 0, 'pos', 'neg')\n- vector: col('name').astype(str).str[:3] + '_' + col('country')\n- row: r['price'] * r['qty']\n- vector date: pd.to_datetime(col('ts')).dt.year"} />
            </label>
            <div className="text-xs text-gray-600 dark:text-gray-300">Tip: use col('colname') for Series, or r['col'] in row mode. pd and np are available.</div>
          </div>
        )
      default:
        return <div className="text-sm text-gray-600 dark:text-gray-300">Pick an operation</div>
    }
  }

  return (
    <div className="space-y-3">
      {render()}
    </div>
  )
}

function FilterBuilder({ onCreate }) {
  const [filters, setFilters] = useState([{ col: '', op: 'eq', value: '' }])
  const [combine, setCombine] = useState('and')
  const add = () => setFilters([...filters, { col: '', op: 'eq', value: '' }])
  const remove = (idx) => setFilters(filters.filter((_, i) => i !== idx))
  const update = (idx, patch) => setFilters(filters.map((f, i) => i === idx ? { ...f, ...patch } : f))
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-900 dark:text-gray-100">Combine</span>
        <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={combine} onChange={e => setCombine(e.target.value)}>
          <option value="and">and</option>
          <option value="or">or</option>
        </select>
      </div>
      <div className="space-y-2">
        {filters.map((f, idx) => (
          <div key={idx} className="grid grid-cols-1 md:grid-cols-6 gap-2 items-end">
            <input className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" placeholder="column" value={f.col} onChange={e => update(idx, { col: e.target.value })} />
            <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={f.op} onChange={e => update(idx, { op: e.target.value })}>
              <option>eq</option><option>ne</option><option>lt</option><option>lte</option><option>gt</option><option>gte</option>
              <option>in</option><option>nin</option><option>contains</option><option>startswith</option><option>endswith</option><option>isnull</option><option>notnull</option>
            </select>
            <input className="border border-gray-300 dark:border-gray-600 rounded p-2 md:col-span-3 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" placeholder="value (JSON list for in/nin)" value={f.value} onChange={e => update(idx, { value: e.target.value })} />
            <button className="px-3 py-2 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={() => remove(idx)}>Remove</button>
          </div>
        ))}
      </div>
      <div className="flex items-center gap-2">
        <button className="px-3 py-2 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={add}>Add condition</button>
        <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => onCreate(filters, combine)}>Add step</button>
      </div>
    </div>
  )
}

function PivotBuilder({ dfOptions, onCreate }) {
  const [mode, setMode] = useState('wider')
  const [state, setState] = useState({})
  useEffect(() => { setState({}) }, [mode])
  const update = (patch) => setState(s => ({ ...s, ...patch }))
  if (mode === 'wider') {
    return (
      <div className="space-y-3">
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-900 dark:text-gray-100">Mode</span>
          <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={mode} onChange={e => setMode(e.target.value)}>
            <option value="wider">wider</option>
            <option value="longer">longer</option>
          </select>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
          <label className="block">
            <span className="block text-sm text-gray-900 dark:text-gray-100">index (comma)</span>
            <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.index || ''} onChange={e => update({ index: e.target.value })} placeholder="id" />
          </label>
          <label className="block">
            <span className="block text-sm text-gray-900 dark:text-gray-100">names_from</span>
            <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.names_from || ''} onChange={e => update({ names_from: e.target.value })} placeholder="category" />
          </label>
          <label className="block md:col-span-2">
            <span className="block text-sm text-gray-900 dark:text-gray-100">values_from (comma)</span>
            <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.values_from || ''} onChange={e => update({ values_from: e.target.value })} placeholder="value" />
          </label>
          <label className="block">
            <span className="block text-sm text-gray-900 dark:text-gray-100">aggfunc</span>
            <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.aggfunc || 'first'} onChange={e => update({ aggfunc: e.target.value })} placeholder="first" />
          </label>
        </div>
        <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
          const index = String(state.index||'').split(',').map(s=>s.trim()).filter(Boolean)
          if (!state.names_from || !String(state.values_from||'').trim()) return
          const values_from = String(state.values_from).split(',').map(s=>s.trim()).filter(Boolean)
          onCreate({ op: 'pivot', params: { mode: 'wider', index, names_from: state.names_from, values_from, aggfunc: state.aggfunc || 'first' } })
        }}>Add step</button>
      </div>
    )
  }
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-900 dark:text-gray-100">Mode</span>
        <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={mode} onChange={e => setMode(e.target.value)}>
          <option value="wider">wider</option>
          <option value="longer">longer</option>
        </select>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
        <label className="block md:col-span-2">
          <span className="block text-sm text-gray-900 dark:text-gray-100">id_vars (comma)</span>
          <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.id_vars || ''} onChange={e => update({ id_vars: e.target.value })} placeholder="id" />
        </label>
        <label className="block md:col-span-2">
          <span className="block text-sm text-gray-900 dark:text-gray-100">value_vars (comma)</span>
          <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.value_vars || ''} onChange={e => update({ value_vars: e.target.value })} placeholder="v1,v2" />
        </label>
        <label className="block">
          <span className="block text-sm text-gray-900 dark:text-gray-100">var_name</span>
          <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.var_name || 'variable'} onChange={e => update({ var_name: e.target.value })} />
        </label>
        <label className="block md:col-span-2">
          <span className="block text-sm text-gray-900 dark:text-gray-100">value_name</span>
          <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={state.value_name || 'value'} onChange={e => update({ value_name: e.target.value })} />
        </label>
      </div>
      <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
        const id_vars = String(state.id_vars||'').split(',').map(s=>s.trim()).filter(Boolean)
        const value_vars = String(state.value_vars||'').split(',').map(s=>s.trim()).filter(Boolean)
        if (value_vars.length === 0) return
        onCreate({ op: 'pivot', params: { mode: 'longer', id_vars, value_vars, var_name: state.var_name || 'variable', value_name: state.value_name || 'value' } })
      }}>Add step</button>
    </div>
  )
}

export default function ChainedOperations() {
  const { engine } = useContext(EngineContext)
  const [dfs, setDfs] = useState([])
  const [loading, setLoading] = useState(false)
  const [steps, setSteps] = useState([])
  const [autoPreview, setAutoPreview] = useState(true)
  const [preview, setPreview] = useState({ loading: false, error: '', steps: [], final: null })
  const [result, setResult] = useState(null)
  const [pipelines, setPipelines] = useState([])
  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [plName, setPlName] = useState('')
  const [plDesc, setPlDesc] = useState('')
  const [plOverwrite, setPlOverwrite] = useState(false)
  const [importText, setImportText] = useState('')
  
  // Confirmation dialog state
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [pipelineToDelete, setPipelineToDelete] = useState('')
  const [isDeleting, setIsDeleting] = useState(false)
  
  // Pagination state for pipelines
  const [pipelinesCurrentPage, setPipelinesCurrentPage] = useState(1)
  const itemsPerPage = parseInt(import.meta.env.VITE_MAX_ITEMS_PER_PAGE || '15', 10)
  
  const navigate = useNavigate()
  const toast = useToast()

  const dfOptions = useMemo(() => (dfs || []).map(d => ({ value: d.name, label: d.name })), [dfs])

  const refresh = async () => {
    setLoading(true)
    try {
      const res = await listDataframes()
      if (res.success) setDfs((res.dataframes || []).sort((a, b) => a.name.localeCompare(b.name)))
    } finally { setLoading(false) }
  }

  const refreshPipelines = async () => {
    setPipelinesLoading(true)
    try {
      const res = await pipelinesList()
      if (res.success) {
        setPipelines((res.pipelines || []).sort((a,b) => a.name.localeCompare(b.name)))
        setPipelinesCurrentPage(1) // Reset to first page when data changes
      }
    } catch (e) { /* ignore */ }
    finally { setPipelinesLoading(false) }
  }

  // Pagination calculations for pipelines
  const totalPipelines = pipelines.length
  const pipelinesStartIndex = (pipelinesCurrentPage - 1) * itemsPerPage
  const pipelinesEndIndex = pipelinesStartIndex + itemsPerPage
  const paginatedPipelines = pipelines.slice(pipelinesStartIndex, pipelinesEndIndex)

  const handlePipelinesPageChange = (page) => {
    setPipelinesCurrentPage(page)
  }

  useEffect(() => { refresh(); refreshPipelines() }, [])

  const triggerPreview = async () => {
    setPreview(p => ({ ...p, loading: true, error: '' }))
    try {
      const res = await pipelinePreview({ steps, preview_rows: 10, engine })
      if (!res.success) throw new Error(res.error || 'Preview failed')
      setPreview({ loading: false, error: '', steps: res.steps || [], final: res.final || null })
    } catch (e) {
      setPreview({ loading: false, error: e.message || 'Preview failed', steps: [], final: null })
    }
  }

  useEffect(() => { if (autoPreview && steps.length > 0) { triggerPreview() } }, [autoPreview, JSON.stringify(steps)])

  const addStep = (s) => { setSteps(prev => [...prev, s]); setResult(null) }
  const removeStep = (idx) => { setSteps(prev => prev.filter((_, i) => i !== idx)); setResult(null) }
  const clearSteps = () => { setSteps([]); setPreview({ loading: false, error: '', steps: [], final: null }); setResult(null) }

  const onRun = async () => {
    if (steps.length === 0) return toast.show('Add at least one step')
    try {
      const res = await pipelineRun({ steps, materialize: true, engine })
      if (!res.success) throw new Error(res.error || 'Run failed')
      setResult(res.created)
      toast.show(`Created ${res.created?.name || 'result'} (${engine} engine)`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Run failed') }
  }

  const onSavePipeline = async () => {
    if (!plName.trim()) return toast.show('Provide a pipeline name')
    if (steps.length === 0) return toast.show('Nothing to save')
    try {
      const res = await pipelineSave({ name: plName.trim(), description: plDesc, start: null, steps }, { overwrite: plOverwrite })
      if (!res.success) throw new Error(res.error || 'Save failed')
      toast.show(`Saved pipeline ${res.pipeline?.name || plName}`)
      await refreshPipelines()
    } catch (e) { toast.show(e.message || 'Save failed') }
  }

  const onLoadPipeline = async (name) => {
    if (!name) return
    try {
      const res = await pipelineGet(name)
      if (!res.success) throw new Error(res.error || 'Load failed')
      const obj = res.pipeline
      setSteps(obj.steps || [])
      setPlName(obj.name || '')
      setPlDesc(obj.description || '')
      setResult(null)
      toast.show(`Loaded ${obj.name}`)
    } catch (e) { toast.show(e.message || 'Load failed') }
  }

  const onDeletePipeline = async (name) => {
    setPipelineToDelete(name)
    setShowDeleteConfirm(true)
  }

  const handleDeleteConfirm = async () => {
    if (!pipelineToDelete) return
    
    setIsDeleting(true)
    try { 
      await pipelineDelete(pipelineToDelete); 
      toast.show('Deleted'); 
      await refreshPipelines() 
    } catch (e) { 
      toast.show(e.message || 'Delete failed') 
    } finally {
      setIsDeleting(false)
      setShowDeleteConfirm(false)
      setPipelineToDelete('')
    }
  }

  const handleDeleteCancel = () => {
    setShowDeleteConfirm(false)
    setPipelineToDelete('')
  }

  const onRunByName = async (name) => {
    try { 
      const res = await pipelineRunByName(name, { materialize: true, engine }); 
      if (!res.success) throw new Error(res.error || 'Run failed'); 
      toast.show(`Created ${res.created?.name || 'result'} (${engine} engine)`); 
      await refresh() 
    } catch (e) { 
      toast.show(e.message || 'Run failed') 
    }
  }

  const onImportYaml = async () => {
    if (!importText.trim()) return toast.show('Paste YAML first')
    try {
      const res = await pipelineImportYaml({ yaml: importText, overwrite: plOverwrite })
      if (!res.success) throw new Error(res.error || 'Import failed')
      toast.show(`Imported ${res.pipeline?.name}`)
      setImportText('')
      await refreshPipelines()
    } catch (e) {
      toast.show(e.message || 'Import failed')
    }
  }

  return (
    <div className="bg-gray-50 dark:bg-gray-900 min-h-screen text-gray-900 dark:text-gray-100 transition-colors flex flex-col">
      <Header title="Chained Operations">
        <div className="text-sm text-gray-300 dark:text-gray-400">{loading ? 'Loading…' : `${dfs.length} dataframes`}</div>
      </Header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6 flex-grow">
        {/* Explanatory Text */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8 text-emerald-500" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M12.316 3.051a1 1 0 01.633 1.265l-4 12a1 1 0 11-1.898-.632l4-12a1 1 0 011.265-.633zM5.707 6.293a1 1 0 010 1.414L3.414 10l2.293 2.293a1 1 0 11-1.414 1.414l-3-3a1 1 0 010-1.414l3-3a1 1 0 011.414 0zm8.586 0a1 1 0 011.414 0l3 3a1 1 0 010 1.414l-3 3a1 1 0 11-1.414-1.414L16.586 10l-2.293-2.293a1 1 0 010-1.414z" clipRule="evenodd" />
              </svg>
            </div>
            <div>
              <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">Chained Operations Pipeline</h2>
              <p className="text-gray-600 dark:text-gray-300 mb-3">
                Build complex data transformation pipelines by chaining multiple operations together. 
                Each step processes the output from the previous step, allowing you to create sophisticated workflows with live previews.
              </p>
              
              {/* Engine Status Indicator */}
              <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600">
                <div className="flex items-center gap-3">
                  <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    Pipeline Engine:
                  </span>
                  <div className="flex items-center gap-2">
                    <span className="text-lg">{ENGINE_INFO[engine]?.icon}</span>
                    <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                      {ENGINE_INFO[engine]?.name}
                    </span>
                    <span className="text-xs text-gray-500 dark:text-gray-400">
                      {ENGINE_INFO[engine]?.description}
                    </span>
                  </div>
                </div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 text-sm">
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Sequential step execution</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Live pipeline previews</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Save & reuse pipelines</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">YAML import/export</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Complex transformations</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
                  <span className="text-gray-700 dark:text-gray-300">Reproducible workflows</span>
                </div>
              </div>
            </div>
          </div>
        </div>
        {/* Save/Load controls */}
        <Section title="Save / Load pipeline">
          <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Pipeline name</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={plName} onChange={e => setPlName(e.target.value)} placeholder="my-pipeline" />
            </label>
            <label className="block md:col-span-3">
              <span className="block text-sm text-gray-900 dark:text-gray-100">Description</span>
              <input className="mt-1 border border-gray-300 dark:border-gray-600 rounded w-full p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={plDesc} onChange={e => setPlDesc(e.target.value)} placeholder="optional" />
            </label>
            <label className="flex items-center gap-2">
              <input type="checkbox" checked={plOverwrite} onChange={e => setPlOverwrite(e.target.checked)} />
              <span className="text-sm text-gray-900 dark:text-gray-100">Overwrite</span>
            </label>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={onSavePipeline}>Save pipeline</button>
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-900 dark:text-gray-100">Load</span>
              <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" onChange={e => onLoadPipeline(e.target.value)} value="">
                <option value="">Select…</option>
                {pipelines.map(p => (<option key={p.name} value={p.name}>{p.name}</option>))}
              </select>
              <button className="px-3 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={refreshPipelines}>{pipelinesLoading ? '…' : 'Refresh'}</button>
            </div>
          </div>
        </Section>

        {/* Pipeline library */}
        <Section title="Pipelines library">
          {pipelinesLoading && (<div className="text-sm text-gray-600 dark:text-gray-300">Loading pipelines…</div>)}
          {!pipelinesLoading && pipelines.length === 0 && (<div className="text-sm text-gray-600 dark:text-gray-300">No saved pipelines</div>)}
          {pipelines.length > 0 && (
            <>
              <div className="overflow-auto border border-gray-200 dark:border-gray-600 rounded">
                <table className="min-w-full text-sm">
                  <thead className="bg-slate-100 dark:bg-gray-700 text-left">
                    <tr>
                      <th className="px-3 py-2 text-gray-900 dark:text-gray-100">Name</th>
                      <th className="px-3 py-2 text-gray-900 dark:text-gray-100">Steps</th>
                      <th className="px-3 py-2 text-gray-900 dark:text-gray-100">Description</th>
                      <th className="px-3 py-2 text-gray-900 dark:text-gray-100">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginatedPipelines.map(p => (
                      <tr key={p.name} className="border-t border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-800">
                        <td className="px-3 py-2 font-medium text-gray-900 dark:text-gray-100">{p.name}</td>
                        <td className="px-3 py-2 text-gray-900 dark:text-gray-100">{p.steps}</td>
                        <td className="px-3 py-2 text-gray-600 dark:text-gray-300">{p.description || '-'}</td>
                        <td className="px-3 py-2 flex flex-wrap gap-2">
                          <button className="px-2 py-1 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={() => onLoadPipeline(p.name)}>Load</button>
                          <button className="px-2 py-1 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={() => onRunByName(p.name)}>Run</button>
                          <a className="px-2 py-1 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-indigo-700 dark:text-indigo-400 hover:bg-gray-50 dark:hover:bg-gray-600" href={buildPipelineExportUrl(p.name)}>Export YML</a>
                          <button className="px-2 py-1 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-red-600 dark:text-red-400 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={() => onDeletePipeline(p.name)}>Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              
              {/* Pagination for pipelines */}
              <div className="mt-4">
                <Pagination
                  currentPage={pipelinesCurrentPage}
                  totalItems={totalPipelines}
                  itemsPerPage={itemsPerPage}
                  onPageChange={handlePipelinesPageChange}
                />
              </div>
            </>
          )}
          <div className="mt-4">
            <div className="text-sm mb-1">Import from YML</div>
            <textarea className="w-full border border-gray-300 dark:border-gray-600 rounded p-2 font-mono text-xs h-32 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={importText} onChange={e => setImportText(e.target.value)} placeholder="# paste YAML here" />
            <div className="mt-2 flex items-center gap-2">
              <button className="px-3 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={onImportYaml}>Import</button>
              <label className="text-xs text-gray-600 dark:text-gray-300 flex items-center gap-2">
                <input type="checkbox" checked={plOverwrite} onChange={e => setPlOverwrite(e.target.checked)} /> Overwrite existing
              </label>
            </div>
          </div>
        </Section>

        {/* ...existing Build pipeline and Previews sections remain unchanged... */}
        <Section title="Build pipeline">
          <div className="flex items-center gap-3">
            <span className="text-sm text-gray-900 dark:text-gray-100">Auto preview</span>
            <input type="checkbox" checked={autoPreview} onChange={e => setAutoPreview(e.target.checked)} />
            <button className="px-3 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={triggerPreview}>Preview now</button>
            <button className="px-3 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-600" onClick={clearSteps}>Clear steps</button>
          </div>
          <div className="mt-4 space-y-4">
            <div className="bg-slate-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded p-3">
              <div className="text-sm font-medium mb-2 text-gray-900 dark:text-gray-100">Add step</div>
              <AddStep dfOptions={dfOptions} onAdd={addStep} stepCount={steps.length} />
            </div>
            {steps.length > 0 && (
              <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded">
                <div className="px-4 py-2 text-sm border-b border-gray-200 dark:border-gray-600 text-gray-900 dark:text-gray-100">Steps</div>
                <ol className="list-decimal pl-6 py-2 space-y-2">
                  {steps.map((s, i) => (
                    <li key={i} className="flex items-start gap-2">
                      <code className="text-xs bg-slate-100 dark:bg-gray-600 rounded px-1 py-0.5 flex-shrink-0 text-gray-900 dark:text-gray-100">{s.op}</code>
                      <span className="text-xs text-gray-700 dark:text-gray-300 break-words flex-1 min-w-0">{JSON.stringify(s.params)}</span>
                      <button className="text-red-600 dark:text-red-400 text-xs underline flex-shrink-0" onClick={() => removeStep(i)}>Remove</button>
                    </li>
                  ))}
                </ol>
              </div>
            )}
            <div className="flex items-center gap-3">
              <button className="px-4 py-2 bg-emerald-600 text-white rounded" onClick={onRun}>Run pipeline</button>
              {result?.name && (
                <span className="text-sm text-gray-900 dark:text-gray-100">Created <button className="underline text-indigo-700" onClick={() => navigate(`/analysis/${encodeURIComponent(result.name)}`)}>{result.name}</button></span>
              )}
            </div>
          </div>
        </Section>

        <Section title="Previews">
          {preview.loading && (
            <div className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300"><img src="/loader.svg" className="w-5 h-5" alt=""/> Generating preview…</div>
          )}
          {preview.error && (
            <div className="text-sm text-red-600">{preview.error}</div>
          )}
          {!preview.loading && !preview.error && preview.steps.length === 0 && (
            <div className="text-sm text-gray-600 dark:text-gray-300">No preview yet</div>
          )}
          <div className="space-y-4">
            {preview.steps.map((st, i) => (
              <div key={i} className="border border-gray-200 dark:border-gray-600 rounded">
                <div className="px-3 py-2 text-xs text-gray-600 dark:text-gray-300 flex items-center gap-2">
                  <span className="font-medium">Step {i+1}:</span>
                  <span>{st.desc || st.op}</span>
                </div>
                <SmallTable columns={st.columns || []} rows={st.preview || []} />
              </div>
            ))}
            {preview.final && (
              <div className="border border-gray-200 dark:border-gray-600 rounded">
                <div className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 flex items-center justify-between">
                  <span>Final ({preview.final.rows} rows)</span>
                  <span className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
                    <span>{ENGINE_INFO[engine]?.icon}</span>
                    <span>{ENGINE_INFO[engine]?.name} engine</span>
                  </span>
                </div>
                <SmallTable columns={preview.final.columns || []} rows={preview.final.preview || []} />
              </div>
            )}
          </div>
        </Section>
      </main>

      <Footer />

      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>

      <ConfirmDialog
        open={showDeleteConfirm}
        title="Delete Pipeline"
        message={`Are you sure you want to delete the pipeline "${pipelineToDelete}"? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        confirming={isDeleting}
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />
    </div>
  )
}

function AddStep({ dfOptions, onAdd, stepCount }) {
  const [op, setOp] = useState('load')
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-900 dark:text-gray-100">Operation</span>
        <select className="border border-gray-300 dark:border-gray-600 rounded p-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" value={op} onChange={e => setOp(e.target.value)}>
          <option value="load">load</option>
          <option value="merge">merge</option>
          <option value="pivot">pivot</option>
          <option value="filter">filter</option>
          <option value="groupby">groupby</option>
          <option value="select">select</option>
          <option value="rename">rename</option>
          <option value="compare">compare</option>
          <option value="datetime">datetime</option>
          <option value="mutate">mutate</option>
        </select>
      </div>
      <ParamInput op={op} dfOptions={dfOptions} onCreate={onAdd} stepCount={stepCount} />
    </div>
  )
}
