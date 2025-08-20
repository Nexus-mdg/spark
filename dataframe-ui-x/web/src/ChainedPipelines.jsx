import React, { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  listDataframes,
  pipelinePreview,
  pipelineRun,
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
    <section className="bg-white rounded-lg shadow p-5">
      <h2 className="text-base font-semibold mb-4">{title}</h2>
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
    <div className="overflow-auto border rounded">
      <table className="min-w-full text-xs">
        <thead className="bg-slate-100">
          <tr>
            {columns.map(c => (<th key={c} className="text-left px-2 py-1 border-b whitespace-nowrap">{c}</th>))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className={i % 2 ? '' : 'bg-slate-50'}>
              {columns.map(c => (
                <td key={c} className="px-2 py-1 border-b max-w-[300px] truncate" title={r[c] !== null && r[c] !== undefined ? String(r[c]) : ''}>
                  {r[c] !== null && r[c] !== undefined ? String(r[c]) : ''}
                </td>
              ))}
            </tr>
          ))}
          {rows.length === 0 && (
            <tr><td className="px-2 py-2 text-slate-500" colSpan={columns.length || 1}>No rows</td></tr>
          )}
        </tbody>
      </table>
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
        <span className="text-sm">Combine</span>
        <select className="border rounded p-2" value={combine} onChange={e => setCombine(e.target.value)}>
          <option value="and">and</option>
          <option value="or">or</option>
        </select>
      </div>
      <div className="space-y-2">
        {filters.map((f, idx) => (
          <div key={idx} className="grid grid-cols-1 md:grid-cols-6 gap-2 items-end">
            <input className="border rounded p-2" placeholder="column" value={f.col} onChange={e => update(idx, { col: e.target.value })} />
            <select className="border rounded p-2" value={f.op} onChange={e => update(idx, { op: e.target.value })}>
              <option>eq</option><option>ne</option><option>lt</option><option>lte</option><option>gt</option><option>gte</option>
              <option>in</option><option>nin</option><option>contains</option><option>startswith</option><option>endswith</option><option>isnull</option><option>notnull</option>
            </select>
            <input className="border rounded p-2 md:col-span-3" placeholder="value (JSON list for in/nin)" value={f.value} onChange={e => update(idx, { value: e.target.value })} />
            <button className="px-3 py-2 rounded border" onClick={() => remove(idx)}>Remove</button>
          </div>
        ))}
      </div>
      <div className="flex items-center gap-2">
        <button className="px-3 py-2 rounded border" onClick={add}>Add condition</button>
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
          <span className="text-sm">Mode</span>
          <select className="border rounded p-2" value={mode} onChange={e => setMode(e.target.value)}>
            <option value="wider">wider</option>
            <option value="longer">longer</option>
          </select>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
          <label className="block">
            <span className="block text-sm">index (comma)</span>
            <input className="mt-1 border rounded w-full p-2" value={state.index || ''} onChange={e => update({ index: e.target.value })} placeholder="id" />
          </label>
          <label className="block">
            <span className="block text-sm">names_from</span>
            <input className="mt-1 border rounded w-full p-2" value={state.names_from || ''} onChange={e => update({ names_from: e.target.value })} placeholder="category" />
          </label>
          <label className="block md:col-span-2">
            <span className="block text-sm">values_from (comma)</span>
            <input className="mt-1 border rounded w-full p-2" value={state.values_from || ''} onChange={e => update({ values_from: e.target.value })} placeholder="value" />
          </label>
          <label className="block">
            <span className="block text-sm">aggfunc</span>
            <input className="mt-1 border rounded w-full p-2" value={state.aggfunc || 'first'} onChange={e => update({ aggfunc: e.target.value })} placeholder="first" />
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
        <span className="text-sm">Mode</span>
        <select className="border rounded p-2" value={mode} onChange={e => setMode(e.target.value)}>
          <option value="wider">wider</option>
          <option value="longer">longer</option>
        </select>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
        <label className="block md:col-span-2">
          <span className="block text-sm">id_vars (comma)</span>
          <input className="mt-1 border rounded w-full p-2" value={state.id_vars || ''} onChange={e => update({ id_vars: e.target.value })} placeholder="id" />
        </label>
        <label className="block md:col-span-2">
          <span className="block text-sm">value_vars (comma)</span>
          <input className="mt-1 border rounded w-full p-2" value={state.value_vars || ''} onChange={e => update({ value_vars: e.target.value })} placeholder="v1,v2" />
        </label>
        <label className="block">
          <span className="block text-sm">var_name</span>
          <input className="mt-1 border rounded w-full p-2" value={state.var_name || 'variable'} onChange={e => update({ var_name: e.target.value })} />
        </label>
        <label className="block md:col-span-2">
          <span className="block text-sm">value_name</span>
          <input className="mt-1 border rounded w-full p-2" value={state.value_name || 'value'} onChange={e => update({ value_name: e.target.value })} />
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

function ChainedPipelineStep({ step, index, availablePipelines, onRemove, onChainPipeline }) {
  const [showChainOptions, setShowChainOptions] = useState(false)
  const [selectedPipeline, setSelectedPipeline] = useState('')

  const handleChainPipeline = () => {
    if (selectedPipeline) {
      onChainPipeline(index, selectedPipeline)
      setSelectedPipeline('')
      setShowChainOptions(false)
    }
  }

  return (
    <div className="border rounded p-3 bg-slate-50">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className="font-medium text-sm">Step {index + 1}:</span>
          <code className="text-xs bg-slate-200 rounded px-1 py-0.5">{step.op}</code>
          <span className="text-xs text-slate-600">{JSON.stringify(step.params)}</span>
        </div>
        <div className="flex items-center gap-2">
          <button 
            className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
            onClick={() => setShowChainOptions(!showChainOptions)}
          >
            Chain Pipeline
          </button>
          <button 
            className="text-xs px-2 py-1 text-red-600 hover:bg-red-50 rounded"
            onClick={() => onRemove(index)}
          >
            Remove
          </button>
        </div>
      </div>
      
      {step.chainedPipelines && step.chainedPipelines.length > 0 && (
        <div className="mt-2 pl-4 border-l-2 border-blue-300">
          <div className="text-xs text-blue-700 mb-1">Chained pipelines:</div>
          {step.chainedPipelines.map((chainedName, idx) => (
            <div key={idx} className="text-xs bg-blue-50 rounded px-2 py-1 mb-1 flex items-center justify-between">
              <span>{chainedName}</span>
              <button 
                className="text-red-500 hover:text-red-700"
                onClick={() => {
                  const updated = step.chainedPipelines.filter((_, i) => i !== idx)
                  onChainPipeline(index, null, updated)
                }}
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}

      {showChainOptions && (
        <div className="mt-2 p-2 bg-white border rounded">
          <div className="text-xs mb-2">Attach pipeline to run at this point:</div>
          <div className="flex items-center gap-2">
            <select 
              className="text-xs border rounded p-1 flex-1"
              value={selectedPipeline}
              onChange={e => setSelectedPipeline(e.target.value)}
            >
              <option value="">Select pipeline...</option>
              {availablePipelines.map(p => (
                <option key={p.name} value={p.name}>{p.name}</option>
              ))}
            </select>
            <button 
              className="text-xs px-2 py-1 bg-blue-600 text-white rounded hover:bg-blue-700"
              onClick={handleChainPipeline}
              disabled={!selectedPipeline}
            >
              Attach
            </button>
            <button 
              className="text-xs px-2 py-1 border rounded hover:bg-gray-50"
              onClick={() => setShowChainOptions(false)}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
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
              <span className="block text-sm">DataFrame</span>
              <select className="mt-1 border rounded w-full p-2" value={state.name || ''} onChange={e => update({ name: e.target.value })}>
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
        
        return (
          <div className="space-y-3">
            {hasCurrentDataframe && (
              <div className="bg-blue-50 border border-blue-200 rounded p-3 text-sm">
                <div className="font-medium text-blue-800">Current dataframe from previous step will be automatically included</div>
                <div className="text-blue-600">Select additional dataframe(s) to merge with</div>
              </div>
            )}
            <div>
              <div className="text-sm mb-1">
                {hasCurrentDataframe ? `Pick ${minDataframesRequired}+ additional dataframes` : `Pick ${minDataframesRequired}+ dataframes`}
              </div>
              <div className="flex flex-wrap gap-2">
                {dfOptions.map(o => (
                  <label key={o.value} className={`px-2 py-1 rounded border cursor-pointer ${((state.names||[]).includes(o.value)) ? 'bg-indigo-50 border-indigo-400' : 'bg-white'}`}>
                    <input type="checkbox" className="mr-1" checked={(state.names||[]).includes(o.value)} onChange={e => pickMany('names', o.value, e.target.checked)} />
                    {o.label}
                  </label>
                ))}
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
              <label className="block">
                <span className="block text-sm">Join keys (comma)</span>
                <input className="mt-1 border rounded w-full p-2" value={state.keys || ''} onChange={e => update({ keys: e.target.value })} placeholder="id" />
              </label>
              <label className="block">
                <span className="block text-sm">Join type</span>
                <select className="mt-1 border rounded w-full p-2" value={state.how || 'inner'} onChange={e => update({ how: e.target.value })}>
                  <option>inner</option>
                  <option>left</option>
                  <option>right</option>
                  <option>outer</option>
                </select>
              </label>
              <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
                const names = state.names || []
                const keys = String(state.keys || '').split(',').map(s=>s.trim()).filter(Boolean)
                if (selectedCount < minDataframesRequired || keys.length === 0) return
                onCreate({ op: 'merge', params: { names, keys, how: state.how || 'inner' } })
              }}>Add step</button>
            </div>
          </div>
        )
      case 'filter':
        return <FilterBuilder onCreate={(filters, combine) => onCreate({ op: 'filter', params: { filters, combine } })} />
      case 'groupby':
        return (
          <div className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm">Group by (comma)</span>
              <input className="mt-1 border rounded w-full p-2" value={state.by || ''} onChange={e => update({ by: e.target.value })} placeholder="country,year" />
            </label>
            <label className="block md:col-span-2">
              <span className="block text-sm">Aggregations (JSON)</span>
              <input className="mt-1 border rounded w-full p-2 font-mono" value={state.aggs || ''} onChange={e => update({ aggs: e.target.value })} placeholder='{"sales":"sum"}' />
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
              <span className="block text-sm">Columns (comma)</span>
              <input className="mt-1 border rounded w-full p-2" value={state.columns || ''} onChange={e => update({ columns: e.target.value })} placeholder="id,name,value" />
            </label>
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={!!state.exclude} onChange={e => update({ exclude: e.target.checked })} />
              <span className="text-sm">Exclude selected</span>
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
              <span className="block text-sm">Mapping (JSON)</span>
              <input className="mt-1 border rounded w-full p-2 font-mono" value={state.map || ''} onChange={e => update({ map: e.target.value })} placeholder='{"old":"new"}' />
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
              <span className="block text-sm">Other DataFrame</span>
              <select className="mt-1 border rounded w-full p-2" value={state.other || ''} onChange={e => update({ other: e.target.value })}>
                <option value="">Select…</option>
                {dfOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
              </select>
            </label>
            <label className="block">
              <span className="block text-sm">Action</span>
              <select className="mt-1 border rounded w-full p-2" value={state.action || 'mismatch'} onChange={e => update({ action: e.target.value })}>
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
                <span className="block text-sm">Action</span>
                <select className="mt-1 border rounded w-full p-2" value={state.action || 'parse'} onChange={e => update({ action: e.target.value })}>
                  <option value="parse">parse (string -&gt; date)</option>
                  <option value="derive">derive parts</option>
                </select>
              </label>
              <label className="block md:col-span-2">
                <span className="block text-sm">Source column</span>
                <input className="mt-1 border rounded w-full p-2" value={state.source || ''} onChange={e => update({ source: e.target.value })} placeholder="date_col" />
              </label>
            </div>
            { (state.action || 'parse') === 'parse' ? (
              <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
                <label className="block">
                  <span className="block text-sm">Format (optional)</span>
                  <input className="mt-1 border rounded w-full p-2" value={state.format || ''} onChange={e => update({ format: e.target.value })} placeholder="%Y-%m-%d" />
                </label>
                <label className="block">
                  <span className="block text-sm">Target (optional)</span>
                  <input className="mt-1 border rounded w-full p-2" value={state.target || ''} onChange={e => update({ target: e.target.value })} placeholder="new_date" />
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={!!state.overwrite} onChange={e => update({ overwrite: e.target.checked })} />
                  <span className="text-sm">Overwrite if exists</span>
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
                  <span className="block text-sm">Month style</span>
                  <select className="mt-1 border rounded w-full p-2" value={state.month_style || 'short'} onChange={e => update({ month_style: e.target.value })}>
                    <option value="short">Jan</option>
                    <option value="short_lower">jan</option>
                    <option value="long">January</option>
                    <option value="num">1..12</option>
                  </select>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'year' in (state.outputs||{}) ? !!state.outputs.year : true} onChange={e => update({ outputs: { ...(state.outputs||{}), year: e.target.checked } })} />
                  <span className="text-sm">year</span>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'month' in (state.outputs||{}) ? !!state.outputs.month : true} onChange={e => update({ outputs: { ...(state.outputs||{}), month: e.target.checked } })} />
                  <span className="text-sm">month</span>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'day' in (state.outputs||{}) ? !!state.outputs.day : true} onChange={e => update({ outputs: { ...(state.outputs||{}), day: e.target.checked } })} />
                  <span className="text-sm">day</span>
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={'year_month' in (state.outputs||{}) ? !!state.outputs.year_month : true} onChange={e => update({ outputs: { ...(state.outputs||{}), year_month: e.target.checked } })} />
                  <span className="text-sm">year_month</span>
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
                <span className="block text-sm">Target column</span>
                <input className="mt-1 border rounded w-full p-2" value={state.target || ''} onChange={e => update({ target: e.target.value })} placeholder="new_col" />
              </label>
              <label className="block">
                <span className="block text-sm">Mode</span>
                <select className="mt-1 border rounded w-full p-2" value={state.mode || 'vector'} onChange={e => update({ mode: e.target.value })}>
                  <option value="vector">vector</option>
                  <option value="row">row</option>
                </select>
              </label>
              <label className="inline-flex items-center gap-2">
                <input type="checkbox" checked={!!state.overwrite} onChange={e => update({ overwrite: e.target.checked })} />
                <span className="text-sm">Overwrite if exists</span>
              </label>
              <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
                const target = String(state.target||'').trim(); const expr = String(state.expr||'').trim();
                if (!target || !expr) return
                onCreate({ op: 'mutate', params: { target, expr, mode: state.mode || 'vector', overwrite: !!state.overwrite } })
              }}>Add step</button>
            </div>
            <label className="block">
              <span className="block text-sm">Expression</span>
              <textarea className="mt-1 border rounded w-full p-2 font-mono text-xs h-28" value={state.expr || ''} onChange={e => update({ expr: e.target.value })} placeholder={"Examples:\n- vector: col('a') + col('b')\n- vector: np.where(col('x') > 0, 'pos', 'neg')\n- vector: col('name').astype(str).str[:3] + '_' + col('country')\n- row: r['price'] * r['qty']\n- vector date: pd.to_datetime(col('ts')).dt.year"} />
            </label>
            <div className="text-xs text-slate-600">Tip: use col('colname') for Series, or r['col'] in row mode. pd and np are available.</div>
          </div>
        )
      default:
        return <div className="text-sm text-slate-600">Pick an operation</div>
    }
  }

  return (
    <div className="space-y-3">
      {render()}
    </div>
  )
}

export default function ChainedPipelines() {
  const [dfs, setDfs] = useState([])
  const [loading, setLoading] = useState(false)
  const [steps, setSteps] = useState([])
  const [autoPreview, setAutoPreview] = useState(true)
  const [pipelines, setPipelines] = useState([])
  const [pipelinesLoading, setPipelinesLoading] = useState(false)
  const [plName, setPlName] = useState('')
  const [plDesc, setPlDesc] = useState('')
  const [plOverwrite, setPlOverwrite] = useState(false)
  const [result, setResult] = useState(null)
  const [preview, setPreview] = useState({ loading: false, error: '', steps: [], final: null })
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
      if (res.success) setPipelines((res.pipelines || []).sort((a,b) => a.name.localeCompare(b.name)))
    } catch (e) { /* ignore */ }
    finally { setPipelinesLoading(false) }
  }

  useEffect(() => { refresh(); refreshPipelines() }, [])

  useEffect(() => { 
    if (autoPreview && steps.length > 0) { 
      triggerPreview() 
    } else if (steps.length === 0) {
      setPreview({ loading: false, error: '', steps: [], final: null })
    }
  }, [autoPreview, JSON.stringify(steps)])

  const addStep = (s) => { 
    setSteps(prev => [...prev, { ...s, chainedPipelines: [] }])
    setResult(null)
  }

  const removeStep = (idx) => { 
    setSteps(prev => prev.filter((_, i) => i !== idx))
    setResult(null)
  }

  const handleChainPipeline = (stepIndex, pipelineName, updatedChained = null) => {
    setSteps(prev => prev.map((step, idx) => {
      if (idx === stepIndex) {
        if (updatedChained !== null) {
          return { ...step, chainedPipelines: updatedChained }
        } else if (pipelineName) {
          return { ...step, chainedPipelines: [...(step.chainedPipelines || []), pipelineName] }
        }
      }
      return step
    }))
  }

  const clearSteps = () => { 
    setSteps([])
    setPreview({ loading: false, error: '', steps: [], final: null })
    setResult(null)
  }

  const convertToExecutableSteps = (stepsWithChains) => {
    const executableSteps = []
    
    stepsWithChains.forEach(step => {
      // Add the main step
      executableSteps.push({ op: step.op, params: step.params })
      
      // Add chained pipeline steps
      if (step.chainedPipelines && step.chainedPipelines.length > 0) {
        step.chainedPipelines.forEach(pipelineName => {
          executableSteps.push({
            op: 'chain_pipeline',
            params: { pipeline: pipelineName }
          })
        })
      }
    })
    
    return executableSteps
  }

  const onRun = async () => {
    if (steps.length === 0) return toast.show('Add at least one step')
    try {
      const executableSteps = convertToExecutableSteps(steps)
      const res = await pipelineRun({ steps: executableSteps, materialize: true })
      if (!res.success) throw new Error(res.error || 'Run failed')
      setResult(res.created)
      toast.show(`Created ${res.created?.name || 'result'}`)
      await refresh()
    } catch (e) { 
      toast.show(e.message || 'Run failed') 
    }
  }

  const onSavePipeline = async () => {
    if (!plName.trim()) return toast.show('Provide a pipeline name')
    if (steps.length === 0) return toast.show('Nothing to save')
    try {
      const executableSteps = convertToExecutableSteps(steps)
      const res = await pipelineSave({ 
        name: plName.trim(), 
        description: plDesc, 
        start: null, 
        steps: executableSteps,
        chainedStructure: steps // Store the UI structure for editing later
      }, { overwrite: plOverwrite })
      if (!res.success) throw new Error(res.error || 'Save failed')
      toast.show(`Saved chained pipeline ${res.pipeline?.name || plName}`)
      await refreshPipelines()
    } catch (e) { toast.show(e.message || 'Save failed') }
  }

  const triggerPreview = async () => {
    if (steps.length === 0) return
    setPreview(p => ({ ...p, loading: true, error: '' }))
    try {
      const executableSteps = convertToExecutableSteps(steps)
      const res = await pipelinePreview({ steps: executableSteps, preview_rows: 10 })
      if (!res.success) throw new Error(res.error || 'Preview failed')
      setPreview({ loading: false, error: '', steps: res.steps || [], final: res.final || null })
      
      // Check if any steps have chained pipelines - if so, refresh dataframe list
      // because chained pipeline results may have been saved during preview
      const hasChainedPipelines = steps.some(step => step.chainedPipelines && step.chainedPipelines.length > 0)
      if (hasChainedPipelines) {
        await refresh()
      }
    } catch (e) {
      setPreview({ loading: false, error: e.message || 'Preview failed', steps: [], final: null })
    }
  }

  return (
    <div className="bg-gray-50 min-h-screen text-gray-900">
      <header className="bg-slate-900 text-white">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button className="text-white/90 hover:text-white" onClick={() => navigate('/')}>← Home</button>
            <h1 className="text-lg font-semibold">Chained Pipelines</h1>
          </div>
          <div className="text-sm text-slate-300">{loading ? 'Loading…' : `${dfs.length} dataframes`}</div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        {/* Info Section */}
        <Section title="About Chained Pipelines">
          <div className="text-sm text-slate-600 space-y-2">
            <p>Chained pipelines allow you to attach secondary pipelines to any step in your main pipeline. 
               The secondary pipelines will execute at that point using the current data state and their results 
               can be utilized by subsequent steps.</p>
            <p><strong>Key features:</strong></p>
            <ul className="list-disc pl-5 space-y-1">
              <li>Attach multiple pipelines to any step</li>
              <li>Secondary pipelines receive current dataframe state</li>
              <li>Results from chained pipelines are available to following steps</li>
              <li>Support for complex data processing workflows</li>
            </ul>
          </div>
        </Section>

        {/* Save/Load controls */}
        <Section title="Save / Load chained pipeline">
          <div className="grid grid-cols-1 md:grid-cols-6 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm">Pipeline name</span>
              <input className="mt-1 border rounded w-full p-2" value={plName} onChange={e => setPlName(e.target.value)} placeholder="my-chained-pipeline" />
            </label>
            <label className="block md:col-span-3">
              <span className="block text-sm">Description</span>
              <input className="mt-1 border rounded w-full p-2" value={plDesc} onChange={e => setPlDesc(e.target.value)} placeholder="optional description" />
            </label>
            <label className="flex items-center gap-2">
              <input type="checkbox" checked={plOverwrite} onChange={e => setPlOverwrite(e.target.checked)} />
              <span className="text-sm">Overwrite</span>
            </label>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={onSavePipeline}>Save chained pipeline</button>
          </div>
        </Section>

        {/* Build chained pipeline */}
        <Section title="Build chained pipeline">
          <div className="flex items-center gap-3 mb-4">
            <span className="text-sm">Auto preview</span>
            <input type="checkbox" checked={autoPreview} onChange={e => setAutoPreview(e.target.checked)} />
            <button className="px-3 py-1.5 rounded border" onClick={triggerPreview}>Preview now</button>
            <button className="px-3 py-1.5 rounded border" onClick={clearSteps}>Clear all steps</button>
            <span className="text-sm text-slate-600">{steps.length} steps</span>
          </div>
          
          <div className="space-y-4">
            <div className="bg-slate-50 border rounded p-3">
              <div className="text-sm font-medium mb-2">Add step</div>
              <AddStep dfOptions={dfOptions} onAdd={addStep} stepCount={steps.length} />
            </div>
            
            {steps.length > 0 && (
              <div className="space-y-3">
                {steps.map((step, index) => (
                  <ChainedPipelineStep
                    key={index}
                    step={step}
                    index={index}
                    availablePipelines={pipelines}
                    onRemove={removeStep}
                    onChainPipeline={handleChainPipeline}
                  />
                ))}
              </div>
            )}
            
            <div className="flex items-center gap-3">
              <button className="px-4 py-2 bg-emerald-600 text-white rounded" onClick={onRun}>Run chained pipeline</button>
              {result?.name && (
                <span className="text-sm">Created <button className="underline text-indigo-700" onClick={() => navigate(`/analysis/${encodeURIComponent(result.name)}`)}>{result.name}</button></span>
              )}
            </div>
          </div>
        </Section>

        {/* Preview Section */}
        <Section title="Preview">
          {preview.loading && (
            <div className="flex items-center gap-2 text-sm text-slate-600">
              <div className="w-4 h-4 border-2 border-slate-400 border-t-transparent rounded-full animate-spin"></div>
              Generating preview…
            </div>
          )}
          {preview.error && (
            <div className="text-sm text-red-600">{preview.error}</div>
          )}
          {!preview.loading && !preview.error && preview.steps.length === 0 && (
            <div className="text-sm text-slate-600">No preview yet</div>
          )}
          <div className="space-y-4">
            {preview.steps.map((st, i) => (
              <div key={i} className="border rounded">
                <div className="px-3 py-2 text-xs text-slate-600 flex items-center gap-2">
                  <span className="font-medium">Step {i+1}:</span>
                  <span>{st.desc || st.op}</span>
                </div>
                <SmallTable columns={st.columns || []} rows={st.preview || []} />
              </div>
            ))}
            {preview.final && (
              <div className="border rounded">
                <div className="px-3 py-2 text-xs text-slate-700">Final ({preview.final.rows} rows)</div>
                <SmallTable columns={preview.final.columns || []} rows={preview.final.preview || []} />
              </div>
            )}
          </div>
        </Section>

        {/* Available Pipelines */}
        <Section title="Available pipelines">
          {pipelinesLoading && (<div className="text-sm text-slate-600">Loading pipelines…</div>)}
          {!pipelinesLoading && pipelines.length === 0 && (<div className="text-sm text-slate-600">No saved pipelines</div>)}
          {pipelines.length > 0 && (
            <div className="overflow-auto border rounded">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-100 text-left">
                  <tr>
                    <th className="px-3 py-2">Name</th>
                    <th className="px-3 py-2">Steps</th>
                    <th className="px-3 py-2">Description</th>
                  </tr>
                </thead>
                <tbody>
                  {pipelines.map(p => (
                    <tr key={p.name} className="border-t">
                      <td className="px-3 py-2 font-medium">{p.name}</td>
                      <td className="px-3 py-2">{p.steps}</td>
                      <td className="px-3 py-2 text-slate-600">{p.description || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Section>
      </main>

      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>
    </div>
  )
}

function AddStep({ dfOptions, onAdd, stepCount }) {
  const [op, setOp] = useState('load')
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <span className="text-sm">Operation</span>
        <select className="border rounded p-2" value={op} onChange={e => setOp(e.target.value)}>
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