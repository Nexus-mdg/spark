// filepath: /home/toavina/Apps/spark/dataframe-ui-x/web/src/ChainedOperations.jsx
import React, { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  listDataframes,
  pipelinePreview,
  pipelineRun,
  getDataframe
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

function ParamInput({ op, dfOptions, onCreate }) {
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
        return (
          <div className="space-y-3">
            <div>
              <div className="text-sm mb-1">Pick 2+ dataframes</div>
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
                if (names.length < 2 || keys.length === 0) return
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
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm">Columns (comma)</span>
              <input className="mt-1 border rounded w-full p-2" value={state.columns || ''} onChange={e => update({ columns: e.target.value })} placeholder="id,name,value" />
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
              const columns = String(state.columns||'').split(',').map(s=>s.trim()).filter(Boolean)
              if (columns.length === 0) return
              onCreate({ op: 'select', params: { columns } })
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

export default function ChainedOperations() {
  const [dfs, setDfs] = useState([])
  const [loading, setLoading] = useState(false)
  const [steps, setSteps] = useState([])
  const [autoPreview, setAutoPreview] = useState(true)
  const [preview, setPreview] = useState({ loading: false, error: '', steps: [], final: null })
  const [result, setResult] = useState(null)
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

  useEffect(() => { refresh() }, [])

  const triggerPreview = async () => {
    setPreview(p => ({ ...p, loading: true, error: '' }))
    try {
      const res = await pipelinePreview({ steps, preview_rows: 10 })
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
      const res = await pipelineRun({ steps, materialize: true })
      if (!res.success) throw new Error(res.error || 'Run failed')
      setResult(res.created)
      toast.show(`Created ${res.created?.name || 'result'}`)
      await refresh()
    } catch (e) { toast.show(e.message || 'Run failed') }
  }

  return (
    <div className="bg-gray-50 min-h-screen text-gray-900">
      <header className="bg-slate-900 text-white">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button className="text-white/90 hover:text-white" onClick={() => navigate('/')}>← Home</button>
            <h1 className="text-lg font-semibold">Chained Operations</h1>
          </div>
          <div className="text-sm text-slate-300">{loading ? 'Loading…' : `${dfs.length} dataframes`}</div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        <Section title="Build pipeline">
          <div className="flex items-center gap-3">
            <span className="text-sm">Auto preview</span>
            <input type="checkbox" checked={autoPreview} onChange={e => setAutoPreview(e.target.checked)} />
            <button className="px-3 py-1.5 rounded border" onClick={triggerPreview}>Preview now</button>
            <button className="px-3 py-1.5 rounded border" onClick={clearSteps}>Clear steps</button>
          </div>
          <div className="mt-4 space-y-4">
            <div className="bg-slate-50 border rounded p-3">
              <div className="text-sm font-medium mb-2">Add step</div>
              <AddStep dfOptions={dfOptions} onAdd={addStep} />
            </div>
            {steps.length > 0 && (
              <div className="bg-white border rounded">
                <div className="px-4 py-2 text-sm border-b">Steps</div>
                <ol className="list-decimal pl-6 py-2 space-y-2">
                  {steps.map((s, i) => (
                    <li key={i} className="flex items-start gap-2">
                      <code className="text-xs bg-slate-100 rounded px-1 py-0.5">{s.op}</code>
                      <span className="text-xs text-slate-700 break-all">{JSON.stringify(s.params)}</span>
                      <button className="ml-auto text-red-600 text-xs underline" onClick={() => removeStep(i)}>Remove</button>
                    </li>
                  ))}
                </ol>
              </div>
            )}
            <div className="flex items-center gap-3">
              <button className="px-4 py-2 bg-emerald-600 text-white rounded" onClick={onRun}>Run pipeline</button>
              {result?.name && (
                <span className="text-sm">Created <button className="underline text-indigo-700" onClick={() => navigate(`/analysis/${encodeURIComponent(result.name)}`)}>{result.name}</button></span>
              )}
            </div>
          </div>
        </Section>

        <Section title="Previews">
          {preview.loading && (
            <div className="flex items-center gap-2 text-sm text-slate-600"><img src="/loader.svg" className="w-5 h-5" alt=""/> Generating preview…</div>
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
      </main>

      <div className={`fixed bottom-4 right-4 ${toast.visible ? '' : 'hidden'}`}>
        <div className="bg-slate-900 text-white px-4 py-2 rounded shadow">{toast.msg}</div>
      </div>
    </div>
  )
}

function AddStep({ dfOptions, onAdd }) {
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
        </select>
      </div>
      <ParamInput op={op} dfOptions={dfOptions} onCreate={onAdd} />
    </div>
  )
}

