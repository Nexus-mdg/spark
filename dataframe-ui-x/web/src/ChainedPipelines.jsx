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
      case 'filter':
        return (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
            <label className="block">
              <span className="block text-sm">Column</span>
              <input className="mt-1 border rounded w-full p-2" value={state.column || ''} onChange={e => update({ column: e.target.value })} placeholder="column_name" />
            </label>
            <label className="block">
              <span className="block text-sm">Operation</span>
              <select className="mt-1 border rounded w-full p-2" value={state.op || 'eq'} onChange={e => update({ op: e.target.value })}>
                <option value="eq">equals</option>
                <option value="ne">not equals</option>
                <option value="gt">greater than</option>
                <option value="lt">less than</option>
              </select>
            </label>
            <label className="block">
              <span className="block text-sm">Value</span>
              <input className="mt-1 border rounded w-full p-2" value={state.value || ''} onChange={e => update({ value: e.target.value })} placeholder="value" />
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
              if (state.column && state.value) {
                onCreate({ op: 'filter', params: { filters: [{ col: state.column, op: state.op || 'eq', value: state.value }], combine: 'and' } })
              }
            }}>Add step</button>
          </div>
        )
      case 'select':
        return (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <label className="block md:col-span-2">
              <span className="block text-sm">Columns (comma separated)</span>
              <input className="mt-1 border rounded w-full p-2" value={state.columns || ''} onChange={e => update({ columns: e.target.value })} placeholder="col1,col2,col3" />
            </label>
            <button className="px-4 py-2 bg-indigo-600 text-white rounded" onClick={() => {
              const columns = String(state.columns || '').split(',').map(s => s.trim()).filter(Boolean)
              if (columns.length > 0) {
                onCreate({ op: 'select', params: { columns } })
              }
            }}>Add step</button>
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
            <button className="px-3 py-1.5 rounded border" onClick={triggerPreview}>Preview</button>
            <button className="px-3 py-1.5 rounded border" onClick={clearSteps}>Clear all steps</button>
            <span className="text-sm text-slate-600">{steps.length} steps</span>
          </div>
          
          <div className="space-y-4">
            <div className="bg-slate-50 border rounded p-3">
              <div className="text-sm font-medium mb-2">Add step</div>
              <AddStep dfOptions={dfOptions} onAdd={addStep} />
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

function AddStep({ dfOptions, onAdd }) {
  const [op, setOp] = useState('load')
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <span className="text-sm">Operation</span>
        <select className="border rounded p-2" value={op} onChange={e => setOp(e.target.value)}>
          <option value="load">load</option>
          <option value="filter">filter</option>
          <option value="select">select</option>
        </select>
      </div>
      <ParamInput op={op} dfOptions={dfOptions} onCreate={onAdd} />
    </div>
  )
}