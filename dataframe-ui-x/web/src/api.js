const getBaseUrl = () => {
  if (typeof window !== 'undefined' && window.APP_CONFIG && window.APP_CONFIG.API_BASE_URL) {
    return window.APP_CONFIG.API_BASE_URL;
  }
  return 'http://localhost:4999';
};

const BASE = () => getBaseUrl();

// API Key for internal access to dataframe-api
const API_KEY = 'dataframe-api-internal-key';

// Helper function to create headers with API key
const getHeaders = (additionalHeaders = {}) => {
  return {
    'X-API-Key': API_KEY,
    ...additionalHeaders
  };
};

export const getStats = async () => {
  const res = await fetch(`${BASE()}/api/stats`, {
    headers: getHeaders()
  });
  if (!res.ok) throw new Error(`Failed stats: ${res.status}`);
  return res.json();
};

export const listDataframes = async () => {
  const res = await fetch(`${BASE()}/api/dataframes`, {
    headers: getHeaders()
  });
  if (!res.ok) throw new Error(`Failed list: ${res.status}`);
  return res.json();
};

export const getDataframe = async (name, { page = 1, page_size = 100, preview = false } = {}) => {
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('page_size', String(page_size));
  params.set('preview', String(preview));
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(name)}?${params.toString()}`, {
    headers: getHeaders()
  });
  if (!res.ok) throw new Error(`Failed get df: ${res.status}`);
  return res.json();
};

export const deleteDataframe = async (name) => {
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(name)}`, { 
    method: 'DELETE',
    headers: getHeaders()
  });
  if (!res.ok) throw new Error(`Failed delete: ${res.status}`);
  return res.json();
};

export const clearCache = async () => {
  const res = await fetch(`${BASE()}/api/cache/clear`, { 
    method: 'DELETE',
    headers: getHeaders()
  });
  if (!res.ok) throw new Error(`Failed clear: ${res.status}`);
  return res.json();
};

export const uploadDataframe = async ({ file, name, description, type = 'static', auto_delete_hours = 10 }) => {
  const form = new FormData();
  if (name) form.set('name', name);
  if (description) form.set('description', description);
  form.set('type', type);
  if (type === 'ephemeral') form.set('auto_delete_hours', auto_delete_hours.toString());
  form.set('file', file);
  const res = await fetch(`${BASE()}/api/dataframes/upload`, { 
    method: 'POST', 
    body: form,
    headers: getHeaders() // Note: Don't set Content-Type for FormData, browser will set it automatically
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upload failed: ${res.status} ${text}`);
  }
  return res.json();
};

// New: Rename a dataframe's name and/or description
export const renameDataframe = async (oldName, { new_name, description } = {}) => {
  const payload = {};
  if (new_name && new_name.trim()) payload.new_name = new_name.trim();
  if (typeof description === 'string') payload.description = description;
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(oldName)}/rename`, {
    method: 'POST', 
    headers: getHeaders({ 'Content-Type': 'application/json' }), 
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    let msg = `Rename failed: ${res.status}`;
    try { const t = await res.text(); if (t) msg += ` ${t}`; } catch {}
    throw new Error(msg);
  }
  return res.json();
};

// URL builders for downloads and share links
export const buildDownloadCsvUrl = (name) => `${BASE()}/api/dataframes/${encodeURIComponent(name)}/download.csv`;
export const buildDownloadJsonUrl = (name) => `${BASE()}/api/dataframes/${encodeURIComponent(name)}/download.json`;

// Profile/analysis endpoint
export const getProfile = async (name) => {
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(name)}/profile`, {
    headers: getHeaders()
  });
  if (!res.ok) throw new Error(`Failed profile: ${res.status}`);
  return res.json();
};

// --- New: Ops endpoints ---
export const opsCompare = async ({ name1, name2, engine = 'pandas' }) => {
  const res = await fetch(`${BASE()}/api/ops/compare`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ name1, name2, engine })
  });
  if (!res.ok) throw new Error(`Compare failed: ${res.status}`);
  return res.json();
};

export const opsMerge = async ({ names, keys, how, engine = 'pandas', left_on, right_on }) => {
  const res = await fetch(`${BASE()}/api/ops/merge`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ names, keys, how, engine, left_on, right_on })
  });
  if (!res.ok) throw new Error(`Merge failed: ${res.status}`);
  return res.json();
};

export const opsPivot = async (payload) => {
  // Add default engine if not specified
  const payloadWithEngine = { engine: 'pandas', ...payload };
  const res = await fetch(`${BASE()}/api/ops/pivot`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(payloadWithEngine)
  });
  if (!res.ok) throw new Error(`Pivot failed: ${res.status}`);
  return res.json();
};

export const opsFilter = async (payload) => {
  // Add default engine if not specified
  const payloadWithEngine = { engine: 'pandas', ...payload };
  const res = await fetch(`${BASE()}/api/ops/filter`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(payloadWithEngine)
  });
  if (!res.ok) throw new Error(`Filter failed: ${res.status}`);
  return res.json();
};

export const opsGroupBy = async (payload) => {
  // Add default engine if not specified
  const payloadWithEngine = { engine: 'pandas', ...payload };
  const res = await fetch(`${BASE()}/api/ops/groupby`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(payloadWithEngine)
  });
  if (!res.ok) throw new Error(`GroupBy failed: ${res.status}`);
  return res.json();
};

export const opsSelect = async ({ name, columns, exclude, engine = 'pandas' }) => {
  const res = await fetch(`${BASE()}/api/ops/select`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ name, columns, engine, ...(exclude ? { exclude: true } : {}) })
  });
  if (!res.ok) throw new Error(`Select failed: ${res.status}`);
  return res.json();
};

// New: Rename columns
export const opsRename = async ({ name, map, engine = 'pandas' }) => {
  const res = await fetch(`${BASE()}/api/ops/rename`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ name, map, engine })
  });
  if (!res.ok) throw new Error(`Rename failed: ${res.status}`);
  return res.json();
}

// New: DateTime operation (parse/derive)
export const opsDatetime = async (payload) => {
  // Add default engine if not specified
  const payloadWithEngine = { engine: 'pandas', ...payload };
  const res = await fetch(`${BASE()}/api/ops/datetime`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(payloadWithEngine)
  })
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Datetime op failed: ${res.status} ${t}`)
  }
  return res.json()
}

// New: Mutate operation (create/overwrite a column via expression)
export const opsMutate = async (payload) => {
  // Add default engine if not specified
  const payloadWithEngine = { engine: 'pandas', ...payload };
  const res = await fetch(`${BASE()}/api/ops/mutate`, {
    method: 'POST', headers: getHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(payloadWithEngine)
  })
  if (!res.ok) {
    const t = await res.text(); throw new Error(`Mutate failed: ${res.status} ${t}`)
  }
  return res.json()
}

// Pipeline endpoints
export const pipelinePreview = async ({ start, steps, preview_rows = 20 }) => {
  const res = await fetch(`${BASE()}/api/pipeline/preview`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ start, steps, preview_rows })
  })
  if (!res.ok) throw new Error(`Pipeline preview failed: ${res.status}`)
  return res.json()
}

export const pipelineRun = async ({ start, steps, materialize = true, name }) => {
  const res = await fetch(`${BASE()}/api/pipeline/run`, {
    method: 'POST',
    headers: getHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ start, steps, materialize, name })
  })
  if (!res.ok) throw new Error(`Pipeline run failed: ${res.status}`)
  return res.json()
}

// Pipelines registry API
export const pipelinesList = async () => {
  const res = await fetch(`${BASE()}/api/pipelines`, {
    headers: getHeaders()
  })
  if (!res.ok) throw new Error(`Pipelines list failed: ${res.status}`)
  return res.json()
}

export const pipelineGet = async (name) => {
  const res = await fetch(`${BASE()}/api/pipelines/${encodeURIComponent(name)}`, {
    headers: getHeaders()
  })
  if (!res.ok) throw new Error(`Pipeline get failed: ${res.status}`)
  return res.json()
}

export const pipelineSave = async ({ name, description, start = null, steps, tags }, { overwrite = false } = {}) => {
  const payload = { name, description, start, steps, tags, overwrite }
  const res = await fetch(`${BASE()}/api/pipelines`, {
    method: 'POST', headers: getHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(payload)
  })
  if (!res.ok) {
    const text = await res.text(); throw new Error(`Pipeline save failed: ${res.status} ${text}`)
  }
  return res.json()
}

export const pipelineDelete = async (name) => {
  const res = await fetch(`${BASE()}/api/pipelines/${encodeURIComponent(name)}`, { 
    method: 'DELETE',
    headers: getHeaders()
  })
  if (!res.ok) throw new Error(`Pipeline delete failed: ${res.status}`)
  return res.json()
}

export const pipelineRunByName = async (name, { materialize = true, outName } = {}) => {
  const res = await fetch(`${BASE()}/api/pipelines/${encodeURIComponent(name)}/run`, {
    method: 'POST', headers: getHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify({ materialize, name: outName })
  })
  if (!res.ok) throw new Error(`Pipeline run failed: ${res.status}`)
  return res.json()
}

export const buildPipelineExportUrl = (name) => `${BASE()}/api/pipelines/${encodeURIComponent(name)}/export.yml`

export const pipelineImportYaml = async ({ yaml, overwrite = false }) => {
  const res = await fetch(`${BASE()}/api/pipelines/import`, {
    method: 'POST', headers: getHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify({ yaml, overwrite })
  })
  if (!res.ok) {
    const text = await res.text(); throw new Error(`Pipeline import failed: ${res.status} ${text}`)
  }
  return res.json()
}
