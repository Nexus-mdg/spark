const getBaseUrl = () => {
  if (typeof window !== 'undefined' && window.APP_CONFIG && window.APP_CONFIG.API_BASE_URL) {
    return window.APP_CONFIG.API_BASE_URL;
  }
  return 'http://localhost:4999';
};

const BASE = () => getBaseUrl();

export const getStats = async () => {
  const res = await fetch(`${BASE()}/api/stats`);
  if (!res.ok) throw new Error(`Failed stats: ${res.status}`);
  return res.json();
};

export const listDataframes = async () => {
  const res = await fetch(`${BASE()}/api/dataframes`);
  if (!res.ok) throw new Error(`Failed list: ${res.status}`);
  return res.json();
};

export const getDataframe = async (name, { page = 1, page_size = 100, preview = false } = {}) => {
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('page_size', String(page_size));
  params.set('preview', String(preview));
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(name)}?${params.toString()}`);
  if (!res.ok) throw new Error(`Failed get df: ${res.status}`);
  return res.json();
};

export const deleteDataframe = async (name) => {
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(name)}`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`Failed delete: ${res.status}`);
  return res.json();
};

export const clearCache = async () => {
  const res = await fetch(`${BASE()}/api/cache/clear`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`Failed clear: ${res.status}`);
  return res.json();
};

export const uploadDataframe = async ({ file, name, description }) => {
  const form = new FormData();
  if (name) form.set('name', name);
  if (description) form.set('description', description);
  form.set('file', file);
  const res = await fetch(`${BASE()}/api/dataframes/upload`, { method: 'POST', body: form });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upload failed: ${res.status} ${text}`);
  }
  return res.json();
};

// URL builders for downloads and share links
export const buildDownloadCsvUrl = (name) => `${BASE()}/api/dataframes/${encodeURIComponent(name)}/download.csv`;
export const buildDownloadJsonUrl = (name) => `${BASE()}/api/dataframes/${encodeURIComponent(name)}/download.json`;

// Profile/analysis endpoint
export const getProfile = async (name) => {
  const res = await fetch(`${BASE()}/api/dataframes/${encodeURIComponent(name)}/profile`);
  if (!res.ok) throw new Error(`Failed profile: ${res.status}`);
  return res.json();
};

// --- New: Ops endpoints ---
export const opsCompare = async ({ name1, name2 }) => {
  const res = await fetch(`${BASE()}/api/ops/compare`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name1, name2 })
  });
  if (!res.ok) throw new Error(`Compare failed: ${res.status}`);
  return res.json();
};

export const opsMerge = async ({ names, keys, how }) => {
  const res = await fetch(`${BASE()}/api/ops/merge`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ names, keys, how })
  });
  if (!res.ok) throw new Error(`Merge failed: ${res.status}`);
  return res.json();
};

export const opsPivot = async (payload) => {
  const res = await fetch(`${BASE()}/api/ops/pivot`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`Pivot failed: ${res.status}`);
  return res.json();
};

export const opsFilter = async (payload) => {
  const res = await fetch(`${BASE()}/api/ops/filter`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`Filter failed: ${res.status}`);
  return res.json();
};

export const opsGroupBy = async (payload) => {
  const res = await fetch(`${BASE()}/api/ops/groupby`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`GroupBy failed: ${res.status}`);
  return res.json();
};

export const opsSelect = async ({ name, columns }) => {
  const res = await fetch(`${BASE()}/api/ops/select`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, columns })
  });
  if (!res.ok) throw new Error(`Select failed: ${res.status}`);
  return res.json();
};

// New: Rename columns
export const opsRename = async ({ name, map }) => {
  const res = await fetch(`${BASE()}/api/ops/rename`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, map })
  });
  if (!res.ok) throw new Error(`Rename failed: ${res.status}`);
  return res.json();
};
