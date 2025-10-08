import { useState } from 'react';

const API = (process.env.NEXT_PUBLIC_API_URL || '').replace(/\/$/, '');

export default function UploadPage() {
  const [file, setFile] = useState(null);
  const [runId, setRunId] = useState('');
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const [result, setResult] = useState(null);

  async function handleUpload(e) {
    e.preventDefault();
    setError('');
    setResult(null);

    if (!file) {
      setError('Please choose a .csv file.');
      return;
    }

    try {
      setStatus('Uploading…');
      const fd = new FormData();
      fd.append('document', file);

      const res = await fetch(`${API}/upload`, { method: 'POST', body: fd });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data));

      setRunId(data.run_id);
      setStatus('Uploaded ✓');
    } catch (err) {
      setStatus('');
      setError(String(err.message || err));
    }
  }

  async function handleValidate() {
    if (!runId) return;
    setError('');
    try {
      setStatus('Validating…');
      const res = await fetch(`${API}/validate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ run_id: runId }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data));

      setResult(data);
      setStatus('Validated ✓');
    } catch (err) {
      setStatus('');
      setError(String(err.message || err));
    }
  }

  async function handleDownload() {
    if (!runId) return;
    // /generate_evidence_pack returns either a zip file directly OR JSON with a presigned URL.
    const url = `${API}/generate_evidence_pack/${runId}`;
    try {
      const head = await fetch(url, { method: 'GET' });
      const contentType = head.headers.get('content-type') || '';

      if (contentType.includes('application/json')) {
        const j = await head.clone().json();
        const presigned = j.s3_presigned_url;
        if (presigned) {
          window.open(presigned, '_blank');
          return;
        }
      }

      const res = await fetch(url);
      const blob = await res.blob();
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `${runId}.zip`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(a.href);
    } catch (err) {
      setError(String(err.message || err));
    }
  }

  return (
    <main style={{ padding: 24, fontFamily: 'system-ui, sans-serif' }}>
      <h1>Upload CSV</h1>

      <form onSubmit={handleUpload} style={{ marginBottom: 16 }}>
        <input
          type="file"
          accept=".csv"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
        />
        <button type="submit" style={{ marginLeft: 8 }} disabled={!file}>
          Upload
        </button>
      </form>

      {runId && (
        <div style={{ marginBottom: 16 }}>
          <div>run_id: <code>{runId}</code></div>
          <button onClick={handleValidate}>Validate</button>
          <button onClick={handleDownload} style={{ marginLeft: 8 }}>
            Download Evidence Pack
          </button>
        </div>
      )}

      {status && <p>Status: {status}</p>}
      {error && <pre style={{ color: 'crimson' }}>{error}</pre>}
      {result && (
        <pre style={{ background: '#111', color: '#0f0', padding: 12 }}>
          {JSON.stringify(result, null, 2)}
        </pre>
      )}
    </main>
  );
}
