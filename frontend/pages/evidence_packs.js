import { useState } from 'react';

const API = (process.env.NEXT_PUBLIC_API_URL || '').replace(/\/$/, '');

export default function EvidencePacksPage() {
  const [runId, setRunId] = useState('');
  const [err, setErr] = useState('');

  async function download() {
    setErr('');
    if (!runId) {
      setErr('Enter a run_id first.');
      return;
    }
    const url = `${API}/generate_evidence_pack/${runId}`;
    try {
      const head = await fetch(url, { method: 'GET' });
      const type = head.headers.get('content-type') || '';
      if (type.includes('application/json')) {
        const j = await head.clone().json();
        if (j.s3_presigned_url) {
          window.open(j.s3_presigned_url, '_blank');
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
    } catch (e) {
      setErr(String(e.message || e));
    }
  }

  return (
    <main style={{ padding: 24, fontFamily: 'system-ui, sans-serif' }}>
      <h1>Evidence Packs</h1>
      <div style={{ marginBottom: 12 }}>
        <input
          placeholder="run_id"
          value={runId}
          onChange={(e) => setRunId(e.target.value)}
          style={{ width: 420 }}
        />
        <button onClick={download} style={{ marginLeft: 8 }}>
          Download
        </button>
      </div>
      {err && <pre style={{ color: 'crimson' }}>{err}</pre>}
      <p style={{ color: '#666' }}>
        Tip: get a <code>run_id</code> by uploading on the <a href="/upload">Upload</a> page,
        or list them on the <a href="/runs">Runs</a> page.
      </p>
    </main>
  );
}
