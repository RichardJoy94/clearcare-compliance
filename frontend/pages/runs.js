import { useEffect, useState } from 'react';

const API = (process.env.NEXT_PUBLIC_API_URL || '').replace(/\/$/, '');

export default function RunsPage() {
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');

  useEffect(() => {
    (async () => {
      try {
        const r = await fetch(`${API}/runs`);
        const j = await r.json();
        if (!r.ok) throw new Error(j.detail || JSON.stringify(j));
        setRuns(j.runs || []);
      } catch (e) {
        setErr(String(e.message || e));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  async function downloadPack(runId) {
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
      alert(String(e.message || e));
    }
  }

  return (
    <main style={{ padding: 24, fontFamily: 'system-ui, sans-serif' }}>
      <h1>Compliance Runs</h1>
      {loading && <p>Loading…</p>}
      {err && <pre style={{ color: 'crimson' }}>{err}</pre>}

      {!loading && !err && (
        runs.length === 0 ? (
          <p>No runs yet.</p>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ background: '#f2f2f2' }}>
                  <th style={{ padding: 8, border: '1px solid #ddd' }}>Run ID</th>
                  <th style={{ padding: 8, border: '1px solid #ddd' }}>Filename</th>
                  <th style={{ padding: 8, border: '1px solid #ddd' }}>Status</th>
                  <th style={{ padding: 8, border: '1px solid #ddd' }}>Updated</th>
                  <th style={{ padding: 8, border: '1px solid #ddd' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((r) => (
                  <tr key={r.run_id}>
                    <td style={{ padding: 8, border: '1px solid #ddd' }}>
                      <code>{r.run_id}</code>
                    </td>
                    <td style={{ padding: 8, border: '1px solid #ddd' }}>
                      {r.filename || '—'}
                    </td>
                    <td style={{ padding: 8, border: '1px solid #ddd', textTransform: 'capitalize' }}>
                      {String(r.status || '').replace('_', ' ')}
                    </td>
                    <td style={{ padding: 8, border: '1px solid #ddd' }}>
                      {r.updated_at ? new Date(r.updated_at).toLocaleString() : '—'}
                    </td>
                    <td style={{ padding: 8, border: '1px solid #ddd' }}>
                      <button onClick={() => downloadPack(r.run_id)}>Download Pack</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      )}
    </main>
  );
}
