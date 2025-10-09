import { useEffect, useState } from 'react';

const API = (process.env.NEXT_PUBLIC_API_URL || '').replace(/\/$/, '');

export default function RunsPage() {
  const [runs, setRuns] = useState([]);
  const [validationDetails, setValidationDetails] = useState({});
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');
  const [pagination, setPagination] = useState({
    total: 0,
    limit: 50,
    offset: 0
  });
  const [filters, setFilters] = useState({
    status: '',
    q: ''
  });

  const fetchRuns = async (offset = 0, status = '', q = '') => {
    try {
      setLoading(true);
      const params = new URLSearchParams({
        limit: pagination.limit.toString(),
        offset: offset.toString()
      });
      
      if (status) params.append('status', status);
      if (q) params.append('q', q);
      
      const r = await fetch(`${API}/runs?${params}`);
      const j = await r.json();
      if (!r.ok) throw new Error(j.detail || JSON.stringify(j));
      
      const runsData = j.items || [];
      setRuns(runsData);
      setPagination({
        total: j.total,
        limit: j.limit,
        offset: j.offset
      });
      
      // Fetch validation details for each run
      const validationPromises = runsData.map(async (run) => {
        try {
          const valRes = await fetch(`${API}/runs/${run.id}/validation`);
          const valData = await valRes.json();
          return { runId: run.id, validation: valData };
        } catch (e) {
          return { runId: run.id, validation: { csv_validation: '-', json_validation: '-' } };
        }
      });
      
      const validationResults = await Promise.all(validationPromises);
      const validationMap = {};
      validationResults.forEach(({ runId, validation }) => {
        validationMap[runId] = validation;
      });
      setValidationDetails(validationMap);
      
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRuns(0, filters.status, filters.q);
  }, [filters]);

  async function downloadPack(runId) {
    const url = `${API}/generate_evidence_pack/${runId}`;
    try {
      // Fetch the evidence pack endpoint
      const response = await fetch(url, { method: 'GET' });
      const contentType = response.headers.get('content-type') || '';
      
      // Check if the response is JSON (S3 presigned URL response)
      if (contentType.includes('application/json')) {
        const data = await response.json();
        
        // If S3 presigned URL is available, navigate to it
        if (data.s3_presigned_url) {
          window.open(data.s3_presigned_url, '_blank');
          return;
        }
        
        // If JSON response but no presigned URL, show error
        alert('Evidence pack generated but no download URL available');
        return;
      }
      
      // Fallback: response is a direct file download (FileResponse)
      const blob = await response.blob();
      const downloadLink = document.createElement('a');
      downloadLink.href = URL.createObjectURL(blob);
      downloadLink.download = `${runId}.zip`;
      document.body.appendChild(downloadLink);
      downloadLink.click();
      document.body.removeChild(downloadLink);
      URL.revokeObjectURL(downloadLink.href);
    } catch (e) {
      alert(String(e.message || e));
    }
  }

  function getBadgeStyle(status) {
    switch (status) {
      case 'PASS':
        return { backgroundColor: '#d4edda', color: '#155724', padding: '4px 8px', borderRadius: '4px', fontSize: '12px', fontWeight: 'bold' };
      case 'FAIL':
        return { backgroundColor: '#f8d7da', color: '#721c24', padding: '4px 8px', borderRadius: '4px', fontSize: '12px', fontWeight: 'bold' };
      default:
        return { backgroundColor: '#e2e3e5', color: '#383d41', padding: '4px 8px', borderRadius: '4px', fontSize: '12px', fontWeight: 'bold' };
    }
  }

  return (
    <main style={{ padding: 24, fontFamily: 'system-ui, sans-serif' }}>
      <h1>Compliance Runs</h1>
      
      {/* Filters */}
      <div style={{ marginBottom: 20, display: 'flex', gap: 10, alignItems: 'center' }}>
        <input
          type="text"
          placeholder="Search filename..."
          value={filters.q}
          onChange={(e) => setFilters({...filters, q: e.target.value})}
          style={{ padding: '8px 12px', border: '1px solid #ddd', borderRadius: '4px' }}
        />
        <select
          value={filters.status}
          onChange={(e) => setFilters({...filters, status: e.target.value})}
          style={{ padding: '8px 12px', border: '1px solid #ddd', borderRadius: '4px' }}
        >
          <option value="">All Statuses</option>
          <option value="uploaded">Uploaded</option>
          <option value="running">Running</option>
          <option value="validated">Validated</option>
          <option value="failed">Failed</option>
          <option value="published">Published</option>
        </select>
      </div>

      {/* Pagination Info */}
      <div style={{ marginBottom: 20, color: '#666' }}>
        Showing {pagination.offset + 1}-{Math.min(pagination.offset + pagination.limit, pagination.total)} of {pagination.total} runs
      </div>

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
                   <th style={{ padding: 8, border: '1px solid #ddd' }}>File Types</th>
                   <th style={{ padding: 8, border: '1px solid #ddd' }}>CSV Rules</th>
                   <th style={{ padding: 8, border: '1px solid #ddd' }}>JSON Schema</th>
                   <th style={{ padding: 8, border: '1px solid #ddd' }}>Created</th>
                   <th style={{ padding: 8, border: '1px solid #ddd' }}>Actions</th>
                 </tr>
               </thead>
              <tbody>
                {runs.map((r) => {
                  const validation = validationDetails[r.id] || { csv_validation: '-', json_validation: '-' };
                  
                  // Determine CSV validation badge from database
                  let csvBadge = '-';
                  if (r.has_csv) {
                    if (r.rules_failed > 0) csvBadge = 'FAIL';
                    else if (r.rules_passed > 0) csvBadge = 'PASS';
                    else csvBadge = '-';
                  }
                  
                  // Determine JSON validation badge from database
                  let jsonBadge = '-';
                  if (r.has_json) {
                    if (r.schema_ok === true) jsonBadge = 'PASS';
                    else if (r.schema_ok === false) jsonBadge = 'FAIL';
                    else jsonBadge = '-';
                  }
                  
                  return (
                    <tr key={r.id}>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        <code style={{ fontSize: '12px' }}>{String(r.id).substring(0, 8)}...</code>
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        {r.filename || '—'}
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd', textTransform: 'capitalize' }}>
                        {String(r.status || '').replace('_', ' ')}
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        <div style={{ display: 'flex', gap: '4px' }}>
                          {r.has_csv && <span style={{ backgroundColor: '#007bff', color: 'white', padding: '2px 6px', borderRadius: '3px', fontSize: '10px' }}>CSV</span>}
                          {r.has_json && <span style={{ backgroundColor: '#28a745', color: 'white', padding: '2px 6px', borderRadius: '3px', fontSize: '10px' }}>JSON</span>}
                        </div>
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        <span style={getBadgeStyle(csvBadge)}>
                          {csvBadge}
                        </span>
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        <span style={getBadgeStyle(jsonBadge)}>
                          {jsonBadge}
                        </span>
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        {r.created_at ? new Date(r.created_at).toLocaleString() : '—'}
                      </td>
                      <td style={{ padding: 8, border: '1px solid #ddd' }}>
                        <button onClick={() => downloadPack(r.id)}>Download Pack</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )
      )}

      {/* Pagination Controls */}
      {!loading && !err && runs.length > 0 && (
        <div style={{ marginTop: 20, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <button
              onClick={() => fetchRuns(Math.max(0, pagination.offset - pagination.limit), filters.status, filters.q)}
              disabled={pagination.offset === 0}
              style={{
                padding: '8px 16px',
                marginRight: '8px',
                backgroundColor: pagination.offset === 0 ? '#ccc' : '#007bff',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: pagination.offset === 0 ? 'not-allowed' : 'pointer'
              }}
            >
              Previous
            </button>
            <button
              onClick={() => fetchRuns(pagination.offset + pagination.limit, filters.status, filters.q)}
              disabled={pagination.offset + pagination.limit >= pagination.total}
              style={{
                padding: '8px 16px',
                backgroundColor: pagination.offset + pagination.limit >= pagination.total ? '#ccc' : '#007bff',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: pagination.offset + pagination.limit >= pagination.total ? 'not-allowed' : 'pointer'
              }}
            >
              Next
            </button>
          </div>
          <div style={{ color: '#666' }}>
            Page {Math.floor(pagination.offset / pagination.limit) + 1} of {Math.ceil(pagination.total / pagination.limit)}
          </div>
        </div>
      )}
    </main>
  );
}
