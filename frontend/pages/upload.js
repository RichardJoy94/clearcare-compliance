import { useState, useEffect } from 'react';

const API = (process.env.NEXT_PUBLIC_API_URL || '').replace(/\/$/, '');

export default function UploadPage() {
  const [file, setFile] = useState(null);
  const [runId, setRunId] = useState('');
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const [result, setResult] = useState(null);
  const [jobId, setJobId] = useState('');
  const [jobStatus, setJobStatus] = useState('');
  const [isPolling, setIsPolling] = useState(false);

  async function handleUpload(e) {
    e.preventDefault();
    setError('');
    setResult(null);

    if (!file) {
      setError('Please choose a .csv or .json file.');
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
    setJobId('');
    setJobStatus('');
    setIsPolling(false);
    
    try {
      setStatus('Validating…');
      const res = await fetch(`${API}/validate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ run_id: runId }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data));

      // Check if this is an async job response
      if (data.job_id) {
        setJobId(data.job_id);
        setJobStatus(data.status);
        setIsPolling(true);
        setStatus('Validation queued…');
      } else {
        // Synchronous response
        setResult(data);
        setStatus('Validated ✓');
      }
    } catch (err) {
      setStatus('');
      setError(String(err.message || err));
    }
  }

  // Poll job status when we have a job ID
  useEffect(() => {
    if (!isPolling || !jobId) return;

    const pollInterval = setInterval(async () => {
      try {
        const res = await fetch(`${API}/tasks/${jobId}`);
        const data = await res.json();
        
        if (!res.ok) {
          throw new Error(data.error || 'Failed to get job status');
        }

        setJobStatus(data.status);
        
        if (data.status === 'finished') {
          setIsPolling(false);
          setResult(data.result);
          setStatus('Validated ✓');
          clearInterval(pollInterval);
        } else if (data.status === 'failed') {
          setIsPolling(false);
          setError(data.error || 'Validation failed');
          setStatus('');
          clearInterval(pollInterval);
        }
      } catch (err) {
        setIsPolling(false);
        setError(String(err.message || err));
        setStatus('');
        clearInterval(pollInterval);
      }
    }, 2000); // Poll every 2 seconds

    return () => clearInterval(pollInterval);
  }, [isPolling, jobId]);

  async function handleDownload() {
    if (!runId) return;
    const url = `${API}/generate_evidence_pack/${runId}`;
    setError('');
    
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
        setError('Evidence pack generated but no download URL available');
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
    } catch (err) {
      setError(String(err.message || err));
    }
  }

  return (
    <main style={{ padding: 24, fontFamily: 'system-ui, sans-serif' }}>
      <h1>Upload File (CSV or JSON)</h1>

      <form onSubmit={handleUpload} style={{ marginBottom: 16 }}>
        <input
          type="file"
          accept=".csv,.json"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
        />
        <button type="submit" style={{ marginLeft: 8 }} disabled={!file}>
          Upload
        </button>
      </form>

      {runId && (
        <div style={{ marginBottom: 16 }}>
          <div>run_id: <code>{runId}</code></div>
          <button 
            onClick={handleValidate} 
            disabled={isPolling || status === 'Validating…' || status === 'Validation queued…'}
          >
            {isPolling ? 'Validating...' : 'Validate'}
          </button>
          <button 
            onClick={handleDownload} 
            style={{ marginLeft: 8 }}
            disabled={!result && !isPolling}
          >
            Download Evidence Pack
          </button>
        </div>
      )}

      {status && <p>Status: {status}</p>}
      {jobId && jobStatus && (
        <p style={{ color: '#666' }}>
          Job ID: <code>{jobId}</code> | Job Status: <strong>{jobStatus}</strong>
        </p>
      )}
      {error && <pre style={{ color: 'crimson' }}>{error}</pre>}
      {result && (
        <pre style={{ background: '#111', color: '#0f0', padding: 12 }}>
          {JSON.stringify(result, null, 2)}
        </pre>
      )}
    </main>
  );
}
