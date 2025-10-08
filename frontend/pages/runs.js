import React, { useEffect, useState } from 'react';
import axios from 'axios';

const RunsPage = () => {
    const [complianceRuns, setComplianceRuns] = useState([]);
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchComplianceRuns = async () => {
            try {
                setLoading(true);
                const response = await axios.get('/runs');
                setComplianceRuns(response.data.runs || []);
            } catch (err) {
                setError(`Error fetching compliance runs: ${err.response?.data?.detail || err.message}`);
            } finally {
                setLoading(false);
            }
        };

        fetchComplianceRuns();
    }, []);

    const handleDownloadEvidencePack = async (runId) => {
        try {
            const response = await axios.get(`/generate_evidence_pack/${runId}`, {
                responseType: 'blob'
            });
            
            // Create download link
            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', `${runId}.zip`);
            document.body.appendChild(link);
            link.click();
            link.remove();
            window.URL.revokeObjectURL(url);
        } catch (err) {
            alert(`Error downloading evidence pack: ${err.response?.data?.detail || err.message}`);
        }
    };

    const getStatusColor = (status) => {
        switch (status) {
            case 'uploaded': return '#3498db';
            case 'validated': return '#27ae60';
            case 'validation_failed': return '#e74c3c';
            case 'published': return '#9b59b6';
            default: return '#95a5a6';
        }
    };

    if (loading) {
        return (
            <div style={{ padding: '20px' }}>
                <h1>Compliance Runs</h1>
                <p>Loading...</p>
            </div>
        );
    }

    return (
        <div style={{ padding: '20px' }}>
            <h1>Compliance Runs</h1>
            {error && <p style={{ color: 'red' }}>{error}</p>}
            {complianceRuns.length === 0 ? (
                <p>No compliance runs available.</p>
            ) : (
                <div style={{ overflowX: 'auto' }}>
                    <table style={{ 
                        width: '100%', 
                        borderCollapse: 'collapse',
                        border: '1px solid #ddd'
                    }}>
                        <thead>
                            <tr style={{ backgroundColor: '#f2f2f2' }}>
                                <th style={{ padding: '12px', border: '1px solid #ddd' }}>Run ID</th>
                                <th style={{ padding: '12px', border: '1px solid #ddd' }}>Filename</th>
                                <th style={{ padding: '12px', border: '1px solid #ddd' }}>Status</th>
                                <th style={{ padding: '12px', border: '1px solid #ddd' }}>Created</th>
                                <th style={{ padding: '12px', border: '1px solid #ddd' }}>Updated</th>
                                <th style={{ padding: '12px', border: '1px solid #ddd' }}>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {complianceRuns.map((run) => (
                                <tr key={run.run_id}>
                                    <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                                        <code>{run.run_id}</code>
                                    </td>
                                    <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                                        {run.filename || 'N/A'}
                                    </td>
                                    <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                                        <span style={{ 
                                            color: getStatusColor(run.status),
                                            fontWeight: 'bold',
                                            textTransform: 'capitalize'
                                        }}>
                                            {run.status.replace('_', ' ')}
                                        </span>
                                    </td>
                                    <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                                        {new Date(run.created_at).toLocaleString()}
                                    </td>
                                    <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                                        {new Date(run.updated_at).toLocaleString()}
                                    </td>
                                    <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                                        <button
                                            onClick={() => handleDownloadEvidencePack(run.run_id)}
                                            style={{
                                                backgroundColor: '#3498db',
                                                color: 'white',
                                                border: 'none',
                                                padding: '8px 16px',
                                                borderRadius: '4px',
                                                cursor: 'pointer',
                                                fontSize: '14px'
                                            }}
                                            onMouseOver={(e) => e.target.style.backgroundColor = '#2980b9'}
                                            onMouseOut={(e) => e.target.style.backgroundColor = '#3498db'}
                                        >
                                            Download Evidence Pack
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
};

export default RunsPage;
