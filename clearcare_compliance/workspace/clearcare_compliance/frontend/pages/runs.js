## workspace/clearcare_compliance/frontend/pages/runs.js

import React, { useEffect, useState } from 'react';
import axios from 'axios';

const RunsPage = () => {
    const [complianceRuns, setComplianceRuns] = useState([]);
    const [error, setError] = useState<string>('');

    useEffect(() => {
        const fetchComplianceRuns = async (): Promise<void> => {
            try {
                const response = await axios.get('/compliance_runs');
                setComplianceRuns(response.data);
            } catch (err) {
                setError(`Error fetching compliance runs: ${err.response?.data?.detail || err.message}`);
            }
        };

        fetchComplianceRuns();
    }, []);

    return (
        <div>
            <h1>Compliance Runs</h1>
            {error && <p style={{ color: 'red' }}>{error}</p>}
            {complianceRuns.length === 0 ? (
                <p>No compliance runs available.</p>
            ) : (
                <table>
                    <thead>
                        <tr>
                            <th>Run ID</th>
                            <th>Status</th>
                            <th>Timestamp</th>
                        </tr>
                    </thead>
                    <tbody>
                        {complianceRuns.map((run) => (
                            <tr key={run.id}>
                                <td>{run.id}</td>
                                <td>{run.status}</td>
                                <td>{new Date(run.timestamp).toLocaleString()}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </div>
    );
};

export default RunsPage;
