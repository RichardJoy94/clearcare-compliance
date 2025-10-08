## workspace/clearcare_compliance/frontend/pages/evidence_packs.js

import React, { useEffect, useState } from 'react';
import axios from 'axios';

const EvidencePacksPage = () => {
    const [evidencePacks, setEvidencePacks] = useState([]);
    const [error, setError] = useState<string>('');
    const [runId, setRunId] = useState<string>('');
    const [zipFilePath, setZipFilePath] = useState<string>('');

    useEffect(() => {
        const fetchEvidencePacks = async (): Promise<void> => {
            try {
                const response = await axios.get('/evidence_packs');
                setEvidencePacks(response.data);
            } catch (err) {
                setError(`Error fetching evidence packs: ${err.response?.data?.detail || err.message}`);
            }
        };

        fetchEvidencePacks();
    }, []);

    const handleGenerateEvidencePack = async (): Promise<void> => {
        if (!runId) {
            setError('Please enter a valid Run ID.');
            return;
        }

        try {
            const response = await axios.get(`/generate_evidence_pack/${runId}`);
            setZipFilePath(response.data);
            setError(''); // Clear any previous errors
        } catch (err) {
            setError(`Error generating evidence pack: ${err.response?.data?.detail || err.message}`);
        }
    };

    return (
        <div>
            <h1>Evidence Packs</h1>
            {error && <p style={{ color: 'red' }}>{error}</p>}
            <h2>Generate Evidence Pack</h2>
            <input
                type="text"
                value={runId}
                onChange={(e) => setRunId(e.target.value)}
                placeholder="Enter Run ID"
            />
            <button onClick={handleGenerateEvidencePack}>Generate Evidence Pack</button>
            {zipFilePath && <p>Evidence Pack generated at: <a href={zipFilePath} download>{zipFilePath}</a></p>}

            <h2>Available Evidence Packs</h2>
            {evidencePacks.length === 0 ? (
                <p>No evidence packs available.</p>
            ) : (
                <table>
                    <thead>
                        <tr>
                            <th>Run ID</th>
                            <th>Download Link</th>
                        </tr>
                    </thead>
                    <tbody>
                        {evidencePacks.map((pack) => (
                            <tr key={pack.run_id}>
                                <td>{pack.run_id}</td>
                                <td>
                                    <a href={pack.zip_file_path} download>{pack.zip_file_path}</a>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </div>
    );
};

export default EvidencePacksPage;
