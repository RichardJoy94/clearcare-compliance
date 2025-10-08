## workspace/clearcare_compliance/frontend/pages/index.js

import React, { useState } from 'react';
import axios from 'axios';

const IndexPage = () => {
    const [document, setDocument] = useState(null);
    const [validationResult, setValidationResult] = useState(null);
    const [uploadStatus, setUploadStatus] = useState('');
    const [publishStatus, setPublishStatus] = useState('');

    const handleFileChange = (event) => {
        setDocument(event.target.files[0]);
    };

    const handleUpload = async () => {
        if (!document) {
            setUploadStatus('Please select a document to upload.');
            return;
        }

        const formData = new FormData();
        formData.append('document', document);

        try {
            const response = await axios.post('/upload', formData, {
                headers: {
                    'Content-Type': 'multipart/form-data',
                },
            });
            setUploadStatus(`Document uploaded successfully. Document ID: ${response.data}`);
        } catch (error) {
            setUploadStatus(`Error uploading document: ${error.response.data.detail}`);
        }
    };

    const handleValidate = async (data) => {
        try {
            const response = await axios.post('/validate', data);
            setValidationResult(response.data);
        } catch (error) {
            setValidationResult({ is_valid: false, errors: error.response.data.detail });
        }
    };

    const handlePublish = async (data) => {
        try {
            const response = await axios.post('/publish', data);
            setPublishStatus(`Data published successfully. Compliance Run ID: ${response.data}`);
        } catch (error) {
            setPublishStatus(`Error publishing data: ${error.response.data.detail}`);
        }
    };

    return (
        <div>
            <h1>Compliance Document Upload</h1>
            <input type="file" onChange={handleFileChange} />
            <button onClick={handleUpload}>Upload Document</button>
            <p>{uploadStatus}</p>

            <h2>Validate Data</h2>
            <button onClick={() => handleValidate({ sampleData: 'example' })}>Validate Sample Data</button>
            {validationResult && (
                <div>
                    <p>Validation Result: {validationResult.is_valid ? 'Valid' : 'Invalid'}</p>
                    {validationResult.errors && <pre>{JSON.stringify(validationResult.errors, null, 2)}</pre>}
                </div>
            )}

            <h2>Publish Data</h2>
            <button onClick={() => handlePublish({ sampleData: 'example' })}>Publish Sample Data</button>
            <p>{publishStatus}</p>
        </div>
    );
};

export default IndexPage;
