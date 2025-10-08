## workspace/clearcare_compliance/frontend/pages/upload.js

import React, { useState } from 'react';
import axios from 'axios';

const UploadPage = () => {
    const [document, setDocument] = useState<File | null>(null);
    const [uploadStatus, setUploadStatus] = useState<string>('');
    
    const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>): void => {
        const files = event.target.files;
        if (files && files.length > 0) {
            setDocument(files[0]);
        }
    };

    const handleUpload = async (): Promise<void> => {
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
            setUploadStatus(`Error uploading document: ${error.response?.data?.detail || error.message}`);
        }
    };

    return (
        <div>
            <h1>Upload Document</h1>
            <input type="file" onChange={handleFileChange} />
            <button onClick={handleUpload}>Upload Document</button>
            <p>{uploadStatus}</p>
        </div>
    );
};

export default UploadPage;
