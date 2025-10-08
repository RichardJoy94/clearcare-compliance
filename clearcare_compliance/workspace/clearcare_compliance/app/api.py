## workspace/clearcare_compliance/app/api.py

from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from .models import Document, EvidencePack, ComplianceRun
from .schemas import DocumentSchema, EvidencePackSchema, ComplianceRunSchema, ValidationResultSchema
from .utils import save_uploaded_file, create_zip_file, generate_evidence_pack_files

app = FastAPI()

class ComplianceAPI:
    """API for compliance operations."""

    def __init__(self) -> None:
        self.compliance_runs: Dict[str, ComplianceRun] = {}

    def upload_document(self, document: UploadFile) -> str:
        """Uploads a document and returns its ID."""
        document_id = f"doc_{len(self.compliance_runs) + 1}"
        file_path = f"uploads/{document_id}_{document.filename}"
        save_uploaded_file(document, file_path)
        return document_id

    def validate_data(self, data: Dict[str, Any]) -> ValidationResultSchema:
        """Validates the provided data."""
        # Placeholder for validation logic
        is_valid = True
        errors = None
        if not data:
            is_valid = False
            errors = {"error": "Data cannot be empty."}
        return ValidationResultSchema(is_valid=is_valid, errors=errors)

    def publish_data(self, data: Dict[str, Any]) -> str:
        """Publishes the provided data and returns a compliance run ID."""
        run_id = f"run_{len(self.compliance_runs) + 1}"
        compliance_run = ComplianceRun(id=run_id)
        self.compliance_runs[run_id] = compliance_run
        return run_id

    def generate_evidence_pack(self, run_id: str) -> str:
        """Generates an evidence pack for the given compliance run ID."""
        if run_id not in self.compliance_runs:
            raise HTTPException(status_code=404, detail="Compliance run not found.")

        run = self.compliance_runs[run_id]
        html_content = f"<html><body><h1>Evidence Pack for {run_id}</h1></body></html>"
        json_data = {"run_id": run_id, "status": run.status}
        csv_data = "run_id,status\n" + f"{run_id},{run.status}"

        files = generate_evidence_pack_files(run_id, html_content, json_data, csv_data)
        zip_file_path = create_zip_file(f"{run_id}_evidence_pack.zip", files)

        return zip_file_path

# API Endpoints
compliance_api = ComplianceAPI()

@app.post("/upload", response_model=str)
async def upload_document(document: UploadFile = File(...)) -> str:
    """Endpoint to upload a document."""
    return compliance_api.upload_document(document)

@app.post("/validate", response_model=ValidationResultSchema)
async def validate_data(data: Dict[str, Any]) -> ValidationResultSchema:
    """Endpoint to validate data."""
    return compliance_api.validate_data(data)

@app.post("/publish", response_model=str)
async def publish_data(data: Dict[str, Any]) -> str:
    """Endpoint to publish data."""
    return compliance_api.publish_data(data)

@app.get("/generate_evidence_pack/{run_id}", response_model=str)
async def generate_evidence_pack(run_id: str) -> str:
    """Endpoint to generate an evidence pack."""
    return compliance_api.generate_evidence_pack(run_id)
