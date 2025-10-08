## workspace/clearcare_compliance/app/schemas.py

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class DocumentSchema(BaseModel):
    """Schema for validating document upload."""
    id: str = Field(..., description="Unique identifier for the document")
    name: str = Field(..., description="Name of the document")
    content: bytes = Field(..., description="Content of the document in bytes")

class EvidencePackSchema(BaseModel):
    """Schema for validating evidence pack generation."""
    run_id: str = Field(..., description="Identifier for the compliance run")
    html_content: str = Field(..., description="HTML content of the evidence pack")
    json_data: Dict[str, Any] = Field(..., description="JSON data of the evidence pack")
    csv_data: str = Field(..., description="CSV data of the evidence pack")

class ComplianceRunSchema(BaseModel):
    """Schema for validating compliance run data."""
    id: str = Field(..., description="Unique identifier for the compliance run")
    status: str = Field(default="pending", description="Status of the compliance run")
    timestamp: Optional[str] = Field(default=None, description="Timestamp of the compliance run")

class ValidationResultSchema(BaseModel):
    """Schema for returning validation results."""
    is_valid: bool = Field(..., description="Indicates if the data is valid")
    errors: Optional[Dict[str, Any]] = Field(default=None, description="Validation errors if any")
