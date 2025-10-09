"""
SQLModel database models for ClearCare Compliance MVP
"""

import uuid
from datetime import datetime
from typing import Optional
from enum import Enum
from sqlmodel import SQLModel, Field, Column, DateTime
from sqlalchemy import text


class RunStatus(str, Enum):
    """Status enum for compliance runs"""
    UPLOADED = "uploaded"
    RUNNING = "running"
    VALIDATED = "validated"
    FAILED = "failed"
    PUBLISHED = "published"


class Run(SQLModel, table=True):
    """
    Database model for compliance runs.
    Stores information about uploaded files and their validation results.
    """
    
    __tablename__ = "runs"
    
    # Primary key
    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        description="Unique identifier for the run"
    )
    
    # File information
    filename: Optional[str] = Field(default=None, description="Original filename")
    
    # File type indicators
    has_json: bool = Field(default=False, description="Whether this run has a JSON file")
    has_csv: bool = Field(default=False, description="Whether this run has a CSV file")
    
    # Profile information
    profile: Optional[str] = Field(default=None, description="Detected CSV profile (cms_csv, simple_csv)")
    
    # Validation results
    schema_ok: Optional[bool] = Field(default=None, description="JSON schema validation result")
    rules_passed: int = Field(default=0, description="Number of rules that passed")
    rules_failed: int = Field(default=0, description="Number of rules that failed")
    
    # Status and timestamps
    status: RunStatus = Field(default=RunStatus.UPLOADED, description="Current status of the run")
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, server_default=text("CURRENT_TIMESTAMP")),
        description="When the run was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, server_default=text("CURRENT_TIMESTAMP"), onupdate=datetime.utcnow),
        description="When the run was last updated"
    )


# Legacy classes for backward compatibility (can be removed later)
class Document:
    """Legacy document class - kept for compatibility"""
    
    def __init__(self, id: str, name: str, content: bytes) -> None:
        self.id: str = id
        self.name: str = name
        self.content: bytes = content


class EvidencePack:
    """Legacy evidence pack class - kept for compatibility"""
    
    def __init__(self, run_id: str, html_content: str, json_data: dict, csv_data: str) -> None:
        self.run_id: str = run_id
        self.html_content: str = html_content
        self.json_data: dict = json_data
        self.csv_data: str = csv_data

    def create_zip(self) -> str:
        """Creates a zip file containing the evidence pack."""
        return f"{self.run_id}_evidence_pack.zip"


class ComplianceRun:
    """Legacy compliance run class - kept for compatibility"""
    
    def __init__(self, id: str, status: str = "pending", timestamp: datetime = None) -> None:
        self.id: str = id
        self.status: str = status
        self.timestamp: datetime = timestamp if timestamp is not None else datetime.now()
