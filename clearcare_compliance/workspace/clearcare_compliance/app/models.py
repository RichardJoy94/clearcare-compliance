## workspace/clearcare_compliance/app/models.py

from datetime import datetime
from typing import Dict, Any
from pydantic import BaseModel

class Document:
    """Represents a document to be uploaded."""
    
    def __init__(self, id: str, name: str, content: bytes) -> None:
        self.id: str = id
        self.name: str = name
        self.content: bytes = content

class EvidencePack:
    """Represents a pack of evidence generated from a compliance run."""
    
    def __init__(self, run_id: str, html_content: str, json_data: Dict[str, Any], csv_data: str) -> None:
        self.run_id: str = run_id
        self.html_content: str = html_content
        self.json_data: Dict[str, Any] = json_data
        self.csv_data: str = csv_data

    def create_zip(self) -> str:
        """Creates a zip file containing the evidence pack."""
        # Implementation for creating a zip file goes here.
        # This is a placeholder for the actual zip creation logic.
        return f"{self.run_id}_evidence_pack.zip"

class ComplianceRun:
    """Represents a compliance run with its status and timestamp."""
    
    def __init__(self, id: str, status: str = "pending", timestamp: datetime = None) -> None:
        self.id: str = id
        self.status: str = status
        self.timestamp: datetime = timestamp if timestamp is not None else datetime.now()
