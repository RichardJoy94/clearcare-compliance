## workspace/clearcare_compliance/tests/test_api.py

import unittest
from fastapi.testclient import TestClient
from clearcare_compliance.app.api import app
from clearcare_compliance.app.schemas import ValidationResultSchema
from clearcare_compliance.app.models import ComplianceRun

class TestComplianceAPI(unittest.TestCase):
    """Unit tests for the Compliance API."""

    def setUp(self) -> None:
        """Set up the test client and any necessary state."""
        self.client = TestClient(app)
        self.compliance_run_id = "run_1"
        self.compliance_run = ComplianceRun(id=self.compliance_run_id)

    def test_upload_document(self) -> None:
        """Test the document upload endpoint."""
        with open("test_file.txt", "wb") as f:
            f.write(b"This is a test document.")

        with open("test_file.txt", "rb") as f:
            response = self.client.post(
                "/upload",
                files={"document": ("test_file.txt", f, "text/plain")}
            )
        
        self.assertEqual(response.status_code, 200)
        self.assertIn("doc_", response.text)

    def test_validate_data(self) -> None:
        """Test the data validation endpoint."""
        data = {"sample_key": "sample_value"}
        response = self.client.post("/validate", json=data)
        
        self.assertEqual(response.status_code, 200)
        validation_result = ValidationResultSchema(**response.json())
        self.assertTrue(validation_result.is_valid)

    def test_publish_data(self) -> None:
        """Test the data publishing endpoint."""
        data = {"sample_key": "sample_value"}
        response = self.client.post("/publish", json=data)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn("run_", response.text)

    def test_generate_evidence_pack(self) -> None:
        """Test the evidence pack generation endpoint."""
        # First, publish data to create a compliance run
        data = {"sample_key": "sample_value"}
        publish_response = self.client.post("/publish", json=data)
        run_id = publish_response.text

        response = self.client.get(f"/generate_evidence_pack/{run_id}")
        
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.text.endswith("_evidence_pack.zip"))

if __name__ == "__main__":
    unittest.main()
