## workspace/clearcare_compliance/tests/test_utils.py

import os
import unittest
from typing import Dict, Any
from fastapi import UploadFile
from clearcare_compliance.app.utils import save_uploaded_file, create_zip_file, generate_evidence_pack_files

class TestUtils(unittest.TestCase):
    """Unit tests for utility functions in the application."""

    def setUp(self) -> None:
        """Set up test environment."""
        self.test_upload_dir = "test_uploads"
        self.test_zip_dir = "test_zips"
        os.makedirs(self.test_upload_dir, exist_ok=True)
        os.makedirs(self.test_zip_dir, exist_ok=True)

    def tearDown(self) -> None:
        """Clean up test environment."""
        for file in os.listdir(self.test_upload_dir):
            os.remove(os.path.join(self.test_upload_dir, file))
        os.rmdir(self.test_upload_dir)

        for file in os.listdir(self.test_zip_dir):
            os.remove(os.path.join(self.test_zip_dir, file))
        os.rmdir(self.test_zip_dir)

    def test_save_uploaded_file(self) -> None:
        """Test saving an uploaded file."""
        test_file_content = b'This is a test file.'
        test_file_name = "test_file.txt"
        test_file_path = os.path.join(self.test_upload_dir, test_file_name)

        # Create a mock UploadFile
        class MockUploadFile:
            def __init__(self, filename: str, content: bytes):
                self.filename = filename
                self.file = content

        mock_file = MockUploadFile(test_file_name, test_file_content)

        saved_file_path = save_uploaded_file(mock_file, test_file_path)
        self.assertEqual(saved_file_path, test_file_path)
        self.assertTrue(os.path.exists(saved_file_path))

        with open(saved_file_path, 'rb') as f:
            content = f.read()
            self.assertEqual(content, test_file_content)

    def test_create_zip_file(self) -> None:
        """Test creating a zip file."""
        test_file_paths = {
            "test_file_1.txt": os.path.join(self.test_upload_dir, "test_file_1.txt"),
            "test_file_2.txt": os.path.join(self.test_upload_dir, "test_file_2.txt"),
        }

        # Create test files
        for file_name, file_path in test_file_paths.items():
            with open(file_path, 'w') as f:
                f.write(f"This is {file_name}.")

        zip_file_name = "test_zip.zip"
        zip_file_path = os.path.join(self.test_zip_dir, zip_file_name)

        created_zip_path = create_zip_file(zip_file_path, test_file_paths)
        self.assertEqual(created_zip_path, zip_file_path)
        self.assertTrue(os.path.exists(created_zip_path))

    def test_generate_evidence_pack_files(self) -> None:
        """Test generating evidence pack files."""
        run_id = "run_123"
        html_content = "<html><body><h1>Evidence Pack</h1></body></html>"
        json_data: Dict[str, Any] = {"run_id": run_id, "status": "completed"}
        csv_data = "run_id,status\nrun_123,completed"

        generated_files = generate_evidence_pack_files(run_id, html_content, json_data, csv_data)

        for file_type, file_path in generated_files.items():
            self.assertTrue(os.path.exists(file_path))
            self.assertIn(file_type, file_path)

if __name__ == "__main__":
    unittest.main()
