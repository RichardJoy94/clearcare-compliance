"""
Tests for file type detection functionality
"""

import os
import tempfile
import pytest
import sys

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from api import detect_file_type


class TestFileDetection:
    """Test cases for file type detection"""
    
    def test_detect_csv_file(self):
        """Test detection of CSV files"""
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age,city\nJohn,25,New York\nJane,30,Boston")
            temp_file = f.name
        
        try:
            assert detect_file_type(temp_file) == 'csv'
        finally:
            os.unlink(temp_file)
    
    def test_detect_json_file(self):
        """Test detection of JSON files"""
        # Create a temporary JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"name": "John", "age": 25, "city": "New York"}')
            temp_file = f.name
        
        try:
            assert detect_file_type(temp_file) == 'json'
        finally:
            os.unlink(temp_file)
    
    def test_detect_json_file_with_array(self):
        """Test detection of JSON files starting with array"""
        # Create a temporary JSON file starting with array
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('[{"name": "John"}, {"name": "Jane"}]')
            temp_file = f.name
        
        try:
            assert detect_file_type(temp_file) == 'json'
        finally:
            os.unlink(temp_file)
    
    def test_detect_unknown_file(self):
        """Test detection of unknown file types"""
        # Create a temporary text file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("This is just plain text")
            temp_file = f.name
        
        try:
            assert detect_file_type(temp_file) == 'unknown'
        finally:
            os.unlink(temp_file)
    
    def test_detect_file_by_extension(self):
        """Test detection by file extension when content detection fails"""
        # Create a CSV file with unusual content
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("unusual content without commas")
            temp_file = f.name
        
        try:
            # Should fall back to extension detection
            assert detect_file_type(temp_file) == 'csv'
        finally:
            os.unlink(temp_file)


if __name__ == "__main__":
    pytest.main([__file__])
