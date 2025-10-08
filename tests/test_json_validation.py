"""
Tests for JSON schema validation functionality
"""

import json
import os
import tempfile
import pytest
from pathlib import Path

# Add the app directory to the path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from json_validator import (
    load_cms_schemas,
    detect_schema_type,
    validate_json_against_schema,
    run_json_schema_validation
)


class TestJSONValidation:
    """Test cases for JSON schema validation"""
    
    def test_load_cms_schemas(self):
        """Test loading CMS schemas from the rules directory"""
        schemas = load_cms_schemas()
        
        # Should have at least the three main schemas
        assert "in-network-rates" in schemas
        assert "allowed-amounts" in schemas
        assert "provider-reference" in schemas
        
        # Each schema should have required JSON schema properties
        for schema_name, schema in schemas.items():
            assert "$schema" in schema
            assert "type" in schema
            assert "properties" in schema
    
    def test_detect_schema_type(self):
        """Test schema type detection"""
        # Test in-network rates detection
        in_network_data = {
            "provider_references": [],
            "in_network": []
        }
        assert detect_schema_type(in_network_data) == "in-network-rates"
        
        # Test allowed amounts detection
        allowed_amounts_data = {
            "provider_references": [],
            "allowed_amounts": []
        }
        assert detect_schema_type(allowed_amounts_data) == "allowed-amounts"
        
        # Test provider reference detection
        provider_ref_data = {
            "provider_group_id": "test",
            "provider_groups": []
        }
        assert detect_schema_type(provider_ref_data) == "provider-reference"
        
        # Test unknown detection
        unknown_data = {"some_other_field": "value"}
        assert detect_schema_type(unknown_data) == "unknown"
    
    def test_validate_json_against_schema(self):
        """Test JSON validation against schemas"""
        schemas = load_cms_schemas()
        
        # Test valid in-network rates data
        valid_data = {
            "provider_references": [
                {
                    "provider_group_id": "test-group",
                    "provider_groups": [
                        {
                            "npi": ["1234567890"]
                        }
                    ]
                }
            ],
            "in_network": [
                {
                    "negotiated_rates": [
                        {
                            "provider_references": [0],
                            "negotiated_prices": [
                                {
                                    "negotiated_type": "negotiated",
                                    "negotiated_rate": 100.0
                                }
                            ]
                        }
                    ],
                    "billing_code_type": "CPT",
                    "billing_code": "99213"
                }
            ]
        }
        
        result = validate_json_against_schema(valid_data, schemas["in-network-rates"])
        assert result["valid"] is True
        assert len(result["errors"]) == 0
    
    def test_validate_json_against_schema_invalid(self):
        """Test JSON validation with invalid data"""
        schemas = load_cms_schemas()
        
        # Test invalid data (missing required fields)
        invalid_data = {
            "provider_references": []  # Missing "in_network" field
        }
        
        result = validate_json_against_schema(invalid_data, schemas["in-network-rates"])
        assert result["valid"] is False
        assert len(result["errors"]) > 0
    
    def test_run_json_schema_validation(self):
        """Test full JSON schema validation pipeline"""
        # Create a temporary JSON file with valid data
        valid_data = {
            "provider_references": [
                {
                    "provider_group_id": "test-group",
                    "provider_groups": [
                        {
                            "npi": ["1234567890"]
                        }
                    ]
                }
            ],
            "in_network": [
                {
                    "negotiated_rates": [
                        {
                            "provider_references": [0],
                            "negotiated_prices": [
                                {
                                    "negotiated_type": "negotiated",
                                    "negotiated_rate": 100.0
                                }
                            ]
                        }
                    ],
                    "billing_code_type": "CPT",
                    "billing_code": "99213"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(valid_data, f)
            temp_file = f.name
        
        try:
            result = run_json_schema_validation(temp_file)
            
            assert "timestamp" in result
            assert "detected_schema_type" in result
            assert "schema_validation" in result
            assert "summary" in result
            
            # Should detect as in-network-rates
            assert result["detected_schema_type"] == "in-network-rates"
            
            # Should be valid
            assert result["schema_validation"]["valid"] is True
            
        finally:
            os.unlink(temp_file)
    
    def test_run_json_schema_validation_invalid_json(self):
        """Test validation with invalid JSON file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            temp_file = f.name
        
        try:
            result = run_json_schema_validation(temp_file)
            
            assert "error" in result
            assert "JSON parse error" in result["error"]
            assert result["summary"]["errors"] > 0
            
        finally:
            os.unlink(temp_file)


if __name__ == "__main__":
    pytest.main([__file__])
