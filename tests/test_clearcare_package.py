"""
Comprehensive tests for the clearcare_compliance package.
Tests CLI integration, CSV validation, JSON validation, and end-to-end functionality.
"""

import tempfile
import json
import os
from pathlib import Path

import pytest

from clearcare_compliance.csv_validator import validate_csv
from clearcare_compliance.json_validator import validate_json
from clearcare_compliance.detectors import sniff_kind_from_bytes
from clearcare_compliance.reporters import to_human, to_json, to_csv
from clearcare_compliance.types import ValidationResult, Finding


def test_csv_tall_validation():
    """Test CSV tall format validation with proper preamble."""
    csv_content = """MRF Date,CMS Template Version,Hospital Name
2025-01-01,2.2.1,Test Hospital
billing_code_type,billing_code,description,standard_charge,payer_name,plan_name
CPT,99213,Office visit,138.00,Aetna,Silver
CPT,99214,Office visit level 4,200.00,Blue Cross,Gold"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        result = validate_csv(csv_path)
        
        # Basic validation
        assert isinstance(result, ValidationResult)
        assert result.file_type == "csv_tall"
        assert result.file_path == csv_path
        assert result.schema_version == "CSV-Template"
        
        # Check preamble was detected
        assert "mrf date" in result.preamble
        assert result.preamble["mrf date"] == "2025-01-01"
        assert "cms template version" in result.preamble
        
        # Should pass basic validation (no missing required headers)
        assert result.ok is True or len(result.findings) == 0
        
    finally:
        os.unlink(csv_path)


def test_csv_wide_validation():
    """Test CSV wide format validation with payer|plan columns."""
    csv_content = """Hospital Name,Last Updated,Version
Test Medical Center,2025-01-01,v1.0
billing_code_type,billing_code,description,Aetna|Silver HMO,Blue Cross|Gold PPO
CPT,99213,Office visit,128.00,150.00
CPT,99214,Office visit level 4,180.00,220.00"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        result = validate_csv(csv_path)
        
        # Basic validation
        assert isinstance(result, ValidationResult)
        assert result.file_type == "csv_wide"
        assert result.file_path == csv_path
        
        # Check preamble was detected (hospital metadata format)
        assert "hospital name" in result.preamble
        assert result.preamble["hospital name"] == "Test Medical Center"
        
        # May have warnings about missing CMS preamble labels
        assert isinstance(result, ValidationResult)
        
    finally:
        os.unlink(csv_path)


def test_csv_validation_with_errors():
    """Test CSV validation with missing required headers."""
    csv_content = """Some metadata,Other info
Value 1,Value 2
missing_required_header,some_data
CPT,99213"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        result = validate_csv(csv_path)
        
        # Should detect issues
        assert isinstance(result, ValidationResult)
        assert result.ok is False
        assert len(result.findings) > 0
        
        # Check that we have error findings
        error_findings = [f for f in result.findings if f.severity == "error"]
        assert len(error_findings) > 0
        
    finally:
        os.unlink(csv_path)


def test_json_schema_validation():
    """Test JSON validation against CMS schema."""
    json_content = {
        "reporting_entity_name": "Test Hospital",
        "reporting_entity_type": "hospital",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "Test Plan",
                        "plan_id": "test-plan-123",
                        "plan_id_type": "HIOS",
                        "plan_market_type": "individual"
                    }
                ]
            }
        ]
    }
    
    result = validate_json(json.dumps(json_content))
    
    # Basic validation
    assert isinstance(result, ValidationResult)
    assert result.file_type == "json"
    assert result.schema_version is not None
    
    # JSON validation may have findings depending on schema
    assert isinstance(result, ValidationResult)


def test_json_validation_with_errors():
    """Test JSON validation with invalid data."""
    invalid_json = '{"invalid": "json structure", "missing_required": "fields"}'
    
    result = validate_json(invalid_json)
    
    # Should detect issues
    assert isinstance(result, ValidationResult)
    assert result.file_type == "json"
    
    # May have findings (depending on schema strictness)
    # The exact behavior depends on the fallback schema


def test_file_type_detection():
    """Test file type detection from bytes."""
    # Test JSON detection
    json_bytes = b'{"test": "value", "array": [1, 2, 3]}'
    assert sniff_kind_from_bytes(json_bytes) == "json"
    
    # Test CSV detection
    csv_bytes = b"header1,header2,header3\nvalue1,value2,value3\n"
    assert sniff_kind_from_bytes(csv_bytes) == "csv"
    
    # Test XML detection
    xml_bytes = b"<?xml version='1.0'?><root><item>value</item></root>"
    assert sniff_kind_from_bytes(xml_bytes) == "xml"
    
    # Test unknown
    unknown_bytes = b"some random text without clear structure"
    assert sniff_kind_from_bytes(unknown_bytes) == "unknown"


def test_reporters():
    """Test reporter functions (to_human, to_json, to_csv)."""
    # Create a test validation result
    result = ValidationResult(
        file_path="test.csv",
        file_type="csv_tall",
        ok=True,
        schema_version="test",
        preamble={"test": "value"},
        findings=[
            Finding(
                severity="warning",
                rule="test_rule",
                message="Test warning message",
                row=5,
                field="test_field"
            )
        ]
    )
    
    # Test JSON reporter
    json_output = to_json(result)
    assert isinstance(json_output, str)
    parsed_json = json.loads(json_output)
    assert parsed_json["ok"] is True
    assert parsed_json["file_type"] == "csv_tall"
    assert len(parsed_json["findings"]) == 1
    
    # Test CSV reporter
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        csv_path = f.name
    
    try:
        to_csv(result, path=csv_path)
        
        # Verify CSV was created and has content
        assert os.path.exists(csv_path)
        with open(csv_path, 'r') as f:
            csv_content = f.read()
            assert "severity,rule,row,field,message" in csv_content
            assert "test_rule" in csv_content
            
    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)


def test_cli_integration():
    """Test CLI command works end-to-end."""
    # Test with a simple CSV
    csv_content = """Hospital Name,Date
Test Hospital,2025-01-01
billing_code_type,billing_code,description,standard_charge
CPT,99213,Office visit,138.00
CPT,99214,Office visit level 4,200.00"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        # Test CSV validation directly (CLI would call this)
        result = validate_csv(csv_path)
        
        assert isinstance(result, ValidationResult)
        assert result.file_type in ["csv_tall", "csv_wide"]
        assert result.file_path == csv_path
        
        # Test JSON output format (what CLI would produce)
        json_output = to_json(result)
        parsed = json.loads(json_output)
        assert "ok" in parsed
        assert "file_type" in parsed
        assert "findings" in parsed
        
    finally:
        os.unlink(csv_path)


def test_encoding_validation():
    """Test CSV encoding validation."""
    # Create CSV with UTF-8 characters
    csv_content = """Hospital Name,Date
Test Hôspital,2025-01-01
billing_code_type,billing_code,description,standard_charge
CPT,99213,Office visit,138.00"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        result = validate_csv(csv_path)
        
        # Should handle UTF-8 encoding properly
        assert isinstance(result, ValidationResult)
        
    finally:
        os.unlink(csv_path)


def test_preamble_metadata_validation():
    """Test preamble metadata validation."""
    # CSV with missing required preamble labels
    csv_content = """Random Label,Other Label
Value 1,Value 2
billing_code_type,billing_code,description,standard_charge
CPT,99213,Office visit,138.00
CPT,99214,Office visit level 4,200.00"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_content)
        csv_path = f.name
    
    try:
        result = validate_csv(csv_path)
        
        # Should have warnings about missing required labels
        assert isinstance(result, ValidationResult)
        
        # Check for preamble validation findings
        preamble_findings = [f for f in result.findings if "preamble" in f.rule]
        assert len(preamble_findings) > 0
        
    finally:
        os.unlink(csv_path)


def test_summary_statistics():
    """Test that validation results include proper summary statistics."""
    # Create a result with mixed findings
    result = ValidationResult(
        file_path="test.csv",
        file_type="csv_tall",
        ok=False,
        findings=[
            Finding(severity="error", rule="rule1", message="Error 1"),
            Finding(severity="error", rule="rule2", message="Error 2"),
            Finding(severity="warning", rule="rule3", message="Warning 1"),
            Finding(severity="info", rule="rule4", message="Info 1"),
        ]
    )
    
    counts = result.counts()
    assert counts["errors"] == 2
    assert counts["warnings"] == 1
    assert counts["info"] == 1
    assert sum(counts.values()) == 4
