"""
CMS JSON Schema Validator
Validates JSON MRF files against CMS Hospital Price Transparency schemas
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, List
from jsonschema import validate, ValidationError, Draft7Validator
from datetime import datetime


def load_cms_schemas() -> Dict[str, Dict[str, Any]]:
    """Load all CMS schemas from the rules directory.
    
    Returns:
        Dict mapping schema names to schema definitions
    """
    schemas = {}
    schemas_dir = Path(__file__).parent.parent / "rules" / "cms" / "json"
    
    if not schemas_dir.exists():
        raise Exception(f"Schemas directory not found: {schemas_dir}")
    
    for schema_file in schemas_dir.glob("*.schema.json"):
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_data = json.load(f)
                schema_name = schema_file.stem.replace('.schema', '')
                schemas[schema_name] = schema_data
        except Exception as e:
            print(f"Warning: Failed to load schema {schema_file}: {e}")
    
    return schemas


def detect_schema_type(json_data: Dict[str, Any]) -> str:
    """Detect which CMS schema type the JSON data matches.
    
    Args:
        json_data: The JSON data to analyze
        
    Returns:
        String indicating the detected schema type or 'unknown'
    """
    # Check for in-network rates
    if 'in_network' in json_data and 'provider_references' in json_data:
        return 'in-network-rates'
    
    # Check for allowed amounts
    if 'allowed_amounts' in json_data and 'provider_references' in json_data:
        return 'allowed-amounts'
    
    # Check for provider reference only
    if 'provider_group_id' in json_data and 'provider_groups' in json_data:
        return 'provider-reference'
    
    return 'unknown'


def validate_json_against_schema(json_data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """Validate JSON data against a specific schema.
    
    Args:
        json_data: The JSON data to validate
        schema: The JSON schema to validate against
        
    Returns:
        Dict containing validation results
    """
    results = {
        "valid": True,
        "errors": [],
        "warnings": []
    }
    
    try:
        # Create validator and validate
        validator = Draft7Validator(schema)
        
        # Get all validation errors
        errors = list(validator.iter_errors(json_data))
        
        if errors:
            results["valid"] = False
            for error in errors:
                error_info = {
                    "message": error.message,
                    "path": list(error.path),
                    "schema_path": list(error.schema_path),
                    "validator": error.validator,
                    "validator_value": error.validator_value
                }
                results["errors"].append(error_info)
        
    except Exception as e:
        results["valid"] = False
        results["errors"].append({
            "message": f"Schema validation failed: {str(e)}",
            "path": [],
            "schema_path": [],
            "validator": "exception",
            "validator_value": None
        })
    
    return results


def run_json_schema_validation(json_path: str) -> Dict[str, Any]:
    """Run CMS JSON schema validation on a JSON file.
    
    Args:
        json_path: Path to the JSON file to validate
        
    Returns:
        Dict containing comprehensive validation results
    """
    try:
        # Load the JSON data
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Load CMS schemas
        schemas = load_cms_schemas()
        
        # Detect schema type
        detected_type = detect_schema_type(json_data)
        
        # Initialize results
        results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "file_path": json_path,
            "detected_schema_type": detected_type,
            "schema_validation": {
                "valid": False,
                "schema_type": None,
                "errors": [],
                "warnings": []
            },
            "summary": {
                "total_schemas_checked": 0,
                "passed": 0,
                "failed": 0,
                "errors": 0
            }
        }
        
        # Try to validate against the detected schema type
        if detected_type in schemas:
            schema = schemas[detected_type]
            validation_result = validate_json_against_schema(json_data, schema)
            
            results["schema_validation"] = {
                "valid": validation_result["valid"],
                "schema_type": detected_type,
                "errors": validation_result["errors"],
                "warnings": validation_result["warnings"]
            }
            
            results["summary"]["total_schemas_checked"] = 1
            if validation_result["valid"]:
                results["summary"]["passed"] = 1
            else:
                results["summary"]["failed"] = 1
        else:
            # Try all schemas if detection failed
            results["schema_validation"]["errors"].append({
                "message": f"Unknown schema type detected: {detected_type}",
                "path": [],
                "schema_path": [],
                "validator": "detection",
                "validator_value": None
            })
            
            # Try each schema
            for schema_name, schema in schemas.items():
                validation_result = validate_json_against_schema(json_data, schema)
                results["summary"]["total_schemas_checked"] += 1
                
                if validation_result["valid"]:
                    results["summary"]["passed"] += 1
                    results["schema_validation"]["valid"] = True
                    results["schema_validation"]["schema_type"] = schema_name
                    break
                else:
                    results["summary"]["failed"] += 1
        
        return results
        
    except json.JSONDecodeError as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "file_path": json_path,
            "error": f"Invalid JSON: {str(e)}",
            "schema_validation": {
                "valid": False,
                "errors": [{"message": f"JSON parse error: {str(e)}", "path": [], "schema_path": [], "validator": "json", "validator_value": None}]
            },
            "summary": {
                "total_schemas_checked": 0,
                "passed": 0,
                "failed": 1,
                "errors": 1
            }
        }
    except Exception as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "file_path": json_path,
            "error": f"Validation failed: {str(e)}",
            "schema_validation": {
                "valid": False,
                "errors": [{"message": f"Validation error: {str(e)}", "path": [], "schema_path": [], "validator": "exception", "validator_value": None}]
            },
            "summary": {
                "total_schemas_checked": 0,
                "passed": 0,
                "failed": 0,
                "errors": 1
            }
        }
