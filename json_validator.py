from __future__ import annotations
from typing import Optional, Tuple
import json, re
from jsonschema import Draft202012Validator
from pathlib import Path
from .types import ValidationResult, Finding

def _load_schema(version: Optional[str]) -> Tuple[dict, str]:
    """Load schema from vendored schemas directory or fallback to rules/cms/json."""
    # First try: vendored schemas in package
    schema_dir = Path(__file__).parent / "schemas" / "json"
    manifest_path = schema_dir / "VERSION.json"
    
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        latest = manifest.get("latest")
        files = manifest.get("files", {})
        
        if latest and latest in files:
            schema_file = schema_dir / list(files[latest].keys())[0]
            if schema_file.exists():
                schema_data = json.loads(schema_file.read_text())
                return schema_data, latest
    
    # Second try: fallback to rules/cms/json directory
    fallback_dir = Path(__file__).parent.parent / "rules" / "cms" / "json"
    if fallback_dir.exists():
        for schema_file in fallback_dir.glob("*.schema.json"):
            try:
                schema_data = json.loads(schema_file.read_text())
                schema_name = schema_file.stem.replace('.schema', '')
                return schema_data, schema_name
            except Exception:
                continue
    
    # Final fallback: create a basic schema
    fallback_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "reporting_entity_name": {"type": "string"},
            "reporting_entity_type": {"type": "string"},
            "reporting_structure": {"type": "array"}
        },
        "required": ["reporting_entity_name", "reporting_entity_type"]
    }
    return fallback_schema, "fallback"

def detect_schema_type(json_data: dict) -> str:
    """Detect which CMS schema type the JSON data matches."""
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

def validate_json(text: str, *, schema_version: Optional[str]=None) -> ValidationResult:
    """Validate JSON against CMS schema."""
    try:
        data = json.loads(text)
        
        # Detect schema type first
        detected_type = detect_schema_type(data)
        
        # Load appropriate schema
        schema, v = _load_schema(schema_version)
        
        vr = ValidationResult(
            file_path="<input>",
            file_type="json", 
            ok=True, 
            schema_version=v
        )
        
        # Add detected schema type to summary
        vr.summary = {"detected_schema_type": detected_type}
        
        validator = Draft202012Validator(schema)
        for err in sorted(validator.iter_errors(data), key=lambda e: e.path):
            path = "$" + "".join([f"[{i}]" if isinstance(i,int) else f".{i}" for i in err.path])
            exp = str(err.schema.get("enum", err.schema.get("type","")))
            vr.findings.append(Finding(
                severity="error",
                rule="jsonschema",
                message=err.message,
                field=path,
                expected=exp or None,
                actual=None
            ))
        
        if vr.findings:
            vr.ok = False
            
        return vr
        
    except json.JSONDecodeError as e:
        return ValidationResult(
            file_path="<input>",
            file_type="json",
            ok=False,
            errors=[f"Invalid JSON: {e}"]
        )
    except Exception as e:
        return ValidationResult(
            file_path="<input>",
            file_type="json", 
            ok=False,
            errors=[f"Validation error: {e}"]
        )