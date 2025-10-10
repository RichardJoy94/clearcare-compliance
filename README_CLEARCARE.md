## ClearCare Compliance - CMS HPT Validator

A reusable Python package for validating Hospital Price Transparency files exactly like CMS, supporting both JSON and CSV (Tall/Wide) formats.

### Package Structure
```
clearcare_compliance/
├── __init__.py
├── cli.py                      # CLI entry point
├── types.py                    # ValidationResult, Finding
├── detectors.py                # File type detection
├── json_validator.py           # JSON validation
├── csv_validator.py            # CSV validation
├── csv_specs.py                # CSV specifications
├── reporters.py                # Output formatting
└── schemas/                    # Data files only
    ├── json/
    │   └── VERSION.json        # Schema manifest
    └── csv/
        ├── preamble.yaml
        ├── tall.yaml
        └── wide.yaml
```

### CLI Usage
```bash
# Install the package
pip install -e .

# Validate any file (auto-detect JSON vs CSV)
clearcare-validate path/to/standardcharges.csv --format human
clearcare-validate path/to/hospital.json --format json > report.json
clearcare-validate path/to/file.csv --format csv --out findings.csv

# Example with test file
clearcare-validate test_hospital.csv --format human
```

### API Integration
```python
# Import validators directly
from clearcare_compliance.csv_validator import validate_csv
from clearcare_compliance.json_validator import validate_json
from clearcare_compliance.reporters import to_json, to_human

# Use in FastAPI or other applications
csv_result = validate_csv("path/to/file.csv")
json_output = to_json(csv_result)
```

### Features

#### JSON Validation
- Validates against CMS Hospital Price Transparency schemas
- Auto-detects schema type (in-network-rates, allowed-amounts, provider-reference)
- Reads from vendored schemas in package or falls back to `rules/cms/json/`
- Provides detailed error messages with field paths

#### CSV Validation
- Supports CMS Tall and Wide layouts
- Validates 3-row preamble (Row1 labels, Row2 values, Row3 headers)
- Handles both strict CMS format and flexible hospital metadata format
- Enforces core data rules:
  - Required headers per layout
  - Description present
  - Coding present (billing_code_type + billing_code)
  - Payer|plan columns for wide layout
  - Drug unit/type pairing
  - Estimated amount recommendations

#### Error Handling
- UTF-8 encoding validation
- Graceful handling of parsing errors
- Human-readable error messages with line numbers
- Expected vs Actual comparisons

### Schemas & Vendoring
- Vendored under `clearcare_compliance/schemas/json/` with a `VERSION.json` manifest
- Auto-updated by `.github/workflows/update-cms-schemas.yml` which opens PRs
- Run vendor script locally: `python scripts/vendor_cms_assets.py`

### FastAPI Integration
The package is already integrated with the existing FastAPI application:
- `app/api.py` imports and uses the new validators
- Maintains backward compatibility with existing frontend
- Preserves all existing validation response formats
- Adds new validation capabilities (encoding, preamble metadata)

### Testing
```bash
# Run all tests
pytest tests/ -v

# Run package-specific tests
pytest tests/test_clearcare_package.py -v

# Run CLI detection tests
pytest tests/test_cli_detects.py -v
```

### Development Setup
```bash
# Install in development mode
pip install -e .

# Run vendor script to populate schemas
python scripts/vendor_cms_assets.py

# Test CLI with sample file
clearcare-validate test_file.csv --format human

# Test FastAPI integration
python -c "from app.api import app; print('FastAPI app imported successfully')"
```

### Deployment Notes
- Package structure is now flat (no nested directories)
- All existing FastAPI code remains intact
- Frontend integration preserved
- Database schema unchanged
- New validation capabilities added seamlessly

### Future Enhancements
- Expand CSV schemas to mirror CMS data dictionary fully
- Add more JSON schema error message improvements
- Include CMS example files in test suite
- Add XML support if CMS publishes hospital XML schemas
