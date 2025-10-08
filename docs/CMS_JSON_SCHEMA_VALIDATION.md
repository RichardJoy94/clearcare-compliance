# CMS JSON Schema Validation

This document describes the CMS Hospital Price Transparency JSON Schema validation functionality added to the ClearCare Compliance MVP.

## Overview

The system now supports validation of both CSV and JSON files:

- **CSV files**: Converted to Parquet and validated against business rules (existing functionality)
- **JSON files**: Validated against CMS Hospital Price Transparency JSON schemas

## Features

### 1. CMS Schema Validation

The system validates JSON MRF (Machine Readable Files) against three CMS schemas:

- **In-Network Rates**: For negotiated rates between providers and payers
- **Allowed Amounts**: For allowed amounts for out-of-network services  
- **Provider Reference**: For provider identification information

### 2. Automatic File Type Detection

The upload endpoint automatically detects file types based on content:

- Files starting with `{` or `[` are treated as JSON
- Files with comma or tab separators are treated as CSV
- Falls back to file extension if content detection fails

### 3. Combined Validation Results

The system provides comprehensive validation results:

- **CSV Validation**: Business rule validation results
- **JSON Validation**: Schema validation results with detection status
- **Combined Summary**: Aggregated pass/fail/error counts

### 4. Enhanced Evidence Packs

Evidence packs now include:

- `result.json`: Complete validation results
- `report.html`: Professional HTML report with both CSV and JSON validation results
- `summary.csv`: Combined validation summary in CSV format

### 5. Updated UI

The Runs page now displays:

- **CSV Rules** column with PASS/FAIL/- badges
- **JSON Schema** column with PASS/FAIL/- badges
- Real-time validation status for each run

## API Endpoints

### Upload Endpoint
```
POST /upload
```
- Automatically detects file type (JSON vs CSV)
- Processes files accordingly
- Returns file type and processing results

### Validation Endpoint
```
POST /validate
```
- Validates both CSV and JSON files if present
- Returns combined validation results
- Updates run status based on results

### Validation Details Endpoint
```
GET /runs/{run_id}/validation
```
- Returns validation badges for UI display
- Provides PASS/FAIL/- status for CSV and JSON validation

## CMS Schema Management

### Schema Sync
```bash
make sync-schemas
```
This command creates/updates the CMS schemas in `rules/cms/json/` directory.

### Schema Files
- `in-network-rates.schema.json`
- `allowed-amounts.schema.json`  
- `provider-reference.schema.json`

## Validation Process

### JSON Validation Flow

1. **File Upload**: JSON file uploaded and detected
2. **Schema Detection**: System detects which CMS schema applies
3. **Schema Validation**: JSON validated against detected schema
4. **Results Storage**: Validation results saved to evidence store
5. **UI Update**: Badges updated in runs interface

### CSV Validation Flow

1. **File Upload**: CSV file uploaded and detected
2. **Parquet Conversion**: CSV converted to Parquet format
3. **Rule Validation**: Business rules applied using existing logic
4. **Results Storage**: Validation results saved to evidence store
5. **UI Update**: Badges updated in runs interface

## Error Handling

The system gracefully handles various error conditions:

- **Invalid JSON**: JSON parse errors are caught and reported
- **Schema Detection Failure**: Unknown schemas are handled gracefully
- **Validation Errors**: Detailed error messages with JSON paths
- **Missing Files**: Appropriate error messages for missing validation files

## Testing

Run the test suite:

```bash
python -m pytest tests/test_json_validation.py -v
python -m pytest tests/test_file_detection.py -v
```

## Dependencies

New dependencies added:

- `jsonschema==4.19.2`: For JSON schema validation
- `PyYAML==6.0.1`: For YAML configuration files

## Configuration

The system uses the existing configuration structure:

- Rules are defined in `rules/registry.yaml`
- CMS schemas are stored in `rules/cms/json/`
- Validation results are saved in `/app/data/evidence/`

## Future Enhancements

Potential future improvements:

1. **Real-time Schema Updates**: Automatically sync schemas from CMS repository
2. **Custom Rule Support**: Allow custom JSON validation rules
3. **Schema Versioning**: Support multiple schema versions
4. **Enhanced Error Reporting**: More detailed error messages with suggestions
5. **Batch Validation**: Support for validating multiple files simultaneously
