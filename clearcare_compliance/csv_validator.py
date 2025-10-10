from __future__ import annotations
from typing import Tuple, Dict, List, Optional
import csv, io, re
import pandas as pd
from .types import ValidationResult, Finding
from .detectors import guess_csv_layout
from .csv_specs import PREAMBLE, TALL, WIDE

def _read_prefix_bytes(path: str, size: int=200_000) -> bytes:
    with open(path,"rb") as f: return f.read(size)

def _csv_cells(line: str) -> List[str]:
    return next(csv.reader([line]))

def find_preamble_and_header_row(text: str, max_scan: int=30) -> Tuple[int, Dict[str,str], List[str]]:
    """
    Returns (header_row_index, preamble_dict, headers_lower)
    header_row_index is 0-based index in lines[] where column headers live (Row3 of CMS structure).
    Enhanced to handle both CMS format and hospital metadata format.
    """
    lines = [ln.rstrip("\n\r") for ln in io.StringIO(text) if ln.strip()]  # Filter empty lines
    wanted = set([w.lower() for w in PREAMBLE.get("required_labels",[])])
    
    # First try: Look for CMS preamble format (mrf date, cms template version)
    for i in range(min(max_scan, len(lines)-2)):
        try:
            r1 = [c.strip().lower() for c in _csv_cells(lines[i])]
            r2 = [c.strip() for c in _csv_cells(lines[i+1])]
            r3 = [c.strip().lower() for c in _csv_cells(lines[i+2])]
        except Exception:
            continue
        if not r1 or not r2 or not r3:
            continue
        hits = sum(1 for k in wanted if k in r1)
        if hits >= 2 and len(r1) == len(r2):
            # build metadata by pairing c1->c2 for keys we know or any non-empty key
            meta = {}
            for k, v in zip(r1, r2):
                if k and v:
                    meta[k] = v
            # treat c3 as true header row
            return i+2, meta, r3
    
    # Second try: Look for hospital metadata format (more flexible)
    for i in range(min(max_scan, len(lines)-2)):
        try:
            r1 = [c.strip().lower() for c in _csv_cells(lines[i])]
            r2 = [c.strip() for c in _csv_cells(lines[i+1])]
            r3 = [c.strip().lower() for c in _csv_cells(lines[i+2])]
        except Exception:
            continue
        if not r1 or not r2 or not r3:
            continue
        
        # Check if this looks like hospital metadata (hospital_name, last_updated_on, etc.)
        hospital_indicators = ['hospital', 'name', 'location', 'address', 'license', 'updated', 'version']
        hospital_hits = sum(1 for cell in r1 if any(indicator in cell for indicator in hospital_indicators))
        
        # Check if row 3 looks like data headers (billing_code, description, etc.)
        data_indicators = ['billing_code', 'description', 'charge', 'price', 'payer', 'code_type']
        data_hits = sum(1 for cell in r3 if any(indicator in cell for indicator in data_indicators))
        
        if hospital_hits >= 2 and data_hits >= 2 and len(r1) == len(r2):
            # This looks like hospital metadata followed by data headers
            meta = {}
            for k, v in zip(r1, r2):
                if k and v:
                    meta[k] = v
            # treat c3 as true header row
            return i+2, meta, r3
    
    # fallback: treat first non-empty row as header
    for j, ln in enumerate(lines):
        cells = [c.strip() for c in _csv_cells(ln)]
        if any(cells):
            return j, {}, [c.lower() for c in cells]
    return 0, {}, []

def _map_headers_to_standard(headers: List[str]) -> List[str]:
    """Map various header formats to standard CMS headers."""
    mapped = []
    for header in headers:
        # Map common variations to standard headers
        if "code" in header and "type" in header:
            mapped.append("billing_code_type")
        elif "code" in header and "type" not in header:
            mapped.append("billing_code")
        else:
            mapped.append(header)
    return list(set(mapped))  # Remove duplicates

def _require_headers(headers: List[str], required: List[str]) -> List[str]:
    # First try exact match
    missing = [h for h in required if h not in headers]
    if not missing:
        return missing
    
    # Then try with header mapping
    mapped_headers = _map_headers_to_standard(headers)
    missing = [h for h in required if h not in mapped_headers]
    return missing

def validate_csv(path: str) -> ValidationResult:
    raw = _read_prefix_bytes(path)
    text = raw.decode("utf-8-sig", errors="ignore")
    hdr_idx, preamble, headers_lower = find_preamble_and_header_row(text)
    layout = guess_csv_layout(headers_lower)
    kind = layout
    res = ValidationResult(
        file_path=path,
        file_type=kind, 
        ok=True, 
        schema_version="CSV-Template", 
        preamble=preamble
    )
    
    # Validate encoding (UTF-8 check)
    try:
        # Try to decode with strict UTF-8 to check for encoding issues
        with open(path, 'r', encoding='utf-8') as f:
            f.read(1000)  # Read first 1000 chars to test encoding
    except UnicodeDecodeError as e:
        res.ok = False
        res.findings.append(Finding(
            severity="error",
            rule="csv.encoding",
            message=f"Invalid UTF-8 encoding: {e}",
            row=1
        ))
    
    # Validate preamble metadata (required labels)
    # Accept either CMS format or hospital metadata format
    cms_labels = ["mrf date", "cms template version"]
    hospital_labels = ["hospital name", "last updated", "version"]
    
    has_cms_format = any(label in preamble for label in cms_labels)
    has_hospital_format = any(label in preamble for label in hospital_labels)
    
    if not has_cms_format and not has_hospital_format:
        res.ok = False
        res.findings.append(Finding(
            severity="warning",
            rule="csv.preamble.required_labels",
            message="Missing recommended preamble labels. Expected CMS format ('mrf date', 'cms template version') or hospital format ('hospital name', 'last updated', 'version')",
            row=1,
            expected="CMS or hospital preamble labels",
            actual="No recognized preamble labels found"
        ))
    elif not has_cms_format:
        # Hospital format detected, suggest CMS format for compliance
        res.findings.append(Finding(
            severity="info",
            rule="csv.preamble.format_suggestion",
            message="Hospital metadata format detected. Consider using CMS format ('mrf date', 'cms template version') for better compliance",
            row=1,
            expected="CMS format",
            actual="Hospital format"
        ))
    # structural header checks
    if layout == "csv_tall":
        missing = _require_headers(headers_lower, TALL["required_headers"])
        if missing:
            res.ok = False
            res.findings.append(Finding(
                severity="error", 
                rule="csv.headers.required", 
                message=f"Missing required headers: {missing}", 
                row=hdr_idx+1,
                expected=str(missing),
                actual=str(headers_lower[:10]) + ("..." if len(headers_lower) > 10 else ""),
                field="headers"
            ))
    else:
        missing = _require_headers(headers_lower, WIDE["base_required_headers"])
        if missing:
            res.ok = False
            res.findings.append(Finding(
                severity="error", 
                rule="csv.headers.required", 
                message=f"Missing base headers: {missing}", 
                row=hdr_idx+1,
                expected=str(missing),
                actual=str(headers_lower[:10]) + ("..." if len(headers_lower) > 10 else ""),
                field="headers"
            ))
        # must have at least one payer|plan column
        sep = WIDE["payer_plan_separator"]
        if not any(sep in h for h in headers_lower):
            res.ok = False
            res.findings.append(Finding(severity="error", rule="csv.headers.payer_plan", message=f"No payer{sep}plan columns detected in wide layout", row=hdr_idx+1))
    # Load dataframe starting at header row
    try:
        df = pd.read_csv(path, header=hdr_idx, dtype=str, encoding='utf-8').fillna("")
    except (UnicodeDecodeError, pd.errors.ParserError) as e:
        res.ok = False
        res.findings.append(Finding(
            severity="error",
            rule="csv.parsing",
            message=f"Failed to parse CSV: {e}",
            row=hdr_idx+1
        ))
        res.summary = {"rows": 0, "columns": []}
        return res
    # helper row->lineno (1-based)
    def line_no(i: int) -> int: return (hdr_idx+1) + 1 + i
    # simple data rules
    if (layout == "csv_tall" and TALL["rules"]["require_description"]) or (layout=="csv_wide" and WIDE["rules"]["require_description"]):
        if "description" in df.columns:
            bad = df["description"].str.strip()== ""
            for i in df[bad].index[:50]:
                res.ok=False; res.findings.append(Finding(severity="error", rule="csv.description.present", message="Description required", row=line_no(i), field="description"))
    # coding present
    if (layout == "csv_tall" and TALL["rules"]["require_coding"]) or (layout=="csv_wide" and WIDE["rules"]["require_coding"]):
        # Find columns that map to billing_code_type and billing_code
        code_type_col = None
        code_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if "code" in col_lower and "type" in col_lower:
                code_type_col = col
            elif "code" in col_lower and "type" not in col_lower:
                code_col = col
        
        if not code_type_col or not code_col:
            res.ok = False
            res.findings.append(Finding(
                severity="error", 
                rule="csv.coding.present", 
                message=f"Missing coding columns. Found: {[col for col in df.columns if 'code' in col.lower()]}, Expected: columns with 'code' and 'code' + 'type'", 
                row=hdr_idx+1,
                expected="billing_code_type, billing_code",
                actual=str([col for col in df.columns if 'code' in col.lower()]),
                field="headers"
            ))
        else:
            bad = (df[code_type_col].str.strip()=="") | (df[code_col].str.strip()=="")
            for i in df[bad].index[:50]:
                res.ok = False
                res.findings.append(Finding(
                    severity="error", 
                    rule="csv.coding.present", 
                    message="Coding fields required", 
                    row=line_no(i), 
                    field=f"{code_type_col},{code_col}"
                ))
    # Tall: if percentage or algorithm present -> require estimated_allowed_amount
    if layout=="csv_tall" and TALL["rules"]["require_estimated_when_percent_or_algorithm"]:
        has_pct_col = "standard_charge_percentage" in df.columns
        has_alg_col = "standard_charge_algorithm" in df.columns
        if (has_pct_col or has_alg_col) and "estimated_allowed_amount" in df.columns:
            cond = False
            if has_pct_col:
                cond = cond | (df["standard_charge_percentage"].str.strip()!="")
            if has_alg_col:
                cond = cond | (df["standard_charge_algorithm"].str.strip()!="")
            need_est = df[cond & (df["estimated_allowed_amount"].str.strip()=="")]
            for i in need_est.index[:50]:
                res.ok=False; res.findings.append(Finding(
                    severity="warning", rule="csv.estimated_amount.required",
                    message="Estimated allowed amount recommended when percentage/algorithm present",
                    row=line_no(i), field="estimated_allowed_amount"))
    # Drug pair rule
    if layout=="csv_tall" and TALL["rules"]["pair_drug_unit_and_type"]:
        u = "drug_unit_of_measurement" in df.columns
        t = "drug_type_of_measurement" in df.columns
        if u and t:
            mask = (df["drug_unit_of_measurement"].str.strip()=="") ^ (df["drug_type_of_measurement"].str.strip()=="")
            for i in df[mask].index[:50]:
                res.ok=False; res.findings.append(Finding(
                    severity="error", rule="csv.drug.fields.pair",
                    message="Drug unit/type must be both provided or both empty",
                    row=line_no(i)))
    res.summary = {"rows": int(df.shape[0]), "columns": list(df.columns)[:50]}
    return res
