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

def _require_headers(headers: List[str], required: List[str]) -> List[str]:
    missing = [h for h in required if h not in headers]
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
    required_labels = ["mrf date", "cms template version"]
    missing_labels = [label for label in required_labels if label not in preamble]
    if missing_labels:
        res.ok = False
        res.findings.append(Finding(
            severity="warning",
            rule="csv.preamble.required_labels",
            message=f"Missing recommended preamble labels: {missing_labels}",
            row=1
        ))
    # structural header checks
    if layout == "csv_tall":
        missing = _require_headers(headers_lower, TALL["required_headers"])
        if missing:
            res.ok = False
            res.findings.append(Finding(severity="error", rule="csv.headers.required", message=f"Missing required headers: {missing}", row=hdr_idx+1))
    else:
        missing = _require_headers(headers_lower, WIDE["base_required_headers"])
        if missing:
            res.ok = False
            res.findings.append(Finding(severity="error", rule="csv.headers.required", message=f"Missing base headers: {missing}", row=hdr_idx+1))
        # must have at least one payer|plan column
        sep = WIDE["payer_plan_separator"]
        if not any(sep in h for h in headers_lower):
            res.ok = False
            res.findings.append(Finding(severity="error", rule="csv.headers.payer_plan", message=f"No payer{sep}plan columns detected in wide layout", row=hdr_idx+1))
    # Load dataframe starting at header row
    df = pd.read_csv(path, header=hdr_idx, dtype=str).fillna("")
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
        need = [c for c in ("billing_code_type","billing_code") if c in df.columns]
        if len(need)<2:
            res.ok=False; res.findings.append(Finding(severity="error", rule="csv.coding.present", message="billing_code_type and billing_code required in headers", row=hdr_idx+1))
        else:
            bad = (df["billing_code_type"].str.strip()=="") | (df["billing_code"].str.strip()=="")
            for i in df[bad].index[:50]:
                res.ok=False; res.findings.append(Finding(severity="error", rule="csv.coding.present", message="Code type and code required", row=line_no(i)))
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
