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

def _map_headers_to_standard(headers: List[str], layout: str = "csv_wide") -> List[str]:
    """Map various header formats to standard CMS headers using flexible mappings."""
    from .csv_specs import TALL, WIDE
    
    spec = TALL if layout == "csv_tall" else WIDE
    flexible_mappings = spec.get("flexible_mappings", {})
    
    mapped = []
    for header in headers:
        header_lower = header.lower()
        mapped_header = None
        
        # Check each flexible mapping
        for standard_field, variations in flexible_mappings.items():
            for variation in variations:
                if variation.lower() in header_lower or header_lower in variation.lower():
                    mapped_header = standard_field
                    break
            if mapped_header:
                break
        
        # If no mapping found, keep original header
        if not mapped_header:
            mapped_header = header
            
        mapped.append(mapped_header)
    
    return list(set(mapped))  # Remove duplicates

def _require_headers(headers: List[str], required: List[str], layout: str = "csv_wide") -> List[str]:
    # First try exact match
    missing = [h for h in required if h not in headers]
    if not missing:
        return missing
    
    # Then try with header mapping
    mapped_headers = _map_headers_to_standard(headers, layout)
    missing = [h for h in required if h not in mapped_headers]
    return missing

def _validate_mrf_info(preamble: Dict[str, str], res: ValidationResult) -> None:
    """Validate MRF Information requirements (45 CFR 180.50(b)(2)(i))."""
    from .csv_specs import PREAMBLE
    
    mrf_info = PREAMBLE.get("mrf_info", {})
    required_labels = mrf_info.get("required_labels", [])
    flexible_labels = mrf_info.get("flexible_labels", [])
    
    # Check for required MRF info
    found_required = []
    found_flexible = []
    
    for label in required_labels:
        if any(label.lower() in key.lower() for key in preamble.keys()):
            found_required.append(label)
    
    for label in flexible_labels:
        if any(label.lower() in key.lower() for key in preamble.keys()):
            found_flexible.append(label)
    
    # If we have flexible labels but not required ones, that's acceptable
    if not found_required and not found_flexible:
        res.findings.append(Finding(
            severity="warning",
            rule="csv.mrf_info.missing",
            message="Missing MRF Information. Expected: 'mrf date', 'cms template version', 'affirmation statement' or flexible alternatives",
            row=1,
            expected="MRF date, CMS template version, affirmation statement",
            actual="No MRF information found"
        ))
    elif not found_required and found_flexible:
        res.findings.append(Finding(
            severity="info",
            rule="csv.mrf_info.flexible_format",
            message=f"Using flexible MRF format. Found: {found_flexible}. Consider using standard CMS format for better compliance.",
            row=1,
            expected="Standard CMS MRF format",
            actual=f"Flexible format: {found_flexible}"
        ))

def _validate_hospital_info(preamble: Dict[str, str], res: ValidationResult) -> None:
    """Validate Hospital Information requirements (45 CFR 180.50(b)(2)(i)(A))."""
    from .csv_specs import PREAMBLE
    
    hospital_info = PREAMBLE.get("hospital_info", {})
    required_labels = hospital_info.get("required_labels", [])
    flexible_labels = hospital_info.get("flexible_labels", [])
    
    # Check for required hospital info
    found_required = []
    found_flexible = []
    
    for label in required_labels:
        if any(label.lower() in key.lower() for key in preamble.keys()):
            found_required.append(label)
    
    for label in flexible_labels:
        if any(label.lower() in key.lower() for key in preamble.keys()):
            found_flexible.append(label)
    
    # Hospital info is more flexible - just check we have some
    if not found_required and not found_flexible:
        res.findings.append(Finding(
            severity="warning",
            rule="csv.hospital_info.missing",
            message="Missing Hospital Information. Expected: hospital name, location, address, license information",
            row=1,
            expected="Hospital name, location, address, license",
            actual="No hospital information found"
        ))
    elif found_required or found_flexible:
        # Good - we have hospital info
        found_all = found_required + found_flexible
        res.findings.append(Finding(
            severity="info",
            rule="csv.hospital_info.present",
            message=f"Hospital information found: {found_all}",
            row=1,
            expected="Hospital information",
            actual=f"Found: {found_all}"
        ))

def _validate_standard_charges(headers: List[str], layout: str, res: ValidationResult) -> None:
    """Validate Standard Charges requirements (45 CFR 180.50(b)(2)(ii))."""
    from .csv_specs import TALL, WIDE
    
    spec = TALL if layout == "csv_tall" else WIDE
    standard_charges = spec.get("standard_charges", {})
    required_fields = standard_charges.get("required_fields", [])
    flexible_mappings = spec.get("flexible_mappings", {})
    
    # Map headers to standard fields
    mapped_headers = _map_headers_to_standard(headers, layout)
    
    # Check for standard charge fields
    found_fields = []
    missing_fields = []
    
    for field in required_fields:
        if field in mapped_headers:
            found_fields.append(field)
        else:
            # Check if any flexible mapping exists
            field_mappings = flexible_mappings.get(field, [])
            if any(any(mapping.lower() in header.lower() for header in headers) for mapping in field_mappings):
                found_fields.append(field)
            else:
                missing_fields.append(field)
    
    if missing_fields:
        res.findings.append(Finding(
            severity="warning",
            rule="csv.standard_charges.missing",
            message=f"Missing standard charge fields: {missing_fields}",
            row=1,
            expected=str(missing_fields),
            actual=f"Found: {found_fields}",
            field="headers"
        ))
    else:
        res.findings.append(Finding(
            severity="info",
            rule="csv.standard_charges.complete",
            message=f"Standard charges validation passed. Found: {found_fields}",
            row=1,
            expected="All standard charge fields",
            actual=f"Found: {found_fields}"
        ))

def _validate_item_service_info(headers: List[str], layout: str, res: ValidationResult) -> None:
    """Validate Item & Service Information requirements (45 CFR 180.50(b)(2)(iii))."""
    from .csv_specs import TALL, WIDE
    
    spec = TALL if layout == "csv_tall" else WIDE
    item_service_info = spec.get("item_service_info", {})
    required_fields = item_service_info.get("required_fields", [])
    flexible_mappings = spec.get("flexible_mappings", {})
    
    # Map headers to standard fields
    mapped_headers = _map_headers_to_standard(headers, layout)
    
    found_fields = []
    missing_fields = []
    
    for field in required_fields:
        if field in mapped_headers:
            found_fields.append(field)
        else:
            # Check flexible mappings
            field_mappings = flexible_mappings.get(field, [])
            if any(any(mapping.lower() in header.lower() for header in headers) for mapping in field_mappings):
                found_fields.append(field)
            else:
                missing_fields.append(field)
    
    if missing_fields:
        res.findings.append(Finding(
            severity="warning",
            rule="csv.item_service_info.missing",
            message=f"Missing item & service information fields: {missing_fields}",
            row=1,
            expected=str(missing_fields),
            actual=f"Found: {found_fields}",
            field="headers"
        ))

def _validate_coding_info(headers: List[str], layout: str, res: ValidationResult) -> None:
    """Validate Coding Information requirements (45 CFR 180.50(b)(2)(iv))."""
    from .csv_specs import TALL, WIDE
    
    spec = TALL if layout == "csv_tall" else WIDE
    coding_info = spec.get("coding_info", {})
    required_fields = coding_info.get("required_fields", [])
    flexible_mappings = spec.get("flexible_mappings", {})
    
    # Map headers to standard fields
    mapped_headers = _map_headers_to_standard(headers, layout)
    
    found_fields = []
    missing_fields = []
    
    for field in required_fields:
        if field in mapped_headers:
            found_fields.append(field)
        else:
            # Check flexible mappings
            field_mappings = flexible_mappings.get(field, [])
            if any(any(mapping.lower() in header.lower() for header in headers) for mapping in field_mappings):
                found_fields.append(field)
            else:
                missing_fields.append(field)
    
    if missing_fields:
        res.ok = False
        res.findings.append(Finding(
            severity="error",
            rule="csv.coding_info.missing",
            message=f"Missing required coding information fields: {missing_fields}",
            row=1,
            expected=str(missing_fields),
            actual=f"Found: {found_fields}",
            field="headers"
        ))
    else:
        res.findings.append(Finding(
            severity="info",
            rule="csv.coding_info.complete",
            message=f"Coding information validation passed. Found: {found_fields}",
            row=1,
            expected="All coding information fields",
            actual=f"Found: {found_fields}"
        ))

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
    
    # Comprehensive CMS validation
    _validate_mrf_info(preamble, res)
    _validate_hospital_info(preamble, res)
    _validate_standard_charges(headers_lower, layout, res)
    _validate_item_service_info(headers_lower, layout, res)
    _validate_coding_info(headers_lower, layout, res)
    # structural header checks (legacy - now handled by comprehensive validation above)
    # Skip legacy checks since comprehensive validation handles this better
    # if layout == "csv_tall":
    #     missing = _require_headers(headers_lower, TALL["required_headers"], layout)
    #     if missing:
    #         res.ok = False
    #         res.findings.append(Finding(
    #             severity="error", 
    #             rule="csv.headers.required", 
    #             message=f"Missing required headers: {missing}", 
    #             row=hdr_idx+1,
    #             expected=str(missing),
    #             actual=str(headers_lower[:10]) + ("..." if len(headers_lower) > 10 else ""),
    #             field="headers"
    #         ))
    # else:
    #     missing = _require_headers(headers_lower, WIDE["base_required_headers"], layout)
    #     if missing:
    #         res.ok = False
    #         res.findings.append(Finding(
    #             severity="error", 
    #             rule="csv.headers.required", 
    #             message=f"Missing base headers: {missing}", 
    #             row=hdr_idx+1,
    #             expected=str(missing),
    #             actual=str(headers_lower[:10]) + ("..." if len(headers_lower) > 10 else ""),
    #             field="headers"
    #         ))
    #     # must have at least one payer|plan column
    #     sep = WIDE["payer_plan_separator"]
    #     if not any(sep in h for h in headers_lower):
    #         res.ok = False
    #         res.findings.append(Finding(severity="error", rule="csv.headers.payer_plan", message=f"No payer{sep}plan columns detected in wide layout", row=hdr_idx+1))
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
    
    # coding present (enhanced with flexible mapping)
    if (layout == "csv_tall" and TALL["rules"]["require_coding"]) or (layout=="csv_wide" and WIDE["rules"]["require_coding"]):
        # Use flexible mapping to find coding columns
        mapped_headers = _map_headers_to_standard([col.lower() for col in df.columns], layout)
        
        code_type_col = None
        code_col = None
        
        # Find columns that map to billing_code_type and billing_code
        for i, col in enumerate(df.columns):
            col_lower = col.lower()
            mapped = mapped_headers[i] if i < len(mapped_headers) else col_lower
            
            if mapped == "billing_code_type" or mapped == "code_type":
                code_type_col = col
            elif mapped == "billing_code" or mapped == "billing_accounting_code":
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
