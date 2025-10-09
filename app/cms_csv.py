from __future__ import annotations
import csv, io, re, json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import polars as pl
import yaml

ASSETS = Path(__file__).resolve().parent.parent / "rules" / "cms" / "csv" / "headers.yaml"

@dataclass
class CMSCSVLayout:
    header_row: int
    headers: List[str]          # Row3 (lowercased)
    layout: str                 # "tall" | "wide"
    metadata: Dict[str, str]    # from Row1 & Row2
    encoding_ok: bool
    notes: List[str]

def _read_prefix_bytes(path: Path, size: int = 200_000) -> bytes:
    with path.open("rb") as f:
        return f.read(size)

def _decode_utf8sig(data: bytes) -> str:
    return data.decode("utf-8-sig", errors="ignore")

def _csv_cells(line: str) -> List[str]:
    return next(csv.reader([line]))

def _try_parse_preamble(lines: List[str], spec: dict, max_scan: int = 20) -> Tuple[int, Dict[str,str], List[str], List[str]]:
    """Return (header_row, metadata, row1, row2). Heuristic:
    - scan for two adjacent non-empty rows (row1 labels, row2 values)
    - next non-empty row is considered data header row
    - row1 must contain >=2 items from preamble.required_labels
    """
    wanted = set(x.lower() for x in spec.get("preamble", {}).get("required_labels", []))
    
    # First try: Look for CMS preamble format (mrf date, cms template version)
    for i in range(min(max_scan, len(lines)-2)):
        r1, r2, r3 = lines[i], lines[i+1], lines[i+2]
        try:
            c1 = [c.strip().lower() for c in _csv_cells(r1)]
            c2 = [c.strip() for c in _csv_cells(r2)]
            c3 = [c.strip().lower() for c in _csv_cells(r3)]
        except Exception:
            continue
        if not c1 or not c2 or not c3:
            continue
        hits = sum(1 for k in wanted if k in c1)
        if hits >= 2 and len(c1) == len(c2):
            # build metadata by pairing c1->c2 for keys we know or any non-empty key
            md = {}
            for k, v in zip(c1, c2):
                if k and v:
                    md[k] = v
            # treat c3 as true header row
            return (i+2, md, c1, c2)
    
    # Second try: Look for hospital metadata format (more flexible)
    for i in range(min(max_scan, len(lines)-2)):
        r1, r2, r3 = lines[i], lines[i+1], lines[i+2]
        try:
            c1 = [c.strip().lower() for c in _csv_cells(r1)]
            c2 = [c.strip() for c in _csv_cells(r2)]
            c3 = [c.strip().lower() for c in _csv_cells(r3)]
        except Exception:
            continue
        if not c1 or not c2 or not c3:
            continue
        
        # Check if this looks like hospital metadata (hospital_name, last_updated_on, etc.)
        hospital_indicators = ['hospital', 'name', 'location', 'address', 'license', 'updated', 'version']
        hospital_hits = sum(1 for cell in c1 if any(indicator in cell for indicator in hospital_indicators))
        
        # Check if row 3 looks like data headers (billing_code, description, etc.)
        data_indicators = ['billing_code', 'description', 'charge', 'price', 'payer', 'code_type']
        data_hits = sum(1 for cell in c3 if any(indicator in cell for indicator in data_indicators))
        
        if hospital_hits >= 2 and data_hits >= 2 and len(c1) == len(c2):
            # This looks like hospital metadata followed by data headers
            md = {}
            for k, v in zip(c1, c2):
                if k and v:
                    md[k] = v
            # treat c3 as true header row
            return (i+2, md, c1, c2)
    # fallback: treat first non-empty row as header
    for j, ln in enumerate(lines):
        c = [x.strip() for x in _csv_cells(ln)]
        if any(c):
            return (j, {}, [], [])
    return (0, {}, [], [])

def sniff_layout_from_headers(headers: List[str]) -> str:
    # "wide" if any header contains a pipe, else "tall"
    if any("|" in h for h in headers):
        return "wide"
    # also wide if there are a lot of columns (e.g., > 25) and base columns are present
    base = {"billing_code_type", "billing_code", "description"}
    if base.issubset(set(headers)) and len(headers) > 25:
        return "wide"
    return "tall"

def parse_cms_csv(path: Path) -> CMSCSVLayout:
    spec = yaml.safe_load(ASSETS.read_text(encoding="utf-8"))
    raw = _decode_utf8sig(_read_prefix_bytes(path))
    lines = [ln for ln in io.StringIO(raw)]
    header_row, metadata, row1, row2 = _try_parse_preamble(lines, spec)
    # extract row3 headers
    headers = []
    try:
        headers = [c.strip().lower() for c in _csv_cells(lines[header_row])]
    except Exception:
        headers = []
    layout = sniff_layout_from_headers(headers)
    encoding_ok = True  # we already decoded utf-8-sig with ignore errors
    notes = []
    return CMSCSVLayout(header_row, headers, layout, metadata, encoding_ok, notes)

def read_parquet_schema_cols(parquet_path: Path) -> List[str]:
    lf = pl.scan_parquet(str(parquet_path))
    return list(lf.schema.keys())

def validate_cms_csv_structure(layout: CMSCSVLayout, schema_cols: List[str]) -> Dict:
    spec = yaml.safe_load(ASSETS.read_text(encoding="utf-8"))
    res = {"ok": True, "errors": [], "alerts": [], "layout": layout.layout}

    # Encoding
    must = (spec.get("encoding", {}) or {}).get("must_be", "utf-8").lower()
    if must != "utf-8":
        layout.notes.append(f"Encoding expectation is {must}, we always decode as utf-8-sig.")
    # Preamble labels
    required_labels = set(x.lower() for x in (spec.get("preamble", {}).get("required_labels") or []))
    label_hits = sum(1 for k in required_labels if k in layout.metadata.keys())
    if required_labels and label_hits < len(required_labels):
        res["alerts"].append({
            "rule": "preamble_labels",
            "message": f"Missing preamble labels: {sorted(required_labels - set(layout.metadata.keys()))}"
        })

    # Header requirement per layout, using the ACTUAL dataset columns (schema_cols)
    # Tall
    if layout.layout == "tall":
        missing = []
        for h in spec["required"]["tall"]:
            if h not in schema_cols:
                missing.append(h)
        if missing:
            res["ok"] = False
            res["errors"].append({
                "rule": "required_headers_tall",
                "message": f"Missing tall headers: {missing}",
                "present_columns": schema_cols[:200]
            })
    # Wide
    else:
        base = spec["required"]["wide"]["base"]
        missing_base = [h for h in base if h not in schema_cols]
        has_payer_plan_cols = any("|" in h for h in schema_cols)
        if missing_base or not has_payer_plan_cols:
            res["ok"] = False
            res["errors"].append({
                "rule": "required_headers_wide",
                "message": f"Missing wide base columns {missing_base} or no payer|plan columns detected",
                "present_columns": schema_cols[:200]
            })

    return res

def validate_cms_data_rules(layout: CMSCSVLayout, schema_cols: List[str]) -> Dict:
    spec = yaml.safe_load(ASSETS.read_text(encoding="utf-8"))
    out = {"ok": True, "errors": [], "alerts": []}

    # description present
    if spec["rules"].get("description_present", True) and "description" not in schema_cols:
        out["ok"] = False
        out["errors"].append({"rule": "description_present", "message": "Missing 'description' column"})

    # coding present
    if spec["rules"].get("coding_present", True):
        need = {"billing_code_type", "billing_code"}
        if not need.issubset(set(schema_cols)):
            out["ok"] = False
            out["errors"].append({"rule": "coding_present", "message": "Missing 'billing_code_type' or 'billing_code'"})

    # charge value types — if percentage/algorithm appear, require estimated_allowed_amount col
    if spec["rules"].get("charge_value_types"):
        # We can only check schema here; deeper content checks can be added later.
        # If a column name indicates algorithm/percentage, enforce estimated allowed amount column.
        indicator_cols = [c for c in schema_cols if re.search(r"(percent|algorithm)", c)]
        if indicator_cols:
            allowed_names = set(spec["rules"]["estimated_allowed_amount"]["column_names"])
            if not any(k in schema_cols for k in allowed_names):
                out["alerts"].append({
                    "rule": "estimated_allowed_amount",
                    "message": "Algorithm/percentage charges detected in header names, but no estimated allowed amount column found"
                })

    # wide payer|plan separator check
    if layout.layout == "wide":
        sep = spec["rules"]["wide_layout"]["payer_plan_separator"]
        if not any(sep in h for h in schema_cols):
            out["ok"] = False
            out["errors"].append({"rule": "payer_plan_separator", "message": f"Wide layout requires payer and plan head names with '{sep}' separator"})

    return out

def analyze_cms_csv(csv_path: Path, parquet_path: Path) -> Dict:
    layout = parse_cms_csv(csv_path)
    # Use Parquet schema for actual dataset columns
    schema_cols = read_parquet_schema_cols(parquet_path) if parquet_path.exists() else layout.headers
    structure = validate_cms_csv_structure(layout, schema_cols)
    data_rules = validate_cms_data_rules(layout, schema_cols)
    ok = structure["ok"] and data_rules["ok"]
    return {
        "ok": ok,
        "layout": layout.layout,
        "header_row": layout.header_row,
        "headers": layout.headers[:200],
        "metadata": layout.metadata,
        "encoding_ok": layout.encoding_ok,
        "structure": structure,
        "data_rules": data_rules,
        "present_columns": schema_cols[:200],
        "notes": layout.notes,
    }
