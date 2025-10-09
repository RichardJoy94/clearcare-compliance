from __future__ import annotations
import csv, io

# Keys we expect in CMS-like standard charges files (case-insensitive).
CMS_KEY_HEADERS = {
    "billing_code",
    "billing_code_type", 
    "description",
    "standard_charge",
    "payer",  # optional but common
    "de-identified",  # optional but common
}


def _one(csv_text: str, line: int) -> list[str] | None:
    """Try to parse line as a CSV row."""
    try:
        return next(csv.reader([csv_text.splitlines()[line]]))
    except Exception:
        return None


def find_header_row(csv_text: str, max_lines: int = 50) -> int:
    """Return the 0-based index of the 'true' header row.
    
    Heuristic: first row containing >= 3 CMS_KEY_HEADERS (case-insensitive).
    If none found in the first max_lines or file shorter than that, return 0.
    """
    lines = csv_text.splitlines()
    limit = min(max_lines, len(lines))
    
    for idx in range(limit):
        try:
            row = next(csv.reader([lines[idx]]))
            lowered = [c.strip().lower() for c in row]
            hits = sum(1 for h in lowered if h in CMS_KEY_HEADERS)
            if hits >= 3:
                return idx
        except Exception:
            continue
    
    return 0


def extract_header(local_csv_path: str, header_row: int) -> list[str]:
    """Re-open the CSV and return the header row (lowercased, stripped)."""
    with open(local_csv_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        for i, row in enumerate(csv.reader(f)):
            if i == header_row:
                return [c.strip().lower() for c in row]
    return []
