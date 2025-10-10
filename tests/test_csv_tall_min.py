"""Test CSV tall format validation."""

import tempfile
import pathlib
from clearcare_compliance.csv_validator import validate_csv


def test_csv_tall_preamble_and_headers():
    """Test CSV tall format with preamble and headers."""
    txt = """MRF Date, CMS Template Version
2025-01-01,2.2.1
billing_code_type,billing_code,description,standard_charge,payer_name,plan_name
CPT,99213,Office visit,138.00,Aetna,Silver"""
    
    p = pathlib.Path(tempfile.gettempdir()) / "mini_tall.csv"
    p.write_text(txt, encoding="utf-8")
    
    res = validate_csv(str(p))
    assert res.file_type == "csv_tall"
    assert res.ok is True or res.counts()["errors"] == 0
