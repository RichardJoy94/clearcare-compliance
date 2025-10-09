# tests/test_csv_header_sniffer.py
import os, tempfile
from app.csv_header_sniffer import find_header_row, extract_header

def test_cms_header_first_line():
    csv_text = "billing_code,billing_code_type,description,standard_charge\n1,abc,visit,100\n"
    idx = find_header_row(csv_text)
    assert idx == 0

def test_cms_header_after_metadata():
    csv_text = (
        "Hospital: Foo General\n"
        "Generated: 2024-01-01\n"
        "Random: notes\n"
        "billing_code,billing_code_type,description,standard_charge,payer\n"
        "99213,CPT,office visit,128.00,ACME\n"
    )
    idx = find_header_row(csv_text)
    assert idx == 3

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as f:
        f.write(csv_text)
        path = f.name
    try:
        headers = extract_header(path, idx)
        assert headers[:4] == ["billing_code", "billing_code_type", "description", "standard_charge"]
    finally:
        os.unlink(path)
