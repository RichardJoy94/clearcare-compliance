import textwrap, tempfile, os, polars as pl
from pathlib import Path
from app.cms_csv import parse_cms_csv, analyze_cms_csv

def _write(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

def test_tall_with_preamble_and_headers(tmp_path: Path):
    # Row1 labels, Row2 values, Row3 headers, Row4 data
    csv_text = textwrap.dedent(\"\"\"\
    MRF Date,CMS Template Version,Other
    2025-01-01,2.0,foo
    billing_code_type,billing_code,description,standard_charge,payer
    CPT,99213,Office Visit,130.00,Aetna
    \"\"\")
    csvp = tmp_path/"tall.csv"; _write(csvp, csv_text)
    # Make a tiny parquet with matching schema_cols
    df = pl.DataFrame({
        "billing_code_type":["CPT"],
        "billing_code":["99213"],
        "description":["Office Visit"],
        "standard_charge":[130.0],
        "payer":["Aetna"],
    })
    pq = tmp_path/"tall.parquet"; df.write_parquet(pq)
    layout = parse_cms_csv(csvp)
    assert layout.header_row == 2
    assert layout.layout == "tall"
    res = analyze_cms_csv(csvp, pq)
    assert res["ok"] is True
    assert res["structure"]["ok"] is True

def test_wide_headers(tmp_path: Path):
    csv_text = textwrap.dedent(\"\"\"\
    MRF Date,CMS Template Version
    2025-01-01,2.0
    billing_code_type,billing_code,description,Aetna|Silver HMO
    CPT,99213,Office Visit,118.5
    \"\"\")
    csvp = tmp_path/"wide.csv"; _write(csvp, csv_text)
    df = pl.DataFrame({"billing_code_type":["CPT"],"billing_code":["99213"],"description":["Office Visit"],"Aetna|Silver HMO":[118.5]})
    pq = tmp_path/"wide.parquet"; df.write_parquet(pq)
    res = analyze_cms_csv(csvp, pq)
    assert res["ok"] is True
    assert res["layout"] == "wide"
