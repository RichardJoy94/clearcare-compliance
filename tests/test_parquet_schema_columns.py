import polars as pl
import tempfile
import os
import json
from app.validator_utils import parquet_columns

def test_parquet_columns_from_schema():
    """Create tiny parquet with CMS-like columns."""
    df = pl.DataFrame({
        "billing_code_type": ["CPT"],
        "billing_code": ["99213"],
        "description": ["Office Visit"],
        "standard_charge": [150.00],
        "payer": ["Aetna"]
    })
    
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        df.write_parquet(f.name)
        parquet_path = f.name
    
    try:
        cols = parquet_columns(parquet_path)
        expected_cols = {"billing_code_type", "billing_code", "description", "standard_charge", "payer"}
        assert expected_cols.issubset(set(cols))
    finally:
        os.unlink(parquet_path)
