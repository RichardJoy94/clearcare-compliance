import json
import pathlib
import polars as pl

RUNS_DIR = pathlib.Path("/app/data/runs")

def load_run_meta(run_id: str) -> dict | None:
    """Load metadata for a given run_id from .meta.json file."""
    meta_path = RUNS_DIR / f"{run_id}.meta.json"
    if meta_path.exists():
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def parquet_columns(parquet_path: str) -> list[str]:
    """Extract column names from Parquet file's schema without loading full data.
    
    Do NOT materialize full data; just read schema
    """
    lf = pl.scan_parquet(parquet_path)
    return list(lf.collect_schema().keys())  # schema is dict(str, pl.DataType); we only need names
