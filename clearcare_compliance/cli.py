#!/usr/bin/env python3
"""
ClearCare Compliance CLI - Entry point for clearcare-validate command
"""

import sys
import json
import click
import pathlib

from .types import ValidationResult
from .detectors import sniff_kind_from_bytes
from .json_validator import validate_json
from .csv_validator import validate_csv
from .reporters import to_human, to_json, to_csv


@click.command()
@click.argument("path", type=click.Path(exists=True, dir_okay=False))
@click.option("--format", "fmt", type=click.Choice(["human", "json", "csv"]), default="human")
@click.option("--out", "out", type=click.Path(dir_okay=False), help="when --format=csv write findings to this file")
@click.option("--schema-version", "schema_version", default=None, help="Force a CMS JSON schema version")
def main(path, fmt, out, schema_version):
    """Validate CMS Hospital Price Transparency files (JSON or CSV)."""
    
    path = pathlib.Path(path)
    data = path.read_bytes()
    
    # Detect file type
    kind = sniff_kind_from_bytes(data)
    
    # Validate based on file type
    if kind == "json":
        text = data.decode("utf-8-sig", errors="ignore")
        res = validate_json(text, schema_version=schema_version)
    elif kind in ["csv", "unknown", "xml"]:
        res = validate_csv(str(path))
    else:
        res = ValidationResult(
            file_path=str(path),
            file_type="unknown",
            ok=False,
            errors=[f"Unrecognized file type; expected JSON or CSV"]
        )
    
    # Output results
    if fmt == "human":
        print(to_human(res))
    elif fmt == "json":
        print(to_json(res))
    else:  # csv
        if out is None:
            out = path.with_suffix(path.suffix + ".validation.csv")
        to_csv(res, path=out)
        print(f"Report written to {out}")
    
    # Exit with appropriate status
    sys.exit(0 if res.ok else 2)


if __name__ == "__main__":
    main()
