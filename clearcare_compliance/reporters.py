from __future__ import annotations
from typing import Iterable, Dict, Any
from .types import ValidationResult, Finding
from rich.console import Console
from rich.table import Table
import csv, json, sys

def to_human(res: ValidationResult, *, stream=None):
    c = Console(file=stream or sys.stdout, highlight=False)
    c.rule("[bold]Validation Summary")
    counts = res.counts()
    c.print(f"Type: {res.file_type} • Schema: {res.schema_version or '-'} • "
            f"[bold red]{counts['errors']} errors[/], [bold yellow]{counts['warnings']} warnings[/]")
    if res.preamble:
        t = Table(title="CSV Preamble (Row1/Row2)", show_header=True)
        t.add_column("Label"); t.add_column("Value")
        for k,v in res.preamble.items():
            t.add_row(k, str(v))
        c.print(t)
    t = Table(title="Findings", show_header=True)
    for col in ("severity","rule","row","field","message","expected","actual"):
        t.add_column(col)
    for f in res.findings:
        t.add_row(f.severity, f.rule, str(f.row or ""), f.field or "", f.message, f.expected or "", f.actual or "")
    c.print(t)

def to_json(res: ValidationResult) -> str:
    return json.dumps({
        "ok": res.ok,
        "file_type": res.file_type,
        "schema_version": res.schema_version,
        "preamble": res.preamble,
        "summary": res.summary,
        "counts": res.counts(),
        "findings": [f.__dict__ for f in res.findings],
    }, indent=2)

def to_csv(res: ValidationResult, *, path: str):
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["severity","rule","row","field","message","expected","actual"])
        for a in res.findings:
            w.writerow([a.severity,a.rule,a.row or "",a.field or "",a.message,a.expected or "",a.actual or ""])
