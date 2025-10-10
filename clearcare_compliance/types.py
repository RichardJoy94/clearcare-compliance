from dataclasses import dataclass, field as dataclass_field
from typing import List, Optional, Dict, Any, Literal

Severity = Literal["error", "warning", "info"]

@dataclass
class Finding:
    severity: Severity
    rule: str
    message: str
    row: Optional[int] = None         # 1-based line number for CSV; None for JSON/global
    field: Optional[str] = None       # column header or JSON path
    expected: Optional[str] = None
    actual: Optional[str] = None
    context: Dict[str, Any] = dataclass_field(default_factory=dict)

@dataclass
class ValidationResult:
    file_path: str
    file_type: Literal["json", "csv_tall", "csv_wide", "unknown"]
    ok: bool
    schema_version: Optional[str] = None
    preamble: Dict[str, str] = dataclass_field(default_factory=dict)  # for CSV
    summary: Dict[str, Any] = dataclass_field(default_factory=dict)
    findings: List[Finding] = dataclass_field(default_factory=list)
    errors: List[str] = dataclass_field(default_factory=list)

    def counts(self):
        from collections import Counter
        c = Counter(f.severity for f in self.findings)
        return {"errors": c.get("error", 0), "warnings": c.get("warning", 0), "info": c.get("info", 0)}