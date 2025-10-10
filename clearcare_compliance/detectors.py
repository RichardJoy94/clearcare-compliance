from __future__ import annotations
from typing import Tuple, Literal
import json

def sniff_kind_from_bytes(b: bytes) -> Literal["json","csv","xml","unknown"]:
    s = b.lstrip()[:256]
    if s.startswith(b"{") or s.startswith(b"["):
        return "json"
    if s.startswith(b"<"):
        return "xml"
    # assume CSV if it has commas/quotes/newlines
    if b"," in s or b"\n" in s or b"\r" in s:
        return "csv"
    return "unknown"

def guess_csv_layout(headers_lower: list[str]) -> Literal["csv_tall","csv_wide"]:
    # Wide if any “payer|plan” style header (pipe) or very many columns
    if any("|" in h for h in headers_lower):
        return "csv_wide"
    base_tall = {"billing_code_type","billing_code","description","standard_charge","payer","plan","payer_name","plan_name"}
    if base_tall & set(headers_lower):
        return "csv_tall"
    return "csv_wide" if len(headers_lower) > 25 else "csv_tall"
