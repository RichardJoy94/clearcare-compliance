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
    # Look for actual payer|plan columns (wide format indicator)
    payer_plan_pipes = [h for h in headers_lower if "|" in h and any(word in h for word in ["payer", "plan", "insurance", "hmo", "ppo"])]
    
    # Wide format if we have multiple payer|plan columns with pipes
    if len(payer_plan_pipes) >= 2:
        return "csv_wide"
    
    # Check for tall format indicators
    base_tall = {"billing_code_type","billing_code","description","standard_charge","payer","plan","payer_name","plan_name"}
    has_tall_indicators = base_tall & set(headers_lower)
    
    # If we have payer_name and plan_name columns, it's definitely tall format
    if "payer_name" in headers_lower and "plan_name" in headers_lower:
        return "csv_tall"
    
    # If we have basic tall indicators and not many payer|plan pipe columns, it's tall
    if has_tall_indicators and len(payer_plan_pipes) <= 1:
        return "csv_tall"
    
    # Default to wide if uncertain and many columns
    return "csv_wide" if len(headers_lower) > 25 else "csv_tall"
