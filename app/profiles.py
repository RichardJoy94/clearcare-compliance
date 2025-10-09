"""
CSV Profile Detection and Header Mapping
Supports CMS standardcharges.csv and simple CSV formats
"""

from typing import Literal, Dict, List, Set

# CMS Hospital Price Transparency standard headers
CMS_STANDARD_HEADERS = {
    # Core identifiers
    "billing_code",
    "billing_code_type",
    "billing_code_type_version",
    
    # Descriptions
    "description",
    "drug_information",
    
    # Prices
    "payer_name",
    "plan_name",
    "standard_charge",
    "standard_charge_methodology",
    
    # Additional codes
    "code",
    "code_system",
    
    # Modifiers and qualifiers
    "modifiers",
    "estimated_amount",
    "contracting_method",
    
    # Common variations (case-insensitive)
    "billing_class",
    "setting",
    "revenue_code",
    "drug_unit_of_measurement",
    "drug_type_of_measurement"
}

# Internal standard schema
INTERNAL_SCHEMA = {
    "code": "code",
    "code_system": "code_system",
    "gross_price": "gross_price",
    "cash_price": "cash_price",
    "date": "date",
    "description": "description"
}


def normalize_header(header: str) -> str:
    """Normalize header to lowercase with underscores."""
    return header.lower().strip().replace(" ", "_").replace("-", "_")


def detect_profile(headers: List[str]) -> Literal["cms_csv", "simple_csv"]:
    """Detect CSV profile based on headers.
    
    Args:
        headers: List of column headers from CSV
        
    Returns:
        "cms_csv" if headers match CMS standard format, else "simple_csv"
        
    Heuristic:
        - If headers contain multiple CMS-specific names, return "cms_csv"
        - Else return "simple_csv"
    """
    normalized_headers = {normalize_header(h) for h in headers}
    
    # CMS-specific headers that strongly indicate CMS format
    cms_indicators = {
        "billing_code",
        "billing_code_type",
        "billing_code_type_version",
        "standard_charge",
        "payer_name"
    }
    
    # Count how many CMS indicators are present
    cms_matches = sum(1 for indicator in cms_indicators if indicator in normalized_headers)
    
    # If we have 2+ CMS-specific headers, it's likely a CMS file
    if cms_matches >= 2:
        return "cms_csv"
    
    return "simple_csv"


def map_to_internal(headers: List[str], profile: Literal["cms_csv", "simple_csv"] = None) -> Dict[str, str]:
    """Map CSV headers to internal schema names.
    
    Args:
        headers: List of column headers from CSV
        profile: Optional profile hint (auto-detected if not provided)
        
    Returns:
        Dict mapping internal schema names to actual column names
        
    Example:
        {"code": "billing_code", "code_system": "billing_code_type", ...}
    """
    if profile is None:
        profile = detect_profile(headers)
    
    normalized_headers = {normalize_header(h): h for h in headers}
    mapping = {}
    
    if profile == "cms_csv":
        # CMS CSV mapping
        mapping_rules = {
            "code": ["billing_code", "code"],
            "code_system": ["billing_code_type", "code_system"],
            "gross_price": ["standard_charge", "gross_price", "gross_charge"],
            "cash_price": ["cash_price", "cash_discount_price", "cash_charge"],
            "description": ["description", "drug_information"],
            "date": ["date", "effective_date", "last_updated"]
        }
    else:
        # Simple CSV mapping (direct match)
        mapping_rules = {
            "code": ["code", "billing_code"],
            "code_system": ["code_system", "billing_code_type"],
            "gross_price": ["gross_price", "standard_charge"],
            "cash_price": ["cash_price"],
            "description": ["description"],
            "date": ["date", "effective_date"]
        }
    
    # Try to map each internal field
    for internal_name, candidates in mapping_rules.items():
        for candidate in candidates:
            if candidate in normalized_headers:
                mapping[internal_name] = normalized_headers[candidate]
                break
    
    return mapping


def get_cms_required_headers() -> Set[str]:
    """Get the set of required CMS headers.
    
    Returns:
        Set of required header names (normalized)
    """
    # Core required headers for CMS Hospital Price Transparency
    return {
        "billing_code",
        "billing_code_type",
        "description",
        "standard_charge"
    }


def validate_cms_headers(headers: List[str]) -> Dict[str, any]:
    """Validate that CMS required headers are present.
    
    Args:
        headers: List of column headers from CSV
        
    Returns:
        Dict with validation results
    """
    normalized_headers = {normalize_header(h) for h in headers}
    required_headers = get_cms_required_headers()
    
    missing_headers = required_headers - normalized_headers
    
    if missing_headers:
        return {
            "valid": False,
            "missing_headers": list(missing_headers),
            "present_headers": list(normalized_headers),
            "message": f"Missing required CMS headers: {', '.join(missing_headers)}"
        }
    
    return {
        "valid": True,
        "missing_headers": [],
        "present_headers": list(normalized_headers),
        "message": "All required CMS headers present"
    }


def get_profile_description(profile: Literal["cms_csv", "simple_csv"]) -> str:
    """Get human-readable description of profile.
    
    Args:
        profile: Profile identifier
        
    Returns:
        Human-readable description
    """
    descriptions = {
        "cms_csv": "CMS Hospital Price Transparency CSV",
        "simple_csv": "Simple CSV Format"
    }
    return descriptions.get(profile, "Unknown Profile")


def is_cms_compatible_header(header: str) -> bool:
    """Check if a header is compatible with CMS standard.
    
    Args:
        header: Column header name
        
    Returns:
        True if header matches CMS standard naming
    """
    normalized = normalize_header(header)
    return normalized in CMS_STANDARD_HEADERS

