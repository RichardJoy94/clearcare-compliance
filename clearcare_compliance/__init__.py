"""
ClearCare Compliance - CMS Hospital Price Transparency Validator

A reusable Python package for validating Hospital Price Transparency files
exactly like CMS, supporting both JSON and CSV (Tall/Wide) formats.
"""

__version__ = "0.1.0"
__author__ = "ClearCare"
__email__ = "dev@clearcare.example"

# Import only the types to avoid circular imports
from .types import ValidationResult

__all__ = [
    "ValidationResult",
]
