"""
Tests for CSV profile detection and header mapping
"""

import pytest
from app.profiles import (
    detect_profile, 
    map_to_internal, 
    validate_cms_headers,
    get_cms_required_headers,
    get_profile_description,
    normalize_header
)


class TestProfileDetection:
    """Test profile detection logic"""
    
    def test_detect_cms_profile_with_standard_headers(self):
        """Should detect CMS profile when CMS standard headers are present"""
        headers = [
            "billing_code",
            "billing_code_type",
            "description",
            "standard_charge",
            "payer_name"
        ]
        assert detect_profile(headers) == "cms_csv"
    
    def test_detect_cms_profile_with_variations(self):
        """Should detect CMS profile with header variations"""
        headers = [
            "Billing Code",  # capital
            "billing-code-type",  # hyphens
            "Description",
            "STANDARD_CHARGE",  # uppercase
            "Payer Name"
        ]
        assert detect_profile(headers) == "cms_csv"
    
    def test_detect_simple_profile(self):
        """Should detect simple profile when CMS headers are missing"""
        headers = [
            "code",
            "code_system",
            "gross_price",
            "cash_price",
            "date"
        ]
        assert detect_profile(headers) == "simple_csv"
    
    def test_detect_simple_profile_minimal_cms_match(self):
        """Should detect simple profile with only one CMS indicator"""
        headers = [
            "billing_code",  # Only one CMS indicator
            "code_system",
            "gross_price",
            "cash_price"
        ]
        assert detect_profile(headers) == "simple_csv"


class TestHeaderMapping:
    """Test header mapping to internal schema"""
    
    def test_map_cms_headers(self):
        """Should map CMS headers to internal schema"""
        headers = [
            "billing_code",
            "billing_code_type",
            "description",
            "standard_charge"
        ]
        mapping = map_to_internal(headers, profile="cms_csv")
        
        assert mapping["code"] == "billing_code"
        assert mapping["code_system"] == "billing_code_type"
        assert mapping["gross_price"] == "standard_charge"
        assert mapping["description"] == "description"
    
    def test_map_simple_headers(self):
        """Should map simple CSV headers directly"""
        headers = [
            "code",
            "code_system",
            "gross_price",
            "cash_price",
            "date"
        ]
        mapping = map_to_internal(headers, profile="simple_csv")
        
        assert mapping["code"] == "code"
        assert mapping["code_system"] == "code_system"
        assert mapping["gross_price"] == "gross_price"
        assert mapping["cash_price"] == "cash_price"
    
    def test_map_partial_headers(self):
        """Should handle partial header matches"""
        headers = [
            "billing_code",
            "description"
        ]
        mapping = map_to_internal(headers, profile="cms_csv")
        
        assert "code" in mapping
        assert "description" in mapping
        assert "cash_price" not in mapping  # Not present


class TestCMSHeaderValidation:
    """Test CMS header validation"""
    
    def test_validate_complete_cms_headers(self):
        """Should pass validation with all required CMS headers"""
        headers = [
            "billing_code",
            "billing_code_type",
            "description",
            "standard_charge"
        ]
        result = validate_cms_headers(headers)
        
        assert result["valid"] is True
        assert len(result["missing_headers"]) == 0
    
    def test_validate_missing_cms_headers(self):
        """Should fail validation when required headers are missing"""
        headers = [
            "billing_code",
            "description"
        ]
        result = validate_cms_headers(headers)
        
        assert result["valid"] is False
        assert "billing_code_type" in result["missing_headers"]
        assert "standard_charge" in result["missing_headers"]
    
    def test_validate_case_insensitive(self):
        """Should handle case-insensitive header matching"""
        headers = [
            "Billing_Code",
            "BILLING_CODE_TYPE",
            "Description",
            "Standard_Charge"
        ]
        result = validate_cms_headers(headers)
        
        assert result["valid"] is True


class TestHelperFunctions:
    """Test helper functions"""
    
    def test_normalize_header(self):
        """Should normalize headers consistently"""
        assert normalize_header("Billing Code") == "billing_code"
        assert normalize_header("billing-code") == "billing_code"
        assert normalize_header("BILLING_CODE") == "billing_code"
        assert normalize_header("  billing code  ") == "billing_code"
    
    def test_get_profile_description(self):
        """Should return human-readable profile descriptions"""
        assert "CMS" in get_profile_description("cms_csv")
        assert "Simple" in get_profile_description("simple_csv")
    
    def test_get_cms_required_headers(self):
        """Should return set of required CMS headers"""
        required = get_cms_required_headers()
        
        assert "billing_code" in required
        assert "billing_code_type" in required
        assert "description" in required
        assert "standard_charge" in required
        assert len(required) >= 4


class TestAutoDetection:
    """Test automatic profile detection (no profile hint)"""
    
    def test_auto_detect_and_map_cms(self):
        """Should auto-detect CMS and map correctly"""
        headers = [
            "billing_code",
            "billing_code_type",
            "standard_charge",
            "payer_name"
        ]
        # No profile hint - should auto-detect
        mapping = map_to_internal(headers)
        
        assert mapping["code"] == "billing_code"
        assert mapping["code_system"] == "billing_code_type"
    
    def test_auto_detect_and_map_simple(self):
        """Should auto-detect simple and map correctly"""
        headers = [
            "code",
            "code_system",
            "gross_price"
        ]
        # No profile hint - should auto-detect
        mapping = map_to_internal(headers)
        
        assert mapping["code"] == "code"
        assert mapping["code_system"] == "code_system"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

