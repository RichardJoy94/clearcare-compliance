"""Light sanity tests for CLI detection."""

from clearcare_compliance.detectors import sniff_kind_from_bytes


def test_sniff_json():
    """Test JSON detection."""
    assert sniff_kind_from_bytes(b'{"a": 1}') == "json"


def test_sniff_csv():
    """Test CSV detection."""
    assert sniff_kind_from_bytes(b"MRF Date,Version\n2025-01-01,2.2.1\ncolA,colB\n1,2\n") == "csv"
