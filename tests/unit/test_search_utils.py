"""Tests for shared search utilities."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

import pytest
from src.mf_etl.utils.search_utils import (
    safe_float,
    safe_numeric,
    generate_fallback_search_terms,
    normalize_sector_result,
)


class TestSafeFloat:
    """Test safe_float utility function."""

    def test_string_float_conversion(self):
        """Test conversion of string float to float."""
        assert safe_float("10.5") == 10.5

    def test_string_with_comma(self):
        """Test conversion of comma-separated values."""
        assert safe_float("1,234.56") == 1234.56

    def test_integer_conversion(self):
        """Test conversion of integer."""
        assert safe_float(10) == 10.0

    def test_float_passthrough(self):
        """Test that float values pass through unchanged."""
        assert safe_float(10.5) == 10.5

    def test_none_returns_default(self):
        """Test that None returns default value."""
        assert safe_float(None) == 0.0
        assert safe_float(None, 99.9) == 99.9

    def test_invalid_string_returns_default(self):
        """Test that invalid strings return default."""
        assert safe_float("invalid") == 0.0
        assert safe_float("not_a_number", 42.0) == 42.0

    def test_whitespace_handling(self):
        """Test that whitespace is handled."""
        assert safe_float("  10.5  ") == 10.5


class TestSafeNumeric:
    """Test safe_numeric type coercion utility."""

    def test_string_to_float(self):
        """Test string to float conversion."""
        assert safe_numeric("10.5", float) == 10.5

    def test_string_to_int(self):
        """Test string to int conversion."""
        assert safe_numeric("10", int) == 10

    def test_float_string_to_int_truncates(self):
        """Test that float strings are truncated to int."""
        assert safe_numeric("10.9", int) == 10

    def test_none_returns_default(self):
        """Test that None returns default."""
        assert safe_numeric(None, float, 0.0) == 0.0
        assert safe_numeric(None, int, 0) == 0

    def test_target_type_passthrough(self):
        """Test that values of target type pass through."""
        assert safe_numeric(10.5, float) == 10.5
        assert safe_numeric(10, int) == 10

    def test_invalid_returns_default(self):
        """Test that invalid values return default."""
        assert safe_numeric("invalid", float, 0.0) == 0.0
        assert safe_numeric("invalid", int, 0) == 0


class TestGenerateFallbackSearchTerms:
    """Test fallback search terms generation."""

    def test_basic_fund_name(self):
        """Test with basic fund name."""
        terms = generate_fallback_search_terms(
            "Motilal Oswal Midcap Direct Growth",
            "Motilal Oswal Midcap Fund-Direct - IDCW Payout/Reinvestment"
        )
        assert len(terms) > 0
        assert terms[0] == "Motilal Oswal Midcap Direct Growth"

    def test_removes_plan_suffixes(self):
        """Test that plan suffixes are removed."""
        terms = generate_fallback_search_terms(
            "Test Fund",
            "Test Fund-Direct - IDCW Payout/Reinvestment"
        )
        # Should have stripped version without the plan suffix
        stripped_terms = [t for t in terms if "IDCW" not in t and "Direct" not in t]
        assert len(stripped_terms) > 0

    def test_returns_list(self):
        """Test that function returns a list."""
        terms = generate_fallback_search_terms("Fund A", "Fund A Official")
        assert isinstance(terms, list)

    def test_no_duplicate_terms(self):
        """Test that duplicate terms are not generated."""
        terms = generate_fallback_search_terms("Test Fund", "Test Fund Official")
        assert len(terms) == len(set(terms))


class TestNormalizeSectorResult:
    """Test sector result normalization."""

    def test_dict_with_floats(self):
        """Test normalization of dict with float values."""
        sector_data = {"Tech": "50.5", "Finance": "49.5"}
        result = normalize_sector_result(sector_data)
        assert result is not None
        assert result["Tech"] == 50.5
        assert result["Finance"] == 49.5

    def test_zero_values_excluded(self):
        """Test that zero values are excluded."""
        sector_data = {"Tech": "50.5", "Finance": "0"}
        result = normalize_sector_result(sector_data)
        assert result is not None
        assert "Finance" not in result
        assert "Tech" in result

    def test_all_zero_returns_none(self):
        """Test that all-zero data returns None."""
        sector_data = {"Tech": "0", "Finance": "0"}
        result = normalize_sector_result(sector_data)
        assert result is None

    def test_empty_dict_returns_none(self):
        """Test that empty dict returns None."""
        result = normalize_sector_result({})
        assert result is None

    def test_none_returns_none(self):
        """Test that None input returns None."""
        result = normalize_sector_result(None)
        assert result is None
