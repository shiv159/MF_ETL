"""Tests for enhanced holding validator with type coercion."""

import sys
from pathlib import Path

# Add src to path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from services.enrichment.holding_validator import validate_holdings


class TestValidateHoldingsBasic:
    """Test basic holding validation."""

    def test_empty_holdings_list(self):
        """Test with empty holdings list."""
        validated, warnings = validate_holdings([])
        assert validated == []
        assert warnings == []

    def test_missing_fund_name(self):
        """Test that holdings without fund_name are skipped."""
        holdings = [
            {"units": 10, "nav": 50.5},  # Missing fund_name
            {"fund_name": "Valid Fund", "units": 10, "nav": 50.5}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["fund_name"] == "Valid Fund"
        assert any("fund_name is missing" in w for w in warnings)

    def test_valid_holding(self):
        """Test with valid holding data."""
        holdings = [
            {"fund_name": "Test Fund", "units": 10.0, "nav": 50.5}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["fund_name"] == "Test Fund"
        assert len(warnings) == 0


class TestTypeCoercion:
    """Test type coercion for numeric fields."""

    def test_string_units_conversion(self):
        """Test that string units are converted to float."""
        holdings = [
            {"fund_name": "Test Fund", "units": "10.5", "nav": 50.5}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["units"] == 10.5
        assert isinstance(validated[0]["units"], float)

    def test_string_nav_conversion(self):
        """Test that string NAV is converted to float."""
        holdings = [
            {"fund_name": "Test Fund", "units": 10, "nav": "50.5"}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["nav"] == 50.5

    def test_string_value_conversion(self):
        """Test that string value is converted to float."""
        holdings = [
            {"fund_name": "Test Fund", "units": 10, "nav": 50.5, "value": "505.0"}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["value"] == 505.0

    def test_string_with_comma_conversion(self):
        """Test conversion of comma-formatted numbers."""
        holdings = [
            {"fund_name": "Test Fund", "units": "1,000.5", "nav": "50.5"}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["units"] == 1000.5

    def test_integer_to_float_conversion(self):
        """Test that integers are converted to float."""
        holdings = [
            {"fund_name": "Test Fund", "units": 10, "nav": 50}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert isinstance(validated[0]["units"], float)
        assert isinstance(validated[0]["nav"], float)


class TestValidationRules:
    """Test holding validation rules."""

    def test_negative_units_rejected(self):
        """Test that negative units are rejected."""
        holdings = [
            {"fund_name": "Test Fund", "units": -10.0, "nav": 50.5}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 0
        assert any("units must be positive" in w for w in warnings)

    def test_negative_nav_rejected(self):
        """Test that negative NAV is rejected."""
        holdings = [
            {"fund_name": "Test Fund", "units": 10.0, "nav": -50.5}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 0
        assert any("nav must be positive" in w for w in warnings)

    def test_zero_units_rejected(self):
        """Test that zero units are rejected."""
        holdings = [
            {"fund_name": "Test Fund", "units": 0.0, "nav": 50.5}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 0

    def test_zero_nav_rejected(self):
        """Test that zero NAV is rejected."""
        holdings = [
            {"fund_name": "Test Fund", "units": 10.0, "nav": 0.0}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 0

    def test_value_deviation_warning(self):
        """Test warning when calculated value deviates from reported value."""
        holdings = [
            {
                "fund_name": "Test Fund",
                "units": 100.0,
                "nav": 50.0,
                "value": 4000.0  # Should be 5000, 20% deviation
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1  # Still valid, but warned
        assert any("deviates" in w for w in warnings)

    def test_small_value_deviation_no_warning(self):
        """Test that small deviations don't generate warnings."""
        holdings = [
            {
                "fund_name": "Test Fund",
                "units": 100.0,
                "nav": 50.0,
                "value": 5001.0  # Only 0.02% deviation
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        deviation_warnings = [w for w in warnings if "deviates" in w]
        assert len(deviation_warnings) == 0


class TestValueCalculation:
    """Test automatic value calculation."""

    def test_value_auto_calculated_when_missing(self):
        """Test that value is calculated from units * nav when missing."""
        holdings = [
            {"fund_name": "Test Fund", "units": 100.0, "nav": 50.0}
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["value"] == 5000.0

    def test_value_preserved_when_valid(self):
        """Test that provided value is preserved when valid."""
        holdings = [
            {
                "fund_name": "Test Fund",
                "units": 100.0,
                "nav": 50.0,
                "value": 5000.0
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["value"] == 5000.0


class TestPartialSuccess:
    """Test that validator handles mixed valid/invalid holdings."""

    def test_mixed_valid_invalid_holdings(self):
        """Test processing of batch with both valid and invalid holdings."""
        holdings = [
            {"fund_name": "Valid Fund 1", "units": 10.0, "nav": 50.5},
            {"fund_name": "Invalid Fund", "units": -5.0, "nav": 50.5},
            {"fund_name": "Valid Fund 2", "units": "20.5", "nav": "100.0"},
            {"units": 10.0, "nav": 50.5},  # Missing fund_name
        ]
        validated, warnings = validate_holdings(holdings)
        
        # Should have 2 valid holdings
        assert len(validated) == 2
        assert validated[0]["fund_name"] == "Valid Fund 1"
        assert validated[1]["fund_name"] == "Valid Fund 2"
        
        # Should have 2 warnings
        assert len(warnings) >= 2

    def test_preserves_other_fields(self):
        """Test that other fields are preserved during validation."""
        holdings = [
            {
                "fund_name": "Test Fund",
                "units": 10.0,
                "nav": 50.5,
                "purchase_date": "2025-12-13"
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["purchase_date"] == "2025-12-13"
