"""
Basic enrichment and validation tests
Tests core enrichment logic, validation, and utilities
"""
import pytest
from services.enrichment.holding_validator import validate_holdings
from services.enrichment.fund_enricher import FundEnricher


class TestHoldingsValidation:
    """Test holdings validation"""
    
    def test_valid_holdings(self):
        """Valid holdings should pass validation"""
        holdings = [
            {
                "fund_name": "HDFC Top 200 Fund",
                "units": "100.5",
                "nav": "1234.56",
                "value": "123456.78"
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 1
        assert validated[0]["fund_name"] == "HDFC Top 200 Fund"

    def test_invalid_units(self):
        """Invalid units should be flagged"""
        holdings = [
            {
                "fund_name": "HDFC Top 200 Fund",
                "units": "invalid",
                "nav": "1234.56",
                "value": "123456.78"
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(warnings) > 0

    def test_invalid_nav(self):
        """Invalid NAV should be flagged"""
        holdings = [
            {
                "fund_name": "HDFC Top 200 Fund",
                "units": "100.5",
                "nav": "invalid",
                "value": "123456.78"
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(warnings) > 0

    def test_empty_holdings_list(self):
        """Empty holdings list should return empty result"""
        holdings = []
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 0

    def test_multiple_holdings(self):
        """Multiple valid holdings should all be validated"""
        holdings = [
            {
                "fund_name": "HDFC Top 200 Fund",
                "units": "100.5",
                "nav": "1234.56",
                "value": "123456.78"
            },
            {
                "fund_name": "PGIM Flexi Cap Fund",
                "units": "50.2",
                "nav": "100.00",
                "value": "5020.00"
            }
        ]
        validated, warnings = validate_holdings(holdings)
        assert len(validated) == 2


class TestFundEnricher:
    """Test FundEnricher initialization and basic functionality"""
    
    def test_enricher_initialization(self):
        """FundEnricher should initialize successfully"""
        enricher = FundEnricher()
        assert enricher is not None

    def test_enricher_has_required_methods(self):
        """FundEnricher should have required methods"""
        enricher = FundEnricher()
        assert hasattr(enricher, 'enrich_async')
        assert callable(getattr(enricher, 'enrich_async'))
        assert hasattr(enricher, 'enrich')
        assert callable(getattr(enricher, 'enrich'))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
