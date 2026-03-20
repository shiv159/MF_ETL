import logging

from services.enrichment.fund_enricher import FundEnricher


class DummyFetcher:
    def __init__(self, result):
        self.result = result

    def search_and_get_fund(self, fund_name, fuzzy_threshold, nav_history_years):
        assert fund_name == "HDFC Mid Cap"
        assert fuzzy_threshold == 85
        assert nav_history_years == 10
        return self.result

    def close(self):
        return None


def build_enricher(result):
    enricher = FundEnricher.__new__(FundEnricher)
    enricher.logger = logging.getLogger(__name__)
    enricher.mfapi_fetcher = DummyFetcher(result)
    enricher.mfapi_fuzzy_threshold = 85
    enricher.nav_history_years = 10
    enricher._fetch_holdings_from_mstar = lambda isin: [{"securityName": "HDFC Bank"}]
    enricher._fetch_sector_from_mstar = lambda isin: {"Financial Services": 30.0}
    return enricher


def test_enrich_returns_canonical_and_input_names():
    canonical_name = "HDFC Mid Cap Fund - Direct Plan - Growth"
    enricher = build_enricher(
        {
            "scheme_name": canonical_name,
            "matched_fund_name": canonical_name,
            "isin_growth": "INF179K01UT0",
            "isin_div_reinvestment": None,
            "fund_house": "HDFC Mutual Fund",
            "scheme_category": "Equity - Mid Cap",
            "current_nav": 123.45,
            "nav_as_of": "20-03-2026",
            "nav_history": {"2026-03": 123.45},
            "status": "SUCCESS",
        }
    )

    enriched = enricher.enrich("HDFC Mid Cap")

    assert enriched is not None
    assert enriched.fund_name == canonical_name
    assert enriched.input_fund_name == "HDFC Mid Cap"
    assert enriched.isin == "INF179K01UT0"
