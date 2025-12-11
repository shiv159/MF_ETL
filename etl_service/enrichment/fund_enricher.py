import logging
import sys
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Dict, Optional, Any

from etl_service.models.response_models import EnrichedFund

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.fetchers.mftool_fetcher import MFToolFetcher  # noqa: E402
from src.fetchers.mstarpy_fetcher import MstarPyFetcher  # noqa: E402
from src.utils.fund_resolver import FundResolver  # noqa: E402


logger = logging.getLogger(__name__)


@dataclass
class SchemeMatch:
    code: str
    name: str
    score: float


class FundEnricher:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.fetcher = MFToolFetcher(self.logger)
        self.mstar_fetcher = MstarPyFetcher(self.logger)
        self.resolver = FundResolver(self.logger)

    # Score candidate schemes via fuzzy matching so resolver fallbacks still return something useful
    def _best_scheme(self, fund_name: str, candidates: List[Dict[str, str]]) -> Optional[SchemeMatch]:
        best: Optional[SchemeMatch] = None
        for scheme in candidates:
            ratio = SequenceMatcher(None, fund_name.lower(), scheme['name'].lower()).ratio()
            if not best or ratio > best.score:
                best = SchemeMatch(code=scheme['code'], name=scheme['name'], score=ratio)
        return best

    # Normalize numeric strings to floats, stripping commas and whitespace
    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(str(value).replace(',', '').strip())
        except ValueError:
            return None

    def _normalize_sector_result(self, sector_result: Any) -> Optional[Dict[str, float]]:
        if not sector_result:
            return None

        sector_data: Dict[str, float] = {}
        if isinstance(sector_result, dict):
            equity = sector_result.get('EQUITY')
            if isinstance(equity, dict):
                portfolio = equity.get('fundPortfolio')
                if isinstance(portfolio, dict):
                    for sector_name, percentage in portfolio.items():
                        if sector_name == 'portfolioDate' or percentage is None:
                            continue
                        amount = self._safe_float(percentage)
                        if amount is not None:
                            sector_data[sector_name] = amount
                    if sector_data:
                        return sector_data
            for sector_name, value in sector_result.items():
                if isinstance(value, (dict, list)):
                    continue
                amount = self._safe_float(value)
                if amount is not None:
                    sector_data[sector_name] = amount
            if sector_data:
                return sector_data
        elif isinstance(sector_result, list):
            for item in sector_result:
                if not isinstance(item, dict):
                    continue
                sector_name = item.get('assetType') or item.get('sectorName')
                percentage = item.get('percentage') or item.get('value') or item.get('sectorValue')
                amount = self._safe_float(percentage)
                if sector_name and amount is not None:
                    sector_data[sector_name] = amount
            if sector_data:
                return sector_data
        elif hasattr(sector_result, 'empty') and not sector_result.empty:
            if 'sectorValue' in sector_result.columns and 'sectorName' in sector_result.columns:
                for _, row in sector_result.iterrows():
                    sector_name = row.get('sectorName')
                    amount = self._safe_float(row.get('sectorValue'))
                    if sector_name and amount is not None:
                        sector_data[sector_name] = amount
                if sector_data:
                    return sector_data

        self.logger.debug("Unable to normalize sector data from %s", type(sector_result))
        return None

    def enrich(self, fund_name: str) -> Optional[EnrichedFund]:
        resolved = self.resolver.resolve_fund(fund_name)
        scheme_code = resolved.get('mftool_scheme_code')

        if not scheme_code:
            candidates = self.fetcher.search_scheme(fund_name)
            best = self._best_scheme(fund_name, candidates)
            if best and best.score >= 0.35:
                scheme_code = best.code
                self.logger.info("Selected fuzzy match %s for %s (score %.2f)", best.name, fund_name, best.score)

        if not scheme_code:
            self.logger.warning("Skipping enrichment for %s, no scheme code", fund_name)
            return None

        nav_data = self.fetcher.get_scheme_nav(scheme_code)
        details = self.fetcher.get_scheme_details(scheme_code)

        sector_allocation = details.get('sector_allocation') or details.get('sectorBreakup')
        top_holdings = details.get('top_holdings') or details.get('top_holdings_data')

        # Prefer any available ISIN so we can enrich via Morningstar
        isin_candidates = [
            scheme_code # Fallback to scheme_code if no ISIN found
        ]
        fund_isin = next((value for value in isin_candidates if value), None)
        search_terms = self._get_mstar_search_terms(resolved)
        holdings_detail = None
        sector_detail = None

        if fund_isin:
            holdings_detail = self._fetch_holdings_from_mstar(fund_isin)
            sector_detail = self._fetch_sector_from_mstar(fund_isin)

        if not holdings_detail and search_terms:
            holdings_detail = self._fetch_holdings_from_mstar_terms(search_terms)
        if holdings_detail:
            top_holdings = holdings_detail

        if not sector_detail and search_terms:
            sector_detail = self._fetch_sector_from_mstar_terms(search_terms)
        if sector_detail:
            sector_allocation = sector_detail

        enriched = EnrichedFund(
            fund_name=fund_name,
            isin=fund_isin or (details.get('isin') or (nav_data.get('isin') if isinstance(nav_data, dict) else None)),
            amc=details.get('fund_house') or details.get('amc_name'),
            category=details.get('fund_category'),
            expense_ratio=self._safe_float(details.get('expense_ratio')),
            sector_allocation=sector_allocation,
            top_holdings=top_holdings,
            current_nav=self._safe_float(nav_data.get('nav')) if isinstance(nav_data, dict) else None,
            nav_as_of=nav_data.get('nav_date') or nav_data.get('as_of') if isinstance(nav_data, dict) else None,
        )

        return enriched

    def _fetch_holdings_from_mstar(self, fund_isin: str) -> Optional[List[Dict[str, Any]]]:
        holdings_df = self.mstar_fetcher.get_fund_holdings(fund_isin)
        if holdings_df is None:
            return None
        try:
            records = holdings_df.to_dict('records')
            return [self._filter_top_holding(record) for record in records]
        except Exception:
            return None

    def _fetch_holdings_from_mstar_terms(self, search_terms: List[str]) -> Optional[List[Dict[str, Any]]]:
        for term in search_terms:
            if not term:
                continue
            holdings = self._fetch_holdings_from_mstar(term)
            if holdings:
                self.logger.debug("Matched Morningstar holdings using term '%s'", term)
                return holdings
        return None

    def _filter_top_holding(self, record: Dict[str, Any]) -> Dict[str, Any]:
        allowed = [
            'securityName',
            'isin',
            'ticker',
            'secId',
            'country',
            'sector',
            'numberOfShare',
            'marketValue',
            'weighting',
            'shareChange',
            'firstBoughtDate',
            'holdingTrend',
            'totalReturn1Year',
            'assessment',
            'stockRating',
            'quantRating',
            'susEsgRiskScore',
            'susEsgRiskCategory',
            'susEsgRiskGlobes',
            'esgAsOfDate',
        ]
        return {key: record.get(key) for key in allowed if key in record}

    def _fetch_sector_from_mstar(self, fund_isin: str) -> Optional[Dict[str, float]]:
        sectors = self.mstar_fetcher.get_sector_allocation(fund_isin)
        normalized = self._normalize_sector_result(sectors)
        return normalized

    def _fetch_sector_from_mstar_terms(self, search_terms: List[str]) -> Optional[Dict[str, float]]:
        for term in search_terms:
            if not term:
                continue
            sectors = self._fetch_sector_from_mstar(term)
            if sectors:
                self.logger.debug("Matched Morningstar sectors using term '%s'", term)
                return sectors
        return None

    def _get_mstar_search_terms(self, resolved: Dict[str, Optional[str]]) -> List[str]:
        terms: List[str] = []
        primary = resolved.get('mstarpy_search_term')
        if primary:
            terms.append(primary)
        for alt in resolved.get('mstarpy_alternate_terms', []) or []:
            if alt and alt not in terms:
                terms.append(alt)
        return terms
