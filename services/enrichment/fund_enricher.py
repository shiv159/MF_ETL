import asyncio
import logging
import sys
import time
from contextvars import ContextVar
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

from services.api.models.response_models import EnrichedFund

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mf_etl.fetchers.mftool_fetcher import MFToolFetcher  # noqa: E402
from src.mf_etl.fetchers.mstarpy_fetcher import MstarPyFetcher  # noqa: E402
from src.mf_etl.services.fund_resolver import FundResolver  # noqa: E402
from src.mf_etl.utils.search_utils import (  # noqa: E402
    generate_fallback_search_terms,
    safe_float,
    normalize_sector_result,
)

# Context variable for correlation ID (shared with API)
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default=None)


def get_correlation_id() -> str:
    """Get current correlation ID from context."""
    return correlation_id_var.get() or "no-id"


logger = logging.getLogger(__name__)


@dataclass
class SchemeMatch:
    code: str
    name: str
    score: float


class FundEnricher:
    def __init__(self, logger: Optional[logging.Logger] = None, enable_caching: bool = True, cache_ttl_minutes: int = 60):
        self.logger = logger or logging.getLogger(__name__)
        self.fetcher = MFToolFetcher(self.logger)
        self.mstar_fetcher = MstarPyFetcher(self.logger)
        self.resolver = FundResolver(self.logger)
        
        # Caching configuration
        self.caching_enabled = enable_caching
        self.logger.info(f"Fund enrichment caching: {'enabled' if enable_caching else 'disabled'}")
        
        # Initialize cache for fund resolutions with configurable TTL
        self._cache: Dict[str, Tuple[Optional[EnrichedFund], float]] = {}
        self._cache_ttl_seconds = cache_ttl_minutes * 60  # Convert minutes to seconds
        
    def _normalize_fund_name(self, fund_name: str) -> str:
        """Normalize fund name for cache key to handle duplicates."""
        return fund_name.strip().lower()
    
    def _is_cache_valid(self, timestamp: float) -> bool:
        """Check if cache entry is still valid based on TTL."""
        if not self.caching_enabled:
            return False
        return (time.time() - timestamp) < self._cache_ttl_seconds
    
    def _clear_expired_cache(self) -> None:
        """Remove expired entries from cache."""
        if not self.caching_enabled:
            return
            
        now = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self._cache.items()
            if (now - timestamp) >= self._cache_ttl_seconds
        ]
        for key in expired_keys:
            del self._cache[key]
        
        if expired_keys:
            self.logger.debug(f"Cache cleanup: removed {len(expired_keys)} expired entries")

    async def enrich_async(self, fund_name: str) -> Optional[EnrichedFund]:
        """
        Async wrapper for enrich() method.
        
        Runs the synchronous enrich() method in a thread pool to avoid blocking
        the event loop when making external API calls. Uses cache to avoid
        redundant enrichment of duplicate fund names if caching is enabled.
        
        Args:
            fund_name: Name of the fund to enrich
            
        Returns:
            EnrichedFund object if enrichment successful, None otherwise
        """
        # Check cache first (only if caching enabled)
        if self.caching_enabled:
            cache_key = self._normalize_fund_name(fund_name)
            if cache_key in self._cache:
                cached_result, timestamp = self._cache[cache_key]
                if self._is_cache_valid(timestamp):
                    self.logger.debug(f"Cache hit for '{fund_name}'")
                    return cached_result
                else:
                    # Remove expired entry
                    del self._cache[cache_key]
        
        # Not in cache or caching disabled, run enrichment
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.enrich, fund_name)

    @staticmethod
    async def enrich_batch_concurrent(
        enricher: 'FundEnricher',
        fund_names: List[str],
        max_concurrent: int = 5,
        timeout_per_fund: int = 15
    ) -> List[Optional[EnrichedFund]]:
        """
        Enrich multiple funds concurrently with semaphore to limit concurrent operations.
        
        This method processes multiple funds in parallel, improving throughput significantly.
        A semaphore limits the number of concurrent operations to prevent resource exhaustion.
        Deduplicates fund names to leverage cache hits on repeated funds in the same batch.
        
        Args:
            enricher: FundEnricher instance to use
            fund_names: List of fund names to enrich
            max_concurrent: Maximum number of concurrent enrichments (default: 5)
            timeout_per_fund: Timeout in seconds per fund (default: 15s)
            
        Returns:
            List of EnrichedFund objects (None for failed enrichments), maintaining original order
            
        Example:
            enricher = FundEnricher()
            results = await FundEnricher.enrich_batch_concurrent(
                enricher,
                ["Fund A", "Fund B", "Fund C"],
                max_concurrent=5
            )
        """
        # Clean up expired cache entries periodically (only if caching enabled)
        if enricher.caching_enabled:
            enricher._clear_expired_cache()
        
        # Deduplicate fund names while preserving order mapping
        unique_funds = []
        fund_name_to_indices: Dict[str, List[int]] = {}
        
        for idx, fund_name in enumerate(fund_names):
            normalized = enricher._normalize_fund_name(fund_name)
            if normalized not in fund_name_to_indices:
                unique_funds.append(fund_name)
                fund_name_to_indices[normalized] = []
            fund_name_to_indices[normalized].append(idx)
        
        if len(unique_funds) < len(fund_names):
            enricher.logger.info(
                f"Deduplicating {len(fund_names)} funds to {len(unique_funds)} unique funds "
                f"({len(fund_names) - len(unique_funds)} duplicates cached)"
            )
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def enrich_with_semaphore(fund_name: str) -> Optional[EnrichedFund]:
            """Enrich a single fund with semaphore protection, timeout, and retry logic."""
            async with semaphore:
                max_attempts = 3
                base_delay = 0.5
                max_delay = 5
                backoff_multiplier = 2
                
                for attempt in range(max_attempts):
                    try:
                        result = await asyncio.wait_for(
                            enricher.enrich_async(fund_name),
                            timeout=timeout_per_fund
                        )
                        if attempt > 0:
                            enricher.logger.info(
                                f"Successfully enriched '{fund_name}' on retry attempt {attempt + 1}"
                            )
                        return result
                    except asyncio.TimeoutError:
                        if attempt < max_attempts - 1:
                            # Calculate backoff delay
                            delay = min(base_delay * (backoff_multiplier ** attempt), max_delay)
                            enricher.logger.warning(
                                f"Timeout enriching '{fund_name}' (exceeded {timeout_per_fund}s), "
                                f"retry attempt {attempt + 1}/{max_attempts} in {delay:.1f}s"
                            )
                            await asyncio.sleep(delay)
                        else:
                            enricher.logger.warning(
                                f"Timeout enriching '{fund_name}' (exceeded {timeout_per_fund}s) - "
                                f"all {max_attempts} attempts failed"
                            )
                            return None
                    except Exception as e:
                        error_str = str(e).lower()
                        # Retry on transient errors (connection errors, server errors)
                        is_transient = any(keyword in error_str for keyword in 
                                          ['timeout', 'connection', '500', 'server error', 'temporarily'])
                        
                        if attempt < max_attempts - 1 and is_transient:
                            delay = min(base_delay * (backoff_multiplier ** attempt), max_delay)
                            enricher.logger.warning(
                                f"Transient error enriching '{fund_name}', "
                                f"retry attempt {attempt + 1}/{max_attempts} in {delay:.1f}s: {str(e)[:80]}"
                            )
                            await asyncio.sleep(delay)
                        else:
                            enricher.logger.warning(
                                f"Failed enriching '{fund_name}': {str(e)}"
                            )
                            return None
                
                return None
        
        # Gather all concurrent tasks for unique funds
        unique_results = await asyncio.gather(
            *[enrich_with_semaphore(fund_name) for fund_name in unique_funds]
        )
        
        # Map results back to original order, handling duplicates
        results: List[Optional[EnrichedFund]] = [None] * len(fund_names)
        unique_name_to_result = {enricher._normalize_fund_name(name): result 
                                 for name, result in zip(unique_funds, unique_results)}
        
        for normalized_name, indices in fund_name_to_indices.items():
            result = unique_name_to_result.get(normalized_name)
            for idx in indices:
                results[idx] = result
        
        return results


    # Score candidate schemes via fuzzy matching so resolver fallbacks still return something useful
    def _best_scheme(self, fund_name: str, candidates: List[Dict[str, str]]) -> Optional[SchemeMatch]:
        best: Optional[SchemeMatch] = None
        for scheme in candidates:
            ratio = SequenceMatcher(None, fund_name.lower(), scheme['name'].lower()).ratio()
            if not best or ratio > best.score:
                best = SchemeMatch(code=scheme['code'], name=scheme['name'], score=ratio)
        return best

    # Normalize numeric strings to floats, using shared utility
    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        result = safe_float(value, default=None)
        return result

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

    def _fetch_isin_from_mstarpy(self, fund_name: str, search_terms: List[str]) -> Optional[str]:
        """
        Try to get ISIN directly from mstarpy by searching for the fund
        
        Args:
            fund_name: Original fund name for logging
            search_terms: List of search terms to try (primary + alternates)
        
        Returns:
            ISIN if found, None otherwise
        """
        for term in search_terms:
            if not term:
                continue
            try:
                self.logger.debug(f"Attempting to fetch ISIN from mstarpy using term: {term}")
                fund = self.mstar_fetcher.get_fund(term)
                if fund and hasattr(fund, 'isin') and fund.isin:
                    self.logger.info(f"Found ISIN '{fund.isin}' for '{fund_name}' using term '{term}'")
                    return fund.isin
            except Exception as e:
                self.logger.debug(f"ISIN lookup failed for term '{term}': {str(e)}")
                continue
        return None

    def enrich(self, fund_name: str) -> Optional[EnrichedFund]:
        # Check cache first (only if caching enabled)
        if self.caching_enabled:
            cache_key = self._normalize_fund_name(fund_name)
            if cache_key in self._cache:
                cached_result, timestamp = self._cache[cache_key]
                if self._is_cache_valid(timestamp):
                    self.logger.debug(f"Cache hit for '{fund_name}'")
                    return cached_result
                else:
                    # Remove expired entry
                    del self._cache[cache_key]
        
        # Perform enrichment
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
            # Cache the failure too (only if caching enabled)
            if self.caching_enabled:
                cache_key = self._normalize_fund_name(fund_name)
                self._cache[cache_key] = (None, time.time())
            return None

        nav_data = self.fetcher.get_scheme_nav(scheme_code)
        details = self.fetcher.get_scheme_details(scheme_code)

        sector_allocation = details.get('sector_allocation') or details.get('sectorBreakup')
        top_holdings = details.get('top_holdings') or details.get('top_holdings_data')

        # Build search terms early for ISIN lookup
        search_terms = self._get_mstar_search_terms(resolved)
        
        # Prefer any available ISIN so we can enrich via Morningstar
        isin_candidates = [
            details.get('isin'),
            nav_data.get('isin') if isinstance(nav_data, dict) else None,
            details.get('fund_isin'),
            details.get('isin_code'),
        ]
        fund_isin = next((value for value in isin_candidates if value), None)
        
        # If no ISIN found in mftool, try to fetch from mstarpy directly
        if not fund_isin and search_terms:
            fund_isin = self._fetch_isin_from_mstarpy(fund_name, search_terms)
        
        # Fallback to scheme_code if still no ISIN
        if not fund_isin:
            fund_isin = scheme_code
        
        holdings_detail = None
        sector_detail = None

        if fund_isin:
            holdings_detail = self._fetch_holdings_from_mstar(fund_isin)
            sector_detail = self._fetch_sector_from_mstar(fund_isin)

        if not holdings_detail and search_terms:
            holdings_detail = self._fetch_holdings_from_mstar_terms(search_terms)
        
        # If primary search terms failed, try fallback search terms
        if not holdings_detail:
            scheme_name = resolved.get('mftool_scheme_name') or ''
            fallback_terms = self._generate_fallback_search_terms(fund_name, scheme_name)
            if fallback_terms:
                self.logger.debug(f"Primary search failed for holdings, trying {len(fallback_terms)} fallback terms for '{fund_name}'")
                holdings_detail = self._fetch_holdings_from_mstar_terms(fallback_terms)
        
        if holdings_detail:
            top_holdings = holdings_detail

        if not sector_detail and search_terms:
            sector_detail = self._fetch_sector_from_mstar_terms(search_terms)
        
        # If primary search terms failed, try fallback search terms
        if not sector_detail:
            scheme_name = resolved.get('mftool_scheme_name') or ''
            fallback_terms = self._generate_fallback_search_terms(fund_name, scheme_name)
            if fallback_terms:
                self.logger.debug(f"Primary search failed for sectors, trying {len(fallback_terms)} fallback terms for '{fund_name}'")
                sector_detail = self._fetch_sector_from_mstar_terms(fallback_terms)
        
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

        # Cache the result (only if caching enabled)
        if self.caching_enabled:
            cache_key = self._normalize_fund_name(fund_name)
            self._cache[cache_key] = (enriched, time.time())
            self.logger.debug(f"Cached enrichment result for '{fund_name}'")

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

    def _generate_fallback_search_terms(self, fund_name: str, scheme_name: str) -> List[str]:
        """
        Generate additional search terms when primary resolution fails in mstarpy.
        Uses shared utility function to ensure consistency with demo and other modules.
        """
        return generate_fallback_search_terms(fund_name, scheme_name)
