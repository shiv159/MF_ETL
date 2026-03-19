import asyncio
import logging
import sys
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from calendar import monthrange

from services.api.models.response_models import EnrichedFund

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mf_etl.fetchers.mftool_fetcher import MFAPIFetcher  # noqa: E402
from src.mf_etl.fetchers.mstarpy_fetcher import MstarPyFetcher  # noqa: E402
from src.mf_etl.utils.search_utils import (  # noqa: E402
    safe_float,
    validate_isin,
)

# Context variable for correlation ID (shared with API)
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default=None)


def get_correlation_id() -> str:
    """Get current correlation ID from context."""
    return correlation_id_var.get() or "no-id"


def aggregate_nav_to_monthly(
    daily_nav_array: List[Dict[str, str]],
    fallback_strategy: str = 'previous_day'
) -> Tuple[Optional[float], Optional[str], Dict[str, float]]:
    """
    Aggregate daily NAV data to monthly and extract latest NAV.
    
    Handles:
    - String to float conversion
    - Date parsing (DD-MM-YYYY format from MFAPI)
    - Month-end NAV extraction with fallback for holidays
    - Returns monthly aggregated data (YYYY-MM: NAV)
    
    Args:
        daily_nav_array: List of {"date": "DD-MM-YYYY", "nav": "123.45"} (sorted by date descending)
        fallback_strategy: "previous_day" - use previous day's NAV if month-end is missing
        
    Returns:
        Tuple of (current_nav: float, nav_as_of_date: str DD-MM-YYYY, nav_history: Dict[YYYY-MM: float])
        
    Example:
        Input: [{"date": "26-12-2025", "nav": "192.59130"}, ...]
        Output: (192.59, "26-12-2025", {"2025-12": 192.59, "2025-11": 194.98, ...})
    """
    if not daily_nav_array:
        return None, None, {}
    
    # Extract latest NAV (first entry, sorted descending)
    latest_entry = daily_nav_array[0]
    current_nav = float(latest_entry['nav'])
    nav_as_of_date = latest_entry['date']  # Already in DD-MM-YYYY format
    
    # Group entries by month for aggregation
    monthly_groups: Dict[str, List[Dict]] = {}
    
    debug_log = logger.getEffectiveLevel() == logging.DEBUG
    
    for entry in daily_nav_array:
        try:
            # Parse date: "26-12-2025" → datetime object
            date_obj = datetime.strptime(entry['date'], "%d-%m-%Y")
            month_key = date_obj.strftime("%Y-%m")  # "2025-12"
            
            if month_key not in monthly_groups:
                monthly_groups[month_key] = []
            
            monthly_groups[month_key].append({
                'date_obj': date_obj,
                'date_str': entry['date'],
                'nav': float(entry['nav']),
                'day': date_obj.day
            })
        except (ValueError, KeyError) as e:
            # Skip malformed entries
            if debug_log:
                logger.debug(f"[AGG-NAV] Skipping malformed entry: {entry}")
            continue
    
    # Extract month-end NAV for each month
    nav_history: Dict[str, float] = {}
    
    logger.info(f"[AGG-NAV] Processing {len(monthly_groups)} months from {len(daily_nav_array)} daily entries")
    
    for month_key in sorted(monthly_groups.keys()):
        month_entries = monthly_groups[month_key]
        
        # Get last day of the month
        year, month = int(month_key.split('-')[0]), int(month_key.split('-')[1])
        last_day_of_month = monthrange(year, month)[1]
        
        # Try to find month-end entry
        month_end_entry = None
        
        # Strategy 1: Look for exact month-end date
        for entry in month_entries:
            if entry['day'] == last_day_of_month:
                month_end_entry = entry
                break
        
        # Strategy 2: Fallback - use previous day if month-end is holiday/weekend
        if not month_end_entry and fallback_strategy == 'previous_day':
            # Sort by day descending to get highest available day
            sorted_entries = sorted(month_entries, key=lambda x: x['day'], reverse=True)
            if sorted_entries:
                month_end_entry = sorted_entries[0]
                logger.debug(
                    f"[AGG-NAV] {month_key}: Month-end ({last_day_of_month}th) not found, "
                    f"using fallback: {month_end_entry['date_str']} (day {month_end_entry['day']})"
                )
        
        # Add to history if found
        if month_end_entry:
            nav_history[month_key] = round(month_end_entry['nav'], 2)
            logger.debug(f"[AGG-NAV] {month_key}: Added NAV={nav_history[month_key]} from {month_end_entry['date_str']}")
        else:
            logger.warning(f"[AGG-NAV] {month_key}: No entry found even with fallback")
    
    logger.info(f"[AGG-NAV] Final result: {len(nav_history)} months aggregated")
    return current_nav, nav_as_of_date, nav_history


logger = logging.getLogger(__name__)


@dataclass
class SchemeMatch:
    code: str
    name: str
    score: float


class FundEnricher:
    """
    Enriches mutual fund data using a simplified two-source approach:
    
    1. MFAPI.in (mfapi.in) - Primary source for:
       - Fund name resolution via two-step API (search + RapidFuzz)
       - NAV data (latest value and date)
       - ISIN codes (isin_growth, isin_div_reinvestment)
       - Fund metadata (AMC, category, scheme name)
    
    2. MstarPy - Secondary source for:
       - Holdings data (via ISIN from MFAPI)
       - Sector allocation (via ISIN from MFAPI)
    
    Flow:
    1. Input: Fund name (e.g., "HDFC Mid Cap Fund")
    2. MFAPI.search_and_get_fund(name, fuzzy_threshold=85)
       - Returns: scheme_code, scheme_name, ISIN, NAV, metadata
    3. MstarPy.get_fund_holdings(isin)
       - Returns: Holdings list using ISIN
    4. MstarPy.get_sector_allocation(isin)
       - Returns: Sector breakdown using ISIN
    5. Return: Complete EnrichedFund object
    
    Benefits over previous approach:
    - Single source of truth (mfapi.in instead of mftool + ISIN cache)
    - No dependency on outdated ISIN cache files
    - RapidFuzz matching centralized in one place (MFAPIFetcher)
    - Simpler flow: 2 API calls instead of 6+
    - ISIN always fresh from API
    """
    
    def __init__(
        self, 
        logger: Optional[logging.Logger] = None,
        mfapi_config: Optional[Dict] = None,
        retry_config: Optional[Dict] = None,
        nav_history_years: int = 10
    ):
        """
        Initialize FundEnricher with MFAPI and MstarPy fetchers.
        
        Args:
            logger: Logger instance
            mfapi_config: Configuration for MFAPI (timeout, max_retries, fuzzy_threshold)
            retry_config: Retry configuration for error handling
            nav_history_years: Years of NAV history to fetch (default: 10)
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # Initialize MFAPI fetcher (primary source for fund resolution and NAV)
        mfapi_cfg = mfapi_config or {}
        mfapi_timeout = mfapi_cfg.get('timeout', 10)
        mfapi_max_retries = mfapi_cfg.get('max_retries', 3)
        self.mfapi_fetcher = MFAPIFetcher(self.logger, timeout=mfapi_timeout, max_retries=mfapi_max_retries)
        self.mfapi_fuzzy_threshold = mfapi_cfg.get('fuzzy_threshold', 85)
        
        # NAV history configuration
        self.nav_history_years = nav_history_years
        
        # Initialize MstarPy fetcher (secondary source for holdings/sectors)
        self.mstar_fetcher = MstarPyFetcher(self.logger)
        
        # Retry configuration
        self.retry_config = retry_config or {}
        self.max_retries = self.retry_config.get('max_retries', 3)
        self.initial_retry_delay = self.retry_config.get('initial_delay', 1)
        self.max_retry_delay = self.retry_config.get('max_delay', 10)
        self.retry_backoff_multiplier = self.retry_config.get('backoff_multiplier', 2)
        self.retry_on_timeout = self.retry_config.get('retry_on_timeout', True)
        self.retry_on_server_error = self.retry_config.get('retry_on_server_error', True)

    async def enrich_async(self, fund_name: str) -> Optional[EnrichedFund]:
        """
        Async wrapper for enrich() method.
        
        Runs the synchronous enrich() method in a thread pool to avoid blocking
        the event loop when making external API calls.
        
        Args:
            fund_name: Name of the fund to enrich
            
        Returns:
            EnrichedFund object if enrichment successful, None otherwise
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.enrich, fund_name)

    @staticmethod
    async def enrich_batch_concurrent(
        enricher: 'FundEnricher',
        fund_names: List[str],
        max_concurrent: int,
        timeout_per_fund: int
    ) -> List[Optional[EnrichedFund]]:
        """
        Enrich multiple funds concurrently with semaphore to limit concurrent operations.
        
        This method processes multiple funds in parallel, improving throughput significantly.
        A semaphore limits the number of concurrent operations to prevent resource exhaustion.
        
        Args:
            enricher: FundEnricher instance to use
            fund_names: List of fund names to enrich
            max_concurrent: Maximum number of concurrent enrichments
            timeout_per_fund: Timeout in seconds per fund
            
        Returns:
            List of EnrichedFund objects (None for failed enrichments), maintaining original order
            
        Example:
            enricher = FundEnricher()
            results = await FundEnricher.enrich_batch_concurrent(
                enricher,
                ["Fund A", "Fund B", "Fund C"],
                max_concurrent=5,
                timeout_per_fund=15
            )
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def enrich_with_semaphore(fund_name: str) -> Optional[EnrichedFund]:
            """Enrich a single fund with semaphore protection, timeout, and retry logic."""
            async with semaphore:
                for attempt in range(enricher.max_retries):
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
                        if attempt < enricher.max_retries - 1 and enricher.retry_on_timeout:
                            delay = min(
                                enricher.initial_retry_delay * (enricher.retry_backoff_multiplier ** attempt),
                                enricher.max_retry_delay
                            )
                            enricher.logger.warning(
                                f"Timeout enriching '{fund_name}' (exceeded {timeout_per_fund}s), "
                                f"retry attempt {attempt + 1}/{enricher.max_retries} in {delay:.1f}s"
                            )
                            await asyncio.sleep(delay)
                        else:
                            enricher.logger.warning(
                                f"Timeout enriching '{fund_name}' (exceeded {timeout_per_fund}s) - "
                                f"all {enricher.max_retries} attempts failed"
                            )
                            return None
                    except Exception as e:
                        error_str = str(e).lower()
                        # Retry on transient errors (connection errors, server errors)
                        is_transient = any(keyword in error_str for keyword in 
                                          ['timeout', 'connection', '500', 'server error', 'temporarily'])
                        
                        if attempt < enricher.max_retries - 1 and is_transient and enricher.retry_on_server_error:
                            delay = min(
                                enricher.initial_retry_delay * (enricher.retry_backoff_multiplier ** attempt),
                                enricher.max_retry_delay
                            )
                            enricher.logger.warning(
                                f"Transient error enriching '{fund_name}', "
                                f"retry attempt {attempt + 1}/{enricher.max_retries} in {delay:.1f}s: {str(e)[:80]}"
                            )
                            await asyncio.sleep(delay)
                        else:
                            enricher.logger.error(
                                f"Failed enriching '{fund_name}': {str(e)}"
                            )
                            return None
                
                return None
        
        # Process all funds concurrently
        results = await asyncio.gather(
            *[enrich_with_semaphore(fund_name) for fund_name in fund_names]
        )
        
        return results

    def enrich(self, fund_name: str) -> Optional[EnrichedFund]:
        """
        Enrich a single mutual fund with complete data.
        
        Three-phase approach:
        
        Phase 1: Fund Resolution via MFAPI (2 API calls)
            - GET /mf/search?q={fund_name} → list of matching funds
            - RapidFuzz token_set_ratio matching (threshold: 85%)
            - GET /mf/{scheme_code}/latest → NAV + ISIN + metadata
        
        Phase 2: Fetch Holdings via MstarPy (using ISIN)
            - Extract ISIN from MFAPI response
            - GET holdings using ISIN (ISIN-based lookup is faster and more accurate)
        
        Phase 3: Fetch Sectors via MstarPy (using ISIN)
            - Get sector allocation using same ISIN
        
        Args:
            fund_name: Name of the fund to enrich (e.g., "HDFC Mid Cap Fund")
            
        Returns:
            EnrichedFund object with complete data if successful, None on failure
            
        Example:
            enricher = FundEnricher()
            fund = enricher.enrich("HDFC Top 100")
            if fund:
                print(f"NAV: {fund.current_nav}, ISIN: {fund.isin}")
        """
        # ===== PHASE 1: Fund Resolution via MFAPI =====
        # Two-step API call with RapidFuzz matching
        self.logger.info(f"[ENRICH-PHASE1] Resolving fund: '{fund_name}'")
        
        mfapi_result = self.mfapi_fetcher.search_and_get_fund(
            fund_name,
            fuzzy_threshold=self.mfapi_fuzzy_threshold,
            nav_history_years=self.nav_history_years
        )
        
        if not mfapi_result:
            self.logger.warning(f"[ENRICH-PHASE1] ✗ Failed to resolve fund: '{fund_name}'")
            return None
        
        if mfapi_result.get("status") != "SUCCESS":
            self.logger.warning(
                f"[ENRICH-PHASE1] ✗ MFAPI returned non-success status: {mfapi_result.get('status')}"
            )
            return None
        
        self.logger.info(
            f"[ENRICH-PHASE1] ✓ Fund resolved: '{mfapi_result.get('scheme_name')}' "
            f"(ISIN(growth): {mfapi_result.get('isin_growth')}, "
            f"ISIN(div_reinvestment): {mfapi_result.get('isin_div_reinvestment')}, "
            f"Current NAV: {mfapi_result.get('current_nav')} as of {mfapi_result.get('nav_as_of')}, "
            f"History: {len(mfapi_result.get('nav_history', {}))} months)"
        )
        
        # Extract key data from MFAPI response
        # Try both ISIN fields in order (growth first, then div_reinvestment)
        isin_candidates = [
            ("isin_growth", mfapi_result.get("isin_growth")),
            ("isin_div_reinvestment", mfapi_result.get("isin_div_reinvestment")),
        ]
        
        fund_isin = None
        isin_source = None
        for source, value in isin_candidates:
            if value and validate_isin(value):
                fund_isin = value
                isin_source = source
                break
        
        scheme_name = mfapi_result.get('scheme_name')
        amc = mfapi_result.get('fund_house')
        category = mfapi_result.get('scheme_category')
        current_nav = mfapi_result.get('current_nav')
        nav_as_of = mfapi_result.get('nav_as_of')
        nav_history = mfapi_result.get('nav_history', {})
        
        # ===== PHASE 2: Fetch Holdings via MstarPy =====
        holdings = None
        if fund_isin:
            self.logger.debug(
                f"[ENRICH-PHASE2] Fetching holdings using ISIN: {fund_isin} (source={isin_source})"
            )
            holdings = self._fetch_holdings_from_mstar(fund_isin)
            if holdings:
                self.logger.info(
                    f"[ENRICH-PHASE2] ✓ Fetched {len(holdings)} holdings for ISIN {fund_isin}"
                )
            else:
                self.logger.debug(
                    f"[ENRICH-PHASE2] ✗ No holdings data for ISIN {fund_isin}"
                )
        else:
            self.logger.warning(
                "[ENRICH-PHASE2] ✗ MFAPI did not provide a usable ISIN; skipping holdings fetch"
            )
        
        # ===== PHASE 3: Fetch Sectors via MstarPy =====
        sectors = None
        if fund_isin:
            self.logger.debug(
                f"[ENRICH-PHASE3] Fetching sector allocation using ISIN: {fund_isin} (source={isin_source})"
            )
            sectors = self._fetch_sector_from_mstar(fund_isin)
            if sectors:
                self.logger.info(
                    f"[ENRICH-PHASE3] ✓ Fetched {len(sectors)} sectors for ISIN {fund_isin}"
                )
            else:
                self.logger.debug(
                    f"[ENRICH-PHASE3] ✗ No sector data for ISIN {fund_isin}"
                )
        
        # ===== Create EnrichedFund object =====
        enriched = EnrichedFund(
            fund_name=scheme_name if scheme_name else fund_name,
            input_fund_name=fund_name,
            isin=fund_isin,
            amc=amc,
            category=category,
            expense_ratio=None,  # Not available from MFAPI
            sector_allocation=sectors,
            top_holdings=holdings,
            current_nav=current_nav,
            nav_as_of=nav_as_of,
        )

        # Preserve NAV history for downstream metadata enrichment
        enriched._nav_history = nav_history
        
        self.logger.info(
            f"[ENRICH] ✓ Enrichment complete for '{fund_name}': "
            f"Current NAV={current_nav}, NAV History={len(nav_history) if nav_history else 0} months, "
            f"Sectors={len(sectors) if sectors else 0}, "
            f"Holdings={len(holdings) if holdings else 0}"
        )
        
        return enriched

    def _fetch_holdings_from_mstar(self, fund_isin: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch holdings data from MstarPy using ISIN.
        
        Args:
            fund_isin: ISIN code from MFAPI
            
        Returns:
            List of holdings, or None if fetch failed
        """
        try:
            holdings_df = self.mstar_fetcher.get_fund_holdings(fund_isin)
            if holdings_df is None:
                return None
            
            records = holdings_df.to_dict('records')
            return [self._filter_top_holding(record) for record in records]
        except Exception as e:
            self.logger.debug(f"Failed to fetch holdings for ISIN {fund_isin}: {str(e)}")
            return None

    def _filter_top_holding(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Filter holding record to allowed fields."""
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
        """
        Fetch sector allocation from MstarPy using ISIN.
        
        Args:
            fund_isin: ISIN code from MFAPI
            
        Returns:
            Dict of sector_name -> percentage, or None if fetch failed
        """
        try:
            sectors = self.mstar_fetcher.get_sector_allocation(fund_isin)
            normalized = self._normalize_sector_result(sectors)
            return normalized
        except Exception as e:
            self.logger.debug(f"Failed to fetch sectors for ISIN {fund_isin}: {str(e)}")
            return None

    def _normalize_sector_result(self, sector_result: Any) -> Optional[Dict[str, float]]:
        """Normalize sector data from various MstarPy response formats."""
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
                        amount = safe_float(percentage)
                        if amount is not None:
                            sector_data[sector_name] = amount
                    if sector_data:
                        return sector_data
            
            # Flat dict format
            for sector_name, value in sector_result.items():
                if isinstance(value, (dict, list)):
                    continue
                amount = safe_float(value)
                if amount is not None:
                    sector_data[sector_name] = amount
            if sector_data:
                return sector_data
        
        elif isinstance(sector_result, list):
            # List of dicts format
            for item in sector_result:
                if not isinstance(item, dict):
                    continue
                sector_name = item.get('assetType') or item.get('sectorName')
                percentage = item.get('percentage') or item.get('value') or item.get('sectorValue')
                amount = safe_float(percentage)
                if sector_name and amount is not None:
                    sector_data[sector_name] = amount
            if sector_data:
                return sector_data
        
        elif hasattr(sector_result, 'empty') and not sector_result.empty:
            # DataFrame format
            if 'sectorValue' in sector_result.columns and 'sectorName' in sector_result.columns:
                for _, row in sector_result.iterrows():
                    sector_name = row.get('sectorName')
                    amount = safe_float(row.get('sectorValue'))
                    if sector_name and amount is not None:
                        sector_data[sector_name] = amount
                if sector_data:
                    return sector_data

        self.logger.debug(f"Unable to normalize sector data from {type(sector_result)}")
        return None

    def close(self):
        """Close all fetcher connections."""
        if hasattr(self, 'mfapi_fetcher') and self.mfapi_fetcher:
            self.mfapi_fetcher.close()
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()

