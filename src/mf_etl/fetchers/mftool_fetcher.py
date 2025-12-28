"""Fetcher for mutual fund data using mfapi.in (free open API)

This module provides access to Indian mutual fund data via mfapi.in:
- NAV data and history (10 years by default)
- ISIN codes (isin_growth, isin_div_reinvestment)
- Fund metadata (AMC, category, scheme name)
- No authentication required
- Free and open API

API Docs: https://www.mfapi.in/docs/

Two-step enrichment flow:
1. search_schemes(query) → Get list of funds with scheme codes
2. search_and_get_fund(name, threshold, nav_history_years) → RapidFuzz match + fetch NAV history
"""

from typing import Dict, Any, Optional, List, Tuple
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from rapidfuzz import fuzz, process
from datetime import datetime, timedelta


class MFAPIFetcher:
    """Fetch mutual fund data using mfapi.in API"""
    
    BASE_URL = "https://api.mfapi.in"
    TIMEOUT = 10
    MAX_RETRIES = 3
    
    def __init__(self, logger: Optional[logging.Logger] = None, timeout: int = 10, max_retries: int = 3):
        """
        Initialize MFAPIFetcher with resilient HTTP session.
        
        Args:
            logger: Logger instance for logging operations
            timeout: API request timeout in seconds (default: 10)
            max_retries: Maximum retry attempts for failed requests (default: 3)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.TIMEOUT = timeout
        self.MAX_RETRIES = max_retries
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with automatic retries for transient failures.
        
        Returns:
            Configured requests.Session with retry strategy
        """
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        self.logger.debug(f"HTTP session created with {self.MAX_RETRIES} retries, {self.TIMEOUT}s timeout")
        return session
    
    def search_schemes(self, query: str) -> List[Dict[str, Any]]:
        """
        Search for mutual fund schemes by name/AMC using mfapi.in search API.
        
        Example:
            GET https://api.mfapi.in/mf/search?q=HDFC
            Returns list of all HDFC schemes with schemeCode and schemeName
        
        Args:
            query: Search query (e.g., "HDFC", "Mid Cap", "Axis")
            
        Returns:
            List of schemes, each with keys:
                - schemeCode: int (e.g., 125497)
                - schemeName: str (e.g., "HDFC Top 100 Fund - Direct Plan - Growth")
        """
        try:
            url = f"{self.BASE_URL}/mf/search"
            params = {"q": query}
            
            self.logger.debug(f"[MFAPI-SEARCH] Searching: {query}")
            
            response = self.session.get(url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            
            schemes = response.json()
            
            if not isinstance(schemes, list):
                self.logger.warning(f"[MFAPI-SEARCH] Unexpected response format for query '{query}'")
                return []
            
            self.logger.info(f"[MFAPI-SEARCH] ✓ Found {len(schemes)} schemes for '{query}'")
            return schemes
            
        except requests.exceptions.Timeout:
            self.logger.error(f"[MFAPI-SEARCH] Timeout searching for '{query}'")
            return []
        except requests.exceptions.ConnectionError:
            self.logger.error(f"[MFAPI-SEARCH] Connection error searching for '{query}'")
            return []
        except Exception as e:
            self.logger.error(f"[MFAPI-SEARCH] Error searching for '{query}': {str(e)}")
            return []
    
    def search_and_get_fund(
        self, 
        input_fund_name: str, 
        fuzzy_threshold: int = 85,
        nav_history_years: int = 10
    ) -> Optional[Dict[str, Any]]:
        """
        Three-step process to resolve fund and get NAV history + ISIN:
        
        Step 1: Search for funds matching input name
        Step 2: Use RapidFuzz to find best match among results
        Step 3: Fetch NAV history + ISIN for selected fund (last N years)
        
        This is the primary method for enrichment.
        
        Example Flow:
            Input: "HDFC Mid Cap"
            Search API returns: [{schemeCode: 125499, schemeName: "HDFC Mid Cap Fund - Direct Plan - Growth"}, ...]
            RapidFuzz: "HDFC Mid Cap Fund - Direct Plan - Growth" scores 95% → Selected
            NAV History API returns: {data: [{date: "26-12-2025", nav: "1485.50"}, ...], ...}
                                     (10 years of daily NAV)
        
        Args:
            input_fund_name: User-provided fund name (e.g., "HDFC Mid Cap Fund")
            fuzzy_threshold: RapidFuzz token_set_ratio threshold (0-100), default 85
            nav_history_years: Years of NAV history to fetch, default 10
            
        Returns:
            Dict with keys:
                - scheme_code: Scheme code
                - scheme_name: Full official scheme name from MFAPI
                - fund_house: AMC/Fund House name
                - scheme_category: Fund category (e.g., "Equity - Mid Cap")
                - scheme_type: Open/Closed ended
                - isin_growth: ISIN for growth option
                - isin_div_reinvestment: ISIN for dividend reinvestment (if available)
                - current_nav: Latest NAV value (extracted from history)
                - nav_as_of: Latest NAV date (format: DD-MM-YYYY)
                - nav_history: Monthly aggregated NAV (Dict[YYYY-MM]: float)
                - matched_fund_name: Name that matched via RapidFuzz
                - fuzzy_score: RapidFuzz match score (0-100)
                - status: "SUCCESS" or "FAILED"
            
            Returns None if no match found above fuzzy_threshold
        """
        # STEP 1: Search API
        self.logger.debug(f"[MFAPI-RESOLVE] Step 1: Searching for '{input_fund_name}'")
        search_results = self.search_schemes(input_fund_name)
        
        if not search_results:
            self.logger.warning(f"[MFAPI-RESOLVE] - No search results for '{input_fund_name}'")
            return None
        
        self.logger.debug(f"[MFAPI-RESOLVE] Found {len(search_results)} candidates")
        
        # STEP 2: RapidFuzz matching + Plan Preference Scoring
        self.logger.debug(f"[MFAPI-RESOLVE] Step 2: RapidFuzz matching (threshold: {fuzzy_threshold}%)")
        scheme_names = [r.get('schemeName') for r in search_results]
        
        # Get all candidates matching the threshold (not just the top 1)
        fuzzy_candidates = self._fuzzy_all_matches(
            input_fund_name,
            scheme_names,
            threshold=fuzzy_threshold
        )
        
        if not fuzzy_candidates:
            self.logger.warning(
                f"[MFAPI-RESOLVE] ✗ No RapidFuzz match above {fuzzy_threshold}% for '{input_fund_name}'"
            )
            return None
        
        self.logger.debug(
            f"[MFAPI-RESOLVE] Found {len(fuzzy_candidates)} candidates above {fuzzy_threshold}% threshold"
        )
        
        # STEP 2b: Apply plan preference scoring (Direct > Regular; Growth > Reinvestment)
        selected_fund, matched_name, score = self._select_best_plan(
            input_fund_name,
            search_results,
            fuzzy_candidates
        )
        
        scheme_code = selected_fund.get('schemeCode')
        
        self.logger.info(
            f"[MFAPI-RESOLVE] ✓ Selected: '{matched_name}' "
            f"(fuzzy_score: {score}%, scheme_code: {scheme_code})"
        )
        
        # STEP 3: Get NAV history (10 years by default)
        self.logger.debug(f"[MFAPI-RESOLVE] Step 3: Fetching {nav_history_years}-year NAV history for scheme {scheme_code}")
        nav_data = self._get_nav_history_internal(scheme_code, nav_history_years)
        
        if not nav_data:
            self.logger.error(f"[MFAPI-RESOLVE] ✗ Failed to fetch NAV history for scheme {scheme_code}")
            return None
        
        # Combine search result + NAV data
        result = {
            "input_fund_name": input_fund_name,
            "matched_fund_name": matched_name,
            "fuzzy_score": score,
            **nav_data  # Includes: scheme_code, scheme_name, fund_house, isin_growth, current_nav, nav_as_of, nav_history, status
        }
        
        self.logger.info(
            f"[MFAPI-RESOLVE] ✓ Fund resolved: {matched_name} | "
            f"ISIN: {nav_data.get('isin_growth')} | "
            f"Current NAV: {nav_data.get('current_nav')} ({nav_data.get('nav_as_of')}) | "
            f"History: {len(nav_data.get('nav_history', {}))} months"
        )
        
        return result
    
    def _get_nav_history_internal(
        self, 
        scheme_code: str,
        nav_history_years: int = 10
    ) -> Optional[Dict[str, Any]]:
        """
        Internal method to fetch NAV history and fund metadata for a scheme.
        
        Fetches daily NAV for specified years and aggregates to monthly.
        Extracts latest NAV for current_nav and nav_as_of fields.
        
        Args:
            scheme_code: Mutual fund scheme code
            nav_history_years: Years of history to fetch (default: 10)
            
        Returns:
            Dict with NAV history, ISIN, and metadata, or None on error
        """
        try:
            # Calculate date range: now - nav_history_years to now
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=365 * nav_history_years)
            
            # Format dates as YYYY-MM-DD for MFAPI
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # IMPORTANT: Use /mf/{scheme_code} (NOT /mf/{scheme_code}/latest) 
            # to get historical data with date range parameters
            url = f"{self.BASE_URL}/mf/{scheme_code}"
            params = {
                'startDate': start_date_str,
                'endDate': end_date_str
            }
            
            self.logger.debug(
                f"[MFAPI-NAV-HISTORY] Fetching {nav_history_years}-year history for scheme {scheme_code} "
                f"({start_date_str} to {end_date_str})"
            )
            
            response = self.session.get(url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("status") != "SUCCESS":
                self.logger.warning(f"[MFAPI-NAV-HISTORY] Status: {data.get('status')} for scheme {scheme_code}")
                return None
            
            meta = data.get("meta", {})
            nav_list = data.get("data", [])
            
            if not nav_list:
                self.logger.warning(f"[MFAPI-NAV-HISTORY] No NAV data for scheme {scheme_code}")
                return None
            
            # Log raw data from MFAPI before aggregation
            self.logger.info(
                f"[MFAPI-NAV-HISTORY] Raw data: scheme {scheme_code} returned {len(nav_list)} daily entries. "
                f"Date range: {nav_list[-1].get('date') if nav_list else 'N/A'} to {nav_list[0].get('date') if nav_list else 'N/A'}"
            )
            
            # Import aggregation function from fund_enricher module
            import sys
            from pathlib import Path
            ROOT = Path(__file__).resolve().parents[3]
            if str(ROOT) not in sys.path:
                sys.path.insert(0, str(ROOT))
            
            from services.enrichment.fund_enricher import aggregate_nav_to_monthly
            
            # Log raw data from MFAPI before aggregation
            self.logger.info(
                f"[MFAPI-NAV-HISTORY] Raw data: scheme {scheme_code} returned {len(nav_list)} daily entries. "
                f"Date range: {nav_list[-1].get('date') if nav_list else 'N/A'} to {nav_list[0].get('date') if nav_list else 'N/A'}"
            )
            
            # Aggregate daily NAV to monthly
            current_nav, nav_as_of, nav_history = aggregate_nav_to_monthly(nav_list)
            
            result = {
                "scheme_code": meta.get("scheme_code"),
                "scheme_name": meta.get("scheme_name"),
                "fund_house": meta.get("fund_house"),
                "scheme_category": meta.get("scheme_category"),
                "scheme_type": meta.get("scheme_type"),
                "isin_growth": meta.get("isin_growth"),
                "isin_div_reinvestment": meta.get("isin_div_reinvestment"),
                "current_nav": current_nav,
                "nav_as_of": nav_as_of,
                "nav_history": nav_history,
                "status": "SUCCESS"
            }
            
            self.logger.debug(
                f"[MFAPI-NAV-HISTORY] ✓ Fetched: Current NAV={result['current_nav']}, "
                f"Monthly history entries={len(nav_history)}, ISIN={result['isin_growth']}"
            )
            
            # Log month range after aggregation
            if nav_history:
                sorted_months = sorted(nav_history.keys())
                self.logger.info(
                    f"[MFAPI-NAV-HISTORY] Aggregated to {len(nav_history)} months: "
                    f"{sorted_months[0]} to {sorted_months[-1]}"
                )
            
            return result
            
        except requests.exceptions.Timeout:
            self.logger.error(f"[MFAPI-NAV-HISTORY] Timeout fetching scheme {scheme_code}")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"[MFAPI-NAV-HISTORY] Connection error fetching scheme {scheme_code}")
            return None
        except Exception as e:
            self.logger.error(f"[MFAPI-NAV-HISTORY] Error fetching scheme {scheme_code}: {str(e)}")
            return None
    
    def _get_latest_nav_internal(self, scheme_code: str) -> Optional[Dict[str, Any]]:
        """
        DEPRECATED: Use _get_nav_history_internal instead.
        
        Internal method to fetch latest NAV and fund metadata for a scheme.
        
        Args:
            scheme_code: Mutual fund scheme code
            
        Returns:
            Dict with NAV, ISIN, and metadata, or None on error
        """
        try:
            url = f"{self.BASE_URL}/mf/{scheme_code}/latest"
            
            response = self.session.get(url, timeout=self.TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("status") != "SUCCESS":
                self.logger.warning(f"[MFAPI-NAV] Status: {data.get('status')} for scheme {scheme_code}")
                return None
            
            meta = data.get("meta", {})
            nav_list = data.get("data", [])
            
            if not nav_list:
                self.logger.warning(f"[MFAPI-NAV] No NAV data for scheme {scheme_code}")
                return None
            
            latest_nav = nav_list[0]
            
            result = {
                "scheme_code": meta.get("scheme_code"),
                "scheme_name": meta.get("scheme_name"),
                "fund_house": meta.get("fund_house"),
                "scheme_category": meta.get("scheme_category"),
                "scheme_type": meta.get("scheme_type"),
                "isin_growth": meta.get("isin_growth"),
                "isin_div_reinvestment": meta.get("isin_div_reinvestment"),
                "nav": self._safe_float(latest_nav.get("nav")),
                "nav_date": latest_nav.get("date"),  # Format: DD-MM-YYYY
                "status": "SUCCESS"
            }
            
            self.logger.debug(
                f"[MFAPI-NAV] ✓ Fetched: NAV={result['nav']}, ISIN={result['isin_growth']}"
            )
            
            return result
            
        except requests.exceptions.Timeout:
            self.logger.error(f"[MFAPI-NAV] Timeout fetching scheme {scheme_code}")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"[MFAPI-NAV] Connection error fetching scheme {scheme_code}")
            return None
        except Exception as e:
            self.logger.error(f"[MFAPI-NAV] Error fetching scheme {scheme_code}: {str(e)}")
            return None
    
    def _fuzzy_all_matches(
        self,
        query: str,
        choices: List[str],
        threshold: int = 85
    ) -> List[Tuple[str, int, int]]:
        """
        Get all fuzzy matches above threshold (not just the top 1).
        
        Returns list of (matched_string, score, index) sorted by score descending.
        
        Args:
            query: Query string (e.g., "Tata Small Cap")
            choices: List of candidates (e.g., scheme names)
            threshold: Minimum score required (0-100)
            
        Returns:
            List of (matched_string, score, index) tuples, sorted by score descending
        """
        if not query or not choices:
            return []
        
        results = process.extract(
            query,
            choices,
            scorer=fuzz.token_set_ratio,
            processor=lambda x: x.lower().strip() if x else "",
            score_cutoff=threshold
        )
        
        # Results are already sorted by score descending
        return results

    def _fuzzy_best_match(
        self, 
        query: str, 
        choices: List[str], 
        threshold: int = 85
    ) -> Optional[Tuple[str, int, int]]:
        """
        Use RapidFuzz token_set_ratio to find best match.
        
        token_set_ratio is ideal for fund names because:
        - Ignores word order: "HDFC Mid Cap" matches "Mid Cap HDFC"
        - Handles duplicates: "HDFC HDFC" matches "HDFC"
        - Robust to suffixes: "Fund Growth" matches "Fund - Growth Option"
        
        Args:
            query: Query string (e.g., "HDFC Mid Cap")
            choices: List of candidates (e.g., scheme names from search)
            threshold: Minimum score required (0-100)
            
        Returns:
            Tuple of (matched_string, score, index) if match found, else None
        """
        if not query or not choices:
            return None
        
        result = process.extractOne(
            query,
            choices,
            scorer=fuzz.token_set_ratio,
            processor=lambda x: x.lower().strip() if x else "",
            score_cutoff=threshold
        )
        
        return result
    
    def _select_best_plan(
        self,
        input_fund_name: str,
        search_results: List[Dict[str, Any]],
        fuzzy_matches: List[Tuple[str, int, int]]
    ) -> Tuple[Dict[str, Any], str, int]:
        """
        From candidates that passed fuzzy matching, select best by plan preference.
        
        Scoring hierarchy (when user doesn't specify plan):
        1. Direct Plan preferred over Regular (Direct +10)
        2. Growth preferred over Reinvestment/IDCW (Growth +5, Reinvestment/IDCW -5)
        3. If tied, use fuzzy match score as tiebreaker
        
        Examples:
            "Tata Small Cap Fund" → picks "Tata Small Cap Fund Direct Plan - Growth"
            "HDFC Mid Cap Direct" → picks "HDFC Mid Cap Fund Direct Plan - Growth"
            "HDFC Mid Cap Growth" → picks highest-scoring growth variant
        
        Args:
            input_fund_name: Original user input
            search_results: All search results from API
            fuzzy_matches: List of (name, score, index) that passed fuzzy threshold
            
        Returns:
            Tuple of (selected_fund_dict, scheme_name, fuzzy_score)
        """
        def plan_preference_score(scheme_name: str) -> int:
            """Compute plan preference bonus for a scheme name.
            
            Scoring hierarchy (when user doesn't specify plan):
            1. Growth is strongly preferred (most common option) → +15
            2. Direct Plan preferred over Regular → +10 / +0
            3. Avoid Reinvestment/IDCW if not explicitly requested → -20
            
            Final ranking example for "Tata Small Cap Fund" (no plan specified):
            - Direct Plan-Growth: 15 + 10 = +25 ✓ BEST
            - Regular Plan-Growth: 15 + 0 = +15
            - Direct Plan-Reinvestment: -20 + 10 = -10
            - Regular Plan-Reinvestment: -20 + 0 = -20
            """
            score = 0
            name_lower = scheme_name.lower()
            
            # Growth vs Reinvestment/IDCW is the PRIMARY distinction
            # (users almost always want Growth unless they explicitly ask for Reinvestment)
            if 'growth' in name_lower:
                score += 15
            elif 'reinvestment' in name_lower or 'idcw' in name_lower:
                score -= 20
            
            # Direct vs Regular is SECONDARY preference (bonus/penalty applied on top)
            if 'direct' in name_lower:
                score += 10
            elif 'regular' in name_lower:
                score += 0
            
            return score
        
        # Rank candidates by: plan preference + fuzzy score
        ranked = []
        for scheme_name, fuzzy_score, idx in fuzzy_matches:
            plan_score = plan_preference_score(scheme_name)
            total_score = fuzzy_score + plan_score  # e.g., 98 + 10 + 5 = 113
            ranked.append((total_score, scheme_name, fuzzy_score, idx))
        
        # Sort by total score descending, then fuzzy score as tiebreaker
        ranked.sort(key=lambda x: (-x[0], -x[2]))
        
        total_score, scheme_name, fuzzy_score, selected_idx = ranked[0]
        selected_fund = search_results[selected_idx]
        
        if len(ranked) > 1:
            self.logger.debug(
                f"[MFAPI-PLAN] Multiple candidates found. Ranked by plan preference:"
            )
            for i, (total, name, fuzz, _) in enumerate(ranked[:3], 1):
                plan_bonus = int(total - fuzz)
                self.logger.debug(
                    f"  [{i}] {name} (fuzzy={int(fuzz)}%, plan_bonus={plan_bonus:+d}, total={int(total)}%)"
                )
            self.logger.debug(f"  → Selected: {scheme_name}")
        
        return selected_fund, scheme_name, fuzzy_score
    
    @staticmethod
    def _safe_float(value) -> Optional[float]:
        """Safely convert value to float, handling strings and None."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except (ValueError, AttributeError):
                return None
        return None
    
    def close(self):
        """Close the HTTP session."""
        if hasattr(self, 'session') and self.session:
            self.session.close()
            self.logger.debug("HTTP session closed")
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()
