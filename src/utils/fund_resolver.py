"""
Fund Resolver - Maps fund names to library-specific identifiers

This utility resolves fund names to the appropriate identifiers required by different libraries:
- mftool: requires scheme codes
- mstarpy: uses fund names directly for search
"""

from typing import Dict, List, Optional
from mftool import Mftool


class FundResolver:
    """Resolve fund names to library-specific identifiers"""
    
    def __init__(self, logger=None):
        """
        Initialize FundResolver
        
        Args:
            logger: Optional logger instance
        """
        self.logger = logger
        self.mftool = Mftool()
        self._scheme_cache = None  # Cache for all schemes
    
    def _log(self, level: str, message: str):
        """Internal logging helper"""
        if self.logger:
            getattr(self.logger, level)(message)
    
    def _get_all_schemes(self) -> Dict:
        """Get all schemes from mftool (cached)"""
        if self._scheme_cache is None:
            self._log('info', 'Loading all mutual fund schemes...')
            self._scheme_cache = self.mftool.get_scheme_codes()
            self._log('info', f'Loaded {len(self._scheme_cache)} schemes')
        return self._scheme_cache
    
    def search_scheme_code(self, fund_name: str) -> Optional[str]:
        """
        Search for scheme code by fund name using multiple strategies
        
        Args:
            fund_name: Name of the fund to search for
            
        Returns:
            Scheme code if found, None otherwise
        """
        all_schemes = self._get_all_schemes()
        
        # Normalize the search name
        search_name = fund_name.lower().strip()
        
        # Remove common suffixes for better matching
        for suffix in [' fund', ' direct', ' growth', ' direct plan', ' growth option', '-direct', '-growth']:
            search_name = search_name.replace(suffix, '')
        
        # Strategy 1: Try exact match first
        for code, name in all_schemes.items():
            if name.lower().strip() == fund_name.lower().strip():
                self._log('debug', f"Exact match found: {name}")
                return code
        
        # Strategy 2: Try normalized match
        for code, name in all_schemes.items():
            name_normalized = name.lower().strip()
            for suffix in [' fund', ' direct', ' growth', ' direct plan', ' growth option', '-direct', '-growth']:
                name_normalized = name_normalized.replace(suffix, '')
            
            if search_name == name_normalized:
                self._log('debug', f"Normalized match found: {name}")
                return code
        
        # Strategy 3: Try partial match with normalized names
        for code, name in all_schemes.items():
            name_normalized = name.lower().strip()
            for suffix in [' fund', ' direct', ' growth', ' direct plan', ' growth option', '-direct', '-growth']:
                name_normalized = name_normalized.replace(suffix, '')
            
            if search_name in name_normalized:
                self._log('debug', f"Partial match found: {name}")
                return code
        
        # Strategy 4: Try word-by-word match with minimum threshold
        search_words = set(search_name.split())
        best_match = None
        best_score = 0
        best_name = None
        
        for code, name in all_schemes.items():
            name_normalized = name.lower().strip()
            for suffix in [' fund', ' direct', ' growth', ' direct plan', ' growth option', '-direct', '-growth']:
                name_normalized = name_normalized.replace(suffix, '')
            
            name_words = set(name_normalized.split())
            common_words = search_words.intersection(name_words)
            score = len(common_words)
            
            # Require at least 60% of search words to match
            if score > best_score and score >= len(search_words) * 0.6:
                best_score = score
                best_match = code
                best_name = name
        
        if best_match:
            self._log('debug', f"Word-based match found: {best_name} (score: {best_score}/{len(search_words)})")
        
        return best_match
    
    def resolve_fund(self, fund_name: str) -> Dict[str, Optional[str]]:
        """
        Resolve a fund name to library-specific identifiers
        
        Args:
            fund_name: Name of the fund
            
        Returns:
            Dict with keys:
                - name: Original fund name
                - mftool_scheme_code: Scheme code for mftool (or None)
                - mstarpy_search_term: Search term for mstarpy
                - mstarpy_alternate_terms: List of alternate search terms
        """
        scheme_code = self.search_scheme_code(fund_name)
        
        if scheme_code:
            self._log('info', f"Found scheme code {scheme_code} for '{fund_name}'")
        else:
            self._log('warning', f"No scheme code found for '{fund_name}'")
        
        # Generate alternate search terms for mstarpy
        alternates = []
        
        # Try with 'Direct Growth' suffix
        if 'direct' not in fund_name.lower():
            alternates.append(f"{fund_name} Direct Growth")
            alternates.append(f"{fund_name}-Direct-Growth")
        
        # Try with 'Growth' suffix only
        if 'growth' not in fund_name.lower():
            alternates.append(f"{fund_name} Growth")
        
        # Try abbreviated versions for common names
        if 'Aditya Birla Sun Life' in fund_name:
            alternates.append(fund_name.replace('Aditya Birla Sun Life', 'ABSL'))
        if 'HDFC' in fund_name and 'Bank' not in fund_name:
            alternates.append(fund_name.replace('HDFC', 'HDFC Mutual Fund'))
        
        result = {
            'name': fund_name,
            'mftool_scheme_code': scheme_code,
            'mstarpy_search_term': fund_name,
            'mstarpy_alternate_terms': alternates
        }
        
        self._log('info', f"Resolved '{fund_name}': scheme_code={scheme_code}, alternates={len(alternates)}")
        return result
    
    def resolve_funds(self, fund_names: List[str]) -> List[Dict[str, Optional[str]]]:
        """
        Resolve multiple fund names
        
        Args:
            fund_names: List of fund names
            
        Returns:
            List of resolution dicts (one per fund)
        """
        results = []
        for fund_name in fund_names:
            results.append(self.resolve_fund(fund_name))
        return results
    
    def get_all_matching_schemes(self, partial_name: str, max_results: int = 10) -> List[Dict[str, str]]:
        """
        Search for schemes matching a partial name
        
        Args:
            partial_name: Partial fund name to search for
            max_results: Maximum number of results to return
            
        Returns:
            List of dicts with 'code' and 'name' keys
        """
        all_schemes = self._get_all_schemes()
        search_term = partial_name.lower().strip()
        
        matches = []
        for code, name in all_schemes.items():
            if search_term in name.lower():
                matches.append({'code': code, 'name': name})
                if len(matches) >= max_results:
                    break
        
        return matches
