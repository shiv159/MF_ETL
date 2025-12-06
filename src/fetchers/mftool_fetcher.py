"""Fetcher for mutual fund data using mftool"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from mftool import Mftool


class MFToolFetcher:
    """Fetch mutual fund data using mftool library"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize MFToolFetcher.
        
        Args:
            logger: Logger instance for logging operations
        """
        self.mf = Mftool()
        self.logger = logger or logging.getLogger(__name__)
    
    def get_scheme_nav(self, scheme_code: str) -> Dict[str, Any]:
        """
        Fetch NAV data for a specific mutual fund scheme.
        
        Args:
            scheme_code: Mutual fund scheme code
            
        Returns:
            Dictionary containing NAV data
        """
        try:
            self.logger.info(f"Fetching NAV data for scheme code: {scheme_code}")
            nav_data = self.mf.get_scheme_quote(scheme_code)
            
            if not nav_data:
                self.logger.warning(f"No data returned for scheme code: {scheme_code}")
                return {}
            
            self.logger.info(f"Successfully fetched NAV data for {scheme_code}")
            return nav_data
            
        except Exception as e:
            self.logger.error(f"Error fetching NAV data for {scheme_code}: {str(e)}")
            return {}
    
    def get_scheme_details(self, scheme_code: str) -> Dict[str, Any]:
        """
        Fetch detailed information about a mutual fund scheme.
        
        Args:
            scheme_code: Mutual fund scheme code
            
        Returns:
            Dictionary containing scheme details
        """
        try:
            self.logger.info(f"Fetching scheme details for: {scheme_code}")
            details = self.mf.get_scheme_details(scheme_code)
            
            if not details:
                self.logger.warning(f"No details found for scheme code: {scheme_code}")
                return {}
            
            self.logger.info(f"Successfully fetched details for {scheme_code}")
            return details
            
        except Exception as e:
            self.logger.error(f"Error fetching scheme details for {scheme_code}: {str(e)}")
            return {}
    
    def get_all_schemes(self) -> List[Dict[str, Any]]:
        """
        Fetch list of all available mutual fund schemes.
        
        Returns:
            List of scheme dictionaries
        """
        try:
            self.logger.info("Fetching all available schemes")
            schemes = self.mf.get_scheme_codes()
            
            if not schemes:
                self.logger.warning("No schemes data returned")
                return []
            
            # Convert to list of dicts
            scheme_list = [
                {"code": code, "name": name} 
                for code, name in schemes.items()
            ]
            
            self.logger.info(f"Successfully fetched {len(scheme_list)} schemes")
            return scheme_list
            
        except Exception as e:
            self.logger.error(f"Error fetching all schemes: {str(e)}")
            return []
    
    def search_scheme(self, search_term: str) -> List[Dict[str, Any]]:
        """
        Search for schemes by name.
        
        Args:
            search_term: Search term for scheme name
            
        Returns:
            List of matching schemes
        """
        try:
            self.logger.info(f"Searching schemes with term: {search_term}")
            all_schemes = self.get_all_schemes()
            
            # Filter schemes matching search term
            matching_schemes = [
                scheme for scheme in all_schemes
                if search_term.lower() in scheme['name'].lower()
            ]
            
            self.logger.info(f"Found {len(matching_schemes)} matching schemes")
            return matching_schemes
            
        except Exception as e:
            self.logger.error(f"Error searching schemes: {str(e)}")
            return []
