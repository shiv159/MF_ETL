"""
mstarpy Fetcher - Fetch mutual fund holdings and details from Morningstar

This module provides functionality to:
- Fetch portfolio holdings for mutual funds
- Retrieve sector allocation data
- Get asset allocation information
- Extract fund metadata
"""

import pandas as pd
from typing import Optional, Dict, Any
import mstarpy


class MstarPyFetcher:
    """Fetcher for mutual fund data using mstarpy (Morningstar)"""
    
    def __init__(self, logger=None):
        """
        Initialize MstarPyFetcher
        
        Args:
            logger: Logger instance for logging operations
        """
        self.logger = logger
    
    def _log(self, level: str, message: str):
        """Internal logging helper"""
        if self.logger:
            getattr(self.logger, level)(message)
    
    def get_fund_holdings(self, fund_isin: str, top_n: int = 20) -> Optional[pd.DataFrame]:
        """
        Fetch portfolio holdings for a mutual fund
        
        Args:
            fund_isin: ISIN code of the mutual fund
            top_n: Number of top holdings to return (default: 20)
            
        Returns:
            DataFrame with top N holdings data or None if fetch fails
        """
        try:
            self._log('info', f"Fetching holdings for fund: {fund_isin}")
            fund = mstarpy.Funds(term=fund_isin)
            holdings = fund.holdings()
            
            if holdings is not None and not holdings.empty:
                # Return only top N holdings
                top_holdings = holdings.head(top_n)
                self._log('info', f"Successfully fetched top {len(top_holdings)} holdings (out of {len(holdings)} total)")
                return top_holdings
            else:
                self._log('warning', f"No holdings data available for {fund_isin}")
                return None
                
        except Exception as e:
            self._log('error', f"Error fetching holdings for {fund_isin}: {str(e)}")
            return None
    
    def get_sector_allocation(self, fund_isin: str):
        """
        Fetch sector allocation for a mutual fund
        
        Args:
            fund_isin: ISIN code of the mutual fund
            
        Returns:
            Dict or DataFrame with sector allocation data or None if fetch fails
        """
        try:
            self._log('info', f"Fetching sector allocation for fund: {fund_isin}")
            fund = mstarpy.Funds(term=fund_isin)
            sectors = fund.sector()
            
            if sectors is not None:
                # Check if it's a DataFrame or dict
                if hasattr(sectors, 'empty'):
                    # It's a DataFrame
                    if not sectors.empty:
                        self._log('info', f"Successfully fetched {len(sectors)} sectors")
                        return sectors
                elif isinstance(sectors, dict) and len(sectors) > 0:
                    # It's a dict
                    self._log('info', f"Successfully fetched {len(sectors)} sectors")
                    return sectors
                
            self._log('warning', f"No sector data available for {fund_isin}")
            return None
                
        except Exception as e:
            self._log('error', f"Error fetching sectors for {fund_isin}: {str(e)}")
            return None
    
    def get_asset_allocation(self, fund_isin: str) -> Optional[pd.DataFrame]:
        """
        Fetch asset allocation for a mutual fund
        
        Args:
            fund_isin: ISIN code of the mutual fund
            
        Returns:
            DataFrame with asset allocation data or None if fetch fails
        """
        try:
            self._log('info', f"Fetching asset allocation for fund: {fund_isin}")
            fund = mstarpy.Funds(term=fund_isin)
            assets = fund.asset_allocation()
            
            if assets is not None and not assets.empty:
                self._log('info', f"Successfully fetched asset allocation")
                return assets
            else:
                self._log('warning', f"No asset allocation data available for {fund_isin}")
                return None
                
        except Exception as e:
            self._log('error', f"Error fetching asset allocation for {fund_isin}: {str(e)}")
            return None
    
    def get_fund_details(self, fund_isin: str) -> Dict[str, Any]:
        """
        Fetch comprehensive fund details
        
        Args:
            fund_isin: ISIN code of the mutual fund
            
        Returns:
            Dictionary with fund details
        """
        details = {'isin': fund_isin}
        
        try:
            fund = mstarpy.Funds(term=fund_isin)
            
            # Fund name
            try:
                details['name'] = fund.name()
            except:
                details['name'] = None
            
            # Fund rating
            try:
                details['rating'] = fund.rating()
            except:
                details['rating'] = None
            
            # Fund category
            try:
                details['category'] = fund.category()
            except:
                details['category'] = None
            
            # NAV
            try:
                details['nav'] = fund.nav()
            except:
                details['nav'] = None
            
            self._log('info', f"Successfully fetched fund details for {fund_isin}")
            
        except Exception as e:
            self._log('error', f"Error fetching fund details for {fund_isin}: {str(e)}")
            details['error'] = str(e)
        
        return details
    
    def get_complete_fund_data(self, fund_isin: str) -> Dict[str, Any]:
        """
        Fetch all available data for a mutual fund
        
        Args:
            fund_isin: ISIN code of the mutual fund
            
        Returns:
            Dictionary containing all fund data
        """
        self._log('info', f"Fetching complete data for fund: {fund_isin}")
        
        return {
            'isin': fund_isin,
            'details': self.get_fund_details(fund_isin),
            'holdings': self.get_fund_holdings(fund_isin),
            'sectors': self.get_sector_allocation(fund_isin),
            'assets': self.get_asset_allocation(fund_isin)
        }
