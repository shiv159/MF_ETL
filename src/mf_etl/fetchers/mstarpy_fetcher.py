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

from src.mf_etl.utils.mstarpy_compat import get_mstarpy


class MstarPyFetcher:
    """Fetcher for mutual fund data using mstarpy (Morningstar)"""
    
    def __init__(self, logger=None):
        """
        Initialize MstarPyFetcher
        
        Args:
            logger: Logger instance for logging operations
        """
        self.logger = logger
        self._fund_cache = {}
        self._mstarpy = get_mstarpy()
    
    def _log(self, level: str, message: str):
        """Internal logging helper"""
        if self.logger:
            getattr(self.logger, level)(message)
    
    def get_fund(self, term: str) -> Optional[Any]:
        """
        Get a fund object directly from mstarpy
        
        Args:
            term: Search term (fund name, ISIN, or ticker)
            
        Returns:
            mstarpy.Funds object or None if not found
        """
        if term in self._fund_cache:
            return self._fund_cache[term]
            
        try:
            self._log('debug', f"Looking up fund: {term}")
            fund = self._mstarpy.Funds(term=term)
            self._fund_cache[term] = fund
            self._log('debug', f"Successfully created Funds object for: {term}")
            return fund
        except Exception as e:
            self._log('error', f"Error creating Funds object for '{term}': {str(e)}")
            return None
    
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
            fund = self.get_fund(fund_isin)
            if fund is None:
                return None
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
            fund = self.get_fund(fund_isin)
            if fund is None:
                return None
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
            fund = self.get_fund(fund_isin)
            if fund is None:
                return None
            
            assets = None
            if hasattr(fund, 'asset_allocation'):
                assets = fund.asset_allocation()
            elif hasattr(fund, 'allocationMap'):
                assets = fund.allocationMap()
            elif hasattr(fund, 'allocationWeighting'):
                assets = fund.allocationWeighting()

            if assets is not None:
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
            fund = self.get_fund(fund_isin)
            if fund is None:
                raise ValueError(f"Could not initialize fund object for {fund_isin}")
            
            # Fund name
            try:
                if hasattr(fund, 'name'):
                    name_attr = getattr(fund, 'name')
                    details['name'] = name_attr() if callable(name_attr) else name_attr
                else:
                    details['name'] = fund.dataPoint('name')[0].get('name') if getattr(fund, 'dataPoint', None) else None
            except:
                details['name'] = None

            # Fund rating
            try:
                if hasattr(fund, 'rating') and callable(fund.rating):
                    details['rating'] = fund.rating()
                elif hasattr(fund, 'starRatingFundDesc'):
                    rating_data = fund.starRatingFundDesc()
                    details['rating'] = rating_data.get('starRatingFund') if isinstance(rating_data, dict) else None
                elif hasattr(fund, 'dataPoint'):
                    points = fund.dataPoint(['starRatingM255'])
                    details['rating'] = points[0].get('starRatingM255') if points else None
            except:
                details['rating'] = None

            # Fund category
            try:
                if hasattr(fund, 'category') and callable(fund.category):
                    details['category'] = fund.category()
                elif hasattr(fund, 'dataPoint'):
                    points = fund.dataPoint(['categoryName'])
                    details['category'] = points[0].get('categoryName') if points else None
            except:
                details['category'] = None

            # NAV
            try:
                if hasattr(fund, 'nav') and callable(fund.nav):
                    import inspect
                    import datetime
                    sig = inspect.signature(fund.nav)
                    if 'start_date' in sig.parameters:
                        # v9 signature requires date range
                        end_date = datetime.datetime.today()
                        start_date = end_date - datetime.timedelta(days=7)
                        nav_data = fund.nav(start_date=start_date, end_date=end_date)
                        if nav_data and isinstance(nav_data, list):
                            # The latest NAV is typically the last element
                            details['nav'] = nav_data[-1].get('nav')
                        else:
                            details['nav'] = None
                    else:
                        details['nav'] = fund.nav()
            except:
                details['nav'] = None
            
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
