"""Fetcher for fund holdings and sector data using yahooquery"""

from typing import Dict, Any, Optional, List
import logging
from yahooquery import Ticker


class YahooQueryFetcher:
    """Fetch fund holdings and sector breakdown using yahooquery"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize YahooQueryFetcher.
        
        Args:
            logger: Logger instance for logging operations
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def get_fund_holdings(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch fund holdings for a given ticker symbol.
        
        Args:
            symbol: Ticker symbol (e.g., 'RELIANCE.NS')
            
        Returns:
            Dictionary containing fund holdings data
        """
        try:
            self.logger.info(f"Fetching fund holdings for: {symbol}")
            ticker = Ticker(symbol)
            holdings = ticker.fund_holding_info
            
            if not holdings or isinstance(holdings, str):
                self.logger.warning(f"No holdings data for {symbol}")
                return {}
            
            # Extract data for the symbol
            holdings_data = holdings.get(symbol, {})
            self.logger.info(f"Successfully fetched holdings for {symbol}")
            return holdings_data
            
        except Exception as e:
            self.logger.error(f"Error fetching holdings for {symbol}: {str(e)}")
            return {}
    
    def get_sector_breakdown(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch sector breakdown for a fund or stock.
        
        Args:
            symbol: Ticker symbol
            
        Returns:
            Dictionary containing sector allocation data
        """
        try:
            self.logger.info(f"Fetching sector breakdown for: {symbol}")
            ticker = Ticker(symbol)
            
            # Try to get fund sector weightings
            sector_data = ticker.fund_sector_weightings
            
            if not sector_data or isinstance(sector_data, str):
                self.logger.warning(f"No sector data for {symbol}")
                return {}
            
            # Extract data for the symbol
            breakdown = sector_data.get(symbol, {})
            self.logger.info(f"Successfully fetched sector breakdown for {symbol}")
            return breakdown
            
        except Exception as e:
            self.logger.error(f"Error fetching sector breakdown for {symbol}: {str(e)}")
            return {}
    
    def get_fund_profile(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch fund profile information.
        
        Args:
            symbol: Ticker symbol
            
        Returns:
            Dictionary containing fund profile
        """
        try:
            self.logger.info(f"Fetching fund profile for: {symbol}")
            ticker = Ticker(symbol)
            profile = ticker.fund_profile
            
            if not profile or isinstance(profile, str):
                self.logger.warning(f"No profile data for {symbol}")
                return {}
            
            profile_data = profile.get(symbol, {})
            self.logger.info(f"Successfully fetched profile for {symbol}")
            return profile_data
            
        except Exception as e:
            self.logger.error(f"Error fetching profile for {symbol}: {str(e)}")
            return {}
    
    def get_summary_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch summary data for a symbol.
        
        Args:
            symbol: Ticker symbol
            
        Returns:
            Dictionary containing summary information
        """
        try:
            self.logger.info(f"Fetching summary data for: {symbol}")
            ticker = Ticker(symbol)
            summary = ticker.summary_detail
            
            if not summary or isinstance(summary, str):
                self.logger.warning(f"No summary data for {symbol}")
                return {}
            
            summary_data = summary.get(symbol, {})
            self.logger.info(f"Successfully fetched summary for {symbol}")
            return summary_data
            
        except Exception as e:
            self.logger.error(f"Error fetching summary for {symbol}: {str(e)}")
            return {}
