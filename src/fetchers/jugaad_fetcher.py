"""Fetcher for NSE index data using jugaad-data"""

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, timedelta
import pandas as pd


class JugaadDataFetcher:
    """Fetch NSE index data using jugaad-data library"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize JugaadDataFetcher.
        
        Args:
            logger: Logger instance for logging operations
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def get_nifty_index_data(
        self, 
        index_name: str = "NIFTY MIDCAP 150",
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Fetch NSE index data.
        
        Args:
            index_name: Name of the index (default: NIFTY MIDCAP 150)
            from_date: Start date for data
            to_date: End date for data
            
        Returns:
            DataFrame containing index data
        """
        try:
            from jugaad_data.nse import index_df
            
            # Set default dates if not provided
            if to_date is None:
                to_date = datetime.now()
            if from_date is None:
                from_date = to_date - timedelta(days=30)
            
            self.logger.info(
                f"Fetching {index_name} data from {from_date.date()} to {to_date.date()}"
            )
            
            # Fetch index data
            df = index_df(
                symbol=index_name,
                from_date=from_date,
                to_date=to_date
            )
            
            if df.empty:
                self.logger.warning(f"No data returned for {index_name}")
                return pd.DataFrame()
            
            self.logger.info(
                f"Successfully fetched {len(df)} records for {index_name}"
            )
            return df
            
        except ImportError:
            self.logger.error("jugaad_data not installed or import failed")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching index data: {str(e)}")
            return pd.DataFrame()
    
    def get_stock_data(
        self,
        symbol: str,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        series: str = "EQ"
    ) -> pd.DataFrame:
        """
        Fetch stock data from NSE.
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE')
            from_date: Start date
            to_date: End date
            series: Series type (default: EQ for equity)
            
        Returns:
            DataFrame containing stock data
        """
        try:
            from jugaad_data.nse import stock_df
            
            # Set default dates
            if to_date is None:
                to_date = datetime.now()
            if from_date is None:
                from_date = to_date - timedelta(days=30)
            
            self.logger.info(
                f"Fetching stock data for {symbol} from {from_date.date()} to {to_date.date()}"
            )
            
            df = stock_df(
                symbol=symbol,
                from_date=from_date,
                to_date=to_date,
                series=series
            )
            
            if df.empty:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            self.logger.info(
                f"Successfully fetched {len(df)} records for {symbol}"
            )
            return df
            
        except ImportError:
            self.logger.error("jugaad_data not installed or import failed")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching stock data: {str(e)}")
            return pd.DataFrame()
    
    def get_index_constituents(self, index_name: str = "NIFTY 50") -> List[str]:
        """
        Get list of stocks in an index.
        
        Args:
            index_name: Name of the index
            
        Returns:
            List of stock symbols
        """
        try:
            self.logger.info(f"Fetching constituents for {index_name}")
            
            # Note: This is a placeholder implementation
            # jugaad_data doesn't directly provide constituents list
            # You might need to scrape or use another source
            
            self.logger.warning("Constituent fetching not fully implemented")
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching constituents: {str(e)}")
            return []
