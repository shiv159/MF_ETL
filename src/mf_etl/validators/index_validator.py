"""Validator for index and market data"""

from typing import Dict, Any, List, Optional
import logging
import pandas as pd


class IndexValidator:
    """Validate index and market data"""
    
    def __init__(
        self,
        min_constituents: int = 10,
        max_price_change_percent: float = 20.0,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize IndexValidator.
        
        Args:
            min_constituents: Minimum expected constituents in index
            max_price_change_percent: Maximum acceptable daily price change
            logger: Logger instance
        """
        self.min_constituents = min_constituents
        self.max_price_change_percent = max_price_change_percent
        self.logger = logger or logging.getLogger(__name__)
        self.validation_errors: List[str] = []
    
    def validate_index_data(self, index_df: pd.DataFrame) -> bool:
        """
        Validate index DataFrame.
        
        Args:
            index_df: DataFrame containing index data
            
        Returns:
            True if validation passes, False otherwise
        """
        self.validation_errors.clear()
        
        if index_df.empty:
            self.validation_errors.append("Index data is empty")
            self.logger.error("Validation failed: Index data is empty")
            return False
        
        # Validate required columns
        if not self._validate_columns(index_df):
            return False
        
        # Validate data completeness
        if not self._validate_completeness(index_df):
            return False
        
        # Validate price changes
        if not self._validate_price_changes(index_df):
            return False
        
        if self.validation_errors:
            self.logger.warning(
                f"Index validation completed with {len(self.validation_errors)} errors"
            )
            return False
        
        self.logger.info("Index validation passed successfully")
        return True
    
    def _validate_columns(self, df: pd.DataFrame) -> bool:
        """Validate required columns exist"""
        required_columns = ['OPEN', 'HIGH', 'LOW', 'CLOSE']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error = f"Missing required columns: {', '.join(missing_columns)}"
            self.validation_errors.append(error)
            self.logger.error(error)
            return False
        
        return True
    
    def _validate_completeness(self, df: pd.DataFrame) -> bool:
        """Validate data completeness (no missing values)"""
        all_valid = True
        
        for column in ['OPEN', 'HIGH', 'LOW', 'CLOSE']:
            if column in df.columns:
                null_count = df[column].isna().sum()
                if null_count > 0:
                    error = f"Column '{column}' has {null_count} missing values"
                    self.validation_errors.append(error)
                    self.logger.warning(error)
                    all_valid = False
        
        return all_valid
    
    def _validate_price_changes(self, df: pd.DataFrame) -> bool:
        """Validate price changes are within acceptable range"""
        if 'CLOSE' not in df.columns or len(df) < 2:
            return True
        
        # Calculate daily percentage changes
        df_sorted = df.sort_index()
        price_changes = df_sorted['CLOSE'].pct_change() * 100
        
        # Find extreme changes
        extreme_changes = price_changes[
            abs(price_changes) > self.max_price_change_percent
        ]
        
        if not extreme_changes.empty:
            for idx, change in extreme_changes.items():
                warning = (
                    f"Extreme price change detected on {idx}: "
                    f"{change:.2f}% (threshold: {self.max_price_change_percent}%)"
                )
                self.validation_errors.append(warning)
                self.logger.warning(warning)
        
        return True  # Don't fail validation, just log warnings
    
    def validate_constituents(self, constituents: List[str]) -> bool:
        """
        Validate index constituents list.
        
        Args:
            constituents: List of constituent symbols
            
        Returns:
            True if validation passes, False otherwise
        """
        self.validation_errors.clear()
        
        if not constituents:
            self.validation_errors.append("Constituents list is empty")
            self.logger.error("Validation failed: Constituents list is empty")
            return False
        
        if len(constituents) < self.min_constituents:
            error = (
                f"Insufficient constituents: found {len(constituents)}, "
                f"minimum required {self.min_constituents}"
            )
            self.validation_errors.append(error)
            self.logger.error(error)
            return False
        
        # Check for duplicates
        duplicates = set([x for x in constituents if constituents.count(x) > 1])
        if duplicates:
            warning = f"Duplicate constituents found: {', '.join(duplicates)}"
            self.validation_errors.append(warning)
            self.logger.warning(warning)
        
        self.logger.info(f"Validated {len(constituents)} constituents")
        return True
    
    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors"""
        return self.validation_errors.copy()
    
    def get_index_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for index data.
        
        Args:
            df: DataFrame containing index data
            
        Returns:
            Dictionary with summary statistics
        """
        if df.empty or 'CLOSE' not in df.columns:
            return {}
        
        df_sorted = df.sort_index()
        
        summary = {
            'total_records': len(df),
            'date_range': {
                'start': str(df_sorted.index[0]) if len(df_sorted) > 0 else None,
                'end': str(df_sorted.index[-1]) if len(df_sorted) > 0 else None
            },
            'close_price': {
                'min': float(df['CLOSE'].min()),
                'max': float(df['CLOSE'].max()),
                'mean': float(df['CLOSE'].mean()),
                'latest': float(df_sorted['CLOSE'].iloc[-1]) if len(df_sorted) > 0 else None
            }
        }
        
        if len(df_sorted) > 1:
            price_change = (
                (df_sorted['CLOSE'].iloc[-1] - df_sorted['CLOSE'].iloc[0]) /
                df_sorted['CLOSE'].iloc[0] * 100
            )
            summary['total_return_percent'] = float(price_change)
        
        return summary
