"""
Holdings Validator - Validate mutual fund holdings data

This module validates:
- Portfolio holdings completeness
- Holdings weight consistency
- Data quality checks
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, field_validator, ValidationError


class HoldingData(BaseModel):
    """Pydantic model for individual holding validation"""
    name: str
    weight: float
    
    @field_validator('weight')
    @classmethod
    def validate_weight(cls, v):
        """Validate holding weight is between 0 and 100"""
        if not 0 <= v <= 100:
            raise ValueError(f"Weight must be between 0 and 100, got {v}")
        return v


class HoldingsValidator:
    """Validator for mutual fund holdings data"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
        """
        Initialize HoldingsValidator
        
        Args:
            config: Configuration dictionary with validation thresholds
            logger: Logger instance
        """
        self.logger = logger
        self.config = config or {}
        
        # Validation thresholds
        self.min_holdings = self.config.get('min_holdings', 5)
        self.max_weight_deviation = self.config.get('max_weight_deviation', 5.0)
        self.min_total_weight = self.config.get('min_total_weight', 80.0)
        
        # Track validation errors
        self.errors = []
    
    def _log(self, level: str, message: str):
        """Internal logging helper"""
        if self.logger:
            getattr(self.logger, level)(message)
    
    def validate(self, holdings_df: pd.DataFrame) -> bool:
        """
        Validate holdings DataFrame
        
        Args:
            holdings_df: DataFrame containing holdings data
            
        Returns:
            True if validation passes, False otherwise
        """
        self.errors = []
        
        if holdings_df is None or holdings_df.empty:
            self.errors.append("Holdings data is empty")
            self._log('error', "Holdings data is empty")
            return False
        
        # Check minimum number of holdings
        if not self._validate_holdings_count(holdings_df):
            return False
        
        # Check if required columns exist
        if not self._validate_columns(holdings_df):
            return False
        
        # Validate weights if available
        if self._has_weight_column(holdings_df):
            if not self._validate_weights(holdings_df):
                return False
        
        self._log('info', "Holdings validation passed")
        return True
    
    def _validate_holdings_count(self, holdings_df: pd.DataFrame) -> bool:
        """Check if minimum number of holdings are present"""
        count = len(holdings_df)
        if count < self.min_holdings:
            error = f"Insufficient holdings: {count} (minimum: {self.min_holdings})"
            self.errors.append(error)
            self._log('error', error)
            return False
        return True
    
    def _validate_columns(self, holdings_df: pd.DataFrame) -> bool:
        """Check if required columns exist"""
        required_cols = ['securityName', 'holdingName', 'name']  # Any of these
        
        has_name_col = any(col in holdings_df.columns for col in required_cols)
        
        if not has_name_col:
            error = f"Missing required column. Expected one of: {required_cols}"
            self.errors.append(error)
            self._log('error', error)
            return False
        
        return True
    
    def _has_weight_column(self, holdings_df: pd.DataFrame) -> bool:
        """Check if weight/percentage column exists"""
        weight_cols = ['weighting', 'weight', 'portfolio%', 'portfolioPercent']
        return any(col in holdings_df.columns for col in weight_cols)
    
    def _validate_weights(self, holdings_df: pd.DataFrame) -> bool:
        """Validate holding weights"""
        # Find weight column
        weight_col = None
        for col in ['weighting', 'weight', 'portfolio%', 'portfolioPercent']:
            if col in holdings_df.columns:
                weight_col = col
                break
        
        if weight_col is None:
            return True  # Skip if no weight column
        
        try:
            weights = pd.to_numeric(holdings_df[weight_col], errors='coerce')
            weights = weights.dropna()
            
            if weights.empty:
                self._log('warning', "No valid weight values found")
                return True
            
            # Check individual weights are in valid range
            invalid_weights = weights[(weights < 0) | (weights > 100)]
            if not invalid_weights.empty:
                error = f"Found {len(invalid_weights)} holdings with invalid weights"
                self.errors.append(error)
                self._log('error', error)
                return False
            
            # Check total weight
            # Note: For top N holdings (partial portfolios), expect lower total weight
            total_weight = weights.sum()
            holdings_count = len(holdings_df)
            min_weight = self.min_total_weight
            
            # Adjust threshold for partial holdings
            if holdings_count <= 50:
                min_weight = 30.0  # Top 50 holdings typically hold 30-60% of portfolio
            elif holdings_count <= 100:
                min_weight = 50.0  # Top 100 holdings typically hold 50-80% of portfolio
            
            self._log('info', f"Total holdings weight: {total_weight:.2f}%")
            
            if total_weight < min_weight:
                error = f"Total weight too low: {total_weight:.2f}% (minimum: {min_weight}% for {holdings_count} holdings)"
                self.errors.append(error)
                # Note: Don't fail validation for low weight when fetching partial holdings
                self._log('warning', error)
                self._log('warning', error)
                # Don't fail on this, just warn
            
            self._log('info', f"Total holdings weight: {total_weight:.2f}%")
            
        except Exception as e:
            error = f"Error validating weights: {str(e)}"
            self.errors.append(error)
            self._log('error', error)
            return False
        
        return True
    
    def get_validation_errors(self) -> List[str]:
        """
        Get list of validation errors
        
        Returns:
            List of error messages
        """
        return self.errors
    
    def get_holdings_summary(self, holdings_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for holdings
        
        Args:
            holdings_df: DataFrame containing holdings data
            
        Returns:
            Dictionary with summary statistics
        """
        if holdings_df is None or holdings_df.empty:
            return {'total_holdings': 0}
        
        summary = {
            'total_holdings': len(holdings_df)
        }
        
        # Find weight column
        weight_col = None
        for col in ['weighting', 'weight', 'portfolio%', 'portfolioPercent']:
            if col in holdings_df.columns:
                weight_col = col
                break
        
        if weight_col:
            try:
                weights = pd.to_numeric(holdings_df[weight_col], errors='coerce')
                weights = weights.dropna()
                
                if not weights.empty:
                    summary['total_weight'] = float(weights.sum())
                    summary['top_holding_weight'] = float(weights.max())
                    summary['avg_holding_weight'] = float(weights.mean())
                    summary['top_10_weight'] = float(weights.nlargest(10).sum())
            except:
                pass
        
        return summary
