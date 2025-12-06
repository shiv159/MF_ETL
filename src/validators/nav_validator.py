"""Validator for NAV (Net Asset Value) data"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
from pydantic import BaseModel, Field, validator


class NAVData(BaseModel):
    """Model for NAV data validation"""
    scheme_code: str = Field(..., description="Mutual fund scheme code")
    nav: float = Field(..., description="Net Asset Value")
    date: str = Field(..., description="NAV date")
    scheme_name: Optional[str] = Field(None, description="Scheme name")
    
    @validator('nav')
    def validate_nav(cls, v):
        """Validate NAV is positive"""
        if v <= 0:
            raise ValueError(f"NAV must be positive, got {v}")
        return v


class NAVValidator:
    """Validate mutual fund NAV data"""
    
    def __init__(
        self,
        min_value: float = 0.01,
        max_value: float = 100000,
        max_age_days: int = 7,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize NAVValidator.
        
        Args:
            min_value: Minimum acceptable NAV value
            max_value: Maximum acceptable NAV value
            max_age_days: Maximum age of NAV data in days
            logger: Logger instance
        """
        self.min_value = min_value
        self.max_value = max_value
        self.max_age_days = max_age_days
        self.logger = logger or logging.getLogger(__name__)
        self.validation_errors: List[str] = []
    
    def validate(self, nav_data: Dict[str, Any]) -> bool:
        """
        Validate NAV data against configured rules.
        
        Args:
            nav_data: Dictionary containing NAV information
            
        Returns:
            True if validation passes, False otherwise
        """
        self.validation_errors.clear()
        
        if not nav_data:
            self.validation_errors.append("NAV data is empty")
            self.logger.error("Validation failed: NAV data is empty")
            return False
        
        # Validate using Pydantic model
        try:
            # Try to create model from data
            nav_model = NAVData(
                scheme_code=nav_data.get('scheme_code', ''),
                nav=float(nav_data.get('nav', 0)),
                date=nav_data.get('date', ''),
                scheme_name=nav_data.get('scheme_name', '')
            )
        except Exception as e:
            self.validation_errors.append(f"Data structure validation failed: {str(e)}")
            self.logger.error(f"NAV data structure validation failed: {str(e)}")
            return False
        
        # Validate NAV value range
        if not self._validate_nav_range(nav_model.nav):
            return False
        
        # Validate NAV date
        if not self._validate_nav_date(nav_model.date):
            return False
        
        if self.validation_errors:
            self.logger.warning(f"NAV validation completed with {len(self.validation_errors)} errors")
            return False
        
        self.logger.info("NAV validation passed successfully")
        return True
    
    def _validate_nav_range(self, nav: float) -> bool:
        """Validate NAV is within acceptable range"""
        if nav < self.min_value:
            error = f"NAV {nav} is below minimum threshold {self.min_value}"
            self.validation_errors.append(error)
            self.logger.error(error)
            return False
        
        if nav > self.max_value:
            error = f"NAV {nav} exceeds maximum threshold {self.max_value}"
            self.validation_errors.append(error)
            self.logger.error(error)
            return False
        
        return True
    
    def _validate_nav_date(self, date_str: str) -> bool:
        """Validate NAV date is recent enough"""
        try:
            # Try parsing date string (format: DD-MM-YYYY)
            try:
                nav_date = datetime.strptime(date_str, '%d-%m-%Y')
            except ValueError:
                # Try alternative format (DD-Mon-YYYY)
                try:
                    nav_date = datetime.strptime(date_str, '%d-%b-%Y')
                except ValueError:
                    # If date_str is empty, skip date validation
                    if not date_str or date_str.strip() == '':
                        self.logger.warning("NAV date is empty, skipping age validation")
                        return True
                    raise
            
            current_date = datetime.now()
            age_days = (current_date - nav_date).days
            
            if age_days > self.max_age_days:
                error = f"NAV data is {age_days} days old, exceeds {self.max_age_days} day threshold"
                self.validation_errors.append(error)
                self.logger.warning(error)
                return False
            
            return True
            
        except ValueError as e:
            error = f"Invalid date format: {date_str}. Expected DD-MM-YYYY or DD-Mon-YYYY"
            self.validation_errors.append(error)
            self.logger.error(error)
            return False
    
    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors"""
        return self.validation_errors.copy()
    
    def validate_batch(self, nav_data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate multiple NAV records.
        
        Args:
            nav_data_list: List of NAV data dictionaries
            
        Returns:
            Dictionary with validation results and statistics
        """
        results = {
            'total': len(nav_data_list),
            'passed': 0,
            'failed': 0,
            'errors': []
        }
        
        for idx, nav_data in enumerate(nav_data_list):
            if self.validate(nav_data):
                results['passed'] += 1
            else:
                results['failed'] += 1
                results['errors'].append({
                    'index': idx,
                    'scheme_code': nav_data.get('scheme_code', 'Unknown'),
                    'errors': self.get_validation_errors()
                })
        
        self.logger.info(
            f"Batch validation complete: {results['passed']}/{results['total']} passed"
        )
        
        return results
