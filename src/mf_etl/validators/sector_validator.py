"""Validator for sector allocation data"""

from typing import Dict, Any, List, Optional
import logging


class SectorValidator:
    """Validate sector breakdown and allocation data"""
    
    def __init__(
        self,
        min_sectors: int = 2,
        total_percentage_tolerance: float = 1.0,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize SectorValidator.
        
        Args:
            min_sectors: Minimum number of sectors expected
            total_percentage_tolerance: Allowed deviation from 100% total
            logger: Logger instance
        """
        self.min_sectors = min_sectors
        self.total_percentage_tolerance = total_percentage_tolerance
        self.logger = logger or logging.getLogger(__name__)
        self.validation_errors: List[str] = []
    
    def validate(self, sector_data: Dict[str, Any]) -> bool:
        """
        Validate sector breakdown data.
        
        Args:
            sector_data: Dictionary containing sector allocations
            
        Returns:
            True if validation passes, False otherwise
        """
        self.validation_errors.clear()
        
        if not sector_data:
            self.validation_errors.append("Sector data is empty")
            self.logger.error("Validation failed: Sector data is empty")
            return False
        
        # Validate minimum number of sectors
        if not self._validate_sector_count(sector_data):
            return False
        
        # Validate sector percentages
        if not self._validate_percentages(sector_data):
            return False
        
        # Validate total allocation
        if not self._validate_total_allocation(sector_data):
            return False
        
        if self.validation_errors:
            self.logger.warning(
                f"Sector validation completed with {len(self.validation_errors)} errors"
            )
            return False
        
        self.logger.info("Sector validation passed successfully")
        return True
    
    def _validate_sector_count(self, sector_data: Dict[str, Any]) -> bool:
        """Validate minimum number of sectors"""
        sector_count = len(sector_data)
        
        if sector_count < self.min_sectors:
            error = (
                f"Insufficient sectors: found {sector_count}, "
                f"minimum required {self.min_sectors}"
            )
            self.validation_errors.append(error)
            self.logger.error(error)
            return False
        
        return True
    
    def _validate_percentages(self, sector_data: Dict[str, Any]) -> bool:
        """Validate individual sector percentages"""
        all_valid = True
        
        for sector, allocation in sector_data.items():
            try:
                # Handle nested dictionaries (yahooquery format)
                if isinstance(allocation, dict):
                    percentage = allocation.get('percentage', 0)
                else:
                    percentage = float(allocation) if allocation is not None else 0
                
                # Check for negative or zero allocations
                if percentage < 0:
                    error = f"Sector '{sector}' has negative allocation: {percentage}%"
                    self.validation_errors.append(error)
                    self.logger.error(error)
                    all_valid = False
                
                # Check for unrealistic allocations (> 100%)
                if percentage > 100:
                    error = f"Sector '{sector}' allocation exceeds 100%: {percentage}%"
                    self.validation_errors.append(error)
                    self.logger.error(error)
                    all_valid = False
                    
            except (ValueError, TypeError) as e:
                error = f"Invalid allocation value for sector '{sector}': {allocation} (error: {e})"
                self.validation_errors.append(error)
                self.logger.error(error)
                all_valid = False
        
        return all_valid
    
    def _validate_total_allocation(self, sector_data: Dict[str, Any]) -> bool:
        """Validate total allocation is close to 100%"""
        total = 0.0
        
        for sector, allocation in sector_data.items():
            if isinstance(allocation, dict):
                percentage = allocation.get('percentage', 0)
            else:
                percentage = float(allocation) if allocation else 0
            
            total += percentage
        
        # Check if total is within tolerance of 100%
        deviation = abs(100.0 - total)
        
        if deviation > self.total_percentage_tolerance:
            error = (
                f"Total allocation {total:.2f}% deviates from 100% "
                f"by {deviation:.2f}% (tolerance: {self.total_percentage_tolerance}%)"
            )
            self.validation_errors.append(error)
            self.logger.warning(error)
            return False
        
        self.logger.info(f"Total allocation: {total:.2f}% (within tolerance)")
        return True
    
    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors"""
        return self.validation_errors.copy()
    
    def get_sector_summary(self, sector_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate summary statistics for sector data.
        
        Args:
            sector_data: Dictionary containing sector allocations
            
        Returns:
            Dictionary with summary statistics
        """
        if not sector_data:
            return {}
        
        percentages = []
        for allocation in sector_data.values():
            if isinstance(allocation, dict):
                percentage = allocation.get('percentage', 0)
            else:
                percentage = float(allocation) if allocation else 0
            percentages.append(percentage)
        
        summary = {
            'total_sectors': len(sector_data),
            'total_allocation': sum(percentages),
            'max_allocation': max(percentages) if percentages else 0,
            'min_allocation': min(percentages) if percentages else 0,
            'avg_allocation': sum(percentages) / len(percentages) if percentages else 0
        }
        
        return summary
