"""Error handling and categorization for ETL API service."""

from enum import Enum
from typing import Optional, List
from services.api.models.response_models import EnrichmentResponse, EnrichmentQuality


class ErrorCategory(Enum):
    """Categories for different types of enrichment errors."""
    VALIDATION_ERROR = "validation_error"
    ENRICHMENT_ERROR = "enrichment_error"
    DATA_UNAVAILABLE = "data_unavailable"
    TIMEOUT_ERROR = "timeout_error"
    INTERNAL_ERROR = "internal_error"


def categorize_error(error_msg: str) -> ErrorCategory:
    """Categorize error message into appropriate error type.
    
    Analyzes the error message and returns the most appropriate
    ErrorCategory for logging and response handling.
    
    Args:
        error_msg: Error message to categorize
        
    Returns:
        ErrorCategory enum value
    """
    error_lower = error_msg.lower()
    
    if "validation" in error_lower or "invalid" in error_lower:
        return ErrorCategory.VALIDATION_ERROR
    elif "timeout" in error_lower or "exceeded" in error_lower:
        return ErrorCategory.TIMEOUT_ERROR
    elif "not found" in error_lower or "unavailable" in error_lower or "no data" in error_lower:
        return ErrorCategory.DATA_UNAVAILABLE
    elif "could not enrich" in error_lower or "skipping enrichment" in error_lower:
        return ErrorCategory.ENRICHMENT_ERROR
    else:
        return ErrorCategory.INTERNAL_ERROR


def build_error_response(
    upload_id: Optional[str],
    error_message: str,
    warnings: Optional[List[str]] = None
) -> EnrichmentResponse:
    """Build a standardized error response with categorized error information.
    
    Args:
        upload_id: Upload identifier from request
        error_message: Primary error message
        warnings: List of warning messages
        
    Returns:
        EnrichmentResponse configured as an error response
    """
    quality = EnrichmentQuality(
        successfully_enriched=0,
        failed_to_enrich=0,
        warnings=warnings or [error_message],
    )
    
    # Categorize the error for better debugging
    error_category = categorize_error(error_message)
    categorized_msg = f"[{error_category.value}] {error_message}"
    
    return EnrichmentResponse(
        upload_id=upload_id or "unknown",
        status="failed",
        duration_seconds=None,
        enriched_funds=[],
        enrichment_quality=quality,
        error_message=categorized_msg,
    )
