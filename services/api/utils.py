"""Utility functions for ETL API service."""

import asyncio
import json
import logging
from typing import Callable, Optional, Any

from services.api.config import (
    MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    RETRY_BACKOFF_MULTIPLIER,
    RETRY_ON_TIMEOUT,
    RETRY_ON_SERVER_ERROR,
)


async def retry_with_backoff(
    func: Callable,
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_RETRY_DELAY,
    max_delay: float = MAX_RETRY_DELAY,
    backoff_multiplier: float = RETRY_BACKOFF_MULTIPLIER,
    is_retriable: Optional[Callable[[Exception], bool]] = None,
    operation_name: str = "operation",
    logger: Optional[logging.Logger] = None,
) -> Any:
    """Execute a function with exponential backoff retry logic.
    
    Attempts to execute an async function with exponential backoff
    retry on transient failures (timeouts, server errors). The delay
    between retries grows exponentially up to max_delay.
    
    Args:
        func: Async function to execute
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay cap in seconds
        backoff_multiplier: Exponential backoff multiplier
        is_retriable: Function to determine if exception is retriable
        operation_name: Name of operation for logging
        logger: Logger instance for logging retry attempts
        
    Returns:
        Result of the function
        
    Raises:
        The last exception if all retries are exhausted
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    if is_retriable is None:
        def is_retriable(exc: Exception) -> bool:
            """Default: retry on timeout and server errors"""
            error_str = str(exc).lower()
            if RETRY_ON_TIMEOUT and ("timeout" in error_str or "exceeded" in error_str):
                return True
            if RETRY_ON_SERVER_ERROR and ("500" in error_str or "server error" in error_str):
                return True
            return False
    
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except Exception as exc:
            last_exception = exc
            
            # Check if we should retry
            if attempt < max_retries and is_retriable(exc):
                logger.warning(
                    f"{operation_name} attempt {attempt + 1}/{max_retries + 1} failed (retriable error), "
                    f"retrying in {delay:.1f}s: {str(exc)[:100]}"
                )
                await asyncio.sleep(delay)
                # Exponential backoff with max delay cap
                delay = min(delay * backoff_multiplier, max_delay)
            else:
                # Not retriable or out of retries
                if attempt >= max_retries:
                    logger.error(
                        f"{operation_name} failed after {max_retries + 1} attempts: {str(exc)}"
                    )
                raise
    
    # Should not reach here, but raise last exception if we do
    raise last_exception


def extract_upload_id_from_body(body: bytes) -> Optional[str]:
    """Extract upload_id from request body JSON.
    
    Safely extracts the upload_id field from a JSON request body,
    returning None if parsing fails.
    
    Args:
        body: Raw request body bytes
        
    Returns:
        Upload ID string or None if not found/parseable
    """
    try:
        payload = json.loads(body)
        return payload.get("upload_id")
    except Exception:
        return None
