"""Dependencies and service initialization for ETL API."""

import logging
from typing import Optional

from services.enrichment.fund_enricher import FundEnricher
from services.api.config import (
    LOG_LEVEL,
    CORRELATION_ID_TRACKING_ENABLED,
    MFAPI_CONFIG,
)
from services.api.middleware.correlation_id import CorrelationIdFilter
from services.api.config import get_retry_config


def initialize_logger(name: str = "etl_service") -> logging.Logger:
    """Initialize and configure the application logger.
    
    Sets up structured logging with:
    - Correlation ID injection for request tracing
    - Configured log level from config.yaml
    - Formatted output with timestamp, level, name, and correlation ID
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        # Include correlation ID in log format
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] [%(correlation_id)s] %(message)s")
        )
        
        # Add correlation ID filter if tracking is enabled
        if CORRELATION_ID_TRACKING_ENABLED:
            correlation_filter = CorrelationIdFilter()
            handler.addFilter(correlation_filter)
        
        logger.addHandler(handler)
    
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    return logger


def initialize_enricher(logger: Optional[logging.Logger] = None) -> FundEnricher:
    """Initialize the FundEnricher service with configured dependencies.
    
    Creates and configures the FundEnricher with:
    - Logger instance
    - MFAPI configuration for fund resolution
    - Retry configuration for resilience
    
    Args:
        logger: Logger instance (creates new one if not provided)
        
    Returns:
        Initialized FundEnricher instance
    """
    if logger is None:
        logger = initialize_logger()
    
    enricher = FundEnricher(
        logger=logger,
        mfapi_config=MFAPI_CONFIG,
        retry_config=get_retry_config()
    )
    return enricher


def log_initialization_info(logger: logging.Logger) -> None:
    """Log initialization configuration and feature flags.
    
    Logs the loaded configuration for debugging and verification.
    """
    from services.api.config import (
        TIMEOUT_SECONDS,
        CORRELATION_ID_TRACKING_ENABLED,
        CONCURRENT_ENRICHMENT_ENABLED,
        MAX_CONCURRENT,
        TIMEOUT_PER_FUND,
        MAX_RETRIES,
        INITIAL_RETRY_DELAY,
        RETRY_BACKOFF_MULTIPLIER,
        MFAPI_TIMEOUT,
        MFAPI_MAX_RETRIES,
        MFAPI_FUZZY_THRESHOLD,
    )
    
    logger.info(f"Feature flags: correlation_id={CORRELATION_ID_TRACKING_ENABLED}, concurrent_enrichment={CONCURRENT_ENRICHMENT_ENABLED}")
    logger.info(f"Global enrichment timeout: {TIMEOUT_SECONDS}s")
    logger.info(f"Timeout per fund: {TIMEOUT_PER_FUND}s")
    logger.info(f"Retry config: max_retries={MAX_RETRIES}, initial_delay={INITIAL_RETRY_DELAY}s, backoff={RETRY_BACKOFF_MULTIPLIER}x")
    logger.info(f"MFAPI config: timeout={MFAPI_TIMEOUT}s, max_retries={MFAPI_MAX_RETRIES}, fuzzy_threshold={MFAPI_FUZZY_THRESHOLD}%")
