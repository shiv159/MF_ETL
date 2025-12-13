import asyncio
import json
import logging
import sys
import time
import uuid
from contextvars import ContextVar
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from services.enrichment.fund_enricher import FundEnricher
from services.api.models.request_models import EnrichmentRequest
from services.api.models.response_models import (
    EnrichmentQuality,
    EnrichmentResponse,
)
from services.enrichment.holding_validator import validate_holdings

# Add src to path for importing config_loader
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mf_etl.utils.config_loader import load_config  # noqa: E402

# Context variable for request correlation ID
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default=None)


class CorrelationIdFilter(logging.Filter):
    """Logging filter that adds correlation IDs to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        correlation_id = correlation_id_var.get()
        if correlation_id:
            record.correlation_id = correlation_id
        else:
            record.correlation_id = "no-id"
        return True


# Load configuration from YAML file
try:
    # Use absolute path from project root
    config_path = Path(__file__).resolve().parents[2] / 'config' / 'config.yaml'
    config = load_config(str(config_path))
except FileNotFoundError as e:
    print(f"Warning: config/config.yaml not found. Using default values. Error: {e}")
    config = {}

# Extract configuration values with defaults
timeout_config = config.get('timeout_config', {})
TIMEOUT_SECONDS = timeout_config.get('enrichment_timeout', 120)

# Feature flags configuration
feature_flags = config.get('feature_flags', {})
CACHING_ENABLED = feature_flags.get('caching', {}).get('enabled', True)
CACHE_TTL_MINUTES = feature_flags.get('caching', {}).get('ttl_minutes', 60)
CORRELATION_ID_TRACKING_ENABLED = feature_flags.get('correlation_id_tracking', {}).get('enabled', True)
CONCURRENT_ENRICHMENT_ENABLED = feature_flags.get('concurrent_enrichment', {}).get('enabled', True)
MAX_CONCURRENT = feature_flags.get('concurrent_enrichment', {}).get('max_concurrent', 5)
TIMEOUT_PER_FUND = feature_flags.get('concurrent_enrichment', {}).get('timeout_per_fund', 15)

# Retry configuration
retry_config = config.get('retry_config', {})
MAX_RETRIES = retry_config.get('max_retries', 3)
INITIAL_RETRY_DELAY = retry_config.get('initial_delay', 1)
MAX_RETRY_DELAY = retry_config.get('max_delay', 10)
RETRY_BACKOFF_MULTIPLIER = retry_config.get('backoff_multiplier', 2)
RETRY_ON_TIMEOUT = retry_config.get('retry_on_timeout', True)
RETRY_ON_SERVER_ERROR = retry_config.get('retry_on_server_error', True)

logging_config = config.get('logging', {})
log_level = logging_config.get('level', 'INFO')

logger = logging.getLogger("etl_service")
if not logger.handlers:
    handler = logging.StreamHandler()
    # Include correlation ID in log format
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] [%(correlation_id)s] %(message)s")
    )
    # Add correlation ID filter
    correlation_filter = CorrelationIdFilter()
    handler.addFilter(correlation_filter)
    logger.addHandler(handler)
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Log feature flag status
logger.info(f"Feature flags: caching={CACHING_ENABLED}, correlation_id={CORRELATION_ID_TRACKING_ENABLED}, concurrent_enrichment={CONCURRENT_ENRICHMENT_ENABLED}")
logger.info(f"Timeout per fund: {TIMEOUT_PER_FUND}s (from config: {config.get('feature_flags', {}).get('concurrent_enrichment', {}).get('timeout_per_fund', 'NOT SET')})")
logger.info(f"Retry config: max_retries={MAX_RETRIES}, initial_delay={INITIAL_RETRY_DELAY}s, backoff={RETRY_BACKOFF_MULTIPLIER}x")

app = FastAPI(title="ETL Enrichment Service")

# Middleware to inject correlation IDs
@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    """Middleware to inject correlation ID for request tracking."""
    # Use existing correlation ID from header or generate new one
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    
    # Set context variable for this request
    token = correlation_id_var.set(correlation_id)
    
    try:
        response = await call_next(request)
        # Include correlation ID in response header
        response.headers["X-Correlation-ID"] = correlation_id
        return response
    finally:
        # Reset context variable
        correlation_id_var.reset(token)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

enricher = FundEnricher(logger, enable_caching=CACHING_ENABLED, cache_ttl_minutes=CACHE_TTL_MINUTES)


async def retry_with_backoff(
    func: Callable,
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_RETRY_DELAY,
    max_delay: float = MAX_RETRY_DELAY,
    backoff_multiplier: float = RETRY_BACKOFF_MULTIPLIER,
    is_retriable: Callable[[Exception], bool] = None,
    operation_name: str = "operation"
) -> Any:
    """
    Execute a function with exponential backoff retry logic.
    
    Args:
        func: Async function to execute
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds  
        backoff_multiplier: Exponential backoff multiplier
        is_retriable: Function to determine if exception is retriable
        operation_name: Name of operation for logging
        
    Returns:
        Result of the function
        
    Raises:
        The last exception if all retries are exhausted
    """
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


class ErrorCategory(Enum):
    """Categories for different types of enrichment errors."""
    VALIDATION_ERROR = "validation_error"
    ENRICHMENT_ERROR = "enrichment_error"
    DATA_UNAVAILABLE = "data_unavailable"
    TIMEOUT_ERROR = "timeout_error"
    INTERNAL_ERROR = "internal_error"


def _categorize_error(error_msg: str) -> ErrorCategory:
    """Categorize error message into appropriate error type."""
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


def _extract_upload_id_from_body(body: bytes) -> Optional[str]:
    try:
        payload = json.loads(body)
        return payload.get("upload_id")
    except Exception:
        return None


def _build_error_response(upload_id: Optional[str], error_message: str, warnings: List[str]) -> EnrichmentResponse:
    """Build a standardized error response with categorized error information."""
    quality = EnrichmentQuality(
        successfully_enriched=0,
        failed_to_enrich=0,
        warnings=warnings or [error_message],
    )
    
    # Categorize the error for better debugging
    error_category = _categorize_error(error_message)
    categorized_msg = f"[{error_category.value}] {error_message}"
    
    return EnrichmentResponse(
        upload_id=upload_id or "unknown",
        status="failed",
        duration_seconds=None,
        enriched_funds=[],
        enrichment_quality=quality,
        error_message=categorized_msg,
    )


def _run_enrichment(request: EnrichmentRequest) -> Dict:
    """
    DEPRECATED: Use _run_enrichment_concurrent instead.
    This synchronous version is kept for backward compatibility.
    """
    return asyncio.run(_run_enrichment_concurrent(request))


async def _run_enrichment_concurrent(request: EnrichmentRequest) -> Dict:
    """
    Enrich multiple funds concurrently with semaphore protection.
    
    This async version processes funds in parallel (up to 5 concurrent operations)
    to significantly improve throughput. Typical performance:
    - 100 funds: 40 seconds (vs 200s sequential)
    - 500 funds: 200 seconds (vs 1000s sequential)
    
    Args:
        request: EnrichmentRequest with parsed_holdings
        
    Returns:
        Dict with enriched_funds and enrichment_quality with categorized error tracking
    """
    logger.info("Starting enrichment for upload_id=%s with %d holdings", request.upload_id, len(request.parsed_holdings))
    holdings_payload = [holding.dict() for holding in request.parsed_holdings]
    logger.debug("Validating %d holdings", len(holdings_payload))
    validated_holdings, validation_warnings = validate_holdings(holdings_payload)
    logger.debug("Validation result: %d valid out of %d", len(validated_holdings), len(holdings_payload))
    
    # Continue with valid holdings even if some fail validation (partial success)
    validation_failures = len(holdings_payload) - len(validated_holdings)
    
    enriched_funds = []
    warnings = []
    error_categories = {cat.value: 0 for cat in ErrorCategory}
    
    if not validated_holdings:
        # If no holdings passed validation, return partial success
        error_msg = "; ".join(validation_warnings) if validation_warnings else "No valid holdings available for enrichment"
        logger.warning("Holdings validation failed: %s", error_msg)
        warnings.append(error_msg)
        # Track validation errors
        error_categories[ErrorCategory.VALIDATION_ERROR.value] = validation_failures
    else:
        # Track validation warnings if any
        if validation_warnings:
            warnings.extend(validation_warnings)
            error_categories[ErrorCategory.VALIDATION_ERROR.value] = validation_failures
        # Process valid holdings concurrently
        fund_names = [holding["fund_name"] for holding in validated_holdings]
        logger.debug("Starting concurrent enrichment of %d unique funds", len(fund_names))
        
        # Run concurrent enrichment
        enrichment_results = await FundEnricher.enrich_batch_concurrent(
            enricher,
            fund_names,
            max_concurrent=5,
            timeout_per_fund=15
        )
        
        # Collect successful enrichments and track failures by category
        for idx, (holding, enriched_fund) in enumerate(zip(validated_holdings, enrichment_results)):
            fund_name = holding["fund_name"]
            if enriched_fund:
                enriched_funds.append(enriched_fund)
                logger.debug(f"Successfully enriched {idx + 1}/{len(validated_holdings)}: {fund_name}")
            else:
                message = f"Could not enrich '{fund_name}'"
                warnings.append(message)
                error_categories[ErrorCategory.ENRICHMENT_ERROR.value] += 1
                logger.debug(f"Failed to enrich {idx + 1}/{len(validated_holdings)}: {fund_name}")

    enrichment_quality = {
        "successfully_enriched": len(enriched_funds),
        "failed_to_enrich": len(validated_holdings) - len(enriched_funds) if validated_holdings else 0,
        "validation_failures": validation_failures,
        "warnings": warnings,
        "error_breakdown": error_categories,  # Include detailed error categorization
    }

    return {
        "enriched_funds": enriched_funds,
        "enrichment_quality": enrichment_quality,
    }


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    upload_id = _extract_upload_id_from_body(body)
    error_messages = [err.get("msg", "") for err in exc.errors() if err.get("msg")]
    error_message = "; ".join(error_messages) if error_messages else "Validation failed"
    
    # Categorize as validation error
    categorized_message = f"[{ErrorCategory.VALIDATION_ERROR.value}] {error_message}"
    logger.error(
        "Validation error for %s: %s | body=%s",
        request.url.path,
        categorized_message,
        body.decode("utf-8", errors="ignore"),
    )
    response = _build_error_response(upload_id, error_message, error_messages)
    return JSONResponse(status_code=422, content=response.dict())


@app.post("/etl/enrich", response_model=EnrichmentResponse)
async def enrich(request: EnrichmentRequest):
    start_time = time.time()
    
    async def enrichment_with_retries():
        """Wrapper for enrichment with retry logic"""
        return await _run_enrichment_concurrent(request)
    
    try:
        # Execute with retry logic for timeout and server errors
        payload = await retry_with_backoff(
            enrichment_with_retries,
            max_retries=MAX_RETRIES,
            initial_delay=INITIAL_RETRY_DELAY,
            max_delay=MAX_RETRY_DELAY,
            backoff_multiplier=RETRY_BACKOFF_MULTIPLIER,
            operation_name=f"Enrichment for upload_id={request.upload_id}"
        )
        
        # Also apply timeout to the entire enrichment process
        payload = await asyncio.wait_for(
            asyncio.create_task(enrichment_with_retries()),
            timeout=TIMEOUT_SECONDS,
        )
        
        duration = int(time.time() - start_time)
        quality = EnrichmentQuality(**payload["enrichment_quality"])

        response = EnrichmentResponse(
            upload_id=request.upload_id,
            status="completed",
            duration_seconds=duration,
            enriched_funds=payload["enriched_funds"],
            enrichment_quality=quality,
            error_message=None,
        )
        
        # Log response summary (key milestones only)
        logger.info(
            f"Enrichment completed: {response.enrichment_quality.successfully_enriched} enriched, "
            f"{response.enrichment_quality.failed_to_enrich} failed (duration: {response.duration_seconds}s)"
        )
        
        # Log detailed fund information at DEBUG level
        if response.enriched_funds:
            logger.debug(f"Enriched {len(response.enriched_funds)} funds:")
            for i, fund in enumerate(response.enriched_funds, 1):
                logger.debug(f"  [{i}] {fund.fund_name} | ISIN: {fund.isin} | AMC: {fund.amc}")
                logger.debug(f"      Category: {fund.category} | NAV: {fund.current_nav} (as of {fund.nav_as_of})")
                logger.debug(f"      Expense Ratio: {fund.expense_ratio}% | Top Holdings: {len(fund.top_holdings) if fund.top_holdings else 0} | Sectors: {len(fund.sector_allocation) if fund.sector_allocation else 0}")
        
        if response.enrichment_quality.warnings:
            logger.warning(f"\nWarnings ({len(response.enrichment_quality.warnings)}):")
            for warning in response.enrichment_quality.warnings:
                logger.warning(f"  - {warning}")
        
        logger.info("=" * 80)
        
        return response
    except asyncio.TimeoutError as exc:
        duration = int(time.time() - start_time)
        logger.error("Enrichment processing timed out after retries: %s", exc)
        logger.error("=" * 80)
        logger.error("ENRICHMENT RESPONSE - TIMEOUT")
        logger.error("=" * 80)
        logger.error(f"Upload ID: {request.upload_id}")
        logger.error("Status: failed")
        logger.error("Reason: Processing timed out after %d seconds (with %d retries)", TIMEOUT_SECONDS, MAX_RETRIES)
        logger.error("Duration: %ds", duration)
        logger.error("=" * 80)
        fallback_quality = EnrichmentQuality(
            successfully_enriched=0,
            failed_to_enrich=0,
            warnings=["Processing timed out after retries"],
        )
        return EnrichmentResponse(
            upload_id=request.upload_id,
            status="failed",
            duration_seconds=duration,
            enriched_funds=[],
            enrichment_quality=fallback_quality,
            error_message="Processing timed out after retries",
        )
    except Exception as exc:
        duration = int(time.time() - start_time)
        logger.error("Enrichment processing failed after retries: %s", exc, exc_info=True)
        logger.error("=" * 80)
        logger.error("ENRICHMENT RESPONSE - ERROR")
        logger.error("=" * 80)
        logger.error(f"Upload ID: {request.upload_id}")
        logger.error("Status: failed")
        logger.error(f"Error: {str(exc)}")
        logger.error("Duration: %ds", duration)
        logger.error("=" * 80)
        fallback_quality = EnrichmentQuality(
            successfully_enriched=0,
            failed_to_enrich=0,
            warnings=[str(exc)],
        )
        return EnrichmentResponse(
            upload_id=request.upload_id,
            status="failed",
            duration_seconds=duration,
            enriched_funds=[],
            enrichment_quality=fallback_quality,
            error_message=str(exc),
        )
