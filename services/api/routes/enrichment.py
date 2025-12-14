"""Enrichment endpoints for ETL API service."""

import asyncio
import time
import logging
from typing import Optional, Dict, Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.enrichment.fund_enricher import FundEnricher
from services.enrichment.holding_validator import validate_holdings
from services.api.models.request_models import EnrichmentRequest
from services.api.models.response_models import EnrichmentResponse, EnrichmentQuality
from services.api.config import (
    TIMEOUT_SECONDS,
    MAX_CONCURRENT,
    TIMEOUT_PER_FUND,
    MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    RETRY_BACKOFF_MULTIPLIER,
)
from services.api.errors import (
    ErrorCategory,
    categorize_error,
    build_error_response,
)
from services.api.utils import (
    retry_with_backoff,
    extract_upload_id_from_body,
)
from services.enrichment.mstarpy_helper import get_mstar_metadata

router = APIRouter()


async def _run_enrichment_concurrent(
    enricher: FundEnricher,
    request: EnrichmentRequest,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Enrich multiple funds concurrently with semaphore protection.
    
    This async function processes funds in parallel (up to 5 concurrent operations)
    to significantly improve throughput. Typical performance:
    - 100 funds: 40 seconds (vs 200s sequential)
    - 500 funds: 200 seconds (vs 1000s sequential)
    
    Args:
        enricher: FundEnricher instance
        request: EnrichmentRequest with parsed_holdings
        logger: Logger instance
        
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
            max_concurrent=MAX_CONCURRENT,
            timeout_per_fund=TIMEOUT_PER_FUND
        )
        
        # Collect successful enrichments and track failures by category
        for idx, (holding, enriched_fund) in enumerate(zip(validated_holdings, enrichment_results)):
            fund_name = holding["fund_name"]
            if enriched_fund:
                # Attach MstarPy metadata when ISIN is available (non-blocking call is fine here)
                try:
                    if getattr(enriched_fund, 'isin', None):
                        meta = get_mstar_metadata(enriched_fund.isin)
                        # If payload is a Pydantic model, set attribute directly
                        try:
                            setattr(enriched_fund, 'mstarpy_metadata', meta)
                        except Exception:
                            # If enriched_fund is plain dict, merge
                            if isinstance(enriched_fund, dict):
                                enriched_fund['mstarpy_metadata'] = meta
                except Exception:
                    logger.debug(f"Failed to fetch MstarPy metadata for ISIN: {getattr(enriched_fund, 'isin', None)}")

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


def register_enrichment_routes(app_router: APIRouter, enricher: FundEnricher, logger: logging.Logger):
    """Register enrichment endpoints to router.
    
    Args:
        app_router: FastAPI router instance
        enricher: FundEnricher service instance
        logger: Logger instance
    """
    
    @app_router.post("/etl/enrich", response_model=EnrichmentResponse)
    async def enrich(request: EnrichmentRequest):
        """Enrich funds with multi-source data (MFAPI + MstarPy).
        
        Handles multi-fund enrichment with:
        - Holdings validation
        - Fund name resolution via MFAPI
        - Concurrent data fetching (NAV, holdings, sectors)
        - Automatic retry on transient failures
        - Comprehensive error categorization
        
        Args:
            request: EnrichmentRequest with upload_id and parsed_holdings
            
        Returns:
            EnrichmentResponse with enriched funds and quality metrics
        """
        start_time = time.time()
        
        async def enrichment_with_retries():
            """Wrapper for enrichment with retry logic"""
            return await _run_enrichment_concurrent(enricher, request, logger)
        
        try:
            # Execute with retry logic for timeout and server errors
            payload = await retry_with_backoff(
                enrichment_with_retries,
                max_retries=MAX_RETRIES,
                initial_delay=INITIAL_RETRY_DELAY,
                max_delay=MAX_RETRY_DELAY,
                backoff_multiplier=RETRY_BACKOFF_MULTIPLIER,
                operation_name=f"Enrichment for upload_id={request.upload_id}",
                logger=logger
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
