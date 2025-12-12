import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

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

# Load configuration from YAML file
try:
    config = load_config('config/config.yaml')
except FileNotFoundError:
    print("Warning: config/config.yaml not found. Using default values.")
    config = {}

# Extract configuration values with defaults
timeout_config = config.get('timeout_config', {})
TIMEOUT_SECONDS = timeout_config.get('enrichment_timeout', 120)

logging_config = config.get('logging', {})
log_level = logging_config.get('level', 'INFO')

logger = logging.getLogger("etl_service")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(handler)
logger.setLevel(getattr(logging, log_level, logging.INFO))

app = FastAPI(title="ETL Enrichment Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

enricher = FundEnricher(logger)
TIMEOUT_SECONDS = 120


def _extract_upload_id_from_body(body: bytes) -> Optional[str]:
    try:
        payload = json.loads(body)
        return payload.get("upload_id")
    except Exception:
        return None


def _build_error_response(upload_id: Optional[str], error_message: str, warnings: List[str]) -> EnrichmentResponse:
    quality = EnrichmentQuality(
        successfully_enriched=0,
        failed_to_enrich=0,
        warnings=warnings or [error_message],
    )
    return EnrichmentResponse(
        upload_id=upload_id or "unknown",
        status="failed",
        duration_seconds=None,
        enriched_funds=[],
        enrichment_quality=quality,
        error_message=error_message,
    )


def _run_enrichment(request: EnrichmentRequest) -> Dict:
    logger.info("Start enrichment for upload_id=%s", request.upload_id)
    holdings_payload = [holding.dict() for holding in request.parsed_holdings]
    logger.info("Validating %d holdings: %s", len(holdings_payload), holdings_payload)
    validated_holdings, validation_warnings = validate_holdings(holdings_payload)
    logger.info("Validation result: %d valid, warnings: %s", len(validated_holdings), validation_warnings)
    if not validated_holdings:
        error_msg = "; ".join(validation_warnings) if validation_warnings else "No valid holdings available for enrichment"
        logger.error("Holdings validation failed: %s", error_msg)
        raise HTTPException(status_code=400, detail=error_msg)

    enriched_funds = []
    warnings = validation_warnings.copy()
    for holding in validated_holdings:
        fund_name = holding["fund_name"]
        try:
            fund_data = enricher.enrich(fund_name)
            if fund_data:
                enriched_funds.append(fund_data)
            else:
                message = f"Could not enrich '{fund_name}'"
                warnings.append(message)
                logger.warning(message)
        except Exception as exc:
            message = f"Enrichment failed for '{fund_name}': {exc}"
            warnings.append(message)
            logger.warning(message)

    enrichment_quality = {
        "successfully_enriched": len(enriched_funds),
        "failed_to_enrich": len(validated_holdings) - len(enriched_funds),
        "warnings": warnings,
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
    logger.error(
        "Validation error for %s: %s | body=%s",
        request.url.path,
        error_message,
        body.decode("utf-8", errors="ignore"),
    )
    response = _build_error_response(upload_id, error_message, error_messages)
    return JSONResponse(status_code=422, content=response.dict())


@app.post("/etl/enrich", response_model=EnrichmentResponse)
async def enrich(request: EnrichmentRequest):
    start_time = time.time()
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(_run_enrichment, request),
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
        
        # Log response details
        logger.info("=" * 80)
        logger.info("ENRICHMENT RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Upload ID: {response.upload_id}")
        logger.info(f"Status: {response.status}")
        logger.info(f"Duration: {response.duration_seconds}s")
        logger.info(f"Successfully enriched: {response.enrichment_quality.successfully_enriched}")
        logger.info(f"Failed to enrich: {response.enrichment_quality.failed_to_enrich}")
        
        if response.enriched_funds:
            logger.info(f"\nEnriched {len(response.enriched_funds)} funds:")
            for i, fund in enumerate(response.enriched_funds, 1):
                logger.info(f"  [{i}] {fund.fund_name}")
                logger.info(f"      ISIN: {fund.isin}")
                logger.info(f"      AMC: {fund.amc}")
                logger.info(f"      Category: {fund.category}")
                logger.info(f"      Current NAV: {fund.current_nav}")
                logger.info(f"      NAV As Of: {fund.nav_as_of}")
                logger.info(f"      Expense Ratio: {fund.expense_ratio}%")
                if fund.top_holdings:
                    logger.info(f"      Top Holdings: {len(fund.top_holdings)} holdings")
                if fund.sector_allocation:
                    logger.info(f"      Sectors: {len(fund.sector_allocation)} sectors")
        
        if response.enrichment_quality.warnings:
            logger.warning(f"\nWarnings ({len(response.enrichment_quality.warnings)}):")
            for warning in response.enrichment_quality.warnings:
                logger.warning(f"  - {warning}")
        
        logger.info("=" * 80)
        
        return response
    except asyncio.TimeoutError as exc:
        logger.error("Enrichment processing timed out: %s", exc)
        logger.error("=" * 80)
        logger.error("ENRICHMENT RESPONSE - TIMEOUT")
        logger.error("=" * 80)
        logger.error(f"Upload ID: {request.upload_id}")
        logger.error("Status: failed")
        logger.error("Reason: Processing timed out after %d seconds", TIMEOUT_SECONDS)
        logger.error("=" * 80)
        fallback_quality = EnrichmentQuality(
            successfully_enriched=0,
            failed_to_enrich=0,
            warnings=["Processing timed out"],
        )
        return EnrichmentResponse(
            upload_id=request.upload_id,
            status="failed",
            duration_seconds=None,
            enriched_funds=[],
            enrichment_quality=fallback_quality,
            error_message="Processing timed out",
        )
    except Exception as exc:
        logger.error("Enrichment processing failed: %s", exc, exc_info=True)
        logger.error("=" * 80)
        logger.error("ENRICHMENT RESPONSE - ERROR")
        logger.error("=" * 80)
        logger.error(f"Upload ID: {request.upload_id}")
        logger.error("Status: failed")
        logger.error(f"Error: {str(exc)}")
        logger.error("=" * 80)
        fallback_quality = EnrichmentQuality(
            successfully_enriched=0,
            failed_to_enrich=0,
            warnings=[str(exc)],
        )
        return EnrichmentResponse(
            upload_id=request.upload_id,
            status="failed",
            duration_seconds=None,
            enriched_funds=[],
            enrichment_quality=fallback_quality,
            error_message=str(exc),
        )
