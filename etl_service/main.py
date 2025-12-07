import asyncio
import logging
import time
from typing import Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from etl_service.enrichment.fund_enricher import FundEnricher
from etl_service.models.request_models import EnrichmentRequest
from etl_service.models.response_models import (
    EnrichmentQuality,
    EnrichmentResponse,
)
from etl_service.validators.holding_validator import validate_holdings

logger = logging.getLogger("etl_service")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

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


def _run_enrichment(request: EnrichmentRequest) -> Dict:
    logger.info("Start enrichment for upload_id=%s", request.upload_id)
    holdings_payload = [holding.dict() for holding in request.parsed_holdings]
    validated_holdings, validation_warnings = validate_holdings(holdings_payload)
    if not validated_holdings:
        raise ValueError("No valid holdings available for enrichment")

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

        return EnrichmentResponse(
            upload_id=request.upload_id,
            status="completed",
            duration_seconds=duration,
            enriched_funds=payload["enriched_funds"],
            enrichment_quality=quality,
            error_message=None,
        )
    except asyncio.TimeoutError as exc:
        logger.error("Enrichment processing timed out: %s", exc)
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
