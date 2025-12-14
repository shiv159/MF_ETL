"""Route registration for ETL API service."""

from fastapi import APIRouter, FastAPI
import logging

from services.api.routes.health import router as health_router
from services.api.routes.enrichment import register_enrichment_routes
from services.enrichment.fund_enricher import FundEnricher


def register_routes(app: FastAPI, enricher: FundEnricher, logger: logging.Logger) -> None:
    """Register all API routes to the application.
    
    Includes:
    - Health check endpoints
    - Enrichment endpoints
    
    Args:
        app: FastAPI application instance
        enricher: FundEnricher service instance
        logger: Logger instance
    """
    # Register health routes
    app.include_router(health_router, tags=["health"])
    
    # Register enrichment routes with enricher and logger
    register_enrichment_routes(app, enricher, logger)
