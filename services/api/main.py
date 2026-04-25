"""ETL Enrichment Service - FastAPI Application Entry Point

A scalable, modular fund enrichment service that provides:
- Multi-source fund data resolution (MFAPI.in + MstarPy)
- Concurrent batch enrichment (5 concurrent operations)
- Comprehensive error handling and categorization
- Request correlation ID tracking
- Structured logging with context propagation
- Automatic retry with exponential backoff

Architecture:
    - config.py: Configuration management
    - dependencies.py: Service initialization
    - middleware/correlation_id.py: Request tracking
    - errors.py: Error categorization
    - utils.py: Utility functions
    - routes/: Endpoint definitions
    - exceptions.py: Global exception handlers

Typical usage:
    uvicorn services.api.main:app --host 0.0.0.0 --port 8081
"""

import os
import sys
import subprocess
import atexit

if sys.platform.startswith("linux"):
    try:
        # Start Xvfb for headless Chrome/mstarpy support
        _xvfb = subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1280x720x24"])
        os.environ["DISPLAY"] = ":99"
        atexit.register(_xvfb.terminate)
    except FileNotFoundError:
        pass

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.api.config import CORRELATION_ID_TRACKING_ENABLED
from services.api.dependencies import (
    initialize_logger,
    initialize_enricher,
    log_initialization_info,
)
from services.api.middleware.correlation_id import add_correlation_id_middleware
from services.api.routes import register_routes
from services.api.exceptions import register_exception_handlers


def get_allowed_origins() -> list[str]:
    """Read comma-separated CORS origins from environment."""
    raw_origins = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:8080")
    return [origin.strip() for origin in raw_origins.split(",") if origin.strip()]


# Initialize logger and services
logger = initialize_logger(name="etl_service")
enricher = initialize_enricher(logger=logger)

# Log initialization configuration
log_initialization_info(logger)

# Create FastAPI application
app = FastAPI(
    title="ETL Enrichment Service",
    description="Fund enrichment service with multi-source data integration",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add correlation ID middleware if tracking is enabled
if CORRELATION_ID_TRACKING_ENABLED:
    app.middleware("http")(add_correlation_id_middleware)

# Register exception handlers
register_exception_handlers(app)

# Register all routes (health, enrichment)
register_routes(app, enricher, logger)

# Lifecycle logging
@app.on_event("startup")
async def startup_event():
    """Log application startup."""
    logger.info("ETL Enrichment Service started")
    logger.info("=" * 80)


@app.on_event("shutdown")
async def shutdown_event():
    """Log application shutdown and cleanup."""
    logger.info("=" * 80)
    logger.info("ETL Enrichment Service shutting down")
    enricher.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8081")),
        log_level="info"
    )
