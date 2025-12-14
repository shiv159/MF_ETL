"""Health check endpoints for ETL API service."""

from fastapi import APIRouter

from services.api.config import (
    MFAPI_CONFIG,
    MFAPI_TIMEOUT,
    MFAPI_MAX_RETRIES,
    MFAPI_FUZZY_THRESHOLD,
)

router = APIRouter()


@router.get("/health")
async def health():
    """Health check endpoint that reports on API availability.
    
    Returns:
        - status: "healthy" if API is up
        - mfapi: Status of MFAPI.in connection and configuration
    """
    response = {
        "status": "healthy",
        "mfapi": {
            "timeout": MFAPI_TIMEOUT,
            "max_retries": MFAPI_MAX_RETRIES,
            "fuzzy_threshold": MFAPI_FUZZY_THRESHOLD,
            "base_url": MFAPI_CONFIG.get('base_url', 'https://api.mfapi.in')
        }
    }
    
    return response
