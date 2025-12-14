"""Exception handlers for ETL API service."""

import logging
from typing import Optional

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from services.api.errors import build_error_response, ErrorCategory
from services.api.utils import extract_upload_id_from_body


def register_exception_handlers(app):
    """Register global exception handlers to FastAPI app.
    
    Args:
        app: FastAPI application instance
    """
    logger = logging.getLogger("etl_service")
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle request validation errors with proper categorization."""
        body = await request.body()
        upload_id = extract_upload_id_from_body(body)
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
        response = build_error_response(upload_id, error_message, error_messages)
        return JSONResponse(status_code=422, content=response.dict())
