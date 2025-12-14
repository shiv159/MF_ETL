"""Middleware components for ETL API service.

Handles request correlation ID tracking and logging context propagation.
"""

import logging
import uuid
from contextvars import ContextVar
from typing import Callable

from fastapi import Request

# Context variable for request correlation ID
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default=None)


class CorrelationIdFilter(logging.Filter):
    """Logging filter that adds correlation IDs to log records.
    
    Injects the current request's correlation ID into all log records,
    enabling cross-service request tracing.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation_id attribute to log record."""
        correlation_id = correlation_id_var.get()
        record.correlation_id = correlation_id if correlation_id else "no-id"
        return True


def get_correlation_id() -> str:
    """Get current request correlation ID."""
    return correlation_id_var.get() or "no-id"


def set_correlation_id(correlation_id: str) -> object:
    """Set correlation ID for current context and return reset token."""
    return correlation_id_var.set(correlation_id)


def reset_correlation_id(token: object) -> None:
    """Reset correlation ID using provided token."""
    correlation_id_var.reset(token)


async def add_correlation_id_middleware(request: Request, call_next: Callable):
    """Middleware to inject correlation ID for request tracking.
    
    Uses existing correlation ID from request header (X-Correlation-ID)
    or generates a new UUID. Propagates the ID through the entire
    request lifecycle and includes it in response headers.
    """
    # Use existing correlation ID from header or generate new one
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    
    # Set context variable for this request
    token = set_correlation_id(correlation_id)
    
    try:
        response = await call_next(request)
        # Include correlation ID in response header
        response.headers["X-Correlation-ID"] = correlation_id
        return response
    finally:
        # Reset context variable
        reset_correlation_id(token)
