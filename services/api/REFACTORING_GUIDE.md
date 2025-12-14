# ETL Enrichment Service - API Refactoring Guide

## Overview

The FastAPI application has been refactored into a clean, modular architecture that separates concerns and improves maintainability. Each module has a specific responsibility.

## Directory Structure

```
services/api/
├── main.py                    # Application entry point - simplified to 70 lines
├── config.py                  # Configuration loading and constants
├── dependencies.py            # Service initialization and logging setup
├── errors.py                  # Error categorization and handling
├── utils.py                   # Utility functions (retry logic, helpers)
├── exceptions.py              # Global exception handlers
├── models/
│   ├── request_models.py      # (unchanged) EnrichmentRequest
│   └── response_models.py     # (unchanged) EnrichmentResponse
├── middleware/
│   ├── __init__.py            # Middleware exports
│   └── correlation_id.py      # Request tracking middleware
└── routes/
    ├── __init__.py            # Route registration
    ├── health.py              # Health check endpoints
    └── enrichment.py          # Enrichment endpoints (/etl/enrich)
```

## Module Responsibilities

### `main.py` (Entry Point - 70 lines)
**Responsibility**: Application initialization and startup

**Key Functions**:
- Initializes logger and services
- Creates FastAPI app instance
- Registers middleware and exception handlers
- Registers all routes
- Handles startup/shutdown events

**Before**: 498 lines of mixed concerns
**After**: 70 lines of clean orchestration

### `config.py` (Configuration)
**Responsibility**: Load and expose all configuration

**Key Features**:
- Loads YAML configuration once at import time
- Exposes all settings as module-level constants
- Provides helper function `get_retry_config()`
- Default values for missing config sections

**Constants Exported**:
- `TIMEOUT_SECONDS`, `MAX_RETRIES`, `MAX_CONCURRENT`
- `MFAPI_CONFIG`, `MFAPI_TIMEOUT`, `MFAPI_FUZZY_THRESHOLD`
- `CORRELATION_ID_TRACKING_ENABLED`, `CONCURRENT_ENRICHMENT_ENABLED`
- `LOG_LEVEL`, `RETRY_CONFIG`

### `dependencies.py` (Service Initialization)
**Responsibility**: Initialize logger and enricher with proper configuration

**Key Functions**:
- `initialize_logger()`: Sets up structured logging with correlation ID support
- `initialize_enricher()`: Creates FundEnricher with config
- `log_initialization_info()`: Logs configuration for debugging

**Benefits**:
- Centralized service initialization
- Single source of truth for logger setup
- Easy to test and mock

### `errors.py` (Error Handling)
**Responsibility**: Categorize and format errors consistently

**Key Components**:
- `ErrorCategory` enum: 5 error types (VALIDATION, ENRICHMENT, DATA_UNAVAILABLE, TIMEOUT, INTERNAL)
- `categorize_error()`: Analyzes error message and returns category
- `build_error_response()`: Creates standardized error response

**Error Categories**:
- `validation_error`: Invalid input data
- `enrichment_error`: Failed fund enrichment
- `data_unavailable`: Missing data from sources
- `timeout_error`: Request timeout
- `internal_error`: Unexpected errors

### `utils.py` (Utilities)
**Responsibility**: Reusable utility functions

**Key Functions**:
- `retry_with_backoff()`: Exponential backoff retry logic (async)
- `extract_upload_id_from_body()`: Safe JSON parsing helper

**Features**:
- Configurable retry parameters
- Custom retriable condition function
- Automatic backoff cap at max_delay

### `middleware/correlation_id.py` (Request Tracking)
**Responsibility**: Inject correlation IDs for request tracing

**Key Components**:
- `CorrelationIdFilter`: Logging filter to add correlation_id to records
- `add_correlation_id_middleware()`: FastAPI middleware
- Context variable management: `get_correlation_id()`, `set_correlation_id()`, `reset_correlation_id()`

**Benefits**:
- Cross-service request tracing
- Correlation ID in all log records
- X-Correlation-ID header in responses

### `exceptions.py` (Exception Handlers)
**Responsibility**: Register and handle global exceptions

**Key Functions**:
- `register_exception_handlers()`: Register handlers to app
- Custom validation error handler with categorization

**Features**:
- Extract upload_id from request body
- Categorize validation errors
- Structured error logging

### `routes/__init__.py` (Route Registration)
**Responsibility**: Register all routes to the application

**Key Functions**:
- `register_routes()`: Includes health and enrichment routes

**Benefits**:
- Single entry point for route registration
- Easy to add new route modules
- Clear dependency injection

### `routes/health.py` (Health Endpoint)
**Responsibility**: Health check endpoints

**Endpoints**:
- `GET /health`: Returns API status and MFAPI configuration

**Response**:
```json
{
  "status": "healthy",
  "mfapi": {
    "timeout": 10,
    "max_retries": 3,
    "fuzzy_threshold": 85,
    "base_url": "https://api.mfapi.in"
  }
}
```

### `routes/enrichment.py` (Enrichment Endpoints)
**Responsibility**: Fund enrichment logic

**Key Components**:
- `_run_enrichment_concurrent()`: Multi-source enrichment with concurrency
- `register_enrichment_routes()`: Register routes to router
- `enrich()`: POST /etl/enrich endpoint

**Features**:
- Holdings validation
- Concurrent fund enrichment (max 5 parallel)
- Retry with exponential backoff
- Categorized error tracking
- Detailed logging at DEBUG level

## Data Flow

```
Request (/etl/enrich)
    ↓
Correlation ID Middleware
├─ Extract/Generate X-Correlation-ID
└─ Set context for request lifecycle
    ↓
Request Validation
├─ FastAPI validates input schema
└─ Exception handler categorizes errors
    ↓
Enrichment Endpoint
├─ Holdings Validation (holding_validator.py)
├─ Fund Name Resolution (MFAPIFetcher)
├─ Concurrent Enrichment (up to 5 concurrent)
│  ├─ NAV from MFAPI
│  ├─ Holdings from MstarPy
│  └─ Sectors from MstarPy
├─ Retry Logic (exponential backoff)
└─ Error Categorization (5 categories)
    ↓
Response (/etl/enrich)
├─ EnrichmentResponse model
├─ X-Correlation-ID header
└─ Structured enrichment_quality
```

## Configuration File (config/config.yaml)

```yaml
timeout_config:
  enrichment_timeout: 120        # Total enrichment timeout

feature_flags:
  correlation_id_tracking:
    enabled: true               # Enable request tracing
  concurrent_enrichment:
    enabled: true               # Enable concurrent processing
    max_concurrent: 5           # Max parallel enrichments
    timeout_per_fund: 15        # Per-fund timeout

retry_config:
  max_retries: 3               # Max retry attempts
  initial_delay: 1             # Initial backoff (seconds)
  max_delay: 10                # Max backoff cap
  backoff_multiplier: 2        # Exponential multiplier
  retry_on_timeout: true       # Retry on timeout
  retry_on_server_error: true  # Retry on 5xx

mfapi:
  timeout: 10                  # MFAPI request timeout
  max_retries: 3               # MFAPI retry attempts
  fuzzy_threshold: 85          # RapidFuzz matching threshold
  base_url: https://api.mfapi.in

logging:
  level: INFO                  # Log level (DEBUG, INFO, WARNING, ERROR)
```

## Running the Service

```bash
# Development
uvicorn services.api.main:app --host 0.0.0.0 --port 8081 --reload

# Production
gunicorn -w 4 -k uvicorn.workers.UvicornWorker services.api.main:app --bind 0.0.0.0:8081
```

## Example Requests

### Health Check
```bash
curl http://localhost:8081/health
```

### Enrichment Request
```bash
curl -X POST http://localhost:8081/etl/enrich \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: req-123" \
  -d '{
    "upload_id": "upload-456",
    "parsed_holdings": [
      {"fund_name": "HDFC Mid Cap Fund", "isin": null, "quantity": 100}
    ]
  }'
```

## Key Improvements

### Before Refactoring
- 498 lines in main.py
- Mixed concerns (config, logging, middleware, routes, error handling)
- Hard to test individual components
- Difficult to add new features
- No clear separation of responsibilities

### After Refactoring
- 70 lines in main.py (86% reduction)
- Clear module responsibilities
- Each module can be tested independently
- Easy to add new routes/middleware/handlers
- Clear code organization and navigation

### Performance
- Same functionality, no performance impact
- Cleaner code = easier debugging
- Better logging with correlation IDs
- Structured error categorization

## Extension Points

### Adding a New Endpoint
1. Create new file in `routes/` (e.g., `routes/analytics.py`)
2. Define endpoint and import necessary dependencies
3. Call `register_new_routes()` in `routes/__init__.py`
4. Import in `main.py`

```python
# routes/analytics.py
router = APIRouter()

@router.get("/analytics/summary")
async def get_summary():
    return {"summary": "data"}

# routes/__init__.py
from services.api.routes.analytics import router as analytics_router
app.include_router(analytics_router, tags=["analytics"])
```

### Adding New Middleware
1. Create in `middleware/` (e.g., `middleware/logging.py`)
2. Define middleware function
3. Register in `main.py`

```python
# middleware/logging.py
async def log_requests(request: Request, call_next):
    # Custom logging logic
    return await call_next(request)

# main.py
from services.api.middleware.logging import log_requests
app.middleware("http")(log_requests)
```

### Adding New Error Category
1. Add to `ErrorCategory` enum in `errors.py`
2. Update `categorize_error()` logic
3. Handle in exception handlers

## Testing

Each module can be tested independently:

```python
# Test config loading
from services.api.config import MFAPI_TIMEOUT
assert MFAPI_TIMEOUT == 10

# Test error categorization
from services.api.errors import categorize_error, ErrorCategory
assert categorize_error("timeout") == ErrorCategory.TIMEOUT_ERROR

# Test retry logic
from services.api.utils import retry_with_backoff
result = await retry_with_backoff(async_function)

# Test middleware
from services.api.middleware import get_correlation_id, set_correlation_id
token = set_correlation_id("test-id")
assert get_correlation_id() == "test-id"
```

## Debugging

### Enable Debug Logging
```yaml
# config/config.yaml
logging:
  level: DEBUG
```

### View Correlation ID
All logs include correlation ID:
```
2025-12-14 10:30:45 INFO [etl_service] [req-123] Starting enrichment...
```

### Check Configuration
```bash
curl http://localhost:8081/health
# Returns current MFAPI and timeout config
```

## Summary

The refactored API is now:
- **Organized**: Clear module structure with single responsibilities
- **Maintainable**: Easy to understand and modify
- **Testable**: Each component can be tested independently
- **Extensible**: Simple to add new routes, middleware, or error handlers
- **Observable**: Comprehensive logging with correlation IDs
- **Resilient**: Robust error handling and retry logic
