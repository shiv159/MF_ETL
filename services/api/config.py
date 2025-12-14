"""Configuration management for ETL API service.

Loads configuration from config/config.yaml and exposes settings
as module-level constants.
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any

# Add src to path for importing config_loader
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mf_etl.utils.config_loader import load_config  # noqa: E402


def _load_configuration() -> Dict[str, Any]:
    """Load configuration from YAML file with proper error handling."""
    try:
        # Use absolute path from project root
        config_path = Path(__file__).resolve().parents[2] / 'config' / 'config.yaml'
        return load_config(str(config_path))
    except FileNotFoundError as e:
        print(f"Warning: config/config.yaml not found. Using default values. Error: {e}")
        return {}


# Load configuration once at module import time
_config = _load_configuration()


# Extract timeout configuration
_timeout_config = _config.get('timeout_config', {})
TIMEOUT_SECONDS = _timeout_config.get('enrichment_timeout', 120)

# Extract feature flags
_feature_flags = _config.get('feature_flags', {})
CORRELATION_ID_TRACKING_ENABLED = _feature_flags.get('correlation_id_tracking', {}).get('enabled', True)
CONCURRENT_ENRICHMENT_ENABLED = _feature_flags.get('concurrent_enrichment', {}).get('enabled', True)
MAX_CONCURRENT = _feature_flags.get('concurrent_enrichment', {}).get('max_concurrent', 5)
TIMEOUT_PER_FUND = _feature_flags.get('concurrent_enrichment', {}).get('timeout_per_fund', 15)

# Extract retry configuration
_retry_config = _config.get('retry_config', {})
MAX_RETRIES = _retry_config.get('max_retries', 3)
INITIAL_RETRY_DELAY = _retry_config.get('initial_delay', 1)
MAX_RETRY_DELAY = _retry_config.get('max_delay', 10)
RETRY_BACKOFF_MULTIPLIER = _retry_config.get('backoff_multiplier', 2)
RETRY_ON_TIMEOUT = _retry_config.get('retry_on_timeout', True)
RETRY_ON_SERVER_ERROR = _retry_config.get('retry_on_server_error', True)

# Extract MFAPI configuration (primary fund resolution source)
MFAPI_CONFIG = _config.get('mfapi', {})
MFAPI_TIMEOUT = MFAPI_CONFIG.get('timeout', 10)
MFAPI_MAX_RETRIES = MFAPI_CONFIG.get('max_retries', 3)
MFAPI_FUZZY_THRESHOLD = MFAPI_CONFIG.get('fuzzy_threshold', 85)

# Extract logging configuration
_logging_config = _config.get('logging', {})
LOG_LEVEL = _logging_config.get('level', 'INFO')

# For backward compatibility
RETRY_CONFIG = _retry_config

def get_retry_config() -> Dict[str, Any]:
    """Get retry configuration dictionary."""
    return {
        'max_retries': MAX_RETRIES,
        'initial_delay': INITIAL_RETRY_DELAY,
        'max_delay': MAX_RETRY_DELAY,
        'backoff_multiplier': RETRY_BACKOFF_MULTIPLIER,
        'retry_on_timeout': RETRY_ON_TIMEOUT,
        'retry_on_server_error': RETRY_ON_SERVER_ERROR,
    }
