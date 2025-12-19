"""Helper to fetch normalized metadata from mstarpy for a given ISIN.

This module encapsulates the optional dependency on `mstarpy` so callers
can gracefully continue if the package is not installed.
"""
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


def _nested_get(d: Dict[str, Any], *path: str):
    """Safely traverse nested dicts and return a leaf value or None.

    Example: _nested_get(raw, 'fundSize', 'properties', 'currency', 'value')
    """
    cur = d or {}
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def get_mstar_metadata(isin: str) -> Optional[Dict[str, Any]]:
    """Fetch and normalize a small set of metadata for the fund identified by ISIN.

    Returns a dict with a fixed set of keys or None if the fetch failed or
    `mstarpy` is not available. Failures are logged for debugging.
    """
    try:
        import mstarpy as ms
    except ImportError:
        logger.debug("mstarpy not available; skipping mstar metadata for ISIN=%s", isin)
        return None
    except Exception as exc:
        # Unexpected import-time error; log it for debugging
        logger.debug("Unexpected error importing mstarpy for ISIN=%s: %s", isin, exc, exc_info=True)
        return None

    try:
        funds = ms.Funds(isin)
        # Request the fields we care about
        raw = funds.dataPoint(field=[
            "name",
            "alpha",
            "fundSize",
            "beta",
            "sharpeRatio",
            "standardDeviation",
            "isIndexFund",
        ])

        # Normalize into a simple dict using the safe accessor
        meta = {
            "name": _nested_get(raw, "name", "value"),
            "alpha": _nested_get(raw, "alpha", "value"),
            "fund_size": _nested_get(raw, "fundSize", "value"),
            "fund_size_currency": _nested_get(raw, "fundSize", "properties", "currency", "value"),
            "fund_size_as_of": _nested_get(raw, "fundSize", "properties", "asOfDate", "value"),
            "beta": _nested_get(raw, "beta", "value"),
            "sharpe_ratio": _nested_get(raw, "sharpeRatio", "value"),
            "stdev": _nested_get(raw, "standardDeviation", "value"),
            "is_index_fund": _nested_get(raw, "isIndexFund", "value"),
        }

        return meta
    except Exception as exc:
        # Log the underlying exception so maintainers can debug unexpected failures
        logger.debug("Failed to fetch/normalize mstar metadata for ISIN=%s: %s", isin, exc, exc_info=True)
        return None
