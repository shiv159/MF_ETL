"""Helper to fetch normalized metadata from mstarpy for a given ISIN.

This module encapsulates the optional dependency on `mstarpy` so callers
can gracefully continue if the package is not installed.
"""
from typing import Optional, Dict, Any
import logging
import asyncio

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


async def get_mstar_metadata(isin: str) -> Optional[Dict[str, Any]]:
    """Fetch and normalize a small set of metadata for the fund identified by ISIN.

    This async function will offload blocking `mstarpy` calls to a thread pool
    via `asyncio.to_thread` so it can be awaited without blocking the event loop.

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
        # Request the fields we care about in a thread to avoid blocking
        raw = await asyncio.to_thread(
            funds.dataPoint,
            field=[
                "name",
                "alpha",
                "fundSize",
                "beta",
                "sharpeRatio",
                "standardDeviation",
                "isIndexFund",
            ],
        )

        # Try to fetch risk/volatility data; this is best-effort and should not
        # fail the entire metadata fetch if unavailable. Offload to a thread.
        risk_raw = None
        try:
            risk_raw = await asyncio.to_thread(funds.riskVolatility)
        except AttributeError:
            logger.debug("mstarpy Funds object has no riskVolatility method for ISIN=%s", isin)
        except Exception as exc:
            logger.debug("Failed to fetch riskVolatility for ISIN=%s: %s", isin, exc, exc_info=True)

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

        # Normalize risk/volatility payload into a separate nested object if available
        if risk_raw:
            risk_meta = {
                "fund_name": _nested_get(risk_raw, "fundName"),
                "category_name": _nested_get(risk_raw, "categoryName"),
                "index_name": _nested_get(risk_raw, "indexName"),
                "calculation_benchmark": _nested_get(risk_raw, "calculationBenchmark"),
                "extended_performance_data": _nested_get(risk_raw, "extendedPerformanceData") or {},
                "fund_risk_volatility": _nested_get(risk_raw, "fundRiskVolatility") or {},
                "category_risk_volatility": _nested_get(risk_raw, "categoryRiskVolatility") or {},
                "index_risk_volatility": _nested_get(risk_raw, "indexRiskVolatility") or {},
                "currency": _nested_get(risk_raw, "cur"),
            }
        else:
            risk_meta = None

        meta["risk_volatility"] = risk_meta

        return meta
    except Exception as exc:
        # Log the underlying exception so maintainers can debug unexpected failures
        logger.debug("Failed to fetch/normalize mstar metadata for ISIN=%s: %s", isin, exc, exc_info=True)
        return None
