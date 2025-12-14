"""Helper to fetch normalized metadata from mstarpy for a given ISIN.

This module encapsulates the optional dependency on `mstarpy` so callers
can gracefully continue if the package is not installed.
"""
from typing import Optional, Dict, Any


def get_mstar_metadata(isin: str) -> Optional[Dict[str, Any]]:
    """Fetch and normalize a small set of metadata for the fund identified by ISIN.

    Returns a dict with a fixed set of keys or None if the fetch failed or
    `mstarpy` is not available.
    """
    try:
        import mstarpy as ms
    except Exception:
        # mstarpy not installed or import error; caller should handle None
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

        # Normalize into a simple dict
        meta = {
            "name": raw.get("name", {}).get("value"),
            "alpha": raw.get("alpha", {}).get("value"),
            "fund_size": raw.get("fundSize", {}).get("value"),
            "fund_size_currency": raw.get("fundSize", {}).get("properties", {}).get("currency", {}).get("value"),
            "fund_size_as_of": raw.get("fundSize", {}).get("properties", {}).get("asOfDate", {}).get("value"),
            "beta": raw.get("beta", {}).get("value"),
            "sharpe_ratio": raw.get("sharpeRatio", {}).get("value"),
            "stdev": raw.get("standardDeviation", {}).get("value"),
            "is_index_fund": raw.get("isIndexFund", {}).get("value"),
        }

        return meta
    except Exception:
        return None
