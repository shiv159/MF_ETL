"""Shared search and data processing utilities for fund enrichment.

This module contains common utilities used across fetchers, enrichers, and demos
to reduce code duplication and ensure consistent behavior.
"""

import re
from typing import Dict, List, Optional


def safe_float(value, default: float = 0.0) -> float:
    """
    Safely convert a value to float, handling common variations.
    
    Args:
        value: Value to convert (str, int, float, etc.)
        default: Default value if conversion fails
        
    Returns:
        Float value or default
        
    Examples:
        >>> safe_float("10.5")
        10.5
        >>> safe_float("1,234.56")
        1234.56
        >>> safe_float("invalid")
        0.0
        >>> safe_float("invalid", 99.9)
        99.9
    """
    if value is None:
        return default
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Remove common formatting characters
        value = value.strip().replace(',', '')
        try:
            return float(value)
        except (ValueError, AttributeError):
            return default
    
    return default


def safe_numeric(value, target_type=float, default=None):
    """
    Safely convert a value to the target numeric type with type coercion.
    
    Args:
        value: Value to convert
        target_type: Target type (float or int)
        default: Default value if conversion fails
        
    Returns:
        Converted value of target_type or default
        
    Examples:
        >>> safe_numeric("10.5", float)
        10.5
        >>> safe_numeric("10", int)
        10
        >>> safe_numeric("invalid", float, 0.0)
        0.0
    """
    if value is None:
        return default
    
    if isinstance(value, target_type):
        return value
    
    try:
        if target_type == float:
            return safe_float(value, default if default is not None else 0.0)
        elif target_type == int:
            # First convert to float to handle "10.5" -> 10
            float_val = safe_float(value)
            return int(float_val) if float_val != 0.0 else (default if default is not None else 0)
        else:
            return target_type(value)
    except (ValueError, TypeError, AttributeError):
        return default


def normalize_sector_result(sector_data: Optional[Dict]) -> Optional[Dict]:
    """
    Normalize sector allocation data from Morningstar.
    
    Args:
        sector_data: Raw sector data dictionary
        
    Returns:
        Normalized sector data with float values, or None if empty
        
    Examples:
        >>> normalize_sector_result({"Tech": "50.5", "Finance": "49.5"})
        {'Tech': 50.5, 'Finance': 49.5}
        >>> normalize_sector_result({"Tech": "0", "Finance": "0"})
        None
    """
    if not sector_data:
        return None
    
    normalized = {}
    for key, value in sector_data.items():
        numeric_value = safe_float(value)
        if numeric_value > 0:  # Only include non-zero values
            normalized[key] = numeric_value
    
    return normalized if normalized else None


def generate_fallback_search_terms(fund_name: str, scheme_name: str) -> List[str]:
    """
    Generate additional search terms when primary resolution fails.
    
    This function creates progressively simpler name variations to improve
    match rate with external data sources (like Morningstar).
    
    Args:
        fund_name: Original user-provided fund name
        scheme_name: Official AMFI scheme name from mftool
    
    Returns:
        List of alternative search terms to try, ordered by specificity
        
    Examples:
        >>> terms = generate_fallback_search_terms(
        ...     "Motilal Oswal Midcap Direct Growth",
        ...     "Motilal Oswal Midcap Fund-Direct - IDCW Payout/Reinvestment"
        ... )
        >>> len(terms) > 0
        True
        >>> terms[0] == "Motilal Oswal Midcap Direct Growth"
        True
    """
    fallback_terms = []
    
    # 1. Try the user-provided name (they might have used a common abbreviation)
    if fund_name and fund_name.lower() != scheme_name.lower():
        fallback_terms.append(fund_name)
    
    # 2. Try removing plan type suffixes (Direct, Regular, Growth, Dividend, etc.)
    plan_suffixes = r'\s*-\s*(Direct|Regular|GROWTH|DIVIDEND|Growth|Dividend|Monthly|Annual|IDCW|Payout|Reinvestment|Growth|Bonus|Hedged).*$'
    stripped_name = re.sub(plan_suffixes, '', scheme_name, flags=re.IGNORECASE).strip()
    if stripped_name and stripped_name not in fallback_terms:
        fallback_terms.append(stripped_name)
    
    # 3. Try removing parenthetical content (NFO info, etc.)
    cleaned = re.sub(r'\s*\(.*?\)\s*', ' ', scheme_name).strip()
    if cleaned and cleaned not in fallback_terms:
        fallback_terms.append(cleaned)
    
    # 4. Try first N words (core fund name, typically 3 words)
    words = cleaned.split()
    if len(words) > 2:
        core_name = ' '.join(words[:3])  # e.g., "Motilal Oswal Midcap"
        if core_name not in fallback_terms:
            fallback_terms.append(core_name)
    
    # 5. Try just AMC + category (e.g., "Motilal Oswal Midcap")
    words = scheme_name.split()
    if len(words) >= 2:
        amc_category = ' '.join(words[:min(3, len(words))])
        if amc_category not in fallback_terms:
            fallback_terms.append(amc_category)
    
    return fallback_terms
