"""Shared search and data processing utilities for fund enrichment.

This module contains common utilities used across fetchers, enrichers, and demos
to reduce code duplication and ensure consistent behavior.
"""

import re
from typing import Dict, List, Optional, Tuple
from rapidfuzz import fuzz, process


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

def fuzzy_score(query: str, target: str, logger=None) -> int:
    """
    Calculate fuzzy match score between query and target using token_set_ratio.
    
    Token set ratio ignores word order and duplicate words, making it ideal for
    Indian fund names where word order varies (e.g., "HDFC Mid Cap" vs "Mid Cap HDFC Fund").
    
    Args:
        query: Query string (e.g., user fund name)
        target: Target string (e.g., AMFI scheme name)
        logger: Optional logger for debug output
        
    Returns:
        Score 0-100 (higher = better match)
        
    Examples:
        >>> fuzzy_score("HDFC Mid Cap", "HDFC Mid Cap Fund")
        95
        >>> fuzzy_score("HDFC Midcapp", "HDFC Midcap")  # Typo tolerance
        92
    """
    if not query or not target:
        return 0
    
    score = fuzz.token_set_ratio(query.lower().strip(), target.lower().strip())
    
    if logger:
        logger.info(f"[FUZZY] token_set_ratio('{query}', '{target}') = {score}%")
    
    return score


def fuzzy_best_match(query: str, choices: List[str], threshold: int = 85, logger=None) -> Optional[Tuple[str, int]]:
    """
    Find the best fuzzy match for a query from a list of choices.
    
    Uses token_set_ratio for robustness to word-order variation and abbreviations.
    
    Args:
        query: Query string (e.g., official AMFI scheme name)
        choices: List of candidates to match against (e.g., ISIN descriptions)
        threshold: Minimum score required (0-100); None returned if no match above this
        logger: Optional logger for debug output
        
    Returns:
        (matched_string, score) if match found above threshold, else None
        
    Examples:
        >>> result = fuzzy_best_match(
        ...     "HDFC Mid Cap",
        ...     ["HDFC Mid Cap Fund", "Axis Midcap Fund"],
        ...     threshold=80
        ... )
        >>> result[0]
        'HDFC Mid Cap Fund'
        >>> result[1]
        95
    """
    if not query or not choices:
        if logger:
            logger.info(f"[FUZZY] No query or choices. Query empty={not query}, Choices={len(choices) if choices else 0}")
        return None
    
    if logger:
        logger.info(f"[FUZZY] Starting fuzzy_best_match - Query: '{query}', Threshold: {threshold}%, Candidates: {len(choices)}")
    
    result = process.extractOne(
        query,
        choices,
        scorer=fuzz.token_set_ratio,
        processor=lambda x: x.lower().strip() if x else "",
        score_cutoff=threshold
    )
    
    if result:
        matched_string, score, index = result
        if logger:
            logger.debug(f"[FUZZY] ✓ MATCH FOUND! Score: {score}%, Matched: '{matched_string}' (index {index}/{len(choices)-1})")
        return (matched_string, score)
    else:
        if logger:
            logger.debug(f"[FUZZY] ✗ NO MATCH FOUND above threshold {threshold}%")
        return None


def fuzzy_top_matches(query: str, choices: List[str], limit: int = 5, threshold: int = 80) -> List[Tuple[str, int]]:
    """
    Find top N fuzzy matches for a query from a list of choices.
    
    Useful for debugging, logging multiple candidates, or for decision-making when
    scores are close.
    
    Args:
        query: Query string
        choices: List of candidates
        limit: Maximum number of results
        threshold: Minimum score required
        
    Returns:
        List of (matched_string, score) tuples, sorted by score descending
        
    Examples:
        >>> matches = fuzzy_top_matches(
        ...     "HDFC Midcap",
        ...     ["HDFC Mid Cap Fund", "HDFC Balanced Fund", "Axis Midcap"],
        ...     limit=2,
        ...     threshold=70
        ... )
        >>> len(matches)
        2
        >>> matches[0][1] > matches[1][1]  # Sorted by score
        True
    """
    if not query or not choices:
        return []
    
    results = process.extract(
        query,
        choices,
        scorer=fuzz.token_set_ratio,
        processor=lambda x: x.lower().strip() if x else "",
        limit=limit,
        score_cutoff=threshold
    )
    
    return [(match, score) for match, score, _ in results]


def validate_isin(isin: Optional[str]) -> bool:
    """
    Validate ISIN format for Indian mutual funds.
    
    Indian ISINs follow the format: INF[A-Z0-9]{9}
    Example: INF247L01890 (12 characters total)
    
    Args:
        isin: ISIN code to validate
        
    Returns:
        True if valid ISIN format, False otherwise
        
    Examples:
        >>> validate_isin("INF247L01890")
        True
        >>> validate_isin("INE001A01020")  # Equity, not mutual fund
        False
        >>> validate_isin("invalid")
        False
        >>> validate_isin(None)
        False
    """
    if not isin:
        return False
    
    isin = str(isin).strip()
    
    # Check format: must be 12 chars, start with IN, all alphanumeric
    if len(isin) != 12:
        return False
    
    if not isin.startswith("IN"):
        return False
    
    if not isin.isalnum():
        return False
    
    # For mutual funds, typically starts with INF
    # But INE (equity), IN9 (corporate bonds), etc. also exist
    # We're specifically validating MF ISINs, so check INF prefix
    if not isin.startswith("INF"):
        return False
    
    return True