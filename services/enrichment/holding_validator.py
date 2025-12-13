import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Add src to path for imports
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mf_etl.utils.search_utils import safe_numeric


def _safe_numeric(value, target_type=float, default=None):
    """
    Safely convert a value to the target numeric type with type coercion.
    
    Args:
        value: Value to convert
        target_type: Target type (float or int)
        default: Default value if conversion fails
        
    Returns:
        Converted value of target_type or default
    """
    return safe_numeric(value, target_type=target_type, default=default)


def validate_holdings(holdings: List[Dict]) -> Tuple[List[Dict], List[str]]:
    """
    Validate mutual fund holdings with type coercion and validation.
    
    Args:
        holdings: List of holding dictionaries with keys: fund_name, units, nav, value, etc.
        
    Returns:
        Tuple of (validated_holdings, validation_warnings)
    """
    validated: List[Dict] = []
    warnings: List[str] = []
    
    for h in holdings:
        fund_name = h.get('fund_name')
        if not fund_name:
            warnings.append("Skipping holding because fund_name is missing")
            continue
        
        # Type coercion for numeric fields
        units = _safe_numeric(h.get('units'), float, None)
        nav = _safe_numeric(h.get('nav'), float, None)
        value = _safe_numeric(h.get('value'), float, None)
        
        if units is not None and units <= 0:
            warnings.append(f"{fund_name}: units must be positive")
            continue

        if nav is not None and nav <= 0:
            warnings.append(f"{fund_name}: nav must be positive")
            continue

        if value is None:
            if units is not None and nav is not None:
                value = units * nav
        else:
            if units is not None and nav is not None:
                expected = units * nav
                if expected > 0:
                    deviation = abs(value - expected) / expected
                    if deviation > 0.02:
                        warnings.append(
                            f"{fund_name}: reported value {value:.2f} deviates from units*nav {expected:.2f} by {deviation * 100:.2f}%"
                        )

        validated.append({
            'fund_name': fund_name,
            'units': units,
            'nav': nav,
            'value': value,
            'purchase_date': h.get('purchase_date')
        })
    return validated, warnings

