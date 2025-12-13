import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def validate_holdings(holdings: List[Dict]) -> Tuple[List[Dict], List[str]]:
    validated: List[Dict] = []
    warnings: List[str] = []
    for h in holdings:
        fund_name = h.get('fund_name')
        if not fund_name:
            warnings.append("Skipping holding because fund_name is missing")
            continue
        units = h.get('units')
        nav = h.get('nav')
        value = h.get('value')

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
