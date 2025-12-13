"""Simple test of API with Phase 1 fixes."""

import sys
import json
from pathlib import Path

# Add paths
ROOT = Path(__file__).resolve().parents[0]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Test imports
print("Testing Phase 1 fixes...")
print("=" * 60)

# Test 1: search_utils imports
print("\n✓ Test 1: Shared utility imports")
try:
    from src.mf_etl.utils.search_utils import (
        safe_float,
        safe_numeric,
        generate_fallback_search_terms,
    )
    print("  ✓ search_utils imports successful")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

# Test 2: holding_validator with type coercion
print("\n✓ Test 2: Enhanced holding validator")
try:
    from services.enrichment.holding_validator import validate_holdings
    
    test_holdings = [
        {"fund_name": "Test Fund 1", "units": "10.5", "nav": "50.5"},
        {"fund_name": "Test Fund 2", "units": 20, "nav": 100.0},
    ]
    
    validated, warnings = validate_holdings(test_holdings)
    print(f"  ✓ Validated {len(validated)} holdings")
    print(f"  ✓ Type coercion working: units is {type(validated[0]['units']).__name__}")
    assert isinstance(validated[0]['units'], float)
    print(f"  ✓ String '10.5' converted to float {validated[0]['units']}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

# Test 3: fund_enricher imports
print("\n✓ Test 3: Fund enricher with new utilities")
try:
    from services.enrichment.fund_enricher import FundEnricher
    print("  ✓ FundEnricher imports successful")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

# Test 4: API main imports
print("\n✓ Test 4: API main with validation changes")
try:
    from services.api.main import app, _run_enrichment
    print("  ✓ API main imports successful")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

# Test 5: Fallback search terms generation
print("\n✓ Test 5: Fallback search terms generation")
try:
    terms = generate_fallback_search_terms(
        "Motilal Oswal Midcap Direct Growth",
        "Motilal Oswal Midcap Fund-Direct - IDCW Payout/Reinvestment"
    )
    print(f"  ✓ Generated {len(terms)} fallback terms")
    print(f"    - Primary: {terms[0]}")
    if len(terms) > 1:
        print(f"    - Fallback 1: {terms[1]}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

# Test 6: safe_float with comma handling
print("\n✓ Test 6: safe_float with comma handling")
try:
    test_value = safe_float("1,234.56")
    assert test_value == 1234.56
    print(f"  ✓ Converted '1,234.56' to {test_value}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

# Test 7: safe_numeric type coercion
print("\n✓ Test 7: safe_numeric type coercion")
try:
    test_float = safe_numeric("10.5", float)
    test_int = safe_numeric("10.9", int)
    assert test_float == 10.5
    assert test_int == 10
    print(f"  ✓ '10.5' -> float: {test_float}")
    print(f"  ✓ '10.9' -> int: {test_int} (truncated)")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    sys.exit(1)

print("\n" + "=" * 60)
print("✅ All Phase 1 fixes verified successfully!")
print("=" * 60)
