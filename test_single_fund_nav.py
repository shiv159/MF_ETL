#!/usr/bin/env python
"""Quick test to debug NAV history aggregation for a single fund."""

import logging
import sys
from pathlib import Path
from datetime import datetime

# Configure logging to show debug messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s'
)

# Add paths
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mf_etl.fetchers.mftool_fetcher import MFAPIFetcher

# Test with PGIM India Flexi Cap
fetcher = MFAPIFetcher(timeout=30, max_retries=3)

print("\n" + "="*80)
print("Testing NAV History Aggregation for PGIM India Flexi Cap (scheme_code: 133839)")
print("="*80 + "\n")

result = fetcher._get_nav_history_internal(scheme_code="133839", nav_history_years=10)

if result:
    print("\nResult:")
    print(f"  Current NAV: {result['current_nav']}")
    print(f"  NAV As Of: {result['nav_as_of']}")
    print(f"  History Months: {len(result['nav_history'])}")
    if result['nav_history']:
        months = sorted(result['nav_history'].keys())
        print(f"  Month Range: {months[0]} to {months[-1]}")
        print(f"  Monthly Data (first 5 and last 5):")
        for month in months[:5]:
            print(f"    {month}: {result['nav_history'][month]}")
        if len(months) > 10:
            print(f"    ... ({len(months) - 10} more) ...")
        for month in months[-5:]:
            print(f"    {month}: {result['nav_history'][month]}")
else:
    print("Failed to fetch NAV history")
