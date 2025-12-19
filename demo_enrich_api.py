"""
Demo: ETL Enrichment API - Replicate /etl/enrich endpoint with hardcoded fund names

This demo shows how to use the FundEnricher to enrich mutual fund holdings.
It simulates the POST /etl/enrich API endpoint with hardcoded fund data.
"""
import asyncio
import json
from services.enrichment.fund_enricher import FundEnricher
from services.enrichment.holding_validator import validate_holdings
from services.enrichment.mstarpy_helper import get_mstar_metadata


# Hardcoded holdings data to enrich
DEMO_HOLDINGS = [
  
    {
        "fund_name": "PGIM India Flexi Cap Fund Regular Growth",
        "units": "50.2",
        "nav": "100.00",
        "value": "5020.00"
    },
    {
        "fund_name": "Axis Mid Cap Fund Growth",
        "units": "25.0",
        "nav": "250.00",
        "value": "6250.00"
    }
]


async def demo_enrich():
    """
    Demo the enrichment API endpoint with hardcoded fund names.
    
    Flow:
    1. Validate holdings structure
    2. Extract fund names
    3. Enrich holdings using FundEnricher (concurrent batch processing)
    4. Display results
    """
    print("=" * 80)
    print("ETL ENRICHMENT API DEMO")
    print("=" * 80)
    print()
    
    # Step 1: Validate holdings
    print("STEP 1: Validating holdings structure...")
    validated, warnings = validate_holdings(DEMO_HOLDINGS)
    print(f"[OK] {len(validated)} holdings validated successfully")
    if warnings:
        print(f"  Warnings: {warnings}")
    print()
    
    # Step 2: Extract fund names
    fund_names = [h["fund_name"] for h in validated]
    print(f"STEP 2: Extracting fund names...")
    for i, name in enumerate(fund_names, 1):
        print(f"  {i}. {name}")
    print()
    
    # Step 3: Initialize enricher and enrich
    print("STEP 3: Enriching holdings (concurrent batch processing)...")
    print(f"  Fetching data from MFAPI, MstarPy...")
    enricher = FundEnricher()
    
    # Enrich all holdings concurrently (max 5 concurrent)
    enriched_results = await enricher.enrich_batch_concurrent(
        enricher=enricher,
        fund_names=fund_names,
        max_concurrent=5,
        timeout_per_fund=15
    )
    print()
    
    # Step 4: Display results
    print("STEP 4: Enrichment Results")
    print("-" * 80)
    
    enriched_count = 0
    failed_count = 0
    
    for i, (original, enriched) in enumerate(zip(validated, enriched_results), 1):
        print(f"\n{i}. Fund: {original['fund_name']}")
        
        if enriched:
            enriched_count += 1
            print(f"   Status: [OK] Successfully Enriched")
            print(f"   ISIN: {enriched.isin}")
            if enriched.current_nav:
                print(f"   NAV: {enriched.current_nav} (as of {enriched.nav_as_of})")
            print(f"   Category: {enriched.category}")
            if enriched.amc:
                print(f"   AMC: {enriched.amc}")
            if enriched.sector_allocation:
                print(f"   Sectors: {len(enriched.sector_allocation)} sectors")
            if enriched.top_holdings:
                print(f"   Top Holdings: {len(enriched.top_holdings)} stocks")
        else:
            failed_count += 1
            print(f"   Status: [FAILED] Enrichment Failed")
            print(f"   (Unable to resolve fund from MFAPI)")
    
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total Holdings: {len(validated)}")
    print(f"Successfully Enriched: {enriched_count}")
    print(f"Failed: {failed_count}")
    print(f"Success Rate: {(enriched_count / len(validated) * 100):.1f}%")
    print()
    
    # Display enrichment response structure (like API would return)
    print("API RESPONSE STRUCTURE (JSON):")
    print("-" * 80)
    # Build enriched holdings by fetching mstar metadata concurrently
    async def _build_enriched_item(e):
        if not e or not getattr(e, 'isin', None):
            return e.dict() if e else None
        try:
            meta = await get_mstar_metadata(e.isin)
        except Exception:
            meta = None
        return {**e.dict(), "mstarpy_metadata": meta}

    enriched_items = await asyncio.gather(*(_build_enriched_item(e) for e in enriched_results))

    response = {
        "upload_id": "demo-001",
        "enriched_holdings": [
            {
                "original_fund_name": h["fund_name"],
                "enriched": enriched_item,
            }
            for (h, _), enriched_item in zip(zip(validated, enriched_results), enriched_items)
        ],
        "summary": {
            "total_requested": len(validated),
            "total_enriched": enriched_count,
            "total_failed": failed_count,
            "success_rate": f"{(enriched_count / len(validated) * 100):.1f}%"
        }
    }
    
    # Pretty print the response (truncated for readability)
    response_str = json.dumps(response, indent=2, default=str)
    if len(response_str) > 500:
        print(response_str[:500] + "\n... (truncated)")
    else:
        print(response_str)
    
    print()
    print("=" * 80)
    return response


def main():
    """Run the demo"""
    try:
        result = asyncio.run(demo_enrich())
        print("\n[OK] Demo completed successfully!")
        return result
    except Exception as e:
        print(f"\n[ERROR] Demo failed with error:")
        print(f"  {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
