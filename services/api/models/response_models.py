from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class EnrichedFund(BaseModel):
    fund_name: str
    isin: Optional[str]
    amc: Optional[str]
    category: Optional[str]
    expense_ratio: Optional[float]
    sector_allocation: Optional[Dict[str, float]]
    top_holdings: Optional[List[Dict[str, Any]]]
    current_nav: Optional[float]
    nav_as_of: Optional[str]


class EnrichmentQuality(BaseModel):
    successfully_enriched: int
    failed_to_enrich: int
    warnings: List[str]


class EnrichmentResponse(BaseModel):
    upload_id: str
    status: str
    duration_seconds: Optional[int]
    enriched_funds: List[EnrichedFund]
    enrichment_quality: EnrichmentQuality
    error_message: Optional[str]
