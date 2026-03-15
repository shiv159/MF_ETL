from typing import Any, Dict, List, Optional
from pydantic import BaseModel, PrivateAttr


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
    mstarpy_metadata: Optional[Dict[str, Any]] = None
    quality_flags: List[str] = []
    missing_fields: List[str] = []
    source_timestamps: Optional[Dict[str, Optional[str]]] = None

    # Store NAV history privately so downstream layers can merge it into mstarpy metadata
    _nav_history: Optional[Dict[str, float]] = PrivateAttr(default=None)


class EnrichmentQuality(BaseModel):
    successfully_enriched: int
    failed_to_enrich: int
    warnings: List[str]


class EnrichmentResponse(BaseModel):
    upload_id: Optional[str]
    status: str
    duration_seconds: Optional[int]
    enriched_funds: List[EnrichedFund]
    enrichment_quality: EnrichmentQuality
    error_message: Optional[str]
