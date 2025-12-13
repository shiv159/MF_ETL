from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict


class ParsedHoldingEntry(BaseModel):
    model_config = ConfigDict(extra='allow')  # Allow extra fields from Spring app
    
    fund_name: str = Field(..., description="Name of the mutual fund")
    units: float = Field(..., description="Number of units held")
    nav: Optional[float] = Field(None, description="Net asset value applied to the holding")
    value: Optional[float] = Field(None, description="Total holding value (units * nav)")
    purchase_date: Optional[str] = Field(None, description="Purchase date if available")
    isin: Optional[str] = Field(None, description="ISIN code of the mutual fund")
    amc: Optional[str] = Field(None, description="Asset Management Company name")
    category: Optional[str] = Field(None, description="Fund category (Equity, Hybrid, Debt, etc.)")
    folio_number: Optional[str] = Field(None, description="Folio number")
    current_value: Optional[float] = Field(None, description="Current value of the holding")
    returns: Optional[float] = Field(None, description="Returns on the holding")
    xirr: Optional[float] = Field(None, description="XIRR (Extended Internal Rate of Return)")


class EnrichmentRequest(BaseModel):
    model_config = ConfigDict(extra='allow')  # Allow extra fields from Spring app
    
    upload_id: str = Field(..., description="Upload identifier supplied by the caller")
    user_id: str = Field(..., description="Identifier of the user who initiated the upload")
    file_type: Optional[str] = Field(None, description="Original file type parsed by Spring Boot")
    parsed_holdings: List[ParsedHoldingEntry] = Field(
        ..., description="Holdings already extracted by the Spring Boot parser"
    )
    enrichment_timestamp: Optional[int] = Field(None, description="Timestamp of enrichment request")
