from typing import List, Optional

from pydantic import BaseModel, Field


class ParsedHoldingEntry(BaseModel):
    fund_name: str = Field(..., description="Name of the mutual fund")
    units: float = Field(..., description="Number of units held")
    nav: float = Field(..., description="Net asset value applied to the holding")
    value: Optional[float] = Field(None, description="Total holding value (units * nav)")
    purchase_date: Optional[str] = Field(None, description="Purchase date if available")


class EnrichmentRequest(BaseModel):
    upload_id: str = Field(..., description="Upload identifier supplied by the caller")
    user_id: str = Field(..., description="Identifier of the user who initiated the upload")
    file_type: Optional[str] = Field(None, description="Original file type parsed by Spring Boot")
    parsed_holdings: List[ParsedHoldingEntry] = Field(
        ..., description="Holdings already extracted by the Spring Boot parser"
    )
