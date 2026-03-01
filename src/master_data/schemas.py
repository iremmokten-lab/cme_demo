from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


class FacilityUpsert(BaseModel):
    facility_id: int | None = None
    name: str = Field(..., min_length=1, max_length=200)
    country: str = Field(default="TR", max_length=100)
    sector: str = Field(default="", max_length=200)
    valid_from: datetime


class ProductUpsert(BaseModel):
    logical_id: str | None = None  # UUID string; empty => create
    name: str = Field(..., min_length=1, max_length=200)
    cn_code: str = Field(..., min_length=2, max_length=32)
    sector: str = Field(default="", max_length=100)
    valid_from: datetime


class CNCodeUpsert(BaseModel):
    code: str = Field(..., min_length=2, max_length=32)
    description: str = ""
    sector: str = ""
    source: str = ""
    valid_from: datetime | None = None
    valid_to: datetime | None = None


class BOMEdgeUpsert(BaseModel):
    parent_product_id: int
    child_product_id: int
    ratio: float = Field(..., gt=0)
    unit: str = Field(default="kg", max_length=50)
    valid_from: datetime
