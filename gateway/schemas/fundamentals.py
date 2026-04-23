"""Fundamentals models — re-exported from empire_schemas, plus gateway-specific models."""

from datetime import datetime
from decimal import Decimal

from empire_schemas.analytics import NormalizedFTD, NormalizedShortData
from empire_schemas.fundamentals import (
    NormalizedEarnings,
    NormalizedFundamentals,
)
from empire_schemas.responses import NormalizedScreenerResult
from pydantic import BaseModel

__all__ = [
    "NormalizedEarnings",
    "NormalizedScreenerResult",
    "NormalizedBorrowCost",
    "NormalizedFundamentals",
    "NormalizedShortData",
    "NormalizedFTD",
]


# Gateway-specific model (not in empire_schemas)
class NormalizedBorrowCost(BaseModel):
    """Borrow cost / short availability data from UW shorts endpoint.

    Based on the UW OpenAPI spec Short Data schema.
    """

    symbol: str
    timestamp: datetime
    fee_rate: Decimal | None = None  # Annual borrow fee rate (percentage)
    rebate_rate: Decimal | None = None  # Rebate rate
    short_shares_available: int | None = None  # Shares available to short
    currency: str | None = None  # Currency denomination
    name: str | None = None  # Company name
    provider: str = "unusual_whales"
