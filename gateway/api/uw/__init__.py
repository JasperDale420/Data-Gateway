"""Unusual Whales API sub-router package.

This package decomposes the monolithic uw.py into domain-specific sub-routers.
The package maintains backward compatibility by re-exporting the combined router.

Sub-routers added so far:
- flow: Flow alerts and darkpool trades
- market: Market tide, institutions, congress, insiders
- greeks: GEX endpoints
- earnings: Earnings calendar
- screener: Stock/option screeners
- options: Options analytics (net-premium, max-pain, iv-rank, oi-change)
- etf: ETF analytics
- shorts: Short interest, FTDs, short volume
"""

from fastapi import APIRouter

from gateway.api.uw import (
    earnings,
    etf,
    flow,
    greeks,
    market,
    options,
    screener,
    shorts,
)

# Create combined router with the original prefix and tags
router = APIRouter(prefix="/api/v1/uw", tags=["unusual_whales"])

# Include all sub-routers (they don't have prefix since parent has it)
router.include_router(flow.router)
router.include_router(market.router)
router.include_router(greeks.router)
router.include_router(earnings.router)
router.include_router(screener.router)
router.include_router(options.router)
router.include_router(etf.router)
router.include_router(shorts.router)

__all__ = ["router"]
