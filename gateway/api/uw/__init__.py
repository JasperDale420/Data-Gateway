"""Unusual Whales API sub-router package.

This package decomposes the monolithic uw.py into domain-specific sub-routers.
"""

from fastapi import APIRouter

from gateway.api.uw import (
    earnings,
    etf,
    flow,
    flow_analytics,
    greeks,
    intelligence,
    market,
    options,
    options_data,
    screener,
    seasonality,
    shorts,
    volatility,
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
router.include_router(volatility.router)
router.include_router(seasonality.router)
router.include_router(options_data.router)
router.include_router(intelligence.router)
router.include_router(flow_analytics.router)

__all__ = ["router"]
