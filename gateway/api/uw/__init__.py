"""Unusual Whales API sub-router package."""

from fastapi import APIRouter

from gateway.api.uw import (
    alerts,
    calendar,
    contracts,
    earnings,
    etf,
    etf_extended,
    extended,
    flow,
    flow_analytics,
    greeks,
    insiders,
    institutions,
    intelligence,
    market,
    market_data,
    misc,
    options,
    options_data,
    politicians,
    screener,
    seasonality,
    shorts,
    stock,
    volatility,
)

router = APIRouter(prefix="/api/v1/uw", tags=["unusual_whales"])

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
router.include_router(market_data.router)
router.include_router(misc.router)
router.include_router(politicians.router)
router.include_router(calendar.router)
router.include_router(institutions.router)
router.include_router(insiders.router)
router.include_router(etf_extended.router)
router.include_router(alerts.router)
router.include_router(stock.router)
router.include_router(contracts.router)
router.include_router(extended.router)

__all__ = ["router"]
