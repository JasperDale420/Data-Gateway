"""Unusual Whales API sub-router package."""

from fastapi import APIRouter

from gateway.api.uw import (
    alerts,
    calendar,
    congress_ext,
    contracts,
    crossasset,
    earnings,
    etf,
    etf_extended,
    extended,
    flow,
    flow_analytics,
    fundamentals,
    greeks,
    insiders,
    institutions,
    intelligence,
    market,
    market_data,
    misc,
    options,
    options_data,
    options_ext,
    politicians,
    predictions,
    private_markets,
    screener,
    seasonality,
    shorts,
    stock,
    volatility,
    volatility_ext,
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
router.include_router(volatility_ext.router)
router.include_router(options_ext.router)
router.include_router(fundamentals.router)
router.include_router(congress_ext.router)
router.include_router(crossasset.router)
router.include_router(predictions.router)
router.include_router(private_markets.router)

__all__ = ["router"]
