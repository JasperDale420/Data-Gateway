"""Alpha Vantage API sub-router package."""

from fastapi import APIRouter

from gateway.api.alphavantage import (
    calendars,
    crypto,
    economic,
    forex,
    fundamentals,
    indicators,
    timeseries,
)

router = APIRouter(prefix="/api/v1/alphavantage", tags=["alphavantage"])

# Time series (quote, intraday, daily, weekly, monthly, search)
router.include_router(timeseries.router)

# Fundamentals (overview, earnings, income, balance, cash flow)
router.include_router(fundamentals.router)

# Technical indicators
router.include_router(indicators.router)

# Forex
router.include_router(forex.router)

# Crypto
router.include_router(crypto.router)

# Economic indicators
router.include_router(economic.router)

# Calendars and listings
router.include_router(calendars.router)

__all__ = ["router"]
