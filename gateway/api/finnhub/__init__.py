"""Finnhub API sub-router package."""

from fastapi import APIRouter

from gateway.api.finnhub import (
    alternative,
    analysis,
    crypto,
    earnings,
    etf,
    forex,
    fundamentals,
    funds,
    news,
    quotes,
)

router = APIRouter(prefix="/api/v1/finnhub", tags=["finnhub"])

# Quotes and bars
router.include_router(quotes.router)

# Company profile and fundamentals
router.include_router(fundamentals.router)

# News
router.include_router(news.router)

# Earnings, estimates, recommendations
router.include_router(earnings.router)

# Analysis and sentiment
router.include_router(analysis.router)

# ETF and indices
router.include_router(etf.router)

# Forex
router.include_router(forex.router)

# Crypto
router.include_router(crypto.router)

# Mutual funds
router.include_router(funds.router)

# Alternative data
router.include_router(alternative.router)

__all__ = ["router"]
