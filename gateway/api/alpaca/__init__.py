"""Alpaca API sub-router package."""

from fastapi import APIRouter

from gateway.api.alpaca import (
    account,
    corporate,
    crypto,
    forex,
    metadata,
    news,
    options,
    screener,
    stock,
    trading,
    watchlists,
)

router = APIRouter(prefix="/api/v1/alpaca", tags=["alpaca"])

# Stock data endpoints
router.include_router(stock.router)

# Options endpoints
router.include_router(options.router)

# Crypto endpoints
router.include_router(crypto.router)

# Forex endpoints
router.include_router(forex.router)

# News endpoints
router.include_router(news.router)

# Screener endpoints
router.include_router(screener.router)

# Corporate actions endpoints
router.include_router(corporate.router)

# Metadata endpoints
router.include_router(metadata.router)

# Trading endpoints (orders, positions, account, assets, clock, calendar)
router.include_router(trading.router)

# Account config & activities endpoints
router.include_router(account.router)

# Watchlist endpoints
router.include_router(watchlists.router)

__all__ = ["router"]
