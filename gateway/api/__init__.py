"""API routers and endpoints."""

from gateway.api.admin import router as admin_router
from gateway.api.alpaca import router as alpaca_router
from gateway.api.alphavantage import router as alphavantage_router
from gateway.api.backfill import router as backfill_router
from gateway.api.bulk import router as bulk_router
from gateway.api.calendar import router as calendar_router
from gateway.api.catalog import router as catalog_router
from gateway.api.corporate import adjustments_router, legacy_adjustments_router
from gateway.api.corporate import legacy_router as legacy_corporate_router
from gateway.api.corporate import router as corporate_router
from gateway.api.finnhub import router as finnhub_router
from gateway.api.health import router as health_router
from gateway.api.market import router as market_router
from gateway.api.news import router as news_router
from gateway.api.replay import router as replay_router  # audit-2026-02: zero consumers
from gateway.api.sec import router as sec_router
from gateway.api.symbology import legacy_router as legacy_symbology_router  # audit-2026-02: zero consumers
from gateway.api.symbology import router as symbology_router  # audit-2026-02: zero consumers
from gateway.api.uw import router as uw_router
from gateway.api.websocket import router as websocket_router
from gateway.api.yf import router as yf_router

__all__ = [
    "health_router",
    "websocket_router",
    "alpaca_router",
    "admin_router",
    "market_router",
    "uw_router",
    "news_router",
    "yf_router",
    "sec_router",
    "finnhub_router",
    "alphavantage_router",
    "backfill_router",
    "bulk_router",
    "replay_router",
    "calendar_router",
    "symbology_router",
    "corporate_router",
    "legacy_corporate_router",
    "adjustments_router",
    "legacy_adjustments_router",
    "catalog_router",
    "legacy_symbology_router",
]
