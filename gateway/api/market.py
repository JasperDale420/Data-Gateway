"""Unified Market Data API endpoints with automatic failover."""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query

from gateway.api.alpaca.common import (
    DESC_BAR_TIMEFRAME,
    DESC_END_TIME,
    DESC_MAX_BARS,
    DESC_START_TIME,
    Client,
    get_registry,
    require_api_key,
)
from gateway.core.execution import execute_provider_failover
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/market", tags=["market"])


@router.get("/stocks/{symbol}/bars", response_model=SuccessResponse)
async def get_stock_bars(
    symbol: str,
    timeframe: str = Query(default="1Day", description=DESC_BAR_TIMEFRAME),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description=DESC_MAX_BARS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for a stock from the best available provider."""
    # Default time range: last 24 hours
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=24)

    bars = await execute_provider_failover(
        capability="stock_bars",
        handler=lambda provider: provider.get_bars(
            symbols=[symbol.upper()],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        ),
        registry=registry,
    )

    return {
        "success": True,
        "data": {
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        },
        "meta": {
            "count": len(bars),
            "provider": "unified",
            "strategy": "failover",
        },
    }


@router.get("/stocks/{symbol}/quotes", response_model=SuccessResponse)
async def get_stock_quote(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a stock from the best available provider."""
    quotes = await execute_provider_failover(
        capability="stock_quotes",
        handler=lambda provider: provider.get_quotes(symbols=[symbol.upper()]),
        registry=registry,
    )

    return {
        "success": True,
        "data": quotes[0].model_dump(mode="json") if quotes else None,
        "meta": {"provider": "unified", "strategy": "failover"},
    }


@router.get("/stocks/{symbol}/trades", response_model=SuccessResponse)
async def get_stock_trades(
    symbol: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for a stock from the best available provider."""
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=1)

    trades = await execute_provider_failover(
        capability="stock_trades",
        handler=lambda provider: provider.get_trades(
            symbols=[symbol.upper()],
            start=start,
            end=end,
            limit=limit,
        ),
        registry=registry,
    )

    return {
        "success": True,
        "data": {
            "symbol": symbol.upper(),
            "trades": [t.model_dump(mode="json") for t in trades],
        },
        "meta": {
            "count": len(trades),
            "provider": "unified",
            "strategy": "failover",
        },
    }


@router.get("/news/{symbol}", response_model=SuccessResponse)
async def get_company_news(
    symbol: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get company news from the best available provider."""
    articles = await execute_provider_failover(
        capability="news",
        handler=lambda provider: provider.get_news(
            symbol=symbol.upper(),
            start=start,
            end=end,
        ),
        registry=registry,
    )

    return {
        "success": True,
        "data": {"symbol": symbol.upper(), "articles": articles},
        "meta": {
            "count": len(articles),
            "provider": "unified",
            "strategy": "failover",
        },
    }
