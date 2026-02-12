"""Alpaca stock data endpoints - bars, quotes, trades, snapshots, auctions."""

import asyncio
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_BAR_TIMEFRAME,
    DESC_COMMA_SYMBOLS,
    DESC_END_TIME,
    DESC_MAX_BARS,
    DESC_START_TIME,
    Client,
    execute_alpaca_provider_call,
    get_registry,
    parse_comma_values,
    require_api_key,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/stocks/{symbol}/bars", response_model=SuccessResponse)
async def get_stock_bars(
    symbol: str,
    timeframe: str = Query(default="1Day", description=DESC_BAR_TIMEFRAME),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description=DESC_MAX_BARS),
    feed: str = Query(default="sip", description="Data feed: sip or iex"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for a stock."""
    # Default time range: last 24 hours
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=24)

    bars = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_bars(
            symbols=[symbol.upper()],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
        ),
    )

    return {
        "success": True,
        "data": {
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "bars": [bar.model_dump() for bar in bars],
        },
        "meta": {
            "count": len(bars),
            "provider": "alpaca",
            "feed": feed,
        },
    }


@router.get("/stocks/{symbol}/quotes", response_model=SuccessResponse)
async def get_stock_quotes(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a stock."""
    quotes = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_quotes(symbols=[symbol.upper()]),
    )
    if not quotes:
        raise HTTPException(status_code=404, detail=f"No quote found for {symbol}")

    return {
        "success": True,
        "data": quotes[0].model_dump(),
        "meta": {"provider": "alpaca"},
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
    """Get historical trades for a stock."""
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=1)

    trades = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_trades(
            symbols=[symbol.upper()],
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "symbol": symbol.upper(),
            "trades": [t.model_dump() for t in trades],
        },
        "meta": {
            "count": len(trades),
            "provider": "alpaca",
        },
    }


@router.get("/stocks/{symbol}/snapshot", response_model=SuccessResponse)
async def get_stock_snapshot(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a stock (latest bar + quote)."""

    async def _fetch_snapshot(provider):
        end = datetime.now(UTC)
        start = end - timedelta(minutes=5)
        return await asyncio.gather(
            provider.get_quotes(symbols=[symbol.upper()]),
            provider.get_bars(
                symbols=[symbol.upper()],
                timeframe="1Min",
                start=start,
                end=end,
                limit=1,
            ),
        )

    quotes, bars = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=_fetch_snapshot,
    )
    return {
        "success": True,
        "data": {
            "symbol": symbol.upper(),
            "quote": quotes[0].model_dump() if quotes else None,
            "latest_bar": bars[0].model_dump() if bars else None,
        },
        "meta": {"provider": "alpaca"},
    }


@router.get("/stocks/bars/latest", response_model=SuccessResponse)
async def get_latest_bars(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bar for each symbol."""
    symbols_list = parse_comma_values(symbols, uppercase=True)
    bars = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_latest_bars(symbols_list),
    )
    return {
        "success": True,
        "data": [b.model_dump() for b in bars],
        "meta": {"count": len(bars), "provider": "alpaca"},
    }


@router.get("/stocks/trades/latest", response_model=SuccessResponse)
async def get_latest_trades(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trade for each symbol."""
    symbols_list = parse_comma_values(symbols, uppercase=True)
    trades = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_latest_trades(symbols_list),
    )
    return {
        "success": True,
        "data": [t.model_dump() for t in trades],
        "meta": {"count": len(trades), "provider": "alpaca"},
    }


@router.get("/stocks/quotes", response_model=SuccessResponse)
async def get_historical_quotes(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    start: datetime = Query(..., description=DESC_START_TIME),
    end: datetime = Query(..., description=DESC_END_TIME),
    limit: int = Query(default=10000, le=10000, description="Max quotes"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical quotes for symbols."""
    symbols_list = parse_comma_values(symbols, uppercase=True)
    quotes = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_historical_quotes(symbols_list, start, end, limit),
    )
    return {
        "success": True,
        "data": [q.model_dump() for q in quotes],
        "meta": {"count": len(quotes), "provider": "alpaca"},
    }


@router.get("/stocks/snapshots", response_model=SuccessResponse)
async def get_snapshots(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshots for symbols."""
    symbols_list = parse_comma_values(symbols, uppercase=True)
    snapshots = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_snapshots(symbols_list),
    )
    return {
        "success": True,
        "data": snapshots,
        "meta": {"count": len(snapshots), "provider": "alpaca"},
    }


@router.get("/stocks/auctions", response_model=SuccessResponse)
async def get_auctions(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description="Max auctions per symbol"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get auction data for symbols."""
    symbols_list = parse_comma_values(symbols, uppercase=True)
    auctions = await execute_alpaca_provider_call(
        registry=registry,
        block=True,
        provider_call=lambda provider: provider.get_auctions(symbols_list, start, end, limit),
    )
    return {
        "success": True,
        "data": auctions,
        "meta": {"count": len(auctions), "provider": "alpaca"},
    }
