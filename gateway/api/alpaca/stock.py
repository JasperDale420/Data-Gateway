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
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
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
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    # Default time range: last 24 hours
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=24)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        bars = await provider.get_bars(
            symbols=[symbol.upper()],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
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
                "provider": "alpaca",
                "feed": feed,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/{symbol}/quotes", response_model=SuccessResponse)
async def get_stock_quotes(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a stock."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        quotes = await provider.get_quotes(symbols=[symbol.upper()])

        if not quotes:
            raise HTTPException(status_code=404, detail=f"No quote found for {symbol}")

        return {
            "success": True,
            "data": quotes[0].model_dump(mode="json"),
            "meta": {"provider": "alpaca"},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


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
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=1)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        trades = await provider.get_trades(
            symbols=[symbol.upper()],
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "trades": [t.model_dump(mode="json") for t in trades],
            },
            "meta": {
                "count": len(trades),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/{symbol}/snapshot", response_model=SuccessResponse)
async def get_stock_snapshot(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a stock (latest bar + quote)."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        end = datetime.now(UTC)
        start = end - timedelta(minutes=5)
        quotes, bars = await asyncio.gather(
            provider.get_quotes(symbols=[symbol.upper()]),
            provider.get_bars(
                symbols=[symbol.upper()],
                timeframe="1Min",
                start=start,
                end=end,
                limit=1,
            ),
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "quote": quotes[0].model_dump(mode="json") if quotes else None,
                "latest_bar": bars[0].model_dump(mode="json") if bars else None,
            },
            "meta": {"provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/bars/latest", response_model=SuccessResponse)
async def get_latest_bars(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bar for each symbol."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        bars = await provider.get_latest_bars(symbols_list)
        return {
            "success": True,
            "data": [b.model_dump(mode="json") for b in bars],
            "meta": {"count": len(bars), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/trades/latest", response_model=SuccessResponse)
async def get_latest_trades(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trade for each symbol."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        trades = await provider.get_latest_trades(symbols_list)
        return {
            "success": True,
            "data": [t.model_dump(mode="json") for t in trades],
            "meta": {"count": len(trades), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


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
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        quotes = await provider.get_historical_quotes(symbols_list, start, end, limit)
        return {
            "success": True,
            "data": [q.model_dump(mode="json") for q in quotes],
            "meta": {"count": len(quotes), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/snapshots", response_model=SuccessResponse)
async def get_snapshots(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshots for symbols."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        snapshots = await provider.get_snapshots(symbols_list)
        return {
            "success": True,
            "data": snapshots,
            "meta": {"count": len(snapshots), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


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
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=True)
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        auctions = await provider.get_auctions(symbols_list, start, end, limit)
        return {
            "success": True,
            "data": auctions,
            "meta": {"count": len(auctions), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
