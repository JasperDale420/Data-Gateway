"""Alpaca stock data endpoints - bars, quotes, trades, snapshots, auctions."""

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
    require_api_key,
)
from gateway.core.registry import ProviderRegistry
from gateway.core.symbology import SymbolResolver
from gateway.schemas import SuccessResponse

router = APIRouter()
_SYMBOL_RESOLVER = SymbolResolver()


def _normalize_stock_symbol_or_raise(symbol: str) -> str:
    """Normalize stock route symbols and reject option contracts before Alpaca."""
    normalized = symbol.strip().upper()
    resolved = _SYMBOL_RESOLVER.resolve(normalized)
    if resolved.type == "option":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4007",
                "message": (
                    f"{normalized} is an option contract, not a stock symbol. "
                    f"Use /api/v1/alpaca/options/{normalized}/bars for option bars."
                ),
                "symbol": normalized,
                "symbol_type": "option",
            },
        )
    return normalized


# --- Static-segment routes MUST be registered before parameterized routes ---


@router.get("/stocks/bars/latest", response_model=SuccessResponse)
async def get_latest_bars(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bar for each symbol."""
    symbols_list = [s.strip().upper() for s in symbols.split(",")]

    async def _call(provider):
        bars = await provider.get_latest_bars(symbols_list)
        return {
            "success": True,
            "data": [b.model_dump(mode="json") for b in bars],
            "meta": {"count": len(bars), "provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


@router.get("/stocks/trades/latest", response_model=SuccessResponse)
async def get_latest_trades(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trade for each symbol."""
    symbols_list = [s.strip().upper() for s in symbols.split(",")]

    async def _call(provider):
        trades = await provider.get_latest_trades(symbols_list)
        return {
            "success": True,
            "data": [t.model_dump(mode="json") for t in trades],
            "meta": {"count": len(trades), "provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


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
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)

    symbols_list = [s.strip().upper() for s in symbols.split(",")]

    async def _call(provider):
        quotes = await provider.get_historical_quotes(symbols_list, start, end, limit)
        return {
            "success": True,
            "data": [q.model_dump(mode="json") for q in quotes],
            "meta": {"count": len(quotes), "provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


@router.get("/stocks/snapshots", response_model=SuccessResponse)
async def get_snapshots(
    symbols: str = Query(..., description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshots for symbols."""
    symbols_list = [s.strip().upper() for s in symbols.split(",")]

    async def _call(provider):
        snapshots = await provider.get_snapshots(symbols_list)
        return {
            "success": True,
            "data": snapshots,
            "meta": {"count": len(snapshots), "provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


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
    symbols_list = [s.strip().upper() for s in symbols.split(",")]

    async def _call(provider):
        auctions = await provider.get_auctions(symbols_list, start, end, limit)
        return {
            "success": True,
            "data": auctions,
            "meta": {"count": len(auctions), "provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


# --- Parameterized routes below ---


@router.get("/stocks/{symbol}/bars", response_model=SuccessResponse)
async def get_stock_bars(
    symbol: str,
    timeframe: str = Query(default="1Day", description=DESC_BAR_TIMEFRAME),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description=DESC_MAX_BARS),
    feed: str | None = Query(
        default=None, description="Data feed override: sip or iex (default: provider-configured feed)"
    ),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for a stock."""
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=24)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)

    normalized = _normalize_stock_symbol_or_raise(symbol)

    async def _call(provider):
        bars = await provider.get_bars(
            symbols=[normalized],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
        )
        return {
            "success": True,
            "data": {
                "symbol": normalized,
                "timeframe": timeframe,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            "meta": {"count": len(bars), "provider": "alpaca", "feed": feed},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


@router.get("/stocks/{symbol}/quotes", response_model=SuccessResponse)
async def get_stock_quotes(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a stock."""
    normalized = symbol.upper()

    async def _call(provider):
        quotes = await provider.get_quotes(symbols=[normalized])
        if not quotes:
            raise HTTPException(status_code=404, detail=f"No quote found for {symbol}")
        return {
            "success": True,
            "data": quotes[0].model_dump(mode="json"),
            "meta": {"provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


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
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)

    normalized = symbol.upper()

    async def _call(provider):
        trades = await provider.get_trades(
            symbols=[normalized],
            start=start,
            end=end,
        )
        return {
            "success": True,
            "data": {
                "symbol": normalized,
                "trades": [t.model_dump(mode="json") for t in trades[:limit]],
            },
            "meta": {"count": len(trades), "provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)


@router.get("/stocks/{symbol}/snapshot", response_model=SuccessResponse)
async def get_stock_snapshot(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a stock (latest bar + quote)."""
    normalized = symbol.upper()

    async def _call(provider):
        quotes = await provider.get_quotes(symbols=[normalized])
        end = datetime.now(UTC)
        start = end - timedelta(minutes=5)
        bars = await provider.get_bars(
            symbols=[normalized],
            timeframe="1Min",
            start=start,
            end=end,
            limit=1,
        )
        return {
            "success": True,
            "data": {
                "symbol": normalized,
                "quote": quotes[0].model_dump(mode="json") if quotes else None,
                "latest_bar": bars[0].model_dump(mode="json") if bars else None,
            },
            "meta": {"provider": "alpaca"},
        }

    return await execute_alpaca_provider_call(registry=registry, provider_call=_call)
