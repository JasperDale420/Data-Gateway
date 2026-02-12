"""Alpaca crypto endpoints - bars, trades, quotes, snapshots, orderbook."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_BAR_TIMEFRAME,
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


@router.get("/crypto/{pair}/bars", response_model=SuccessResponse)
async def get_crypto_bars(
    pair: str,
    timeframe: str = Query(default="1Hour", description=DESC_BAR_TIMEFRAME),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description=DESC_MAX_BARS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for a crypto pair (e.g., BTC/USD)."""
    bars = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_bars(
            pair=pair.upper(),
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "pair": pair.upper(),
            "timeframe": timeframe,
            "bars": [bar.model_dump() for bar in bars],
        },
        "meta": {"count": len(bars), "provider": "alpaca"},
    }


@router.get("/crypto/{pair}/trades", response_model=SuccessResponse)
async def get_crypto_trades(
    pair: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for a crypto pair."""
    trades = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_trades(
            pair=pair.upper(),
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "pair": pair.upper(),
            "trades": [trade.model_dump() for trade in trades],
        },
        "meta": {"count": len(trades), "provider": "alpaca"},
    }


@router.get("/crypto/{pair}/quotes", response_model=SuccessResponse)
async def get_crypto_quotes(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a crypto pair."""
    quote = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_quotes(pair=pair.upper()),
    )

    if not quote:
        raise HTTPException(status_code=404, detail=f"No quote found for {pair}")

    return {
        "success": True,
        "data": quote.model_dump(),
        "meta": {"provider": "alpaca"},
    }


@router.get("/crypto/{pair}/snapshot", response_model=SuccessResponse)
async def get_crypto_snapshot(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a crypto pair."""
    snapshot = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_snapshot(pair=pair.upper()),
    )

    return {
        "success": True,
        "data": snapshot,
        "meta": {"provider": "alpaca"},
    }


@router.get("/crypto/{pair}/orderbook", response_model=SuccessResponse)
async def get_crypto_orderbook(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get crypto orderbook for a trading pair."""
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_orderbook(pair=pair.upper()),
    )

    # Handle both dict and NormalizedOrderbook
    if hasattr(data, "model_dump"):
        data_dict = data.model_dump()
    else:
        data_dict = data

    return {
        "success": True,
        "data": data_dict,
        "meta": {
            "pair": pair.upper(),
            "provider": "alpaca",
        },
    }


@router.get("/crypto/bars/latest", response_model=SuccessResponse)
async def get_crypto_latest_bars(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bars for crypto pairs."""
    pairs_list = parse_comma_values(pairs, uppercase=True)
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_latest_bars(pairs_list),
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/crypto/trades/latest", response_model=SuccessResponse)
async def get_crypto_latest_trades(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trades for crypto pairs."""
    pairs_list = parse_comma_values(pairs, uppercase=True)
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_latest_trades(pairs_list),
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }
