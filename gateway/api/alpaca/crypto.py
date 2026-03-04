"""Alpaca crypto endpoints - bars, trades, quotes, snapshots, orderbook."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_BAR_TIMEFRAME,
    DESC_END_TIME,
    DESC_MAX_BARS,
    DESC_START_TIME,
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    map_alpaca_exception,
    parse_crypto_pairs_or_400,
    require_api_key,
    require_provider_rate_limit,
    validate_crypto_pair_or_400,
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
    normalized_pair = validate_crypto_pair_or_400(pair)
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        bars = await provider.get_crypto_bars(
            pair=normalized_pair,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "pair": normalized_pair,
                "timeframe": timeframe,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            "meta": {"count": len(bars), "provider": "alpaca"},
        }

    except Exception as e:
        raise map_alpaca_exception(e) from e


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
    normalized_pair = validate_crypto_pair_or_400(pair)
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        trades = await provider.get_crypto_trades(
            pair=normalized_pair,
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "pair": normalized_pair,
                "trades": [trade.model_dump(mode="json") for trade in trades],
            },
            "meta": {"count": len(trades), "provider": "alpaca"},
        }

    except Exception as e:
        raise map_alpaca_exception(e) from e


@router.get("/crypto/{pair}/quotes", response_model=SuccessResponse)
async def get_crypto_quotes(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a crypto pair."""
    normalized_pair = validate_crypto_pair_or_400(pair)
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        quote = await provider.get_crypto_quotes(pair=normalized_pair)

        if not quote:
            raise HTTPException(status_code=404, detail=f"No quote found for {normalized_pair}")

        return {
            "success": True,
            "data": quote.model_dump(mode="json"),
            "meta": {"provider": "alpaca"},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise map_alpaca_exception(e) from e


@router.get("/crypto/{pair}/snapshot", response_model=SuccessResponse)
async def get_crypto_snapshot(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a crypto pair."""
    normalized_pair = validate_crypto_pair_or_400(pair)
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        snapshot = await provider.get_crypto_snapshot(pair=normalized_pair)

        return {
            "success": True,
            "data": snapshot,
            "meta": {"provider": "alpaca"},
        }

    except Exception as e:
        raise map_alpaca_exception(e) from e


@router.get("/crypto/{pair}/orderbook", response_model=SuccessResponse)
async def get_crypto_orderbook(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get crypto orderbook for a trading pair."""
    normalized_pair = validate_crypto_pair_or_400(pair)
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await provider.get_crypto_orderbook(pair=normalized_pair)

        # Handle both dict and NormalizedOrderbook
        if hasattr(data, "model_dump"):
            data_dict = data.model_dump(mode="json")
        else:
            data_dict = data

        return {
            "success": True,
            "data": data_dict,
            "meta": {
                "pair": normalized_pair,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise map_alpaca_exception(e) from e


@router.get("/crypto/bars/latest", response_model=SuccessResponse)
async def get_crypto_latest_bars(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bars for crypto pairs."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = parse_crypto_pairs_or_400(pairs)
        data = await provider.get_crypto_latest_bars(pairs_list)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "provider": "alpaca"},
        }
    except Exception as e:
        raise map_alpaca_exception(e) from e


@router.get("/crypto/trades/latest", response_model=SuccessResponse)
async def get_crypto_latest_trades(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trades for crypto pairs."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = parse_crypto_pairs_or_400(pairs)
        data = await provider.get_crypto_latest_trades(pairs_list)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "provider": "alpaca"},
        }
    except Exception as e:
        raise map_alpaca_exception(e) from e
