"""Alpaca crypto endpoints - bars, trades, quotes, snapshots, orderbook."""

from datetime import datetime

import structlog
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
from gateway.core.security import InputValidator
from gateway.schemas import SuccessResponse

router = APIRouter()
logger = structlog.get_logger(__name__)
_INPUT_VALIDATOR = InputValidator()


def _validate_crypto_pair_or_raise(pair: str) -> str:
    """Validate and normalize a crypto pair string."""
    normalized_pair = pair.strip().upper()
    error = _INPUT_VALIDATOR.validate_symbol(normalized_pair, symbol_type="crypto")
    if error is not None:
        logger.warning(
            "alpaca_invalid_crypto_pair",
            pair=pair,
            normalized_pair=normalized_pair,
            error_code=error.code,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Invalid crypto pair format: '{pair}'. Expected format like BTC/USD.",
        )
    return normalized_pair


def _validate_crypto_pairs_or_raise(pairs: list[str]) -> list[str]:
    """Validate and normalize a list of crypto pairs."""
    normalized_pairs = [_validate_crypto_pair_or_raise(pair) for pair in pairs]
    if not normalized_pairs:
        raise HTTPException(status_code=400, detail="At least one crypto pair is required.")
    return normalized_pairs


@router.get("/crypto/{pair:path}/bars", response_model=SuccessResponse)
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
    normalized_pair = _validate_crypto_pair_or_raise(pair)
    bars = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_bars(
            pair=normalized_pair,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "pair": normalized_pair,
            "timeframe": timeframe,
            "bars": [bar.model_dump() for bar in bars],
        },
        "meta": {"count": len(bars), "provider": "alpaca"},
    }


@router.get("/crypto/{pair:path}/trades", response_model=SuccessResponse)
async def get_crypto_trades(
    pair: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for a crypto pair."""
    normalized_pair = _validate_crypto_pair_or_raise(pair)
    trades = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_trades(
            pair=normalized_pair,
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "pair": normalized_pair,
            "trades": [trade.model_dump() for trade in trades],
        },
        "meta": {"count": len(trades), "provider": "alpaca"},
    }


@router.get("/crypto/quotes/latest", response_model=SuccessResponse)
async def get_crypto_latest_quotes(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quotes for crypto pairs."""
    pairs_list = _validate_crypto_pairs_or_raise(parse_comma_values(pairs, uppercase=True, drop_empty=True))
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_quotes(pairs_list),
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/crypto/{pair:path}/quotes/historical", response_model=SuccessResponse)
async def get_historical_crypto_quotes(
    pair: str,
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=10000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical quotes for a crypto pair."""
    normalized_pair = _validate_crypto_pair_or_raise(pair)
    quotes = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_historical_crypto_quotes(
            pair=normalized_pair,
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "pair": normalized_pair,
            "quotes": [quote.model_dump() for quote in quotes],
        },
        "meta": {"count": len(quotes), "provider": "alpaca"},
    }


@router.get("/crypto/snapshots", response_model=SuccessResponse)
async def get_crypto_snapshots(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for crypto pairs."""
    pairs_list = _validate_crypto_pairs_or_raise(parse_comma_values(pairs, uppercase=True, drop_empty=True))
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_snapshots(pairs_list),
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/crypto/{pair:path}/orderbook", response_model=SuccessResponse)
async def get_crypto_orderbook(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get crypto orderbook for a trading pair."""
    normalized_pair = _validate_crypto_pair_or_raise(pair)
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_orderbook(pair=normalized_pair),
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
            "pair": normalized_pair,
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
    pairs_list = _validate_crypto_pairs_or_raise(parse_comma_values(pairs, uppercase=True, drop_empty=True))
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
    pairs_list = _validate_crypto_pairs_or_raise(parse_comma_values(pairs, uppercase=True, drop_empty=True))
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_crypto_latest_trades(pairs_list),
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }
