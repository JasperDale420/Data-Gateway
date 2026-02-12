"""Alpaca options endpoints - chains, bars, quotes, trades, snapshots."""

from datetime import UTC, datetime, timedelta
from datetime import date as date_type

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


@router.get("/options/chain/{underlying}", response_model=SuccessResponse)
async def get_option_chain(
    underlying: str,
    expiration_date: str | None = Query(default=None, description="Exact expiration (YYYY-MM-DD)"),
    expiration_date_gte: str | None = Query(default=None, description="Expiration on or after"),
    expiration_date_lte: str | None = Query(default=None, description="Expiration on or before"),
    strike_price_gte: float | None = Query(default=None, description="Strike at or above"),
    strike_price_lte: float | None = Query(default=None, description="Strike at or below"),
    option_type: str | None = Query(default=None, description="call or put"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get full option chain with greeks for an underlying."""
    contracts = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_chain(
            underlying=underlying.upper(),
            expiration_date=expiration_date,
            expiration_gte=expiration_date_gte,
            expiration_lte=expiration_date_lte,
            strike_gte=strike_price_gte,
            strike_lte=strike_price_lte,
            option_type=option_type,
        ),
        block=True,
    )

    return {
        "success": True,
        "data": {
            "underlying": underlying.upper(),
            "contracts": [c.model_dump() for c in contracts],
        },
        "meta": {
            "count": len(contracts),
            "provider": "alpaca",
        },
    }


@router.get("/options/chain/{underlying}/snapshot", response_model=SuccessResponse)
async def get_option_chain_snapshot(
    underlying: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current option chain snapshot for an underlying."""
    today = date_type.today()
    contracts = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_chain(
            underlying=underlying.upper(),
            expiration_gte=today.isoformat(),
            limit=100,
        ),
        block=True,
    )

    return {
        "success": True,
        "data": {
            "underlying": underlying.upper(),
            "contracts": [c.model_dump() for c in contracts],
        },
        "meta": {
            "count": len(contracts),
            "total_available": len(contracts),
            "provider": "alpaca",
        },
    }


@router.get("/options/{contract}/bars", response_model=SuccessResponse)
async def get_option_bars(
    contract: str,
    timeframe: str = Query(default="1Day", description=DESC_BAR_TIMEFRAME),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description=DESC_MAX_BARS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for an option contract."""
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(days=30)

    bars = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_bars(
            contracts=[contract.upper()],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": {
            "contract": contract.upper(),
            "timeframe": timeframe,
            "bars": [bar.model_dump() for bar in bars],
        },
        "meta": {
            "count": len(bars),
            "provider": "alpaca",
        },
    }


@router.get("/options/{contract}/quotes", response_model=SuccessResponse)
async def get_option_quotes(
    contract: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for an option contract."""
    quotes = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_quotes(contracts=[contract.upper()]),
        block=True,
    )
    if not quotes:
        raise HTTPException(status_code=404, detail=f"No quote found for {contract}")

    return {
        "success": True,
        "data": quotes[0].model_dump(),
        "meta": {"provider": "alpaca"},
    }


@router.get("/options/quotes", response_model=SuccessResponse)
async def get_option_quotes_batch(
    symbols: str = Query(..., description="Comma-separated option contracts"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quotes for a comma-separated batch of option contracts.

    This route preserves compatibility for downstream consumers that request
    `/options/quotes?symbols=...` and expect a symbol-keyed quote payload.
    """
    contracts = parse_comma_values(symbols, uppercase=True)
    quotes = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_quotes(contracts=contracts),
        block=True,
    )
    quotes_by_symbol = {quote.symbol: quote.model_dump(mode="json") for quote in quotes}
    if not quotes_by_symbol:
        raise HTTPException(status_code=404, detail="No quotes found for requested contracts")

    return {
        "success": True,
        "data": {"quotes": quotes_by_symbol},
        "meta": {
            "count": len(quotes_by_symbol),
            "requested": len(contracts),
            "provider": "alpaca",
        },
    }


@router.get("/options/trades", response_model=SuccessResponse)
async def get_options_trades(
    contracts: str = Query(..., description="Comma-separated option contracts"),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=1000, le=10000, description="Max trades"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for option contracts."""
    contracts_list = parse_comma_values(contracts, uppercase=True)
    trades = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_trades(contracts_list, start, end, limit),
    )
    return {
        "success": True,
        "data": [t.model_dump() for t in trades],
        "meta": {"count": len(trades), "provider": "alpaca"},
    }


@router.get("/options/trades/latest", response_model=SuccessResponse)
async def get_options_latest_trades(
    contracts: str = Query(..., description="Comma-separated option contracts"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trades for option contracts."""
    contracts_list = parse_comma_values(contracts, uppercase=True)
    trades = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_latest_trades(contracts_list),
    )
    return {
        "success": True,
        "data": [t.model_dump() for t in trades],
        "meta": {"count": len(trades), "provider": "alpaca"},
    }


@router.get("/options/snapshots/{underlying}", response_model=SuccessResponse)
async def get_options_snapshots(
    underlying: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get snapshots for all options on an underlying."""
    snapshots = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_option_snapshots(underlying),
    )
    return {
        "success": True,
        "data": snapshots,
        "meta": {
            "count": len(snapshots),
            "underlying": underlying.upper(),
            "provider": "alpaca",
        },
    }
