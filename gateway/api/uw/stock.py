"""Stock module analytics endpoints for Unusual Whales."""

from typing import Any

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


def _response(
    data: Any,
    *,
    symbol: str | None = None,
    include_count: bool = False,
    extra_meta: dict[str, Any] | None = None,
) -> dict:
    count = len(data) if include_count else None
    return make_response(data, symbol=symbol, count=count, extra_meta=extra_meta)


@router.get("/stock/{symbol}/info", response_model=SuccessResponse)
async def get_stock_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stock/ticker information."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:info:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_stock_info(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol),
    )


@router.get("/stock/{symbol}/candles", response_model=SuccessResponse)
async def get_stock_candles(
    symbol: str,
    timeframe: str = Query(default="1d", description="Timeframe (1m, 5m, 1h, 1d)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get OHLC candle data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:candles:{symbol}:{timeframe}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_stock_candles(symbol=symbol, timeframe=timeframe),
        build_response=lambda data: _response(
            data,
            symbol=symbol,
            include_count=True,
            extra_meta={"timeframe": timeframe},
        ),
    )


@router.get("/stock/{symbol}/option-chains", response_model=SuccessResponse)
async def get_stock_option_chains(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get tradeable option contracts for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:option-chains:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_stock_option_chains(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/option-contracts", response_model=SuccessResponse)
async def get_stock_option_contracts(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all option contracts for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:option-contracts:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_stock_option_contracts(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/oi-per-strike", response_model=SuccessResponse)
async def get_oi_per_strike(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get open interest per strike for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:oi-strike:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_oi_per_strike(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/oi-per-expiry", response_model=SuccessResponse)
async def get_oi_per_expiry(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get open interest per expiry for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:oi-expiry:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_oi_per_expiry(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/greeks-by-strike/{expiry}", response_model=SuccessResponse)
async def get_greeks_by_strike_expiry(
    symbol: str,
    expiry: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option greeks by strike for a specific expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:greeks-strike-expiry:{symbol}:{expiry}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_greeks_by_strike_expiry(symbol=symbol, expiry=expiry),
        build_response=lambda data: _response(
            data,
            symbol=symbol,
            include_count=True,
            extra_meta={"expiry": expiry},
        ),
    )


@router.get("/stock/{symbol}/greek-exposure-by-strike-expiry/{expiry}", response_model=SuccessResponse)
async def get_greek_exposure_by_strike_expiry(
    symbol: str,
    expiry: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek exposure by strike for a specific expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:greek-exp-strike-expiry:{symbol}:{expiry}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_greek_exposure_by_strike_expiry(
            symbol=symbol,
            expiry=expiry,
        ),
        build_response=lambda data: _response(
            data,
            symbol=symbol,
            include_count=True,
            extra_meta={"expiry": expiry},
        ),
    )


@router.get("/stock/{symbol}/atm-options", response_model=SuccessResponse)
async def get_atm_option_contracts(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ATM option contracts for all expiries."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:atm-options:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_atm_option_contracts(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/daily-expiry-breakdown", response_model=SuccessResponse)
async def get_daily_expiry_breakdown(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option order flow grouped by expiry for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:daily-expiry-breakdown:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_daily_expiry_breakdown(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/flow-per-strike-intraday", response_model=SuccessResponse)
async def get_flow_per_strike_intraday(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow per strike for intraday data."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:flow-strike-intraday:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_flow_per_strike_intraday(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/risk-reversal-skew/{expiry}", response_model=SuccessResponse)
async def get_risk_reversal_skew(
    symbol: str,
    expiry: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical risk reversal skew by expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:risk-reversal:{symbol}:{expiry}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_risk_reversal_skew(symbol=symbol, expiry=expiry),
        build_response=lambda data: _response(
            data,
            symbol=symbol,
            include_count=True,
            extra_meta={"expiry": expiry},
        ),
    )


@router.get("/sectors/{sector}/tickers", response_model=SuccessResponse)
async def get_sector_tickers(
    sector: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get tickers for a given sector."""
    cache_key = f"uw:sectors:tickers:{sector}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_sector_tickers(sector=sector),
        build_response=lambda data: _response(
            data,
            include_count=True,
            extra_meta={"sector": sector},
        ),
    )


@router.get("/stock/{symbol}/state", response_model=SuccessResponse)
async def get_stock_state(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stock OHLC and volume state."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:state:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_stock_state(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol),
    )


@router.get("/stock/{symbol}/volume-price-levels", response_model=SuccessResponse)
async def get_stock_volume_price_levels(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stock volume price levels."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:volume-price-levels:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_stock_volume_price_levels(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/option-volume-by-price", response_model=SuccessResponse)
async def get_option_volume_by_price_level(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get call and put volume per price level."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:option-volume-price:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_option_volume_by_price_level(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/volume-oi-by-expiry", response_model=SuccessResponse)
async def get_volume_oi_by_expiry(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volume and OI per expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:volume-oi-expiry:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_volume_oi_by_expiry(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/spot-exposures", response_model=SuccessResponse)
async def get_spot_exposures(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get spot GEX exposures per minute."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:spot-exposures:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_spot_exposures(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/options-volume", response_model=SuccessResponse)
async def get_options_volume(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get options volume and premium for a trading date."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:options-volume:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_options_volume(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/greek-flow-by-expiry/{expiry}", response_model=SuccessResponse)
async def get_greek_flow_by_expiry(
    symbol: str,
    expiry: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek flow by expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:greek-flow-expiry:{symbol}:{expiry}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_greek_flow_by_expiry(symbol=symbol, expiry=expiry),
        build_response=lambda data: _response(
            data,
            symbol=symbol,
            include_count=True,
            extra_meta={"expiry": expiry},
        ),
    )


@router.get("/stock/{symbol}/insider-trades", response_model=SuccessResponse)
async def get_stock_insider_trades(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider trades for a specific stock."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:insider-trades:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_stock_insider_trades(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )


@router.get("/stock/{symbol}/spot-exposures-by-expiry-strike/{expiry}", response_model=SuccessResponse)
async def get_spot_exposures_by_expiry_strike(
    symbol: str,
    expiry: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get spot GEX exposures by expiry and strike."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:spot-exposures-expiry-strike:{symbol}:{expiry}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_spot_exposures_by_expiry_strike(
            symbol=symbol,
            expiry=expiry,
        ),
        build_response=lambda data: _response(
            data,
            symbol=symbol,
            include_count=True,
            extra_meta={"expiry": expiry},
        ),
    )


@router.get("/stock/{symbol}/flow-recent", response_model=SuccessResponse)
async def get_flow_recent(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recent flow for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:stock:flow-recent:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=30,
        fetcher=lambda provider: provider.get_flow_recent(symbol=symbol),
        build_response=lambda data: _response(data, symbol=symbol, include_count=True),
    )
