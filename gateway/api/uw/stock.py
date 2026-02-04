"""Stock module analytics endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    get_cache,
    get_registry,
    get_uw_provider,
    require_api_key,
    require_provider_rate_limit,
)

router = APIRouter(tags=["unusual_whales"])


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_info(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_candles(symbol=symbol, timeframe=timeframe)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_option_chains(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_option_contracts(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_oi_per_strike(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_oi_per_expiry(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greeks_by_strike_expiry(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "symbol": symbol,
            "expiry": expiry,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get(
    "/stock/{symbol}/greek-exposure-by-strike-expiry/{expiry}", response_model=SuccessResponse
)
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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_exposure_by_strike_expiry(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "symbol": symbol,
            "expiry": expiry,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_atm_option_contracts(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_daily_expiry_breakdown(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_per_strike_intraday(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_risk_reversal_skew(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "symbol": symbol,
            "expiry": expiry,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/sectors/{sector}/tickers", response_model=SuccessResponse)
async def get_sector_tickers(
    sector: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get tickers for a given sector."""
    cache_key = f"uw:sectors:tickers:{sector}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_sector_tickers(sector=sector)

    response = {
        "success": True,
        "data": data,
        "meta": {"sector": sector, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_state(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_volume_price_levels(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_volume_by_price_level(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_volume_oi_by_expiry(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_spot_exposures(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_options_volume(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_flow_by_expiry(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "symbol": symbol,
            "expiry": expiry,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=300)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_insider_trades(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get(
    "/stock/{symbol}/spot-exposures-by-expiry-strike/{expiry}", response_model=SuccessResponse
)
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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_spot_exposures_by_expiry_strike(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "symbol": symbol,
            "expiry": expiry,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_recent(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=30)
    return response
