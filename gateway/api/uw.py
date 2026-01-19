"""Unusual Whales API endpoints (PRD-aligned)."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/uw", tags=["unusual_whales"])

# Query description constants
DESC_DATE = "Date (YYYY-MM-DD)"

# Error message constants
PROVIDER_NOT_AVAILABLE = "Unusual Whales provider not available"


def _paginate_response(
    data: list,
    limit: int,
    cursor: str | None = None,
) -> dict:
    """Build paginated response per PRD spec."""
    # Simple cursor-based pagination (offset encoded in cursor)
    offset = 0
    if cursor:
        try:
            import base64

            offset = int(base64.b64decode(cursor).decode())
        except Exception:
            offset = 0

    total_count = len(data)
    has_more = total_count > limit
    paginated_data = data[:limit]

    next_cursor = None
    if has_more:
        import base64

        next_cursor = base64.b64encode(str(offset + limit).encode()).decode()

    return {
        "success": True,
        "data": paginated_data,
        "pagination": {
            "next_cursor": next_cursor,
            "has_more": has_more,
            "total_count": total_count,
        },
    }


@router.get("/flow/all", response_model=SuccessResponse)
async def get_flow_all(
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None, description="Pagination cursor"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent options flow alerts."""
    cache_key = f"uw:flow:all:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    alerts = await provider.get_flow_alerts(limit=limit + 1)  # +1 to check has_more
    data = [a.model_dump(mode="json") for a in alerts]

    response = _paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/flow/{symbol}", response_model=SuccessResponse)
async def get_flow_symbol(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get options flow for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:flow:{symbol}:{limit}:{date or 'latest'}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    alerts = await provider.get_ticker_flow(symbol=symbol, date_str=date)
    data = [a.model_dump(mode="json") for a in alerts]

    response = _paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/darkpool/all", response_model=SuccessResponse)
async def get_darkpool_all(
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent darkpool trades."""
    cache_key = f"uw:darkpool:all:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    trades = await provider.get_darkpool_recent(limit=limit + 1)
    data = [t.model_dump(mode="json") for t in trades]

    response = _paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/darkpool/{symbol}", response_model=SuccessResponse)
async def get_darkpool_symbol(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get darkpool trades for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:darkpool:{symbol}:{limit}:{date or 'latest'}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    trades = await provider.get_darkpool_ticker(symbol=symbol, date_str=date)
    data = [t.model_dump(mode="json") for t in trades]

    response = _paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/institutions/{symbol}", response_model=SuccessResponse)
async def get_institutions(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get 13F institutional holdings for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:institutions:{symbol}:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    holdings = await provider.get_institutions(symbol=symbol)

    response = _paginate_response(holdings, limit, cursor)
    cache.set(cache_key, response, ttl=300)  # 5min cache for institutions
    return response


@router.get("/congress/{symbol}", response_model=SuccessResponse)
async def get_congress(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trades for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:congress:{symbol}:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    trades = await provider.get_congress_trades(symbol=symbol)

    response = _paginate_response(trades, limit, cursor)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/insiders/{symbol}", response_model=SuccessResponse)
async def get_insiders(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider transactions for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:insiders:{symbol}:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    transactions = await provider.get_insiders(symbol=symbol)

    response = _paginate_response(transactions, limit, cursor)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/tide", response_model=SuccessResponse)
async def get_market_tide(
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide/sentiment data."""
    cache_key = f"uw:market:tide:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    tides = await provider.get_market_tide(date_str=date)

    response = {
        "success": True,
        "data": [t.model_dump(mode="json") for t in tides],
        "pagination": {
            "next_cursor": None,
            "has_more": False,
            "total_count": len(tides),
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 1: Greek Exposure (GEX) Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/gex/{symbol}", response_model=SuccessResponse)
async def get_gex(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get Greek exposure (GEX) data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:gex:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_exposure(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "symbol": symbol,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/gex/{symbol}/strike", response_model=SuccessResponse)
async def get_gex_by_strike(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get Greek exposure by strike price for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:gex:strike:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_exposure_by_strike(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "symbol": symbol,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/gex/{symbol}/expiry", response_model=SuccessResponse)
async def get_gex_by_expiry(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get Greek exposure by expiration date for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:gex:expiry:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_exposure_by_expiry(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "symbol": symbol,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 1: Earnings Calendar Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/earnings/premarket", response_model=SuccessResponse)
async def get_earnings_premarket(
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get premarket earnings calendar."""
    cache_key = f"uw:earnings:premarket:{date or 'latest'}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_earnings_premarket(date_str=date)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/earnings/afterhours", response_model=SuccessResponse)
async def get_earnings_afterhours(
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get afterhours earnings calendar."""
    cache_key = f"uw:earnings:afterhours:{date or 'latest'}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_earnings_afterhours(date_str=date)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/earnings/{symbol}", response_model=SuccessResponse)
async def get_earnings_ticker(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical earnings for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:earnings:{symbol}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_earnings_ticker(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 1: Screener Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/screener/stocks", response_model=SuccessResponse)
async def get_screener_stocks(
    limit: int = Query(default=20, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stock screener results."""
    cache_key = f"uw:screener:stocks:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_screener(limit=limit)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/screener/options", response_model=SuccessResponse)
async def get_screener_options(
    limit: int = Query(default=20, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get hottest option chains/contracts."""
    cache_key = f"uw:screener:options:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_hottest_chains(limit=limit)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 2: Options Analytics Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/{symbol}/net-premium", response_model=SuccessResponse)
async def get_net_premium(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get net premium ticks for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:net-premium:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_net_premium_ticks(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/max-pain", response_model=SuccessResponse)
async def get_max_pain(
    symbol: str,
    expiry: str | None = Query(default=None, description="Expiration (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get max pain data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:max-pain:{symbol}:{expiry or 'all'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_max_pain(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/iv-rank", response_model=SuccessResponse)
async def get_iv_rank(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV rank for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-rank:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_iv_rank(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"IV rank not found for {symbol}")

    response = {
        "success": True,
        "data": data.model_dump(mode="json"),
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/oi-change", response_model=SuccessResponse)
async def get_oi_change(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get OI change data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:oi-change:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_oi_change(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 2: ETF Analytics Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/etf/{symbol}/holdings", response_model=SuccessResponse)
async def get_etf_holdings(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF holdings."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:holdings:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_holdings(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/etf/{symbol}/exposure", response_model=SuccessResponse)
async def get_etf_exposure(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF exposure for a stock (which ETFs hold it)."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:exposure:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_exposure(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/etf/{symbol}/flows", response_model=SuccessResponse)
async def get_etf_flows(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF inflow/outflow data."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:flows:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_flows(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 2: Shorts Data Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/{symbol}/short-interest", response_model=SuccessResponse)
async def get_short_interest(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short interest data."""
    symbol = symbol.upper()
    cache_key = f"uw:short-interest:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_interest(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/ftds", response_model=SuccessResponse)
async def get_ftds(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get failures to deliver data."""
    symbol = symbol.upper()
    cache_key = f"uw:ftds:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_ftds(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/short-volume", response_model=SuccessResponse)
async def get_short_volume(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short volume data."""
    symbol = symbol.upper()
    cache_key = f"uw:short-volume:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_volume(symbol=symbol)

    response = _paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 3: Volatility Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/{symbol}/iv-term-structure", response_model=SuccessResponse)
async def get_iv_term_structure(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV term structure for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-term-structure:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_iv_term_structure(symbol=symbol)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/realized-vol", response_model=SuccessResponse)
async def get_realized_vol(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get realized volatility for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:realized-vol:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_realized_volatility(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"Volatility data not found for {symbol}")

    response = {
        "success": True,
        "data": data.model_dump(mode="json"),
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/vol-stats", response_model=SuccessResponse)
async def get_vol_stats(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volatility stats for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:vol-stats:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_volatility_stats(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"Volatility stats not found for {symbol}")

    response = {
        "success": True,
        "data": data.model_dump(mode="json"),
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 3: Seasonality Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/seasonality/market", response_model=SuccessResponse)
async def get_market_seasonality(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market-wide seasonality data."""
    cache_key = "uw:seasonality:market"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_seasonality()

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/seasonality/{symbol}", response_model=SuccessResponse)
async def get_ticker_seasonality(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get monthly returns/seasonality for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:seasonality:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_monthly_returns(symbol=symbol)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 5: Options Data
# ─────────────────────────────────────────────────────────────────


@router.get("/{symbol}/option-volume", response_model=SuccessResponse)
async def get_historic_option_volume(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historic option volume and open interest by expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:option-volume:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_historic_option_volume(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/contract/{contract_id}/intraday", response_model=SuccessResponse)
async def get_intraday_option_data(
    contract_id: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday 1-min OHLC data for an option contract."""
    cache_key = f"uw:intraday:{contract_id}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_intraday_option_data(contract_id=contract_id, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "contract": contract_id,
            "date": date,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/screener/contracts", response_model=SuccessResponse)
async def get_options_screener(
    min_volume: int | None = Query(default=None, description="Min volume"),
    min_premium: float | None = Query(default=None, description="Min premium"),
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option contracts screener with filters."""
    cache_key = f"uw:contracts-screener:{min_volume}:{min_premium}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_options_screener(
        min_volume=min_volume, min_premium=min_premium, limit=limit
    )

    response = _paginate_response(data, limit)
    response["meta"] = {"count": len(data), "provider": "unusual_whales"}

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/iv-surface", response_model=SuccessResponse)
async def get_iv_surface(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get implied volatility surface for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-surface:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_iv_surface(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 6: Market Intelligence
# ─────────────────────────────────────────────────────────────────


@router.get("/darkpool/{symbol}/levels", response_model=SuccessResponse)
async def get_off_lit_levels(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get dark pool volume per price level."""
    symbol = symbol.upper()
    cache_key = f"uw:off-lit-levels:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_off_lit_levels(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/congress/recent", response_model=SuccessResponse)
async def get_recent_congress_trades(
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recent congressional trades across all tickers."""
    cache_key = f"uw:congress-recent:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_recent_congress_trades(limit=limit)

    response = _paginate_response(data, limit)
    response["meta"] = {"count": len(data), "provider": "unusual_whales"}

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/nope", response_model=SuccessResponse)
async def get_nope(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get NOPE (Net Options Pricing Effect) for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:nope:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_nope(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"NOPE data not found for {symbol}")

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/pc-ratio", response_model=SuccessResponse)
async def get_put_call_ratio(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical put/call ratio for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:pc-ratio:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_put_call_ratio(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 7: Flow Analytics
# ─────────────────────────────────────────────────────────────────


@router.get("/{symbol}/spot-exposures", response_model=SuccessResponse)
async def get_spot_exposures_by_strike(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get gamma and volume confluence per strike."""
    symbol = symbol.upper()
    cache_key = f"uw:spot-exposures:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_spot_exposures_by_strike(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/flow-strike", response_model=SuccessResponse)
async def get_flow_per_strike(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow data aggregated by strike price."""
    symbol = symbol.upper()
    cache_key = f"uw:flow-strike:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_per_strike(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/flow-expiry", response_model=SuccessResponse)
async def get_flow_per_expiry(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow data aggregated by expiration date."""
    symbol = symbol.upper()
    cache_key = f"uw:flow-expiry:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_per_expiry(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/greek-flow", response_model=SuccessResponse)
async def get_greek_flow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek flow data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:greek-flow:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_flow(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/net-flow-expiry", response_model=SuccessResponse)
async def get_net_flow_expiry(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get net premium flow by expiration category."""
    cache_key = "uw:net-flow-expiry"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_net_flow_expiry()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/interpolated-iv", response_model=SuccessResponse)
async def get_interpolated_iv(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get interpolated implied volatility."""
    symbol = symbol.upper()
    cache_key = f"uw:interpolated-iv:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_interpolated_iv(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 8: Market Intelligence
# ─────────────────────────────────────────────────────────────────


@router.get("/market/calendar", response_model=SuccessResponse)
async def get_economic_calendar(
    start_date: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get economic calendar events."""
    cache_key = f"uw:calendar:{start_date or 'start'}:{end_date or 'end'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_economic_calendar(start_date=start_date, end_date=end_date)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/market/sector/{sector}/tide", response_model=SuccessResponse)
async def get_sector_tide(
    sector: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide for a specific sector."""
    cache_key = f"uw:sector-tide:{sector}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_sector_tide(sector=sector)

    if not data:
        raise HTTPException(status_code=404, detail=f"Sector {sector} not found")

    response = {
        "success": True,
        "data": data,
        "meta": {"sector": sector, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/top-impact", response_model=SuccessResponse)
async def get_top_net_impact(
    limit: int = Query(default=20, le=100),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get top tickers by net premium (bullish and bearish)."""
    cache_key = f"uw:top-impact:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_top_net_impact(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "bullish_count": len(data.get("bullish", [])),
            "bearish_count": len(data.get("bearish", [])),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/alerts", response_model=SuccessResponse)
async def get_custom_alerts(
    min_premium: float | None = Query(default=None, description="Min premium"),
    min_volume: int | None = Query(default=None, description="Min volume"),
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get custom filtered flow alerts."""
    cache_key = f"uw:alerts:{min_premium}:{min_volume}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_custom_alerts(
        min_premium=min_premium, min_volume=min_volume, limit=limit
    )

    response = _paginate_response(data, limit)
    response["meta"] = {"count": len(data), "provider": "unusual_whales"}

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/market/correlations", response_model=SuccessResponse)
async def get_market_correlations(
    start_date: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get cross-asset correlations."""
    cache_key = f"uw:correlations:{start_date or 'start'}:{end_date or 'end'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_correlations(start_date=start_date, end_date=end_date)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 9: Final Gaps
# ─────────────────────────────────────────────────────────────────


@router.get("/etf/{symbol}/tide", response_model=SuccessResponse)
async def get_etf_tide(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF-level tide data (premium flow sentiment)."""
    symbol = symbol.upper()
    cache_key = f"uw:etf-tide:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_tide(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"ETF tide not found for {symbol}")

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/volume-levels", response_model=SuccessResponse)
async def get_option_volume_levels(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option volume levels per price."""
    symbol = symbol.upper()
    cache_key = f"uw:volume-levels:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_volume_levels(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/trades/full-tape/{date}", response_model=SuccessResponse)
async def get_full_tape(
    date: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full options trade tape for a date."""
    cache_key = f"uw:full-tape:{date}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_full_tape(date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/contract/{contract_id}/volume-profile", response_model=SuccessResponse)
async def get_volume_profile(
    contract_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volume profile of an option contract by fill price."""
    cache_key = f"uw:volume-profile:{contract_id}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_volume_profile(contract_id=contract_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"contract": contract_id, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/greek-flow-expiry", response_model=SuccessResponse)
async def get_greek_flow_expiry(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek flow data aggregated by expiration."""
    symbol = symbol.upper()
    cache_key = f"uw:greek-flow-expiry:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_flow_expiry(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 1: News Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/news/headlines", response_model=SuccessResponse)
async def get_news_headlines(
    sources: str | None = Query(default=None, description="Comma-separated sources"),
    search_term: str | None = Query(default=None, description="Search term"),
    major_only: bool | None = Query(default=None, description="Major news only"),
    limit: int = Query(default=50, le=200, ge=1),
    page: int | None = Query(default=None, description="Page number"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest financial news headlines."""
    cache_key = f"uw:news:headlines:{sources or 'all'}:{search_term or 'none'}:{major_only}:{limit}:{page or 1}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    sources_list = sources.split(",") if sources else None
    data = await provider.get_news_headlines(
        sources=sources_list,
        search_term=search_term,
        major_only=major_only,
        limit=limit,
        page=page,
    )

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 1: Politician Portfolios Endpoints (Enterprise-only)
# ─────────────────────────────────────────────────────────────────


@router.get("/politicians/people", response_model=SuccessResponse)
async def get_politician_people(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all politician names and IDs."""
    cache_key = "uw:politicians:people"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_people()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)  # 1 hour cache
    return response


@router.get("/politicians/recent-trades", response_model=SuccessResponse)
async def get_politician_recent_trades(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest transacted trades by congress members."""
    cache_key = f"uw:politicians:trades:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_recent_trades(limit=limit)

    response = _paginate_response(data, limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/politicians/{politician_id}/portfolios", response_model=SuccessResponse)
async def get_politician_portfolios(
    politician_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all portfolios and holdings for a politician."""
    cache_key = f"uw:politicians:{politician_id}:portfolios"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_portfolios(politician_id=politician_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"politician_id": politician_id, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/politicians/{symbol}/holders", response_model=SuccessResponse)
async def get_politician_holders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get politician portfolio holders for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:politicians:holders:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_holders(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 2: Market Calendar & Data Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/market/economic-calendar", response_model=SuccessResponse)
async def get_economic_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get economic calendar events."""
    cache_key = "uw:market:economic-calendar"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_economic_calendar()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/market/fda-calendar", response_model=SuccessResponse)
async def get_fda_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get FDA calendar events."""
    cache_key = "uw:market:fda-calendar"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_fda_calendar()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/market/holidays", response_model=SuccessResponse)
async def get_market_holidays(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market holidays."""
    cache_key = "uw:market:holidays"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_holidays()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=86400)  # 24 hour cache
    return response


@router.get("/market/imbalances", response_model=SuccessResponse)
async def get_market_imbalances(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market imbalances data."""
    cache_key = "uw:market:imbalances"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_imbalances()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/market/options-volume", response_model=SuccessResponse)
async def get_market_options_volume(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get total market options volume."""
    cache_key = "uw:market:options-volume"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_options_volume()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/market/insider-trades", response_model=SuccessResponse)
async def get_market_insider_trades(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market-wide insider trades."""
    cache_key = "uw:market:insider-trades"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_insider_trades()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/sector-stats", response_model=SuccessResponse)
async def get_sector_stats(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get sector statistics."""
    cache_key = "uw:market:sector-stats"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_sector_stats()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/{etf}/etf-tide", response_model=SuccessResponse)
async def get_market_tide_by_etf(
    etf: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide data for a specific ETF."""
    etf = etf.upper()
    cache_key = f"uw:market:etf-tide:{etf}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_tide_by_etf(etf=etf)

    response = {
        "success": True,
        "data": data,
        "meta": {"etf": etf, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 3: Institution Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/institutions", response_model=SuccessResponse)
async def get_all_institutions(
    limit: int = Query(default=100, le=500, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get list of all institutions."""
    cache_key = f"uw:institutions:all:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_all_institutions(limit=limit)

    response = _paginate_response(data, limit)
    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/latest-filings", response_model=SuccessResponse)
async def get_latest_institutional_filings(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest institutional filings."""
    cache_key = f"uw:institutions:filings:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_latest_institutional_filings(limit=limit)

    response = _paginate_response(data, limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/institutions/{institution_id}/activity", response_model=SuccessResponse)
async def get_institution_activity(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest activity by an institution."""
    cache_key = f"uw:institutions:{institution_id}:activity"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institution_activity(institution_id=institution_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"institution_id": institution_id, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/{institution_id}/holdings", response_model=SuccessResponse)
async def get_institution_holdings(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get current holdings of an institution."""
    cache_key = f"uw:institutions:{institution_id}:holdings"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institution_holdings(institution_id=institution_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"institution_id": institution_id, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/{institution_id}/sectors", response_model=SuccessResponse)
async def get_institution_sector_exposure(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get sector exposure of an institution."""
    cache_key = f"uw:institutions:{institution_id}:sectors"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institution_sector_exposure(institution_id=institution_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"institution_id": institution_id, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/{symbol}/ownership", response_model=SuccessResponse)
async def get_institutional_ownership(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get institutional ownership for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:institutions:ownership:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institutional_ownership(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 3: Insider Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/insider/transactions", response_model=SuccessResponse)
async def get_insider_transactions(
    limit: int = Query(default=100, le=500, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent insider transactions."""
    cache_key = f"uw:insider:transactions:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_insider_transactions(limit=limit)

    response = _paginate_response(data, limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/insider/sector-flow", response_model=SuccessResponse)
async def get_insider_sector_flow(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider trading flow by sector."""
    cache_key = "uw:insider:sector-flow"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_insider_sector_flow()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/insider/ticker-flow", response_model=SuccessResponse)
async def get_insider_ticker_flow(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider trading flow by ticker."""
    cache_key = "uw:insider:ticker-flow"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_insider_ticker_flow()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/insider/{symbol}/insiders", response_model=SuccessResponse)
async def get_ticker_insiders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insiders for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:insider:insiders:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_ticker_insiders(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 5: Additional ETF Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/etf/{symbol}/info", response_model=SuccessResponse)
async def get_etf_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF information."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:info:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_info(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/etf/{symbol}/inflow-outflow", response_model=SuccessResponse)
async def get_etf_inflow_outflow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF inflow/outflow data."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:inflow-outflow:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_inflow_outflow(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/etf/{symbol}/ticker-exposure", response_model=SuccessResponse)
async def get_etf_ticker_exposure(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ticker exposure within an ETF."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:ticker-exposure:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_ticker_exposure(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/etf/{symbol}/country-weights", response_model=SuccessResponse)
async def get_etf_country_weights(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF country weights."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:country-weights:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_country_weights(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 5: Screener/Alerts Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/screener/analysts", response_model=SuccessResponse)
async def get_analyst_ratings(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst ratings from screener."""
    cache_key = f"uw:screener:analysts:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_analyst_ratings(limit=limit)

    response = _paginate_response(data, limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/alerts/all", response_model=SuccessResponse)
async def get_all_alerts(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all alerts."""
    cache_key = f"uw:alerts:all:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_all_alerts(limit=limit)

    response = _paginate_response(data, limit)
    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/alerts/configuration", response_model=SuccessResponse)
async def get_alerts_configuration(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get alerts configuration."""
    cache_key = "uw:alerts:configuration"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_alerts_configuration()

    response = {
        "success": True,
        "data": data,
        "meta": {"provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


# ─────────────────────────────────────────────────────────────────
# Phase 4: Stock Module - Additional Analytics Endpoints
# ─────────────────────────────────────────────────────────────────


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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_info(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_candles(symbol=symbol, timeframe=timeframe)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "timeframe": timeframe, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_option_chains(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_option_contracts(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_oi_per_strike(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_oi_per_expiry(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greeks_by_strike_expiry(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "expiry": expiry, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_exposure_by_strike_expiry(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "expiry": expiry, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_atm_option_contracts(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_daily_expiry_breakdown(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_per_strike_intraday(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_risk_reversal_skew(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "expiry": expiry, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_sector_tickers(sector=sector)

    response = {
        "success": True,
        "data": data,
        "meta": {"sector": sector, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_state(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_volume_price_levels(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_volume_by_price_level(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_volume_oi_by_expiry(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_spot_exposures(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_options_volume(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_flow_by_expiry(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "expiry": expiry, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_insider_trades(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


# ─────────────────────────────────────────────────────────────────
# Final Batch: Remaining SDK Endpoints for True 100% Coverage
# ─────────────────────────────────────────────────────────────────


@router.get("/option-contract/{option_symbol}/flow", response_model=SuccessResponse)
async def get_option_contract_flow(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow for a specific option contract."""
    cache_key = f"uw:option-contract:flow:{option_symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_flow(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/option-contract/{option_symbol}/historic", response_model=SuccessResponse)
async def get_option_contract_historic(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historic data for a specific option contract."""
    cache_key = f"uw:option-contract:historic:{option_symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_historic(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/option-contract/{option_symbol}/intraday", response_model=SuccessResponse)
async def get_option_contract_intraday(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday data for a specific option contract."""
    cache_key = f"uw:option-contract:intraday:{option_symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_intraday(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/option-contract/{option_symbol}/volume-profile", response_model=SuccessResponse)
async def get_option_contract_volume_profile(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volume profile for a specific option contract."""
    cache_key = f"uw:option-contract:volume-profile:{option_symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_volume_profile(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/contract/{option_symbol}/price-history", response_model=SuccessResponse)
async def get_contract_price_history(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get price history for an option contract."""
    cache_key = f"uw:contract:price-history:{option_symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_contract_price_history(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/congress/late-reports", response_model=SuccessResponse)
async def get_congress_late_reports(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get late congressional trading reports."""
    cache_key = f"uw:congress:late-reports:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_congress_late_reports(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/congress/reports", response_model=SuccessResponse)
async def get_congress_reports(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trading reports."""
    cache_key = f"uw:congress:reports:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_congress_reports(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/flow/contract/{option_symbol}", response_model=SuccessResponse)
async def get_contract_flow(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow for a specific contract."""
    cache_key = f"uw:flow:contract:{option_symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_contract_flow(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/flow/full-tape", response_model=SuccessResponse)
async def get_full_tape(
    limit: int = Query(default=100, ge=1, le=1000, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full options tape."""
    cache_key = f"uw:flow:full-tape:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_full_tape(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/screener/option-contracts", response_model=SuccessResponse)
async def get_screener_option_contracts(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option contracts from screener."""
    cache_key = f"uw:screener:option-contracts:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_screener_option_contracts(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/screener/stocks", response_model=SuccessResponse)
async def get_screener_stocks(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stocks from screener."""
    cache_key = f"uw:screener:stocks:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_screener_stocks(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/seasonality/monthly-top-performers/{month}", response_model=SuccessResponse)
async def get_monthly_top_performers(
    month: int,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get top performing stocks by month (1-12)."""
    cache_key = f"uw:seasonality:monthly-top-performers:{month}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_monthly_top_performers(month=month)

    response = {
        "success": True,
        "data": data,
        "meta": {"month": month, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/seasonality/{symbol}/price-changes-by-month", response_model=SuccessResponse)
async def get_price_changes_by_month_year(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get price changes by month and year for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:seasonality:price-changes:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_price_changes_by_month_year(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/shorts/{symbol}/data", response_model=SuccessResponse)
async def get_shorts_data(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:data:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_shorts_data(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/shorts/{symbol}/interest-float", response_model=SuccessResponse)
async def get_short_interest_float(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short interest as percentage of float."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:interest-float:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_interest_float(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/shorts/{symbol}/volumes-by-exchange", response_model=SuccessResponse)
async def get_short_volumes_by_exchange(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short volumes by exchange."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:volumes-by-exchange:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_volumes_by_exchange(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/spike", response_model=SuccessResponse)
async def get_market_spike(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market spike data."""
    cache_key = "uw:market:spike"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_spike()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_spot_exposures_by_expiry_strike(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "expiry": expiry, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_recent(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=30)
    return response
