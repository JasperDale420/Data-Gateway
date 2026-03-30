"""Finnhub company profile and fundamentals endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.finnhub.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    datetime,
    execute_finnhub_cached,
    get_cache,
    get_registry,
    require_api_key,
)
from gateway.core.metrics import record_route_cache
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/profile/{symbol}", response_model=SuccessResponse)
async def get_company_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company profile information."""
    key = cache_key("finnhub:profile", symbol.upper())

    async def fetcher(provider):
        data = await provider.get_company_profile(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No profile for symbol: {symbol}")
        return data

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_company_profile", status, "finnhub")
    return response


@router.get("/financials/{symbol}", response_model=SuccessResponse)
async def get_financials(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get financial metrics (P/E, EPS, beta, etc)."""
    key = cache_key("finnhub:financials", symbol.upper())

    async def fetcher(provider):
        return await provider.get_financials(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_financials", status, "finnhub")
    return response


@router.get("/peers/{symbol}", response_model=SuccessResponse)
async def get_peers(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get peer companies for a symbol."""
    key = cache_key("finnhub:peers", symbol.upper())

    async def fetcher(provider):
        return await provider.get_peers(symbol)

    def cache_transform(data):
        return {"symbol": symbol.upper(), "peers": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_peers", status, "finnhub")
    return response


@router.get("/metrics/{symbol}", response_model=SuccessResponse)
async def get_metrics(
    symbol: str,
    metric: str = Query(default="all", description="all, margin, valuation, price, profitability"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get key financial metrics for a symbol."""
    key = cache_key("finnhub:metrics", symbol.upper(), metric)

    async def fetcher(provider):
        return await provider.get_metrics(symbol, metric=metric)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_metrics", status, "finnhub")
    return response


@router.get("/executives/{symbol}", response_model=SuccessResponse)
async def get_executives(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company executives and compensation."""
    key = cache_key("finnhub:executives", symbol.upper())

    async def fetcher(provider):
        return await provider.get_executives(symbol)

    def cache_transform(data):
        return {"symbol": symbol.upper(), "executives": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_executives", status, "finnhub")
    return response


@router.get("/ownership/{symbol}", response_model=SuccessResponse)
async def get_ownership(
    symbol: str,
    limit: int = Query(default=20, le=100, description="Max owners to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get institutional ownership data."""
    key = cache_key("finnhub:ownership", symbol.upper(), str(limit))

    async def fetcher(provider):
        return await provider.get_ownership(symbol, limit=limit)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_ownership", status, "finnhub")
    return response


@router.get("/fund-ownership/{symbol}", response_model=SuccessResponse)
async def get_fund_ownership(
    symbol: str,
    limit: int = Query(default=20, le=100, description="Max fund owners to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund and ETF ownership data."""
    key = cache_key("finnhub:fund-ownership", symbol.upper(), str(limit))

    async def fetcher(provider):
        return await provider.get_fund_ownership(symbol, limit=limit)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_fund_ownership", status, "finnhub")
    return response


@router.get("/insider-transactions/{symbol}", response_model=SuccessResponse)
async def get_insider_transactions(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider transactions (SEC Form 4)."""
    key = cache_key("finnhub:insider-tx", symbol.upper(), start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_insider_transactions(symbol, start=start_dt, end=end_dt)

    def cache_transform(data):
        return {"symbol": symbol.upper(), "transactions": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_insider_transactions", status, "finnhub")
    return response
