"""Finnhub company profile and fundamentals endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.finnhub.common import (
    PROVIDER_NOT_AVAILABLE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    datetime,
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
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
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:profile", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_company_profile(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No profile for symbol: {symbol}")

        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/financials/{symbol}", response_model=SuccessResponse)
async def get_financials(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get financial metrics (P/E, EPS, beta, etc)."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:financials", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_financials(symbol)
        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/peers/{symbol}", response_model=SuccessResponse)
async def get_peers(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get peer companies for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:peers", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        peers = await provider.get_peers(symbol)
        data = {"symbol": symbol.upper(), "peers": peers}
        await cache.set(key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(peers), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/metrics/{symbol}", response_model=SuccessResponse)
async def get_metrics(
    symbol: str,
    metric: str = Query(default="all", description="all, margin, valuation, price, profitability"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get key financial metrics for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:metrics", symbol.upper(), metric)
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_metrics(symbol, metric=metric)
        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/executives/{symbol}", response_model=SuccessResponse)
async def get_executives(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company executives and compensation."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:executives", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        execs = await provider.get_executives(symbol)
        data = {"symbol": symbol.upper(), "executives": execs}
        await cache.set(key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(execs), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ownership/{symbol}", response_model=SuccessResponse)
async def get_ownership(
    symbol: str,
    limit: int = Query(default=20, le=100, description="Max owners to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get institutional ownership data."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:ownership", symbol.upper(), str(limit))
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_ownership(symbol, limit=limit)
        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/fund-ownership/{symbol}", response_model=SuccessResponse)
async def get_fund_ownership(
    symbol: str,
    limit: int = Query(default=20, le=100, description="Max fund owners to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund and ETF ownership data."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:fund-ownership", symbol.upper(), str(limit))
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_fund_ownership(symbol, limit=limit)
        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


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
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:insider-tx", symbol.upper(), start, end)
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        txs = await provider.get_insider_transactions(symbol, start=start_dt, end=end_dt)
        data = {"symbol": symbol.upper(), "transactions": txs}
        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(txs), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
