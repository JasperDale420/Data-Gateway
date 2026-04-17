"""FastAPI dependency injection and shared route utilities."""

from collections.abc import Awaitable, Callable
from functools import lru_cache
from typing import Any

import httpx
from fastapi import Depends, Header, HTTPException, Request

from gateway.config import get_settings
from gateway.core.audit import AuditLogger
from gateway.core.auth import Client, ClientAuthenticator
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.connections import ConnectionManager

# Re-exported from core.globals for backward compatibility.
# All API-layer callers import these from here.
from gateway.core.globals import (
    get_multiplexer,  # noqa: F401
    get_registry,  # noqa: F401
    get_sink_registry,  # noqa: F401
    set_multiplexer,  # noqa: F401
    set_registry,  # noqa: F401
    set_sink_registry,  # noqa: F401
)
from gateway.core.logger import logger
from gateway.core.rate_limiter import EndpointRateLimiter


@lru_cache
def get_authenticator() -> ClientAuthenticator:
    """Get cached client authenticator."""
    settings = get_settings()
    return ClientAuthenticator(settings.clients_config_path)


@lru_cache
def get_cache() -> InMemoryCache | HybridCache:
    """Get cached cache instance.

    Returns HybridCache (L1 memory + L2 Redis) if Redis is configured,
    otherwise returns InMemoryCache.
    """
    settings = get_settings()
    if settings.cache_redis_enabled and settings.cache_redis_url:
        return HybridCache(
            redis_url=settings.cache_redis_url,
            max_size=settings.cache_max_size,
            default_ttl=settings.cache_default_ttl,
        )
    return InMemoryCache(
        max_size=settings.cache_max_size,
        default_ttl=settings.cache_default_ttl,
    )


@lru_cache
def get_connection_manager() -> ConnectionManager:
    """Get cached connection manager."""
    settings = get_settings()
    return ConnectionManager(max_clients=settings.ws_max_clients)


def get_endpoint_rate_limiter() -> EndpointRateLimiter:
    """Get the endpoint rate limiter singleton."""
    return EndpointRateLimiter.get_instance()


def get_audit_logger() -> AuditLogger:
    """Get the audit logger singleton."""
    return AuditLogger.get_instance()


def require_api_key(
    request: Request,
    x_gateway_key: str | None = Header(None, alias="X-Gateway-Key"),
    authenticator: ClientAuthenticator = Depends(get_authenticator),
) -> Client:
    """Require valid API key in X-Gateway-Key header.

    Returns authenticated Client on success.
    Raises HTTPException 401 on missing/invalid key.
    """
    if not x_gateway_key:
        raise HTTPException(
            status_code=401,
            detail={"code": "GW-E2001", "message": "Missing X-Gateway-Key header"},
        )

    # Extract actor context for audit logging
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent")

    client = authenticator.authenticate(
        x_gateway_key,
        ip=client_ip,
        user_agent=user_agent,
    )

    if not client:
        raise HTTPException(
            status_code=401,
            detail={"code": "GW-E2002", "message": "Invalid API key"},
        )

    # Attach client to request state for downstream middleware
    request.state.client = client

    # Enforce role-based access for admin endpoints
    _enforce_admin_role(request.url.path, client)
    # Enforce role-based access for trading endpoints
    _enforce_trading_role(request.url.path, client)

    # Enforce provider permissions based on path
    provider = _extract_provider_from_path(request.url.path)
    if provider:
        _enforce_provider_permission(client, provider)

    # Enforce per-request symbol limits on common list query params
    _enforce_symbol_limits(request, client)

    return client


def _enforce_admin_role(path: str, client: Client) -> None:
    """Restrict admin endpoints to admin/super_admin roles."""
    if path.startswith("/api/v1/admin") or path == "/api/v1/status":
        if client.role not in ("admin", "super_admin"):
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "GW-E2005",
                    "message": "Admin access required",
                },
            )


def _enforce_trading_role(path: str, client: Client) -> None:
    """Restrict Alpaca trading/account endpoints to trader/admin roles."""
    trading_prefixes = (
        "/api/v1/alpaca/account",
        "/api/v1/alpaca/orders",
        "/api/v1/alpaca/positions",
        "/api/v1/alpaca/portfolio",
        "/api/v1/alpaca/watchlists",
        "/api/v1/alpaca/assets",
        "/api/v1/alpaca/clock",
        "/api/v1/alpaca/calendar",
    )
    if path.startswith(trading_prefixes):
        if client.role not in ("trader", "admin", "super_admin"):
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "GW-E2008",
                    "message": "Trading access required",
                },
            )


def _extract_provider_from_path(path: str) -> str | None:
    """Extract provider name from /api/v1/{provider}/... paths."""
    if not path.startswith("/api/v1/"):
        return None
    parts = path.split("/")
    if len(parts) < 4:
        return None
    provider = parts[3]
    allowed = {
        "alpaca",
        "uw",
        "finnhub",
        "alphavantage",
        "yf",
        "sec",
        "news",
    }
    return provider if provider in allowed else None


def _enforce_provider_permission(client: Client, provider: str) -> None:
    """Ensure client has access to provider."""
    allowed = set(client.permissions.providers or [])
    if not allowed:
        return

    # Normalize aliases
    aliases = {
        "uw": "unusual_whales",
        "yf": "yfinance",
    }
    provider_normalized = aliases.get(provider, provider)

    if provider in allowed:
        return
    if provider_normalized in allowed:
        return
    # Allow alias forms in client config
    if provider == "uw" and "unusual_whales" in allowed:
        return
    if provider == "yf" and "yfinance" in allowed:
        return

    raise HTTPException(
        status_code=403,
        detail={
            "code": "GW-E2006",
            "message": f"Provider access denied: {provider}",
        },
    )


def _enforce_symbol_limits(request: Request, client: Client) -> None:
    """Enforce per-request symbol limits on common comma-separated params."""
    max_symbols = client.permissions.max_symbols
    if max_symbols <= 0:
        return

    list_params = ("symbols", "contracts", "pairs", "isins", "underlyings")
    for key in list_params:
        value = request.query_params.get(key)
        if value is None:
            continue
        items = [s for s in value.split(",") if s.strip()]
        if len(items) > max_symbols:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "GW-E8002",
                    "message": f"Maximum {max_symbols} {key} allowed",
                    "value": len(items),
                },
            )


# ─────────────────────────────────────────────────────────────────────────────
# Provider Rate Limiting
# ─────────────────────────────────────────────────────────────────────────────

from gateway.core.rate_limiter import (
    ProviderRateLimitManager,
    RateLimitExceeded,
    get_rate_limiter,
)


def get_provider_rate_limiter() -> ProviderRateLimitManager:
    """Get the provider rate limiter singleton."""
    return get_rate_limiter()


async def require_provider_rate_limit(
    provider: str,
    block: bool = True,
) -> bool:
    """FastAPI dependency that checks provider rate limit.

    Queues requests by default (block=True), waiting up to 30s for a slot
    rather than immediately returning 429. Only raises 429 if the wait
    deadline is exceeded.

    Usage in endpoint:
        @router.get("/quote/{symbol}")
        async def get_quote(
            symbol: str,
            _: bool = Depends(lambda: require_provider_rate_limit("finnhub")),
        ):
    """
    limiter = get_rate_limiter()

    try:
        await limiter.acquire(provider, block=block)
        return True
    except RateLimitExceeded as e:
        headers = limiter.get_headers(provider)
        headers["Retry-After"] = str(e.retry_after)
        raise HTTPException(
            status_code=429,
            detail={
                "code": "GW-E4002",
                "message": f"Provider rate limit exceeded: {provider}",
                "provider": provider,
                "retry_after": e.retry_after,
            },
            headers=headers,
        )


def provider_rate_limit_dependency(provider: str, block: bool = False):
    """Create a rate limit dependency for a specific provider.

    Usage:
        @router.get("/quote/{symbol}")
        async def get_quote(
            symbol: str,
            _rate_limit = Depends(provider_rate_limit_dependency("finnhub")),
        ):
    """

    async def _check():
        return await require_provider_rate_limit(provider, block)

    return _check


# ─────────────────────────────────────────────────────────────────────────────
# Shared Cache Key Utilities
# ─────────────────────────────────────────────────────────────────────────────


def normalize_cache_arg(value: Any) -> str:
    """Normalize a single cache key argument to a deterministic string."""
    if value is None:
        return "<none>"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value == "":
        return "<empty>"
    return str(value)


def make_cache_key(prefix: str, *args: Any) -> str:
    """Generate a colon-separated cache key from a prefix and arbitrary arguments."""
    parts = [prefix] + [normalize_cache_arg(a) for a in args]
    return ":".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Unified Provider Cached Execution
# ─────────────────────────────────────────────────────────────────────────────

CacheBackend = InMemoryCache | HybridCache


async def execute_provider_cached(
    *,
    provider_name: str,
    registry: Any,
    cache: CacheBackend,
    cache_key: str,
    ttl: int,
    fetcher: Callable[[Any], Awaitable[Any]],
    cache_transform: Callable[[Any], Any] | None = None,
    build_response: Callable[[Any, bool], dict[str, Any]] | None = None,
    miss_meta: dict[str, Any] | None = None,
    miss_meta_builder: Callable[..., dict[str, Any] | None] | None = None,
    route_label: str | None = None,
    cache_mode: str = "default",
    cache_enabled: bool = True,
    error_label: str = "provider_request_failed",
    error_handlers: list[Callable[[Exception], None]] | None = None,
    not_available_msg: str | None = None,
) -> dict[str, Any]:
    """Unified cache -> rate-limit -> fetch -> error-handle -> store -> respond pipeline.

    Consolidates the duplicated execute_*_cached() pattern used by all providers.

    Args:
        provider_name: Registry key (e.g. "finnhub", "unusual_whales", "alphavantage").
        registry: ProviderRegistry instance.
        cache: Cache backend (InMemoryCache or HybridCache).
        cache_key: Pre-built cache key string.
        ttl: Cache TTL in seconds.
        fetcher: ``async (provider) -> raw_result`` callable. Receives the resolved provider.
        cache_transform: Optional ``(raw_result) -> cached_value`` to reshape before caching.
            When *None*, the raw result is cached as-is.
        build_response: Optional ``(data, cached: bool) -> dict`` to fully control the response.
            When supplied, *miss_meta* / *miss_meta_builder* are ignored and the callable is
            responsible for producing the complete ``{success, data, meta}`` envelope.
        miss_meta: Static extra metadata dict merged into ``meta`` on cache misses.
        miss_meta_builder: Optional ``(raw_result, cached_value) -> dict | None`` for dynamic
            miss metadata. May also accept a single argument ``(cached_value) -> dict | None``.
        route_label: If given, records ``record_route_cache(route_label, hit|miss, cache_mode)``.
        cache_mode: Tag passed to ``record_route_cache`` (default ``"default"``).
        cache_enabled: Set *False* to bypass cache entirely (always fetch).
        error_label: Structlog event name for provider errors.
        error_handlers: Extra ``(exception) -> None`` callables run before the default handler.
            If one raises ``HTTPException`` the default handling is skipped.
        not_available_msg: Custom 503 detail when provider is missing from registry.

    Returns:
        ``{"success": True, "data": ..., "meta": {"cached": bool, "provider": str, ...}}``
    """
    # 1. Cache check
    if cache_enabled:
        cached = await cache.get(cache_key)
        if cached is not None:
            if route_label:
                from gateway.core.metrics import record_route_cache

                record_route_cache(route_label, "hit", cache_mode)
            if build_response:
                return build_response(cached, True)
            meta: dict[str, Any] = {"cached": True, "provider": provider_name}
            return {"success": True, "data": cached, "meta": meta}

    if route_label:
        from gateway.core.metrics import record_route_cache

        record_route_cache(route_label, "miss", cache_mode)

    # 2. Provider lookup
    provider = registry.get(provider_name)
    if not provider:
        detail = not_available_msg or f"{provider_name} provider not available"
        raise HTTPException(status_code=503, detail=detail)

    # 3. Rate limit
    await require_provider_rate_limit(provider_name)

    # 4. Fetch with error handling
    try:
        result = await fetcher(provider)
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        if status_code < 500:
            logger.warning(error_label, error=str(e), status_code=status_code)
        else:
            logger.error(error_label, exc_info=True, status_code=status_code)
        if error_handlers:
            for handler in error_handlers:
                handler(e)  # may raise HTTPException
        raise HTTPException(status_code=status_code, detail=f"Upstream provider error: {status_code}")
    except Exception as exc:
        logger.error(error_label, exc_info=True)
        if error_handlers:
            for handler in error_handlers:
                handler(exc)
        raise HTTPException(status_code=502, detail="Upstream provider error")

    # 5. Transform + cache store
    cached_value = cache_transform(result) if cache_transform else result
    if cache_enabled:
        await cache.set(cache_key, cached_value, ttl=ttl)

    # 6. Build response
    if build_response:
        return build_response(cached_value, False)

    extra_meta = miss_meta or {}
    if miss_meta_builder:
        try:
            built = miss_meta_builder(result, cached_value)
        except TypeError:
            built = miss_meta_builder(cached_value)
        if built:
            extra_meta = {**extra_meta, **built}

    return {
        "success": True,
        "data": cached_value,
        "meta": {"cached": False, "provider": provider_name, **extra_meta},
    }
