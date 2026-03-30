"""FastAPI dependency injection."""

from functools import lru_cache

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
    return ConnectionManager()


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
