"""FastAPI dependency injection."""

from functools import lru_cache
from typing import TYPE_CHECKING

from fastapi import Depends, Header, HTTPException

from gateway.config import get_settings
from gateway.core.auth import Client, ClientAuthenticator
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.core.registry import ProviderRegistry

if TYPE_CHECKING:
    from gateway.core.data_sink import DataSinkRegistry
    from gateway.core.multiplexer import StreamMultiplexer


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


# Global registry instance (initialized in lifespan)
_registry: ProviderRegistry | None = None


def set_registry(registry: ProviderRegistry) -> None:
    """Set the global registry (called during startup)."""
    global _registry
    _registry = registry


def get_registry() -> ProviderRegistry:
    """Get the provider registry."""
    if _registry is None:
        raise RuntimeError("Provider registry not initialized")
    return _registry


# Global multiplexer instance (initialized in lifespan)
_multiplexer: "StreamMultiplexer | None" = None


def set_multiplexer(multiplexer: "StreamMultiplexer") -> None:
    """Set the global multiplexer (called during startup)."""
    global _multiplexer
    _multiplexer = multiplexer


def get_multiplexer() -> "StreamMultiplexer":
    """Get the stream multiplexer."""
    if _multiplexer is None:
        raise RuntimeError("Stream multiplexer not initialized")
    return _multiplexer


# Global data sink registry (initialized in lifespan)
_sink_registry: "DataSinkRegistry | None" = None


def set_sink_registry(registry: "DataSinkRegistry") -> None:
    """Set the global data sink registry (called during startup)."""
    global _sink_registry
    _sink_registry = registry


def get_sink_registry() -> "DataSinkRegistry | None":
    """Get the data sink registry (may be None if not configured)."""
    return _sink_registry


def require_api_key(
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

    client = authenticator.authenticate(x_gateway_key)

    if not client:
        raise HTTPException(
            status_code=401,
            detail={"code": "GW-E2002", "message": "Invalid API key"},
        )

    return client


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
    block: bool = False,
) -> bool:
    """FastAPI dependency that checks provider rate limit.

    Usage in endpoint:
        @router.get("/quote/{symbol}")
        async def get_quote(
            symbol: str,
            _: bool = Depends(lambda: require_provider_rate_limit("finnhub")),
        ):

    Raises HTTPException 429 if rate limited.
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
