"""Common dependencies and constants for Alpaca API endpoints."""

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from fastapi import Depends, HTTPException

from gateway.api.deps import get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.registry import ProviderRegistry

# Query description constants
DESC_BAR_TIMEFRAME = "Bar timeframe"
DESC_START_TIME = "Start time (ISO 8601)"
DESC_END_TIME = "End time (ISO 8601)"
DESC_MAX_BARS = "Max bars to return"
DESC_COMMA_SYMBOLS = "Comma-separated symbols"

# Error message constants
ERR_PROVIDER_NOT_AVAILABLE = "Alpaca provider not available"
T = TypeVar("T")


def parse_comma_values(
    raw: str,
    *,
    uppercase: bool = False,
    drop_empty: bool = False,
) -> list[str]:
    """Parse comma-separated values with whitespace trimming."""
    values = [item.strip() for item in raw.split(",")]
    if drop_empty:
        values = [item for item in values if item]
    parsed = [item.upper() if uppercase else item for item in values]
    return parsed


async def get_alpaca_provider(
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get the Alpaca provider or raise 503 if unavailable."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)
    return provider


async def execute_alpaca_provider_call(
    *,
    registry: ProviderRegistry,
    provider_call: Callable[[Any], Awaitable[T]],
    block: bool = False,
) -> T:
    """Run Alpaca provider call with shared provider lookup, rate-limit, and error handling."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca", block=block)
        return await provider_call(provider)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}") from e


__all__ = [
    "DESC_BAR_TIMEFRAME",
    "DESC_START_TIME",
    "DESC_END_TIME",
    "DESC_MAX_BARS",
    "DESC_COMMA_SYMBOLS",
    "ERR_PROVIDER_NOT_AVAILABLE",
    "parse_comma_values",
    "get_alpaca_provider",
    "execute_alpaca_provider_call",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "ProviderRegistry",
    "get_registry",
]
