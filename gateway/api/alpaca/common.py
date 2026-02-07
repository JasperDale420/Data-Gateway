"""Common dependencies and constants for Alpaca API endpoints."""

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


__all__ = [
    "DESC_BAR_TIMEFRAME",
    "DESC_START_TIME",
    "DESC_END_TIME",
    "DESC_MAX_BARS",
    "DESC_COMMA_SYMBOLS",
    "ERR_PROVIDER_NOT_AVAILABLE",
    "parse_comma_values",
    "get_alpaca_provider",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "ProviderRegistry",
    "get_registry",
]
