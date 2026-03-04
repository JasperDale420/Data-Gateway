"""Common dependencies and constants for Alpaca API endpoints."""

import json
from typing import Any, cast

import structlog
from fastapi import Depends, HTTPException

from gateway.api.deps import get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.registry import ProviderRegistry
from gateway.core.security import get_input_validator


def _load_alpaca_api_error_type() -> type[Exception] | None:
    """Load Alpaca APIError type if dependency is installed."""
    try:
        from alpaca.common.exceptions import APIError

        return cast(type[Exception], APIError)
    except Exception:  # pragma: no cover - runtime dependency is present in production
        return None


ALPACA_API_ERROR_TYPE = _load_alpaca_api_error_type()

logger = structlog.get_logger()

# Query description constants
DESC_BAR_TIMEFRAME = "Bar timeframe"
DESC_START_TIME = "Start time (ISO 8601)"
DESC_END_TIME = "End time (ISO 8601)"
DESC_MAX_BARS = "Max bars to return"
DESC_COMMA_SYMBOLS = "Comma-separated symbols"

# Error message constants
ERR_PROVIDER_NOT_AVAILABLE = "Alpaca provider not available"


def _is_alpaca_api_error(exc: Exception) -> bool:
    """Return True if exception is an Alpaca APIError instance."""
    return ALPACA_API_ERROR_TYPE is not None and isinstance(exc, ALPACA_API_ERROR_TYPE)


def normalize_crypto_pair(pair: str) -> str:
    """Normalize a crypto pair string for validation/use."""
    return pair.strip().upper()


def validate_crypto_pair_or_400(pair: str) -> str:
    """Validate a crypto pair and raise HTTP 400 on invalid input."""
    normalized = normalize_crypto_pair(pair)
    error = get_input_validator().validate_symbol(normalized, symbol_type="crypto")
    if error:
        logger.warning(
            "alpaca_invalid_crypto_pair",
            pair=pair,
            normalized_pair=normalized,
            error_code=error.code,
        )
        raise HTTPException(
            status_code=400,
            detail={
                "code": error.code,
                "message": f"Invalid crypto pair format: {normalized}",
            },
        )
    return normalized


def parse_crypto_pairs_or_400(pairs: str) -> list[str]:
    """Parse and validate comma-separated crypto pairs."""
    parsed = [normalize_crypto_pair(raw) for raw in pairs.split(",") if raw.strip()]
    if not parsed:
        raise HTTPException(
            status_code=400,
            detail={"code": "GW-E8007", "message": "At least one crypto pair is required"},
        )
    return [validate_crypto_pair_or_400(pair) for pair in parsed]


def _parse_alpaca_error_payload(exc: Exception) -> dict[str, Any]:
    """Best-effort extraction of provider error payload."""
    if _is_alpaca_api_error(exc):
        raw = str(exc)
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            return {"message": raw}
    return {"message": str(exc)}


def _extract_alpaca_status(exc: Exception, payload: dict[str, Any]) -> int:
    """Map Alpaca errors to HTTP status codes for client-facing responses."""
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        if 400 <= status <= 499:
            return status
        if status >= 500:
            return 502

    provider_code = payload.get("code")
    if provider_code is not None:
        code_str = str(provider_code)
        if len(code_str) >= 3 and code_str[:3].isdigit():
            prefix = int(code_str[:3])
            if 400 <= prefix <= 499:
                return prefix

    message = str(payload.get("message", "")).lower()
    if "badly formed hexadecimal uuid" in message:
        return 400
    if "insufficient" in message or "not eligible" in message or "forbidden" in message:
        return 403
    return 502


def map_alpaca_exception(exc: Exception) -> HTTPException:
    """Convert provider/client exceptions to API HTTPException with correct status."""
    if isinstance(exc, HTTPException):
        return exc

    if isinstance(exc, ValueError):
        return HTTPException(
            status_code=400,
            detail={"code": "GW-E8001", "message": str(exc)},
        )

    if _is_alpaca_api_error(exc):
        payload = _parse_alpaca_error_payload(exc)
        status_code = _extract_alpaca_status(exc, payload)
        return HTTPException(
            status_code=status_code,
            detail={
                "message": str(payload.get("message", str(exc))),
                "provider": "alpaca",
                "provider_code": payload.get("code"),
            },
        )

    return HTTPException(status_code=502, detail=f"Provider error: {str(exc)}")


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
    "map_alpaca_exception",
    "normalize_crypto_pair",
    "parse_crypto_pairs_or_400",
    "validate_crypto_pair_or_400",
    "get_alpaca_provider",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "ProviderRegistry",
    "get_registry",
]
