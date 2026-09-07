"""Shared provider error-logging helpers."""

from typing import Any

import httpx

from gateway.core.logger import logger


def log_provider_http_error(event: str, exc: Exception, **fields: Any) -> None:
    """Log a provider HTTP error at a severity matched to its cause.

    Client-caused failures (HTTP 4xx — bad symbol, rate limit, auth) are logged
    at WARNING; server/transport failures (5xx, timeouts, connection errors) at
    ERROR. Without this split every transient bad-symbol 400 floods the ERROR
    log — the 2026-03 finnhub/alphavantage pattern where a single index symbol
    produced thousands of ERROR lines. Callers still re-raise after calling
    this; it only sets log severity, never control flow.
    """
    status_code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
    payload = {**fields, "error": str(exc)}
    if status_code is not None:
        payload["status_code"] = status_code
        payload["status"] = status_code
    if status_code is not None and 400 <= status_code < 500:
        logger.warning(event, **payload)
    else:
        logger.error(event, **payload)
