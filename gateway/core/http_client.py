"""
Shared HTTP client utilities with structured logging and retry support.

Provides factory functions for creating httpx clients with consistent
defaults (timeouts, logging hooks, follow_redirects) and a pre-configured
tenacity retry decorator for transient HTTP failures.

Usage:
    from gateway.core.http_client import create_async_http_client, http_retry

    client = create_async_http_client(base_url="http://upstream:8080")

    @http_retry
    async def fetch_upstream():
        resp = await client.get("/data")
        raise_for_status(resp)
        return resp.json()
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from gateway.core.logger import logger

DEFAULT_TIMEOUT = 30.0


def _should_retry_http_error(exc: BaseException) -> bool:
    """Determine if an HTTP exception should trigger a retry."""
    if isinstance(exc, httpx.TransportError | httpx.TimeoutException):
        return True

    if isinstance(exc, httpx.HTTPStatusError):
        # 429 Too Many Requests
        # 502 Bad Gateway
        # 503 Service Unavailable
        # 504 Gateway Timeout
        return exc.response.status_code in {429, 502, 503, 504}

    return False


def _log_request(request: httpx.Request) -> None:
    """Event hook: log outgoing HTTP requests at DEBUG level."""
    logger.debug("http_request", method=str(request.method), url=str(request.url))


def _log_response(response: httpx.Response) -> None:
    """Event hook: log incoming HTTP responses at DEBUG level."""
    try:
        elapsed = response.elapsed.total_seconds() if response.elapsed else 0.0
    except RuntimeError:
        elapsed = 0.0
    logger.debug(
        "http_response",
        method=str(response.request.method),
        url=str(response.request.url),
        status_code=response.status_code,
        elapsed_s=round(elapsed, 3),
    )


async def _async_log_request(request: httpx.Request) -> None:
    """Async event hook: log outgoing HTTP requests at DEBUG level."""
    _log_request(request)


async def _async_log_response(response: httpx.Response) -> None:
    """Async event hook: log incoming HTTP responses at DEBUG level."""
    _log_response(response)


def _merge_event_hooks(
    defaults: dict[str, list],
    overrides: dict[str, list] | None,
) -> dict[str, list]:
    """Merge caller-provided event hooks with the default logging hooks."""
    if not overrides:
        return defaults
    merged: dict[str, list] = {}
    for key in {*defaults, *overrides}:
        merged[key] = defaults.get(key, []) + overrides.get(key, [])
    return merged


def create_http_client(
    base_url: str = "",
    timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> httpx.Client:
    """Create a configured synchronous httpx.Client with standard defaults."""
    caller_hooks = kwargs.pop("event_hooks", None)
    hooks = _merge_event_hooks(
        {"request": [_log_request], "response": [_log_response]},
        caller_hooks,
    )
    return httpx.Client(
        base_url=base_url,
        timeout=timeout,
        headers=headers or {},
        follow_redirects=True,
        event_hooks=hooks,
        **kwargs,
    )


def create_async_http_client(
    base_url: str = "",
    timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Create a configured asynchronous httpx.AsyncClient with standard defaults."""
    caller_hooks = kwargs.pop("event_hooks", None)
    hooks = _merge_event_hooks(
        {"request": [_async_log_request], "response": [_async_log_response]},
        caller_hooks,
    )
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        headers=headers or {},
        follow_redirects=True,
        event_hooks=hooks,
        **kwargs,
    )


def raise_for_status(response: httpx.Response) -> None:
    """Validate HTTP response status, raising with structured context on failure."""
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(
            "http_error",
            method=str(response.request.method),
            url=str(response.request.url),
            status_code=response.status_code,
            response_text=response.text[:500],
        )
        raise


http_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_should_retry_http_error),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
