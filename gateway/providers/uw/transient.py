"""Classify upstream UW errors as transient vs persistent.

Transient errors are routine upstream brownouts (5xx, network resets, empty
bodies). They warrant WARN-level logging and retry, not ERROR-level alarm.
"""

from __future__ import annotations

import json
import ssl

import httpx

BODY_PREVIEW_CHARS = 160

_TRANSIENT_HTTPX_TYPES: tuple[type[BaseException], ...] = (
    httpx.ReadTimeout,
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.PoolTimeout,
)

_TRANSIENT_MESSAGE_FRAGMENTS: tuple[str, ...] = (
    "Server disconnected",
    "UNEXPECTED_EOF_WHILE_READING",
)


def is_transient_upstream_error(exc: BaseException) -> bool:
    """Return True for upstream errors that should be treated as transient."""
    if isinstance(exc, json.JSONDecodeError):
        return True
    if isinstance(exc, _TRANSIENT_HTTPX_TYPES):
        return True
    if isinstance(exc, ssl.SSLError):
        return True

    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int) and status >= 500:
        return True

    message = str(exc)
    return any(fragment in message for fragment in _TRANSIENT_MESSAGE_FRAGMENTS)


def _uw_error_context(
    exc: BaseException,
    *,
    provider_endpoint: str,
    path: str | None = None,
    symbol: str | None = None,
) -> dict[str, object]:
    """Build stable UW provider-error log fields."""
    context: dict[str, object] = {"provider_endpoint": provider_endpoint}
    if path:
        context["path"] = path
    if symbol:
        context["symbol"] = symbol

    response = getattr(exc, "response", None) or getattr(exc, "_uw_response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        context["status_code"] = status_code

    body = getattr(response, "text", None)
    if isinstance(body, str) and body:
        context["body_preview"] = body[:BODY_PREVIEW_CHARS]

    return context


__all__ = ["is_transient_upstream_error"]
