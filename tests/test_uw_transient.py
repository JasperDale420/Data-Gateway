from __future__ import annotations

import json
import ssl

import httpx
import pytest

from gateway.providers.uw.transient import _uw_error_context, is_transient_upstream_error


def test_json_decode_error_is_transient() -> None:
    try:
        json.loads("")
    except json.JSONDecodeError as exc:
        assert is_transient_upstream_error(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ReadTimeout("read timeout"),
        httpx.ConnectError("connect error"),
        httpx.ConnectTimeout("connect timeout"),
        httpx.RemoteProtocolError("remote protocol"),
        httpx.ReadError("read error"),
        httpx.WriteError("write error"),
        httpx.PoolTimeout("pool timeout"),
    ],
)
def test_httpx_network_errors_are_transient(exc: Exception) -> None:
    assert is_transient_upstream_error(exc) is True


def test_ssl_error_is_transient() -> None:
    exc = ssl.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF in violation of protocol")
    assert is_transient_upstream_error(exc) is True


def test_http_5xx_status_error_is_transient() -> None:
    request = httpx.Request("GET", "https://example.test")
    response = httpx.Response(503, request=request)
    exc = httpx.HTTPStatusError("503", request=request, response=response)
    assert is_transient_upstream_error(exc) is True


def test_http_4xx_status_error_is_not_transient() -> None:
    request = httpx.Request("GET", "https://example.test")
    response = httpx.Response(401, request=request)
    exc = httpx.HTTPStatusError("401", request=request, response=response)
    assert is_transient_upstream_error(exc) is False


def test_server_disconnected_message_is_transient() -> None:
    exc = RuntimeError("Server disconnected without sending a response.")
    assert is_transient_upstream_error(exc) is True


def test_unexpected_eof_message_is_transient() -> None:
    exc = RuntimeError("[SSL: UNEXPECTED_EOF_WHILE_READING] something")
    assert is_transient_upstream_error(exc) is True


def test_unrelated_exception_is_not_transient() -> None:
    assert is_transient_upstream_error(ValueError("bad input")) is False
    assert is_transient_upstream_error(RuntimeError("custom application bug")) is False
    assert is_transient_upstream_error(KeyError("missing")) is False


def test_uw_error_context_includes_http_response_preview() -> None:
    request = httpx.Request("GET", "https://api.unusualwhales.com/api/stock/AAPL/iv-rank")
    response = httpx.Response(503, text="service unavailable from upstream", request=request)
    exc = httpx.HTTPStatusError("503", request=request, response=response)

    assert _uw_error_context(exc, provider_endpoint="iv_rank", path="/api/stock/AAPL/iv-rank", symbol="AAPL") == {
        "provider_endpoint": "iv_rank",
        "path": "/api/stock/AAPL/iv-rank",
        "symbol": "AAPL",
        "status_code": 503,
        "body_preview": "service unavailable from upstream",
    }
