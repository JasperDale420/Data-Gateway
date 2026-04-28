"""Trading SDK session must have a default HTTP timeout to prevent thread-pool leak.

The alpaca-py SDK creates a bare ``requests.Session()`` with no timeout. Our
async wall-clock timeout fires at 15s but ``run_in_executor`` doesn't cancel
the underlying thread, so a hung upstream call keeps the thread occupied
indefinitely. Past-2-day audit saw 4 ``alpaca_trading_call_timeout`` events;
each one of those leaks a thread until the OS gives up. Wrapping the session
to inject a default timeout caps the leak at our chosen budget.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from gateway.providers.alpaca._base import _install_session_default_timeout


class _RecordingTransport:
    """Minimal requests adapter that records send-time kwargs."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def send(self, request, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        response = requests.Response()
        response.status_code = 200
        response._content = b"{}"
        response.url = request.url
        response.encoding = "utf-8"
        return response

    def close(self) -> None:
        pass


def test_install_session_default_timeout_injects_when_caller_omits() -> None:
    session = requests.Session()
    transport = _RecordingTransport()
    session.mount("https://", transport)

    _install_session_default_timeout(session, timeout_seconds=12.5)

    session.request("GET", "https://example.test/foo")

    assert transport.calls, "the wrapped session must still reach its adapter"
    assert transport.calls[0].get("timeout") == 12.5


def test_install_session_default_timeout_preserves_caller_override() -> None:
    session = requests.Session()
    transport = _RecordingTransport()
    session.mount("https://", transport)

    _install_session_default_timeout(session, timeout_seconds=12.5)

    session.request("GET", "https://example.test/foo", timeout=3.0)

    assert transport.calls[0].get("timeout") == 3.0, (
        "the wrapper must not override an explicit timeout the caller passed in"
    )


@pytest.mark.asyncio
async def test_provider_initialize_installs_default_timeout_on_trading_session(monkeypatch) -> None:
    monkeypatch.setenv("APCA_API_KEY_ID", "test_key")  # pragma: allowlist secret
    monkeypatch.setenv("APCA_API_SECRET_KEY", "test_secret")  # pragma: allowlist secret

    from gateway.providers.alpaca import AlpacaProvider

    provider = AlpacaProvider()
    await provider.initialize({"feed": "iex"})

    assert provider._trading_client is not None, "trading client should initialize when creds set"

    session = provider._trading_client._session
    transport = _RecordingTransport()
    session.mount("https://", transport)

    session.request("GET", "https://paper-api.alpaca.markets/v2/account")

    assert transport.calls, "session must have reached its adapter"
    timeout = transport.calls[0].get("timeout")
    assert timeout is not None and timeout > 0, (
        "AlpacaProvider.initialize must install a default HTTP timeout on the trading session"
    )

    await provider.shutdown()
