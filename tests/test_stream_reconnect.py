"""Regression tests for stream reconnect and non-recoverable auth failures."""

import asyncio

import pytest

from gateway.core.stream import AlpacaStreamType, UpstreamConnection


async def _on_message(_: dict) -> None:
    """No-op callback for UpstreamConnection tests."""


class _DummyWebSocket:
    """Simple websocket stub tracking close calls."""

    def __init__(self) -> None:
        self.close_calls: list[tuple[int, str]] = []

    async def close(self, code: int, reason: str) -> None:
        self.close_calls.append((code, reason))


@pytest.mark.asyncio
async def test_reconnect_stops_on_non_recoverable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reconnect stops immediately on account-level auth failures."""
    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",
        api_secret="s",
        on_message=_on_message,
        max_retries=5,
    )
    conn._running = True

    attempts = 0

    async def _fast_sleep(_: float) -> None:
        return None

    async def _fail_connect() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("Auth failed: Connection Limit Exceeded")

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)
    monkeypatch.setattr(conn, "connect", _fail_connect)

    await conn._reconnect_with_backoff()

    assert attempts == 1
    assert conn._running is False


@pytest.mark.asyncio
async def test_connect_and_run_closes_websocket_before_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Main loop closes stale websocket before reconnect attempts."""
    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",
        api_secret="s",
        on_message=_on_message,
        max_retries=1,
    )
    conn._running = True
    dummy_ws = _DummyWebSocket()
    reconnect_calls = 0

    async def _connect() -> None:
        conn._ws = dummy_ws

    async def _authenticate() -> None:
        conn._authenticated = True

    async def _receive_loop() -> None:
        raise RuntimeError("boom")

    async def _reconnect() -> None:
        nonlocal reconnect_calls
        reconnect_calls += 1
        conn._running = False

    monkeypatch.setattr(conn, "connect", _connect)
    monkeypatch.setattr(conn, "authenticate", _authenticate)
    monkeypatch.setattr(conn, "_receive_loop", _receive_loop)
    monkeypatch.setattr(conn, "_reconnect_with_backoff", _reconnect)

    await conn._connect_and_run()

    assert reconnect_calls == 1
    assert dummy_ws.close_calls == [(1000, "Gateway reconnecting")]


@pytest.mark.asyncio
async def test_reconnect_stops_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reconnect loop stops running after retries are exhausted."""
    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",
        api_secret="s",
        on_message=_on_message,
        max_retries=3,
    )
    conn._running = True
    attempts = 0

    async def _fast_sleep(_: float) -> None:
        return None

    async def _fail_connect() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("temporary upstream failure")

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)
    monkeypatch.setattr(conn, "connect", _fail_connect)

    await conn._reconnect_with_backoff()

    assert attempts == 3
    assert conn._running is False
