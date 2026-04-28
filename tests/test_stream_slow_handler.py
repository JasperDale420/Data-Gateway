"""UpstreamConnection should warn when on_message blocks the receive loop too long.

Slow on_message stalls the receive loop, fills Alpaca's outbound buffer, and can
trigger their slow-client (407) disconnect. We log so this is visible in audits.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from gateway.core.stream import AlpacaStreamType, UpstreamConnection


class _RecordingLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def warning(self, event: str, **kw: Any) -> None:
        self.calls.append(("warning", event, kw))

    def info(self, event: str, **kw: Any) -> None:
        self.calls.append(("info", event, kw))

    def error(self, event: str, **kw: Any) -> None:
        self.calls.append(("error", event, kw))

    def debug(self, event: str, **kw: Any) -> None:
        self.calls.append(("debug", event, kw))

    def exception(self, event: str, **kw: Any) -> None:
        self.calls.append(("exception", event, kw))


class _SingleMessageWs:
    def __init__(self, payload: str) -> None:
        self._payload = payload
        self._sent = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self._sent:
            raise StopAsyncIteration
        self._sent = True
        return self._payload


@pytest.fixture
def recording_stream_logger(monkeypatch):
    rec = _RecordingLogger()
    monkeypatch.setattr("gateway.core.stream.logger", rec)
    return rec


@pytest.mark.asyncio
async def test_receive_loop_warns_when_on_message_exceeds_slow_threshold(
    recording_stream_logger,
) -> None:
    async def _slow(_msg: dict) -> None:
        await asyncio.sleep(0.12)

    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_slow,
    )
    conn._ws = cast(Any, _SingleMessageWs('{"T":"q","S":"AAPL"}'))

    await conn._receive_loop()

    slow_warnings = [(lvl, ev) for lvl, ev, _ in recording_stream_logger.calls if ev == "on_message_slow"]
    assert slow_warnings == [("warning", "on_message_slow")], (
        f"Expected one WARNING for slow on_message, got: {recording_stream_logger.calls}"
    )

    slow_call = next(kw for lvl, ev, kw in recording_stream_logger.calls if ev == "on_message_slow")
    assert slow_call.get("stream") == "stocks_sip"
    assert slow_call.get("duration_seconds", 0) >= 0.1


@pytest.mark.asyncio
async def test_receive_loop_does_not_warn_when_on_message_is_fast(
    recording_stream_logger,
) -> None:
    async def _fast(_msg: dict) -> None:
        return

    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_fast,
    )
    conn._ws = cast(Any, _SingleMessageWs('{"T":"q","S":"AAPL"}'))

    await conn._receive_loop()

    slow_warnings = [(lvl, ev) for lvl, ev, _ in recording_stream_logger.calls if ev == "on_message_slow"]
    assert slow_warnings == [], f"Expected no slow warnings for fast on_message, got: {recording_stream_logger.calls}"
