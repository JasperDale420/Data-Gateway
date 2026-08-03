"""Tests for _on_stream_envelope — the single streaming → Heber dispatch path.

This callback fires once per upstream envelope and is the only place streaming
events reach the Redis sink, yet it had zero direct coverage. These tests also
pin the FROZEN wire contract (topic ``heber:events`` + feed forwarding): assert
the current values, do not change them without a coordinated Heber update.
"""

import asyncio

import pytest

import gateway.main as main_mod
from gateway.core.stream import StreamTransportFatalError
from gateway.main import _on_stream_envelope


class _FakeRegistry:
    def __init__(self, *, raise_exc: BaseException | None = None) -> None:
        self.calls: list[dict] = []
        self._raise = raise_exc

    async def publish_all(self, topic, payload, *, source=None, feed=None) -> int:
        self.calls.append({"topic": topic, "payload": payload, "source": source, "feed": feed})
        if self._raise is not None:
            raise self._raise
        return 1


@pytest.fixture
def dispatch_events(monkeypatch):
    """Capture the dispatch lifecycle events and isolate the sink-registry global."""
    events: list[str] = []
    monkeypatch.setattr(main_mod, "record_stream_sink_dispatch_event", events.append)
    original = main_mod._stream_sink_registry
    yield events
    main_mod._set_stream_sink_registry(original)


async def test_dispatch_publishes_to_frozen_topic_with_feed(dispatch_events):
    """Happy path: publish to heber:events, forward the feed, record completed."""
    registry = _FakeRegistry()
    main_mod._set_stream_sink_registry(registry)

    await _on_stream_envelope({"event_id": "e1", "feed": "quotes", "payload": {}})

    assert len(registry.calls) == 1
    call = registry.calls[0]
    assert call["topic"] == "heber:events"  # FROZEN — do not change without Heber
    assert call["feed"] == "quotes"  # feed forwarded for downstream routing
    assert call["payload"] == {"event_id": "e1", "feed": "quotes", "payload": {}}
    assert call["source"] == "websocket"
    assert dispatch_events == ["scheduled", "completed"]


async def test_dispatch_no_registry_is_noop(dispatch_events):
    """With no sink registry configured, dispatch is a silent no-op (no crash)."""
    main_mod._set_stream_sink_registry(None)

    await _on_stream_envelope({"event_id": "e1", "feed": "bars"})

    assert dispatch_events == []  # returns before recording "scheduled"


async def test_dispatch_swallows_publish_failure(dispatch_events):
    """A publish failure is recorded + logged but must NOT propagate (keep stream alive)."""
    registry = _FakeRegistry(raise_exc=RuntimeError("redis down"))
    main_mod._set_stream_sink_registry(registry)

    await _on_stream_envelope({"event_id": "e1", "feed": "trades"})  # must not raise

    assert dispatch_events == ["scheduled", "failed"]


async def test_durable_dispatch_failure_is_fatal(dispatch_events):
    registry = _FakeRegistry(raise_exc=RuntimeError("outbox full"))
    registry.has_durable_admission = True
    main_mod._set_stream_sink_registry(registry)

    with pytest.raises(StreamTransportFatalError, match="outbox full"):
        await _on_stream_envelope({"event_id": "e1", "feed": "trades"})

    assert dispatch_events == ["scheduled", "failed"]


async def test_dispatch_reraises_cancellation(dispatch_events):
    """CancelledError must propagate (records cancelled, re-raises)."""
    registry = _FakeRegistry(raise_exc=asyncio.CancelledError())
    main_mod._set_stream_sink_registry(registry)

    with pytest.raises(asyncio.CancelledError):
        await _on_stream_envelope({"event_id": "e1", "feed": "news"})

    assert dispatch_events == ["scheduled", "cancelled"]
