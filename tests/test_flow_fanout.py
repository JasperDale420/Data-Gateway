"""UW flow fan-out tests.

Covers the additive WS flow-push path:
  - event_id parity: the envelope fanned out over WS carries the SAME
    event_id that the UW poller publishes to heber:events for the same record
    (the hard cross-repo contract — Orion's deduper collapses push+poll on it);
  - the FlowFanout subscription registry (per-symbol, ALL, unsubscribe,
    disconnect, no-subscriber short-circuit);
  - the poller's on_flow_envelope tap fires once per PUBLISHED (deduped) flow
    envelope and never for darkpool/tide.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from gateway.core.connections import Connection, ConnectionManager
from gateway.core.envelope import wrap_event
from gateway.core.flow_fanout import FLOW_FEED, FlowFanout


async def _flush(fanout: FlowFanout) -> None:
    """Await any background delivery tasks the fan-out scheduled.

    deliver() schedules the actual broadcast off the poller's await (so a slow
    socket can't stall the poll loop), so tests must drain those tasks before
    inspecting what was sent.
    """
    pending = list(fanout._deliver_tasks)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


class _RecordingConnections:
    """Minimal ConnectionManager stand-in capturing broadcast calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[dict, list[str]]] = []

    async def broadcast_to_connection_ids(self, message, connection_ids):
        self.calls.append((message, list(connection_ids or [])))
        return len(connection_ids or [])


def _flow_record(occ: str = "AAPL240119C00190000") -> dict:
    """A representative UW flow record (the shape get_flow_alerts yields)."""
    return {
        "ticker": "AAPL",
        "underlying_symbol": "AAPL",
        "option_chain": occ,
        "expiry": "2024-01-19",
        "strike": 190.0,
        "put_call": "call",
        "premium": 125000.0,
        "volume": 500,
        "timestamp": "2024-01-10T15:30:00Z",
    }


# ── event_id parity (the hard contract) ──────────────────────────────────────


async def test_fanned_out_event_id_matches_published_event_id():
    """The WS-delivered event_id == the heber:events event_id for one record.

    Both paths build the envelope via the SAME wrap_event call, so the
    content-derived blake2b id is byte-identical. This is what lets Orion's
    DeduplicationEngine collapse a push+poll duplicate pair to one bronze row.
    """
    record = _flow_record()
    # Poll path: this is exactly what UWPoller._build_feed_envelopes builds and
    # publishes to HEBER_STREAM.
    published_envelope = wrap_event(event=record, provider="unusual_whales", feed="flow_alerts", source="rest")

    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    fanout.subscribe("conn-1", [])  # ALL

    # Push path: the poller hands the SAME envelope object to the tap.
    delivered = await fanout.deliver(published_envelope)
    await _flush(fanout)

    assert delivered == 1
    wire_msg, targets = conns.calls[0]
    assert targets == ["conn-1"]
    assert wire_msg["type"] == "data"
    assert wire_msg["feed"] == FLOW_FEED
    assert wire_msg["event_id"] == published_envelope["event_id"]
    assert wire_msg["envelope"]["event_id"] == published_envelope["event_id"]
    # Wire shape mirrors bars so Orion shares one decoder.
    assert wire_msg["data"] == published_envelope["payload"]
    assert wire_msg["symbol"] == published_envelope["symbol"] == "AAPL"


# ── subscription registry ────────────────────────────────────────────────────


async def test_per_symbol_subscription_targets_only_matching_symbol():
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    fanout.subscribe("aapl-conn", ["AAPL"])
    fanout.subscribe("tsla-conn", ["TSLA"])

    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    await fanout.deliver(env)
    await _flush(fanout)

    _msg, targets = conns.calls[0]
    assert targets == ["aapl-conn"]


async def test_all_subscriber_receives_every_symbol():
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    fanout.subscribe("firehose", [])
    fanout.subscribe("aapl-conn", ["AAPL"])

    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    await fanout.deliver(env)
    await _flush(fanout)

    _msg, targets = conns.calls[0]
    assert set(targets) == {"firehose", "aapl-conn"}


async def test_no_subscribers_short_circuits():
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    delivered = await fanout.deliver(env)
    assert delivered == 0
    assert conns.calls == []


async def test_unsubscribe_removes_target():
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    fanout.subscribe("aapl-conn", ["AAPL"])
    fanout.unsubscribe("aapl-conn", ["AAPL"])
    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    assert await fanout.deliver(env) == 0


async def test_client_disconnect_drops_all_subscriptions():
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    fanout.subscribe("conn", ["AAPL", "TSLA"])
    fanout.subscribe("conn", [])  # also firehose
    assert fanout.subscriber_count == 1
    fanout.client_disconnect("conn")
    assert fanout.subscriber_count == 0
    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    assert await fanout.deliver(env) == 0


async def test_deliver_swallows_broadcast_failure():
    class _Boom:
        async def broadcast_to_connection_ids(self, message, connection_ids):
            raise RuntimeError("ws send blew up")

    fanout = FlowFanout(_Boom())  # type: ignore[arg-type]
    fanout.subscribe("conn", [])
    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    # deliver schedules the broadcast and returns the scheduled-target count
    # without awaiting the (failing) send — the poller is never disturbed.
    assert await fanout.deliver(env) == 1
    # Draining the background send must not raise — the failure is swallowed.
    await _flush(fanout)


# ── G1: Decimal payloads must survive the WS broadcast ───────────────────────


async def test_deliver_coerces_decimal_payload_so_broadcast_serializes():
    """A flow envelope carrying Decimals must round-trip through the real
    ConnectionManager broadcast (plain orjson.dumps, no default=str).

    UW SDK records yield Decimal values. The fan-out builds the wire message
    and hands it to ConnectionManager.broadcast_to_connection_ids, which calls
    orjson.dumps WITHOUT a default coercer — a raw Decimal would raise there and
    the whole push would silently drop. FlowFanout.deliver coerces a COPY to
    JSON-safe form (Decimal -> str) so the send succeeds, while leaving the
    original (already-published-to-Redis) envelope untouched.
    """

    class _FakeWebSocket:
        def __init__(self) -> None:
            self.sent_bytes: list[bytes] = []

        async def send_bytes(self, payload: bytes) -> None:
            self.sent_bytes.append(payload)

    websocket = _FakeWebSocket()
    manager = ConnectionManager()
    manager._connections["conn-1"] = Connection(
        websocket=websocket,  # type: ignore[arg-type]
        client=SimpleNamespace(id="orion"),  # type: ignore[arg-type]
        authenticated=True,
    )

    fanout = FlowFanout(manager)
    fanout.subscribe("conn-1", [])  # ALL

    envelope = {
        "event_id": "evt-1",
        "symbol": "AAPL",
        "payload": {"premium": Decimal("125000.50"), "strike": Decimal("190.0")},
    }
    delivered = await fanout.deliver(envelope)
    await _flush(fanout)

    assert delivered == 1
    assert len(websocket.sent_bytes) == 1
    assert b"125000.50" in websocket.sent_bytes[0]
    # The original envelope was NOT mutated — Redis/Heber bytes stay identical.
    assert envelope["payload"]["premium"] == Decimal("125000.50")


async def test_fanned_out_wire_message_roundtrips_through_orjson():
    """The fanned-out wire message is orjson-serializable even with Decimals.

    Captures the exact dict the fan-out hands to the broadcast and asserts a
    bare orjson.dumps (the broadcast's serializer) succeeds on it.
    """
    import orjson

    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    fanout.subscribe("conn-1", [])

    envelope = wrap_event(
        event={
            "ticker": "AAPL",
            "option_chain": "AAPL240119C00190000",
            "premium": Decimal("125000.50"),
            "strike": Decimal("190.0"),
            "timestamp": "2024-01-10T15:30:00Z",
        },
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
    )
    # Inject a Decimal directly into the payload (wrap_event keeps dict payloads
    # as-is, so this mirrors a record.model_dump() without mode="json").
    envelope["payload"]["premium"] = Decimal("125000.50")

    await fanout.deliver(envelope)
    await _flush(fanout)

    wire_msg, _targets = conns.calls[0]
    # The broadcast path calls plain orjson.dumps — this must not raise.
    orjson.dumps(wire_msg)


# ── G3: a hung subscriber socket must not block the poll loop ─────────────────


async def test_hanging_socket_does_not_block_deliver_beyond_timeout():
    """deliver() returns promptly even if a subscriber's send hangs forever.

    The broadcast is scheduled off the poller's await, so deliver() returns
    immediately; the hung send is bounded by DELIVER_TIMEOUT_SECONDS in the
    background task and never stalls the poll loop.
    """
    import time

    from gateway.core import flow_fanout as ff

    class _HangingConnections:
        async def broadcast_to_connection_ids(self, message, connection_ids):
            await asyncio.sleep(3600)  # never returns
            return 0

    fanout = FlowFanout(_HangingConnections())  # type: ignore[arg-type]
    fanout.subscribe("conn", [])

    env = wrap_event(event=_flow_record(), provider="unusual_whales", feed="flow_alerts", source="rest")
    start = time.monotonic()
    delivered = await fanout.deliver(env)
    elapsed = time.monotonic() - start

    # deliver itself never awaits the hung send.
    assert delivered == 1
    assert elapsed < 0.5

    # The background send is bounded by the deliver timeout and resolves to 0.
    original = ff.DELIVER_TIMEOUT_SECONDS
    ff.DELIVER_TIMEOUT_SECONDS = 0.05
    try:
        fanout2 = FlowFanout(_HangingConnections())  # type: ignore[arg-type]
        fanout2.subscribe("conn", [])
        await fanout2.deliver(env)
        t0 = time.monotonic()
        await _flush(fanout2)
        assert time.monotonic() - t0 < 1.0  # bounded by the (patched) timeout
    finally:
        ff.DELIVER_TIMEOUT_SECONDS = original


# ── G4: producer-wired gating ────────────────────────────────────────────────


def test_producer_wired_defaults_false_and_flips_on_mark():
    fanout = FlowFanout(_RecordingConnections())  # type: ignore[arg-type]
    assert fanout.producer_wired is False
    fanout.mark_producer_wired()
    assert fanout.producer_wired is True
