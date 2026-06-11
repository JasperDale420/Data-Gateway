"""WebSocket subscribe/unsubscribe routing for the UW flow channel.

Exercises gateway.api.websocket._handle_message directly (the TestClient's
default test key lacks uw/flow permissions, so we build a permissioned fake
connection). Verifies:
  - provider=uw + feed=flow routes to the FlowFanout (subscribe ack),
  - the connection is registered so a delivered envelope targets it,
  - unsubscribe + disconnect drop the registration,
  - bars still route to the multiplexer path (back-compat untouched).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import gateway.core.globals as globals_module
from gateway.api import websocket as ws
from gateway.core.envelope import wrap_event
from gateway.core.flow_fanout import FLOW_FEED, FlowFanout


async def _flush(fanout: FlowFanout) -> None:
    """Drain the fan-out's scheduled background delivery tasks."""
    pending = list(fanout._deliver_tasks)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


class _RecordingConnections:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, list[str]]] = []
        self._conn = SimpleNamespace(
            client=SimpleNamespace(
                permissions=SimpleNamespace(
                    providers=["uw", "alpaca"],
                    feeds=["flow", "bars"],
                    max_symbols=100,
                    ws_subscriptions_max=1000,
                )
            ),
            subscriptions=set(),
            client_id="orion",
            authenticated=True,
        )

    def get(self, _connection_id):
        return self._conn

    async def broadcast_to_connection_ids(self, message, connection_ids):
        self.calls.append((message, list(connection_ids or [])))
        return len(connection_ids or [])


@pytest.fixture
def wired_fanout():
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    # Simulate the production wiring (gateway.main attaches the UW poller tap),
    # so a flow subscribe ACKs "ok" rather than the not-yet-wired warning.
    fanout.mark_producer_wired()
    original = globals_module._flow_fanout
    globals_module.set_flow_fanout(fanout)
    try:
        yield conns, fanout
    finally:
        globals_module.set_flow_fanout(original)


async def test_subscribe_flow_routes_to_fanout(wired_fanout):
    conns, fanout = wired_fanout
    result = await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    assert result["type"] == "subscription_ack"
    assert result["status"] == "ok"
    assert result["feeds"] == [FLOW_FEED]
    assert result["subscribed"] == ["AAPL"]

    # The connection is now a delivery target for that symbol.
    env = wrap_event(
        event={"ticker": "AAPL", "option_chain": "AAPL240119C00190000", "timestamp": "2024-01-10T15:30:00Z"},
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
    )
    delivered = await fanout.deliver(env)
    await _flush(fanout)
    assert delivered == 1
    assert conns.calls[0][1] == ["conn-1"]


async def test_subscribe_flow_empty_symbols_is_firehose(wired_fanout):
    conns, fanout = wired_fanout
    result = await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow_alerts"], "symbols": []},
        connection_id="conn-all",
        connections=conns,  # type: ignore[arg-type]
    )
    assert result["status"] == "ok"
    # G5: the firehose subscription is recorded in the connection's accounting
    # under a sentinel, so it's visible in status/quota (an empty symbols list
    # would otherwise count as zero and be invisible).
    assert f"{FLOW_FEED}:*" in conns._conn.subscriptions
    env = wrap_event(
        event={"ticker": "ZZZ", "option_chain": "ZZZ240119C00050000", "timestamp": "2024-01-10T15:30:00Z"},
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
    )
    assert await fanout.deliver(env) == 1


async def test_unsubscribe_flow_firehose_clears_accounting(wired_fanout):
    """G5: dropping the firehose removes the sentinel from connection accounting."""
    conns, fanout = wired_fanout
    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow_alerts"], "symbols": []},
        connection_id="conn-all",
        connections=conns,  # type: ignore[arg-type]
    )
    assert f"{FLOW_FEED}:*" in conns._conn.subscriptions
    result = await ws._handle_message(
        {"action": "unsubscribe", "provider": "uw", "feeds": ["flow_alerts"], "symbols": []},
        connection_id="conn-all",
        connections=conns,  # type: ignore[arg-type]
    )
    assert result["status"] == "ok"
    assert f"{FLOW_FEED}:*" not in conns._conn.subscriptions


async def test_subscribe_symbol_then_firehose_clears_stale_per_symbol(wired_fanout):
    """G5b: subscribing a symbol then the firehose leaves no stale per-symbol entry.

    The fan-out drops the per-symbol bucket internally when a connection goes
    firehose; the connection accounting must mirror that so the per-symbol entry
    isn't left dangling (which would double-count against the quota and misreport
    status).
    """
    conns, fanout = wired_fanout
    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    assert f"{FLOW_FEED}:AAPL" in conns._conn.subscriptions

    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": []},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )

    # The per-symbol entry is gone; only the firehose sentinel remains.
    assert f"{FLOW_FEED}:AAPL" not in conns._conn.subscriptions
    assert f"{FLOW_FEED}:*" in conns._conn.subscriptions
    flow_entries = {s for s in conns._conn.subscriptions if s.startswith(f"{FLOW_FEED}:")}
    assert flow_entries == {f"{FLOW_FEED}:*"}


async def test_firehose_respects_subscription_quota(wired_fanout):
    """G5b: the firehose sentinel counts toward ws_subscriptions_max.

    Without counting the sentinel, a firehose could be added even at the cap
    (quota off-by-one). With the connection already at the cap via a non-flow
    subscription, a firehose subscribe must be rejected.
    """
    conns, fanout = wired_fanout
    conns._conn.client.permissions.ws_subscriptions_max = 1
    # Occupy the single slot with a non-flow subscription so the firehose can't fit.
    conns._conn.subscriptions.add("bars:MSFT")

    result = await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": []},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )

    assert result["error_code"] == "GW-E8002"
    assert f"{FLOW_FEED}:*" not in conns._conn.subscriptions


async def test_firehose_quota_allows_when_subsuming_own_per_symbol(wired_fanout):
    """G5b: a firehose that subsumes the connection's own per-symbol flow entries
    fits within the cap — those entries don't double-count post-subscribe."""
    conns, fanout = wired_fanout
    conns._conn.client.permissions.ws_subscriptions_max = 1
    # The connection already holds one per-symbol flow entry (at the cap).
    conns._conn.subscriptions.add(f"{FLOW_FEED}:AAPL")

    result = await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": []},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )

    # The firehose subsumes flow_alerts:AAPL, so total_after stays at 1 → allowed.
    assert result["status"] == "ok"
    assert conns._conn.subscriptions == {f"{FLOW_FEED}:*"}


async def test_resubscribe_same_flow_symbol_at_cap_is_idempotent(wired_fanout):
    """G6: re-subscribing the SAME flow symbol while at the quota cap must not be
    rejected. The subscription is stored as flow_alerts:<symbol>, so the quota
    must compute new_entries off that canonical key — not the caller's 'flow'
    alias, which would never match the stored key and wrongly count a brand-new
    slot at the cap."""
    conns, fanout = wired_fanout
    conns._conn.client.permissions.ws_subscriptions_max = 1

    first = await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    assert first["status"] == "ok"
    assert f"{FLOW_FEED}:AAPL" in conns._conn.subscriptions

    # Idempotent re-subscribe of the same symbol — already at the cap of 1.
    second = await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    assert second["status"] == "ok"
    assert conns._conn.subscriptions == {f"{FLOW_FEED}:AAPL"}


async def test_empty_unsubscribe_clears_firehose_and_per_symbol(wired_fanout):
    """G7: an empty-symbols flow unsubscribe drops EVERYTHING in the fan-out
    (client_disconnect), so connection accounting must clear ALL flow entries —
    not just the firehose sentinel, which would leave stale per-symbol entries
    from earlier per-symbol subscribes."""
    conns, fanout = wired_fanout

    # Subscribe a per-symbol flow entry first, then the firehose.
    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["MSFT"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": []},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    # Simulate a stale per-symbol entry that a buggy firehose-subscribe could
    # have left behind, to prove the empty unsubscribe sweeps it regardless.
    conns._conn.subscriptions.add(f"{FLOW_FEED}:MSFT")

    result = await ws._handle_message(
        {"action": "unsubscribe", "provider": "uw", "feeds": ["flow"], "symbols": []},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )

    assert result["status"] == "ok"
    flow_entries = {s for s in conns._conn.subscriptions if s.startswith(f"{FLOW_FEED}:")}
    assert flow_entries == set()


async def test_subscribe_flow_warns_when_producer_not_wired():
    """G4: fan-out exists but no producer attached ⇒ warn, don't ACK bare 'ok'."""
    conns = _RecordingConnections()
    fanout = FlowFanout(conns)  # type: ignore[arg-type]
    # Deliberately do NOT mark_producer_wired().
    original = globals_module._flow_fanout
    globals_module.set_flow_fanout(fanout)
    try:
        result = await ws._handle_message(
            {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
            connection_id="conn-1",
            connections=conns,  # type: ignore[arg-type]
        )
    finally:
        globals_module.set_flow_fanout(original)
    assert result["status"] == "warning"
    assert result["warning_code"] == "GW-W5003"
    # The subscription is still registered (it will receive data once wired).
    assert f"{FLOW_FEED}:AAPL" in conns._conn.subscriptions


async def test_unsubscribe_flow_drops_target(wired_fanout):
    conns, fanout = wired_fanout
    await ws._handle_message(
        {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    result = await ws._handle_message(
        {"action": "unsubscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    assert result["type"] == "unsubscription_ack"
    assert result["status"] == "ok"
    env = wrap_event(
        event={"ticker": "AAPL", "option_chain": "AAPL240119C00190000", "timestamp": "2024-01-10T15:30:00Z"},
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
    )
    assert await fanout.deliver(env) == 0


async def test_flow_subscribe_without_fanout_returns_error():
    conns = _RecordingConnections()
    original = globals_module._flow_fanout
    globals_module.set_flow_fanout(None)
    try:
        result = await ws._handle_message(
            {"action": "subscribe", "provider": "uw", "feeds": ["flow"], "symbols": ["AAPL"]},
            connection_id="conn-1",
            connections=conns,  # type: ignore[arg-type]
        )
    finally:
        globals_module.set_flow_fanout(original)
    assert result["status"] == "error"
    assert result["error_code"] == "GW-E5002"


def test_is_uw_flow_request_matches_aliases_and_feeds():
    assert ws._is_uw_flow_request("uw", ["flow"]) is True
    assert ws._is_uw_flow_request("unusual_whales", ["flow_alerts"]) is True
    assert ws._is_uw_flow_request("uw", ["bars"]) is False
    assert ws._is_uw_flow_request("alpaca", ["flow"]) is False


def test_flow_feed_permission_normalizes():
    assert ws._normalize_feed_permission("flow") == "flow"
    assert ws._normalize_feed_permission("flow_alerts") == "flow"
    # Back-compat: existing mappings unchanged.
    assert ws._normalize_feed_permission("stock_bars") == "bars"
