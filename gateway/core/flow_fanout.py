"""UW flow WebSocket fan-out.

Additive delivery path that fans the UW poller's already-built flow envelopes
out to subscribed WebSocket clients, reusing the generic
``ConnectionManager.broadcast_to_connection_ids`` primitive.

This does NOT touch the Alpaca multiplexer or the Redis ``heber:events``
publish. The poller publishes to Redis first (unchanged) and then — only when
this fan-out is wired and a client has subscribed — calls :meth:`deliver` with
the same envelope object it just published. Because the envelope is the exact
object Heber writes to Silver, its ``event_id`` is byte-identical to the
poll-path id Orion reads back, so a push/poll duplicate pair collapses in
Orion's deduper with zero new id logic.

Subscriptions are per WebSocket connection: a connection registers either for a
specific set of underlying symbols or, with an empty symbol list, for ALL flow.
Subscriptions die with the socket via :meth:`client_disconnect`.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from gateway.core.logger import logger

if TYPE_CHECKING:
    from gateway.core.connections import ConnectionManager

# The wire ``feed`` label clients match on. Mirrors the UW poller's feed name so
# the delivered message is indistinguishable from a Heber-bound flow envelope.
FLOW_FEED = "flow_alerts"

# Per-delivery send budget. A dead-but-not-closed subscriber socket must never
# stall the UW poll loop (which also drives darkpool/tide/EOD), so each fan-out
# broadcast is bounded by this timeout and scheduled off the poller's await.
DELIVER_TIMEOUT_SECONDS = 2.0


def _json_safe(value: Any) -> Any:
    """Coerce ``value`` into an orjson-serializable structure.

    The UW poller taps the same envelope it published to Redis, whose payload
    can carry ``Decimal`` values (UW SDK models). Redis tolerates them
    (redis_sink uses ``orjson.dumps(default=str)``), but the WS broadcast path
    calls plain ``orjson.dumps`` which RAISES on ``Decimal`` — the fan-out
    would swallow the error and Orion would receive no push. Coercing here, on
    a COPY built for the wire, keeps the original envelope (and therefore the
    Redis/Heber bytes and the precomputed ``event_id`` string) untouched.
    """
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, Decimal):
        # Mirror orjson(default=str) so Redis and WS see the same string form.
        return str(value)
    return value


class FlowFanout:
    """Per-connection subscription registry + envelope delivery for UW flow.

    The registry maps an underlying symbol to the set of connection ids that
    asked for it, plus a separate ``_all`` bucket for symbol-less (firehose)
    subscribers. Every connection id that appears anywhere here also appears in
    ``_by_connection`` so :meth:`client_disconnect` is O(symbols-for-that-conn).
    """

    def __init__(self, connections: ConnectionManager):
        self._connections = connections
        # symbol -> {connection_id}
        self._by_symbol: dict[str, set[str]] = {}
        # connection_ids subscribed to ALL flow (empty-symbol subscribe)
        self._all: set[str] = set()
        # connection_id -> {symbols} (empty set means ALL bucket)
        self._by_connection: dict[str, set[str]] = {}
        # True once the producer hook (UW poller's flow tap) is wired. Until
        # then a subscribe can be ACKed but no envelope will ever arrive, so the
        # subscribe handler must surface a warning instead of a bare "ok".
        self._producer_wired = False
        # Strong refs to in-flight delivery tasks so they aren't GC'd mid-send
        # (asyncio holds only weak refs to bare-created tasks).
        self._deliver_tasks: set[asyncio.Task[int]] = set()

    @property
    def producer_wired(self) -> bool:
        """Whether a flow producer (the UW poller tap) has been attached."""
        return self._producer_wired

    def mark_producer_wired(self) -> None:
        """Record that the flow producer hook is attached (see gateway.main)."""
        self._producer_wired = True

    def subscribe(self, connection_id: str, symbols: list[str] | None) -> None:
        """Register ``connection_id`` for ``symbols`` (empty/None ⇒ ALL flow)."""
        normalized = [s.upper() for s in (symbols or []) if isinstance(s, str) and s]
        conn_symbols = self._by_connection.setdefault(connection_id, set())
        if not normalized:
            self._all.add(connection_id)
            # An ALL subscription supersedes any prior per-symbol entries: drop
            # this connection from every per-symbol bucket it held, then mark its
            # bucket empty to signal firehose membership (so client_disconnect
            # and _by_symbol stay consistent).
            for symbol in conn_symbols:
                holders = self._by_symbol.get(symbol)
                if holders:
                    holders.discard(connection_id)
                    if not holders:
                        self._by_symbol.pop(symbol, None)
            conn_symbols.clear()
            return
        for symbol in normalized:
            self._by_symbol.setdefault(symbol, set()).add(connection_id)
            conn_symbols.add(symbol)

    def unsubscribe(self, connection_id: str, symbols: list[str] | None) -> None:
        """Remove ``connection_id`` from ``symbols`` (empty/None ⇒ ALL + every symbol)."""
        normalized = [s.upper() for s in (symbols or []) if isinstance(s, str) and s]
        if not normalized:
            self.client_disconnect(connection_id)
            return
        conn_symbols = self._by_connection.get(connection_id)
        for symbol in normalized:
            holders = self._by_symbol.get(symbol)
            if holders:
                holders.discard(connection_id)
                if not holders:
                    self._by_symbol.pop(symbol, None)
            if conn_symbols is not None:
                conn_symbols.discard(symbol)
        if conn_symbols is not None and not conn_symbols and connection_id not in self._all:
            self._by_connection.pop(connection_id, None)

    def client_disconnect(self, connection_id: str) -> None:
        """Drop all of a connection's flow subscriptions (socket closed)."""
        self._all.discard(connection_id)
        conn_symbols = self._by_connection.pop(connection_id, None)
        if not conn_symbols:
            return
        for symbol in conn_symbols:
            holders = self._by_symbol.get(symbol)
            if holders:
                holders.discard(connection_id)
                if not holders:
                    self._by_symbol.pop(symbol, None)

    @property
    def subscriber_count(self) -> int:
        """Number of distinct connections subscribed (per-symbol or ALL)."""
        return len(set(self._by_connection) | self._all)

    def _targets(self, symbol: str) -> list[str]:
        targets = set(self._all)
        if symbol:
            targets |= self._by_symbol.get(symbol.upper(), set())
        return list(targets)

    async def deliver(self, envelope: dict[str, Any]) -> int:
        """Fan one flow envelope out to its subscribers.

        The poller awaits this on its flow-poll path, so it must NOT block on a
        slow client: target selection and the JSON-safe wire build happen
        inline (cheap), then the actual broadcast is scheduled as a background
        task bounded by :data:`DELIVER_TIMEOUT_SECONDS`. A dead-but-not-closed
        Orion socket can therefore never stall flow polling (and with it
        darkpool/tide/EOD). Returns the number of subscribers the message was
        *scheduled* for (0 when there are no subscribers, so the hot path does
        no work in the common case). Mirrors the bars wire shape so clients
        share a single decoder.
        """
        if not envelope:
            return 0
        symbol = str(envelope.get("symbol") or "")
        targets = self._targets(symbol)
        if not targets:
            return 0

        # Build a JSON-safe wire message. The envelope/payload may carry Decimal
        # values (UW SDK models); plain orjson.dumps in the broadcast path would
        # raise on those and the whole push would silently drop. Coerce a COPY so
        # the original envelope (already published to Redis) stays byte-identical
        # — the event_id is a precomputed string and is not recomputed here.
        wire_msg = {
            "type": "data",
            "feed": FLOW_FEED,
            "symbol": symbol,
            "event_id": envelope.get("event_id"),
            "envelope": _json_safe(envelope),
            "data": _json_safe(envelope.get("payload")),
        }

        task = asyncio.create_task(self._deliver_bounded(wire_msg, targets, symbol))
        self._deliver_tasks.add(task)
        task.add_done_callback(self._deliver_tasks.discard)
        return len(targets)

    async def _deliver_bounded(self, wire_msg: dict[str, Any], targets: list[str], symbol: str) -> int:
        """Broadcast ``wire_msg`` to ``targets`` under a hard timeout.

        Runs off the poller's await (scheduled by :meth:`deliver`) so a slow or
        hung subscriber socket can never delay flow polling. A timeout or send
        error is logged and swallowed — fan-out is best-effort relative to the
        Redis publish that already succeeded.
        """
        try:
            return await asyncio.wait_for(
                self._connections.broadcast_to_connection_ids(wire_msg, targets),
                timeout=DELIVER_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning("flow_fanout_deliver_timeout", symbol=symbol, targets=len(targets))
            return 0
        except Exception as exc:  # never let a fan-out failure disturb the poller
            logger.warning("flow_fanout_deliver_failed", symbol=symbol, error=str(exc))
            return 0
