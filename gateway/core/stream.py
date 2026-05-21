"""WebSocket multiplexer for upstream Alpaca connections.

Maintains single upstream WebSocket connections per stream type (stocks, options,
crypto, news) and fans out received data to subscribed downstream clients.
"""

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from time import perf_counter
from typing import Any

import msgpack
import orjson
import websockets
from websockets.asyncio.client import ClientConnection

from gateway.core.envelope import fast_wrap_streaming_event
from gateway.core.logger import logger
from gateway.core.metrics import (
    record_stream_fanout_batch_size,
    record_stream_fanout_dispatch_event,
    set_stream_fanout_limits_metrics,
)
from gateway.core.security import SYMBOL_PATTERNS
from gateway.core.validator import get_validator

# OCC option contract format (e.g. "AAPL260116C00200000"). Used by
# _sanitize_subscription_request to keep equity tickers off the OPRA
# upstream and option contracts off SIP/IEX — Alpaca silently drops the
# entire upstream subscribe when the symbol shape doesn't match the
# stream, leaving downstream clients connected with 0 ticks.
_OCC_OPTION_PATTERN = SYMBOL_PATTERNS["option_occ"]


def _is_occ_option_symbol(symbol: str) -> bool:
    return bool(_OCC_OPTION_PATTERN.match(symbol))


DEFAULT_FANOUT_MAX_INFLIGHT = 100
DEFAULT_FANOUT_BATCH_SIZE = 32
# Warn when a single on_message call holds the receive loop this long;
# long handlers fill Alpaca's outbound buffer and can trigger their
# slow-client (407) disconnect path, surfacing on our side as code 1006.
_ON_MESSAGE_SLOW_THRESHOLD_SECONDS = 0.1
MESSAGE_TYPE_TO_DATA_TYPE = {
    "b": "bars",
    "q": "quotes",
    "t": "trades",
    "n": "news",
}
_VALIDATABLE_FEEDS = frozenset({"bars", "quotes", "trades"})


def _orjson_default(obj: Any) -> str:
    """Fallback serializer for orjson.dumps.

    Handles non-serializable types that Alpaca's WebSocket SDK may include
    in raw event dicts:
    - pandas.Timestamp, datetime — have .isoformat()
    - msgpack.Timestamp (OPRA options stream) — has .to_datetime()
    """
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "to_datetime"):
        return obj.to_datetime().isoformat()
    raise TypeError(f"Type is not JSON serializable: {type(obj).__name__}")


class SequenceTracker:
    """Per-symbol, per-feed sequence counter for gap detection (PRD §Sequence Numbers).

    Clients see monotonically increasing integers starting at 1.
    A gap (new_seq != last_seq + 1) signals missed messages.
    All counters reset on gateway restart.

    Thread safety: Both next_seq() and reset() are synchronous with no await
    points, so they execute atomically within a single asyncio event loop
    iteration. No lock is needed.
    """

    def __init__(self) -> None:
        self._counters: dict[tuple[str, str], int] = {}

    def next_seq(self, symbol: str, feed: str) -> int:
        """Return the next sequence number for (symbol, feed)."""
        key = (symbol, feed)
        seq = self._counters.get(key, 0) + 1
        self._counters[key] = seq
        return seq

    def reset(self) -> None:
        """Clear all counters (called on gateway restart)."""
        self._counters.clear()

    def get_status(self) -> dict:
        """Diagnostic snapshot of tracked keys and total messages."""
        return {
            "tracked_keys": len(self._counters),
            "total_messages": sum(self._counters.values()),
        }


class AlpacaStreamType(Enum):
    """Alpaca WebSocket stream types per PRD."""

    STOCKS_SIP = "stocks_sip"
    STOCKS_IEX = "stocks_iex"
    OPTIONS = "options"
    CRYPTO = "crypto"
    NEWS = "news"

    @classmethod
    def from_feed(cls, feed: str) -> "AlpacaStreamType":
        """Convert feed name to stream type."""
        mapping = {
            # Stocks
            "stock_bars": cls.STOCKS_SIP,
            "stock_quotes": cls.STOCKS_SIP,
            "stock_trades": cls.STOCKS_SIP,
            "stock_dailyBars": cls.STOCKS_SIP,
            "stock_updatedBars": cls.STOCKS_SIP,
            "stock_lulds": cls.STOCKS_SIP,
            "stock_statuses": cls.STOCKS_SIP,
            "stock_imbalances": cls.STOCKS_SIP,
            # Options
            "option_bars": cls.OPTIONS,
            "option_quotes": cls.OPTIONS,
            "option_trades": cls.OPTIONS,
            # Crypto
            "crypto_bars": cls.CRYPTO,
            "crypto_quotes": cls.CRYPTO,
            "crypto_trades": cls.CRYPTO,
            "crypto_dailyBars": cls.CRYPTO,
            "crypto_updatedBars": cls.CRYPTO,
            "crypto_orderbooks": cls.CRYPTO,
            # News
            "news": cls.NEWS,
        }
        return mapping.get(feed, cls.STOCKS_SIP)

    @property
    def endpoint(self) -> str:
        """Get WebSocket endpoint URL for this stream type."""
        endpoints = {
            AlpacaStreamType.STOCKS_SIP: "wss://stream.data.alpaca.markets/v2/sip",
            AlpacaStreamType.STOCKS_IEX: "wss://stream.data.alpaca.markets/v2/iex",
            AlpacaStreamType.OPTIONS: "wss://stream.data.alpaca.markets/v1beta1/opra",
            AlpacaStreamType.CRYPTO: "wss://stream.data.alpaca.markets/v1beta3/crypto/us",
            AlpacaStreamType.NEWS: "wss://stream.data.alpaca.markets/v1beta1/news",
        }
        return endpoints[self]

    @property
    def instrument_type(self) -> str:
        """Get canonical instrument type for this stream."""
        if self in (AlpacaStreamType.STOCKS_SIP, AlpacaStreamType.STOCKS_IEX, AlpacaStreamType.NEWS):
            return "equity"
        if self == AlpacaStreamType.OPTIONS:
            return "option"
        if self == AlpacaStreamType.CRYPTO:
            return "crypto"
        return "equity"


_FEED_NAMES = ("bars", "quotes", "trades", "news")


@dataclass
class ClientSubscription:
    """Tracks a client's subscriptions for a stream type."""

    client_id: str
    feeds: dict[str, set[str]] = field(default_factory=lambda: {name: set() for name in _FEED_NAMES})


class SubscriptionManager:
    """Manages subscription aggregation across clients.

    Tracks which clients want which symbols and computes the union
    for upstream subscription.
    """

    def __init__(self) -> None:
        self._subscriptions: dict[str, ClientSubscription] = {}
        self._index: dict[str, dict[str, set[str]]] = {name: {} for name in _FEED_NAMES}

    def _subscribe_feed(
        self,
        client_id: str,
        sub: ClientSubscription,
        feed: str,
        symbols: list[str],
    ) -> set[str]:
        """Add symbols for a single feed. Returns newly added symbols (no other client had them)."""
        new_symbols: set[str] = set()
        client_set = sub.feeds[feed]
        index = self._index[feed]
        for symbol in symbols:
            if symbol in client_set:
                continue
            client_set.add(symbol)
            clients = index.get(symbol)
            if clients is None:
                index[symbol] = {client_id}
                new_symbols.add(symbol)
            else:
                clients.add(client_id)
        return new_symbols

    def _unsubscribe_feed(
        self,
        client_id: str,
        sub: ClientSubscription,
        feed: str,
        symbols: list[str],
    ) -> set[str]:
        """Remove symbols for a single feed. Returns symbols no longer needed upstream."""
        removed: set[str] = set()
        client_set = sub.feeds[feed]
        index = self._index[feed]
        for symbol in symbols:
            if symbol not in client_set:
                continue
            client_set.discard(symbol)
            clients = index.get(symbol)
            if clients:
                clients.discard(client_id)
                if not clients:
                    index.pop(symbol, None)
                    removed.add(symbol)
        return removed

    def _remove_client_feed(
        self,
        client_id: str,
        sub: ClientSubscription,
        feed: str,
    ) -> set[str]:
        """Remove all of a client's symbols for a feed. Returns symbols no longer needed upstream."""
        removed: set[str] = set()
        index = self._index[feed]
        for symbol in sub.feeds[feed]:
            clients = index.get(symbol)
            if clients:
                clients.discard(client_id)
                if not clients:
                    index.pop(symbol, None)
                    removed.add(symbol)
        return removed

    def subscribe(
        self,
        client_id: str,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> tuple[set[str], set[str], set[str], set[str]]:
        """Add subscriptions for a client. Returns newly added symbols."""
        if client_id not in self._subscriptions:
            self._subscriptions[client_id] = ClientSubscription(client_id=client_id)
        sub = self._subscriptions[client_id]
        feeds = {"bars": bars, "quotes": quotes, "trades": trades, "news": news}
        return tuple(  # type: ignore[return-value]
            self._subscribe_feed(client_id, sub, name, syms) if syms else set() for name, syms in feeds.items()
        )

    def unsubscribe(
        self,
        client_id: str,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> tuple[set[str], set[str], set[str], set[str]]:
        """Remove subscriptions for a client. Returns symbols to unsubscribe upstream."""
        if client_id not in self._subscriptions:
            return set(), set(), set(), set()
        sub = self._subscriptions[client_id]
        feeds = {"bars": bars, "quotes": quotes, "trades": trades, "news": news}
        return tuple(  # type: ignore[return-value]
            self._unsubscribe_feed(client_id, sub, name, syms) if syms else set() for name, syms in feeds.items()
        )

    def remove_client(self, client_id: str) -> tuple[set[str], set[str], set[str], set[str]]:
        """Remove all subscriptions for a client."""
        if client_id not in self._subscriptions:
            return set(), set(), set(), set()
        sub = self._subscriptions.pop(client_id)
        return tuple(  # type: ignore[return-value]
            self._remove_client_feed(client_id, sub, name) for name in _FEED_NAMES
        )

    def get_clients_for_symbol(self, symbol: str, data_type: str) -> list[str]:
        """Get all clients subscribed to a symbol for a data type (returns copy)."""
        return list(self.get_clients_for_symbol_view(symbol, data_type))

    def get_clients_for_symbol_view(self, symbol: str, data_type: str) -> frozenset[str]:
        """Get clients subscribed to a symbol as an immutable snapshot.

        Returns a frozenset to prevent accidental mutation of the internal
        subscription index during async fanout dispatch.
        """
        return frozenset(self._index.get(data_type, {}).get(symbol, set()))

    def _aggregate(self) -> tuple[set[str], set[str], set[str], set[str]]:
        """Compute union of all client subscriptions."""
        return (
            set(self._index["bars"].keys()),
            set(self._index["quotes"].keys()),
            set(self._index["trades"].keys()),
            set(self._index["news"].keys()),
        )

    def get_all_subscriptions(self) -> tuple[set[str], set[str], set[str], set[str]]:
        """Get current aggregate subscriptions for resubscription on reconnect."""
        return self._aggregate()


class UpstreamConnection:
    """Single upstream WebSocket connection to Alpaca.

    Handles authentication, subscription management, heartbeat monitoring,
    and reconnection with exponential backoff.
    """

    def __init__(
        self,
        stream_type: AlpacaStreamType,
        api_key: str,
        api_secret: str,
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
        endpoint: str | None = None,
        base_delay: float = 1.0,
        max_delay: float = 16.0,
        max_retries: int = 10,
    ) -> None:
        self.stream_type = stream_type
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_message = on_message
        self.endpoint = endpoint or self.stream_type.endpoint
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries

        self._ws: ClientConnection | None = None
        self._authenticated = False
        self._running = False
        self._receive_task: asyncio.Task | None = None
        self._subscriptions = SubscriptionManager()
        self._connected_event = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        # websockets 16.0 uses .state attribute, not .open/.closed
        if self._ws is None:
            return False
        try:
            from websockets.protocol import State

            return self._ws.state == State.OPEN and self._authenticated
        except (AttributeError, ImportError):
            # Fallback for older versions
            return getattr(self._ws, "open", False) and self._authenticated

    @property
    def subscriptions(self) -> SubscriptionManager:
        return self._subscriptions

    async def connect(self) -> None:
        """Establish WebSocket connection.

        Closes any existing connection first to avoid leaking connection slots
        on Alpaca's side (each endpoint allows only 1 concurrent connection).
        """
        # Close existing connection to release Alpaca's connection slot
        await self._close_ws()

        endpoint = self.endpoint
        logger.info("connecting_upstream", stream=self.stream_type.value, endpoint=endpoint)

        self._ws = await websockets.connect(
            endpoint,
            ping_interval=30,
            ping_timeout=30,
            close_timeout=5,
        )
        logger.info("connected_upstream", stream=self.stream_type.value)

    def _is_msgpack_stream(self) -> bool:
        """Check if this stream uses MessagePack format (OPRA options)."""
        return self.stream_type == AlpacaStreamType.OPTIONS

    def _decode_message(self, message: bytes | str) -> Any:
        """Decode a message from either JSON or MessagePack format."""
        if isinstance(message, bytes):
            return msgpack.unpackb(message, raw=False)
        return json.loads(message)

    def _encode_message(self, data: dict[str, Any]) -> bytes | str:
        """Encode a message to either JSON or MessagePack format."""
        if self._is_msgpack_stream():
            return msgpack.packb(data)
        return orjson.dumps(data).decode()

    async def authenticate(self) -> None:
        """Send authentication message and wait for response.

        Handles both JSON (most streams) and MessagePack (OPRA options) formats.
        """
        if not self._ws:
            raise RuntimeError("Not connected")

        # Alpaca sends a welcome message first: [{"T":"success","msg":"connected"}]
        welcome = await self._ws.recv()
        welcome_data = self._decode_message(welcome)
        logger.debug("welcome_received", stream=self.stream_type.value, data=welcome_data)

        # Send auth - always encoded in the appropriate format
        auth_msg = {
            "action": "auth",
            "key": self.api_key,
            "secret": self.api_secret,
        }
        await self._ws.send(self._encode_message(auth_msg))

        # Wait for auth response
        response = await self._ws.recv()
        data = self._decode_message(response)

        # Alpaca sends array of messages
        if isinstance(data, list):
            for msg in data:
                if msg.get("T") == "success" and msg.get("msg") == "authenticated":
                    self._authenticated = True
                    logger.info("authenticated_upstream", stream=self.stream_type.value)
                    return
                if msg.get("T") == "error":
                    raise RuntimeError(f"Auth failed: {msg.get('msg')}")

        logger.error("unexpected_auth_response", stream=self.stream_type.value, data=data)
        raise RuntimeError(f"Unexpected auth response: {data}")

    async def subscribe(
        self,
        bars: set[str] | None = None,
        quotes: set[str] | None = None,
        trades: set[str] | None = None,
        news: set[str] | None = None,
    ) -> list[str]:
        """Subscribe to symbols upstream. Returns list of warnings (if any)."""
        if not self.is_connected:
            logger.warning("subscribe_not_connected", stream=self.stream_type.value)
            return []

        bars, quotes, trades, news, warnings = self._sanitize_subscription_request(bars, quotes, trades, news)

        msg: dict[str, Any] = {"action": "subscribe"}
        if bars:
            msg["bars"] = list(bars)
        if quotes:
            msg["quotes"] = list(quotes)
        if trades:
            msg["trades"] = list(trades)
        if news:
            msg["news"] = list(news)

        if len(msg) > 1:  # Has at least one subscription type
            await self._ws.send(self._encode_message(msg))
            logger.info(
                "subscribed_upstream",
                stream=self.stream_type.value,
                bars=list(bars) if bars else [],
                quotes=list(quotes) if quotes else [],
                trades=list(trades) if trades else [],
                news=list(news) if news else [],
            )

        return warnings

    async def unsubscribe(
        self,
        bars: set[str] | None = None,
        quotes: set[str] | None = None,
        trades: set[str] | None = None,
        news: set[str] | None = None,
    ) -> None:
        """Unsubscribe from symbols upstream."""
        if not self.is_connected:
            return

        bars, quotes, trades, news, _warnings = self._sanitize_subscription_request(bars, quotes, trades, news)

        msg: dict[str, Any] = {"action": "unsubscribe"}
        if bars:
            msg["bars"] = list(bars)
        if quotes:
            msg["quotes"] = list(quotes)
        if trades:
            msg["trades"] = list(trades)
        if news:
            msg["news"] = list(news)

        if len(msg) > 1:
            await self._ws.send(self._encode_message(msg))
            logger.info(
                "unsubscribed_upstream",
                stream=self.stream_type.value,
                bars=list(bars) if bars else [],
                quotes=list(quotes) if quotes else [],
                trades=list(trades) if trades else [],
                news=list(news) if news else [],
            )

    def _sanitize_subscription_request(
        self,
        bars: set[str] | None,
        quotes: set[str] | None,
        trades: set[str] | None,
        news: set[str] | None,
    ) -> tuple[set[str] | None, set[str] | None, set[str] | None, set[str] | None, list[str]]:
        """Sanitize subscription request, returning (bars, quotes, trades, news, warnings).

        Filters out symbols that don't match the upstream stream's expected
        shape: equity tickers off the OPTIONS (OPRA) stream, option contracts
        off the STOCKS_SIP/STOCKS_IEX streams. Alpaca silently drops the
        entire upstream subscribe payload when any symbol mismatches the
        stream — without this filter a downstream client subscribe still
        gets ``subscription_ack: ok`` but no ticks ever flow (2026-05-01 RCA).
        """
        warnings: list[str] = []
        if self.stream_type == AlpacaStreamType.OPTIONS and bars:
            warnings.append(
                f"Options stream does not support bars subscriptions ({len(bars)} symbols ignored). "
                "Use option_quotes or option_trades instead."
            )
            logger.warning(
                "options_stream_ignoring_bars_subscription",
                count=len(bars),
                hint="Alpaca options websocket supports quotes and trades, not bars",
            )
            bars = None

        bars, quotes, trades, warnings = self._filter_symbols_by_stream_shape(
            bars=bars, quotes=quotes, trades=trades, warnings=warnings
        )
        return bars, quotes, trades, news, warnings

    def _filter_symbols_by_stream_shape(
        self,
        *,
        bars: set[str] | None,
        quotes: set[str] | None,
        trades: set[str] | None,
        warnings: list[str],
    ) -> tuple[set[str] | None, set[str] | None, set[str] | None, list[str]]:
        """Drop symbols whose shape doesn't match the upstream stream type.

        - OPTIONS stream accepts only OCC-shaped symbols (e.g. ``AAPL260116C00200000``).
        - STOCKS_SIP / STOCKS_IEX accept everything except OCC-shaped symbols.
        - Other stream types (CRYPTO, NEWS) are passed through unchanged — their
          symbol shapes (``BTC/USD``, free text) are out of scope here.
        """
        if self.stream_type == AlpacaStreamType.OPTIONS:
            keep = _is_occ_option_symbol
            label = "options"
        elif self.stream_type in (AlpacaStreamType.STOCKS_SIP, AlpacaStreamType.STOCKS_IEX):
            keep = lambda sym: not _is_occ_option_symbol(sym)  # noqa: E731
            label = "stocks"
        else:
            return bars, quotes, trades, warnings

        for feed_name, symbol_set in (("bars", bars), ("quotes", quotes), ("trades", trades)):
            if not symbol_set:
                continue
            kept = {s for s in symbol_set if keep(s)}
            dropped = symbol_set - kept
            if dropped:
                warnings.append(
                    f"{label} stream ignoring {len(dropped)} symbol(s) "
                    f"with mismatched shape on {feed_name}: {sorted(dropped)[:5]}" + ("..." if len(dropped) > 5 else "")
                )
                logger.warning(
                    "stream_symbol_shape_mismatch",
                    stream=self.stream_type.value,
                    feed=feed_name,
                    dropped_count=len(dropped),
                    sample=sorted(dropped)[:5],
                )
            if feed_name == "bars":
                bars = kept or None
            elif feed_name == "quotes":
                quotes = kept or None
            else:
                trades = kept or None
        return bars, quotes, trades, warnings

    async def start(self) -> None:
        """Start the connection and receive loop."""
        self._running = True
        await self._connect_and_run()

    async def stop(self) -> None:
        """Stop the connection gracefully with aggressive cleanup.

        Ensures WebSocket is properly closed to release connection slots on Alpaca's
        side, preventing 'connection limit exceeded' errors on restart.
        """
        self._running = False
        self._authenticated = False

        # Cancel receive task first
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                logger.debug("receive_task_cancelled", stream=self.stream_type.value)

        # Close the WebSocket connection
        await self._close_ws()

    async def _close_ws(self) -> None:
        """Close the current WebSocket connection and release Alpaca's connection slot.

        Safe to call even if no connection exists. This must be called before opening
        a new connection to the same endpoint, since Alpaca only allows 1 concurrent
        connection per endpoint.
        """
        if not self._ws:
            return

        ws = self._ws
        self._ws = None
        self._authenticated = False
        self._connected_event.clear()

        try:
            await asyncio.wait_for(
                ws.close(code=1000, reason="Gateway reconnecting"),
                timeout=3.0,
            )
            logger.debug("websocket_closed", stream=self.stream_type.value)
        except TimeoutError:
            logger.warning(
                "websocket_close_timeout",
                stream=self.stream_type.value,
                action="forcing_close",
            )
            try:
                ws.transport.abort()
            except Exception as abort_err:
                logger.debug("websocket_abort_failed", error=str(abort_err), exc_info=True)
        except Exception as e:
            logger.warning(
                "websocket_close_error",
                stream=self.stream_type.value,
                error=str(e),
            )

    async def _connect_and_run(self) -> None:
        """Main connection loop with reconnection.

        Each loop iteration performs **exactly one** connect attempt. A
        successful connection is held until ``_receive_loop`` returns (clean
        disconnect, transport error, or cancellation); at that point we
        release Alpaca's connection slot, sleep for exponential backoff, and
        try again.

        Previously this loop was paired with a separate
        ``_reconnect_with_backoff()`` helper that also did its own
        connect/auth/subscribe. After a successful reconnect it returned,
        and the outer loop would then call ``connect()`` a second time —
        tearing down the just-established WS (via ``_close_ws()``) and
        opening a fresh one. That doubled Alpaca-side connection churn on
        every flap and left a 1–3s window after each recovery where bars
        were lost, which was the proximate cause of the simultaneous
        stale-data events we saw across downstream clients on Apr 15–17.
        """
        attempt = 0
        while self._running:
            try:
                await self.connect()
                await self.authenticate()
                self._connected_event.set()

                # Resubscribe to all symbols
                bars, quotes, trades, news = self._subscriptions.get_all_subscriptions()
                if bars or quotes or trades or news:
                    await self.subscribe(bars, quotes, trades, news)

                if attempt > 0:
                    logger.info(
                        "reconnected",
                        stream=self.stream_type.value,
                        attempt=attempt,
                    )
                # A successful connection resets the retry budget. The outer
                # loop continues to _receive_loop which runs until the WS dies.
                attempt = 0

                await self._receive_loop()

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(
                    "connection_closed",
                    stream=self.stream_type.value,
                    code=e.code,
                    reason=e.reason,
                )
            except Exception as exc:
                error_msg = str(exc)
                if any(msg in error_msg for msg in self._NON_RECOVERABLE_ERRORS):
                    logger.error(
                        "non_recoverable_error",
                        stream=self.stream_type.value,
                        error=error_msg,
                        hint="Check for stale connections or competing applications using the same Alpaca credentials.",
                    )
                    self._running = False
                    self._authenticated = False
                    await self._close_ws()
                    logger.info(
                        "stream_dormant",
                        stream=self.stream_type.value,
                        hint="Will restart on next client subscription",
                    )
                    return
                logger.exception("connection_error", stream=self.stream_type.value)

            self._authenticated = False
            # Release Alpaca's connection slot before backing off / retrying.
            await self._close_ws()

            if not self._running:
                return

            attempt += 1
            if attempt > self.max_retries:
                logger.error(
                    "reconnect_exhausted",
                    stream=self.stream_type.value,
                    max_retries=self.max_retries,
                )
                self._running = False
                logger.info(
                    "stream_dormant",
                    stream=self.stream_type.value,
                    hint="Will restart on next client subscription",
                )
                return

            delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
            jitter = delay * 0.2 * (random.random() * 2 - 1)  # ±20%
            wait = max(0.0, delay + jitter)
            logger.info(
                "reconnecting",
                stream=self.stream_type.value,
                attempt=attempt,
                delay=round(wait, 3),
            )
            await asyncio.sleep(wait)

    async def _receive_loop(self) -> None:
        """Receive and dispatch messages.

        Handles both JSON (stocks, crypto, news) and MessagePack (OPRA options) formats.

        Note: This loop is inherently sequential — `async for message in ws` yields
        one frame at a time and `on_message` is awaited inline. Backpressure is applied
        via a per-message timeout: if on_message takes longer than the threshold, we
        log a warning. The websockets library itself applies TCP-level backpressure
        by not reading from the socket while we're blocked in on_message.
        """
        if not self._ws:
            return

        async for message in self._ws:
            try:
                data = self._decode_message(message)

                # Alpaca sends arrays of messages
                messages = data if isinstance(data, list) else [data]
                for msg in messages:
                    start = perf_counter()
                    await self.on_message(msg)
                    elapsed = perf_counter() - start
                    if elapsed >= _ON_MESSAGE_SLOW_THRESHOLD_SECONDS:
                        logger.warning(
                            "on_message_slow",
                            stream=self.stream_type.value,
                            duration_seconds=round(elapsed, 3),
                            message_type=msg.get("T") if isinstance(msg, dict) else None,
                        )

            except Exception as e:
                logger.error(
                    "message_parse_error",
                    stream=self.stream_type.value,
                    error=str(e),
                )

    # Auth error messages that indicate account-level issues, not transient failures
    _NON_RECOVERABLE_ERRORS = ("connection limit exceeded",)


class StreamMultiplexer:
    """Manages all upstream connections and routes messages to clients.

    This is the main entry point for the streaming subsystem.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        on_data: Callable[[str, str, dict[str, Any]], Awaitable[None]],
        use_iex: bool = False,
        options_feed: str = "opra",
        lazy_connect: bool = True,
        fanout_max_inflight: int = DEFAULT_FANOUT_MAX_INFLIGHT,
        fanout_batch_size: int = DEFAULT_FANOUT_BATCH_SIZE,
        on_broadcast: Callable[[dict[str, Any] | str | bytes, list[str]], Awaitable[int]] | None = None,
        on_envelope: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        eager_connect_types: list[str] | None = None,
    ) -> None:
        """Initialize multiplexer.

        Args:
            api_key: Alpaca API key
            api_secret: Alpaca API secret
            on_data: Callback for per-client delivery in the FALLBACK fanout
                path (client_id, data_type, message). NOT called when
                ``on_broadcast`` is set — the broadcast fast-path delivers
                directly without invoking on_data per client. Per-envelope
                side-effects (e.g. sink publish) must use ``on_envelope``
                instead, not on_data, otherwise they only fire in the
                fallback path.
            use_iex: Use IEX feed instead of SIP for stocks
            options_feed: Alpaca options stream feed (`opra` or `indicative`)
            lazy_connect: If True, only connect to streams when first client subscribes
            fanout_max_inflight: Max concurrent callback deliveries per fanout batch.
            fanout_batch_size: Number of clients to dispatch per gather batch.
            on_broadcast: Optional callback for efficient broadcast (message, client_ids) -> count
            on_envelope: Optional callback fired ONCE per envelope, regardless
                of which fanout path is taken (broadcast fast-path OR fallback
                per-client). Use this for per-envelope side-effects like sink
                publishing that must happen for every upstream message,
                independent of client fanout. Until 2026-05-21, sink dispatch
                was inside ``on_data`` which the broadcast fast-path never
                invokes — all streaming events bypassed the Heber sink in
                production. This callback is the fix; it is the single source
                of truth for "fired once per envelope" side-effects.
            eager_connect_types: Subset of stream-type names ("stocks", "options",
                "crypto", "news") to connect eagerly at start(), regardless of
                lazy_connect. Use this when you want a hot stocks pipe ready for
                the 9:30 ET open without paying for an always-on options feed.
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_data = on_data
        self._options_feed = str(options_feed or "opra").strip().lower()
        if self._options_feed not in {"opra", "indicative"}:
            raise ValueError("options_feed must be 'opra' or 'indicative'")
        self._lazy_connect = lazy_connect
        self._eager_connect_types = {t.strip().lower() for t in (eager_connect_types or []) if t.strip()}
        self._fanout_max_inflight = max(1, fanout_max_inflight)
        self._fanout_client_batch_size = max(1, fanout_batch_size)
        self._on_broadcast = on_broadcast
        self._on_envelope = on_envelope
        set_stream_fanout_limits_metrics(
            max_inflight=self._fanout_max_inflight,
            batch_size=self._fanout_client_batch_size,
        )

        stock_type = AlpacaStreamType.STOCKS_IEX if use_iex else AlpacaStreamType.STOCKS_SIP

        self._connections: dict[AlpacaStreamType, UpstreamConnection] = {
            stock_type: UpstreamConnection(
                stream_type=stock_type,
                api_key=api_key,
                api_secret=api_secret,
                on_message=lambda m: self._handle_message(stock_type, m),
            ),
            AlpacaStreamType.OPTIONS: UpstreamConnection(
                stream_type=AlpacaStreamType.OPTIONS,
                api_key=api_key,
                api_secret=api_secret,
                endpoint=f"wss://stream.data.alpaca.markets/v1beta1/{self._options_feed}",
                on_message=lambda m: self._handle_message(AlpacaStreamType.OPTIONS, m),
            ),
            AlpacaStreamType.CRYPTO: UpstreamConnection(
                stream_type=AlpacaStreamType.CRYPTO,
                api_key=api_key,
                api_secret=api_secret,
                on_message=lambda m: self._handle_message(AlpacaStreamType.CRYPTO, m),
            ),
            AlpacaStreamType.NEWS: UpstreamConnection(
                stream_type=AlpacaStreamType.NEWS,
                api_key=api_key,
                api_secret=api_secret,
                on_message=lambda m: self._handle_message(AlpacaStreamType.NEWS, m),
            ),
        }

        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._fanout_semaphore = asyncio.Semaphore(self._fanout_max_inflight)
        self._validator: Any | None = None
        self._seq_tracker = SequenceTracker()

    def _get_stream_validator(self) -> Any:
        """Lazily resolve and cache the shared market-data validator."""
        if self._validator is None:
            self._validator = get_validator()
        return self._validator

    async def start(self) -> None:
        """Start the multiplexer.

        Three connection modes (in order of precedence):
        1. lazy_connect=False  → eagerly start ALL stream types (needs multi-conn plan).
        2. eager_connect_types → eagerly start only the named stream types
           (e.g. ["stocks"]) and leave others lazy. This is the recommended
           production mode: stocks is hot for the 9:30 open, options/crypto/news
           connect on demand. Avoids paying the upstream Alpaca cold-start cost
           on the first client subscribe at market open (~30s observed on
           2026-04-29).
        3. lazy_connect=True with empty eager set → all streams lazy (Basic plan).
        """
        self._running = True
        logger.info(
            "multiplexer_starting",
            lazy_connect=self._lazy_connect,
            eager_connect_types=sorted(self._eager_connect_types),
        )

        if not self._lazy_connect:
            # Eager mode: start all connections immediately (requires multi-connection plan)
            for stream_type, conn in self._connections.items():
                task = asyncio.create_task(conn.start())
                self._tasks.append(task)
                logger.info("stream_started", stream=stream_type.value)
        elif self._eager_connect_types:
            # Selective eager: start only the named stream types, leave rest lazy.
            for stream_type, conn in self._connections.items():
                if self._stream_type_in_eager_set(stream_type):
                    task = asyncio.create_task(conn.start())
                    self._tasks.append(task)
                    logger.info("stream_started_eager", stream=stream_type.value)
                else:
                    logger.info("stream_lazy", stream=stream_type.value)
        else:
            logger.info("lazy_connect_enabled", message="Streams will connect on first subscription")

    def _stream_type_in_eager_set(self, stream_type: AlpacaStreamType) -> bool:
        """Match stream type against eager_connect_types config (case-insensitive).

        "stocks" matches both STOCKS_SIP and STOCKS_IEX; the configured feed
        (use_iex flag) determines which one is actually present in self._connections.
        """
        name = stream_type.value.lower()  # e.g. "stocks_sip", "stocks_iex", "options"
        if name in self._eager_connect_types:
            return True
        if name.startswith("stocks_") and "stocks" in self._eager_connect_types:
            return True
        return False

    def is_stream_ready(self, stream_type: AlpacaStreamType) -> bool:
        """Return True if the named upstream is connected and authenticated.

        UpstreamConnection.is_connected already gates on both socket OPEN and
        the auth ACK. Used by the readiness probe to distinguish "Gateway process
        up" from "Gateway can actually deliver streaming data right now."
        """
        conn = self._connections.get(stream_type)
        if conn is None:
            return False
        return bool(conn.is_connected)

    async def stop(self) -> None:
        """Stop all upstream connections with aggressive cleanup.

        Ensures all WebSocket connections are properly closed to release
        Alpaca connection slots, preventing connection limit issues on restart.
        """
        self._running = False
        logger.info("multiplexer_stopping", connections=len(self._connections))

        # Stop all connections concurrently with timeout
        stop_tasks = [conn.stop() for conn in self._connections.values()]
        if stop_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=10.0,
                )
                logger.info("all_connections_stopped")
            except TimeoutError:
                logger.warning(
                    "multiplexer_stop_timeout",
                    action="forcing_task_cancellation",
                )

        # Cancel stream tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (TimeoutError, asyncio.CancelledError):
                    logger.debug("stream_task_cancelled")

        self._tasks.clear()
        logger.info("multiplexer_stopped")

    async def _ensure_connected(self, stream_type: AlpacaStreamType) -> bool:
        """Ensure connection is established for lazy connect mode.

        Returns True if connection is ready, False if failed.
        Can restart a dormant stream that previously gave up after max retries.
        """
        conn = self._get_connection(stream_type)
        if not conn:
            return False

        # Already connected
        if conn.is_connected:
            return True

        # Connection task is actively running (connecting or retrying) —
        # wait for it to finish establishing rather than returning True blindly
        if conn._running:
            try:
                await asyncio.wait_for(conn._connected_event.wait(), timeout=30.0)
                return conn.is_connected
            except TimeoutError:
                logger.warning(
                    "lazy_connect_wait_timeout",
                    stream=stream_type.value,
                    hint="Connection task is running but hasn't authenticated yet",
                )
                return False

        # Stream is dormant (gave up after max retries) — restart it
        logger.info("restarting_dormant_stream", stream=stream_type.value)

        # Reset the event before starting so we get a clean wait
        conn._connected_event.clear()

        task = asyncio.create_task(conn.start())
        self._tasks.append(task)

        # Wait for connection to establish with generous timeout
        try:
            await asyncio.wait_for(conn._connected_event.wait(), timeout=30.0)
            logger.info("lazy_connect_established", stream=stream_type.value)
            return True
        except TimeoutError:
            logger.warning(
                "lazy_connect_timeout",
                stream=stream_type.value,
                hint="Stream task started but connection not established within timeout. "
                "Client should retry subscription.",
            )
            return False

    async def client_subscribe(
        self,
        client_id: str,
        stream_type: AlpacaStreamType,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> dict[str, Any]:
        """Subscribe a client to symbols.

        Returns subscription acknowledgment.
        """
        # Find connection for this stream type (handle SIP/IEX mapping)
        conn = self._get_connection(stream_type)
        if not conn:
            return {
                "type": "subscription_ack",
                "status": "error",
                "error_code": "GW-E3002",
                "message": f"Stream type not available: {stream_type.value}",
            }

        # Lazy connect: ensure stream is connected before subscribing
        if self._lazy_connect:
            connected = await self._ensure_connected(stream_type)
            if not connected:
                return {
                    "type": "subscription_ack",
                    "status": "error",
                    "error_code": "GW-E3003",
                    "message": f"Failed to connect to stream: {stream_type.value}",
                }

        # Track client subscription
        new_bars, new_quotes, new_trades, new_news = conn.subscriptions.subscribe(client_id, bars, quotes, trades, news)

        # Subscribe upstream only for new symbols
        warnings: list[str] = []
        if new_bars or new_quotes or new_trades or new_news:
            warnings = await conn.subscribe(new_bars, new_quotes, new_trades, new_news)

        subscribed = list(set((bars or []) + (quotes or []) + (trades or []) + (news or [])))
        result: dict[str, Any] = {
            "type": "subscription_ack",
            "status": "ok",
            "subscribed": subscribed,
            "failed": [],
        }
        if warnings:
            result["warnings"] = warnings
        return result

    async def client_unsubscribe(
        self,
        client_id: str,
        stream_type: AlpacaStreamType,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> dict[str, Any]:
        """Unsubscribe a client from symbols."""
        conn = self._get_connection(stream_type)
        if not conn:
            return {"type": "unsubscription_ack", "status": "ok", "unsubscribed": []}

        removed_bars, removed_quotes, removed_trades, removed_news = conn.subscriptions.unsubscribe(
            client_id, bars, quotes, trades, news
        )

        # Unsubscribe upstream only for symbols no client wants
        if removed_bars or removed_quotes or removed_trades or removed_news:
            await conn.unsubscribe(removed_bars, removed_quotes, removed_trades, removed_news)

        unsubscribed = list(set((bars or []) + (quotes or []) + (trades or []) + (news or [])))
        return {
            "type": "unsubscription_ack",
            "status": "ok",
            "unsubscribed": unsubscribed,
        }

    async def client_disconnect(self, client_id: str) -> None:
        """Remove all subscriptions for a disconnecting client."""
        for conn in self._connections.values():
            removed_bars, removed_quotes, removed_trades, removed_news = conn.subscriptions.remove_client(client_id)
            if removed_bars or removed_quotes or removed_trades or removed_news:
                await conn.unsubscribe(removed_bars, removed_quotes, removed_trades, removed_news)

    def _get_connection(self, stream_type: AlpacaStreamType) -> UpstreamConnection | None:
        """Get connection for stream type, handling SIP/IEX mapping."""
        if stream_type in self._connections:
            return self._connections[stream_type]

        # Map generic stocks to whatever we have
        if stream_type in (AlpacaStreamType.STOCKS_SIP, AlpacaStreamType.STOCKS_IEX):
            for st in (AlpacaStreamType.STOCKS_SIP, AlpacaStreamType.STOCKS_IEX):
                if st in self._connections:
                    return self._connections[st]

        return None

    async def _handle_message(self, stream_type: AlpacaStreamType, message: dict[str, Any]) -> None:
        """Route incoming message to subscribed clients."""
        msg_type = message.get("T", "")

        data_type = MESSAGE_TYPE_TO_DATA_TYPE.get(msg_type)
        if not data_type:
            # System message, heartbeat, etc.
            return

        # Find connection and get subscribed clients
        conn = self._get_connection(stream_type)
        if not conn:
            return

        symbols: list[str]
        if data_type == "news":
            symbols = []
            symbol_field = message.get("S")
            if symbol_field:
                symbols.append(symbol_field)
            raw_symbols = message.get("symbols")
            if isinstance(raw_symbols, list):
                symbols.extend([s for s in raw_symbols if s])
            if not symbols:
                symbols = ["*"]
            symbol_for_log = symbols[0] if symbols else "*"
        else:
            symbol = message.get("S", "")
            if not symbol:
                return
            symbols = [symbol]
            symbol_for_log = symbol

        clients: set[str] = set()
        if data_type == "news":
            clients.update(conn.subscriptions.get_clients_for_symbol_view("*", data_type))
        for sym in dict.fromkeys(symbols):
            clients.update(conn.subscriptions.get_clients_for_symbol_view(sym, data_type))
        if not clients:
            return

        if data_type in _VALIDATABLE_FEEDS:
            validator = self._get_stream_validator()
            if data_type == "bars":
                result = validator.validate_bar(
                    {
                        "symbol": message.get("S"),
                        "timestamp": message.get("t"),
                        "open": message.get("o"),
                        "high": message.get("h"),
                        "low": message.get("l"),
                        "close": message.get("c"),
                        "volume": message.get("v"),
                    }
                )
            elif data_type == "quotes":
                result = validator.validate_quote(
                    {
                        "symbol": message.get("S"),
                        "timestamp": message.get("t"),
                        "bid_price": message.get("bp"),
                        "ask_price": message.get("ap"),
                        "bid_size": message.get("bs"),
                        "ask_size": message.get("as"),
                    }
                )
            else:
                result = validator.validate_trade(
                    {
                        "symbol": message.get("S"),
                        "timestamp": message.get("t"),
                        "price": message.get("p"),
                        "size": message.get("s"),
                    }
                )

            if not result.valid:
                logger.info(
                    "stream_validation_failed",
                    error_codes=result.error_codes,
                    data_type=data_type,
                    symbol=message.get("S"),
                )
                return

        # Use fast-path wrapper for streaming
        # Optimizations:
        # 1. Skip per-message UUID/BLAKE2b hash (use fast random ID)
        # 2. Skip instrument inference (we know the stream type)
        # 3. Skip Pydantic validation (assumes upstream structure is somewhat trusted/consistent)
        ts_ingest = datetime.now(UTC)
        seq = self._seq_tracker.next_seq(symbol_for_log, data_type)

        # Inject timeframe for bars — Alpaca WebSocket bars are always 1Min
        # but the raw message doesn't include a timeframe field. Without it,
        # Heber writes bars with timeframe=null, making them indistinguishable
        # from daily bars in Silver.
        if data_type == "bars" and "timeframe" not in message:
            message["timeframe"] = "1Min"

        envelope = fast_wrap_streaming_event(
            event=message,
            provider="alpaca",
            feed=data_type,
            instrument_type=stream_type.instrument_type if stream_type else "equity",
            ts_ingest=ts_ingest,
            sequence=seq,
        )

        # Serialize once if we are going to broadcast
        # envelope is a dict here, we can serialize it to string once for efficiency
        # This matches what we did for RedisSink optimization
        envelope_json = orjson.dumps(envelope, default=_orjson_default)

        # Per-envelope side-effects (e.g. sink publish to Heber). Fires ONCE
        # per upstream envelope, regardless of which fanout path is taken
        # below — broadcast fast-path OR fallback per-client. Until 2026-05-21
        # this work lived inside on_data which the broadcast fast-path never
        # invokes — sink publish silently bypassed for all streaming events.
        # Run on_envelope BEFORE fanout so a sink stall (bounded by the
        # registry's 100ms producer-block timeout) doesn't delay client
        # delivery indefinitely, and so a sink failure cannot leave clients
        # holding events that aren't in Heber.
        if self._on_envelope:
            try:
                await self._on_envelope(envelope)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "stream_on_envelope_failed",
                    symbol=symbol_for_log,
                    data_type=data_type,
                    event_id=envelope.get("event_id", "unknown"),
                    stream_type=stream_type.value if stream_type else "unknown",
                    error=str(e),
                )

        if self._on_broadcast:
            # Efficient O(1) loop-level broadcast via ConnectionManager
            # We pass the list of clients and let ConnectionManager handle the async write loop
            try:
                # on_broadcast expects (message, client_ids)
                await self._on_broadcast(envelope_json, list(clients))
                record_stream_fanout_dispatch_event("delivered")
            except Exception as e:
                record_stream_fanout_dispatch_event("error")
                logger.error(
                    "broadcast_error",
                    symbol=symbol_for_log,
                    data_type=data_type,
                    event_id=envelope.get("event_id", "unknown"),
                    client_count=len(clients),
                    stream_type=stream_type.value if stream_type else "unknown",
                    error=str(e),
                    exc_info=True,
                )
            return

        # Fallback to manual fanout if no broadcast callback (e.g. tests)
        _fanout_timeout = 5.0  # per-client send timeout to prevent hung client blocking batch

        async def _send(client_id: str) -> None:
            try:
                async with self._fanout_semaphore:
                    await asyncio.wait_for(
                        self.on_data(client_id, data_type, envelope),
                        timeout=_fanout_timeout,
                    )
                record_stream_fanout_dispatch_event("delivered")
            except TimeoutError:
                record_stream_fanout_dispatch_event("timeout")
                logger.warning(
                    "fanout_timeout",
                    client_id=client_id,
                    symbol=symbol_for_log,
                    timeout=_fanout_timeout,
                )
            except Exception as e:
                record_stream_fanout_dispatch_event("error")
                logger.error(
                    "fanout_error",
                    client_id=client_id,
                    symbol=symbol_for_log,
                    event_id=envelope.get("event_id", "unknown"),
                    error=str(e),
                )

        for client_batch in self._iter_client_batches(clients):
            if len(client_batch) == 1:
                await _send(client_batch[0])
                continue
            await asyncio.gather(*(_send(client_id) for client_id in client_batch))

    def _iter_client_batches(self, clients: set[str]) -> Iterator[list[str]]:
        """Yield bounded client batches to avoid per-message task bursts."""
        batch_size = self._fanout_client_batch_size
        batch: list[str] = []
        for client_id in clients:
            batch.append(client_id)
            if len(batch) == batch_size:
                record_stream_fanout_batch_size(len(batch))
                yield batch
                batch = []
        if batch:
            record_stream_fanout_batch_size(len(batch))
            yield batch
