"""WebSocket multiplexer for upstream Alpaca connections.

Maintains single upstream WebSocket connections per stream type (stocks, options,
crypto, news) and fans out received data to subscribed downstream clients.
"""

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import msgpack
import structlog
import websockets
from websockets.client import WebSocketClientProtocol

from gateway.core.envelope import fast_wrap_streaming_event
from gateway.core.metrics import (
    record_stream_fanout_batch_size,
    record_stream_fanout_dispatch_event,
    set_stream_fanout_limits_metrics,
)
from gateway.core.validator import get_validator

logger = structlog.get_logger()

DEFAULT_FANOUT_MAX_INFLIGHT = 100
DEFAULT_FANOUT_BATCH_SIZE = 32
MESSAGE_TYPE_TO_DATA_TYPE = {
    "b": "bars",
    "q": "quotes",
    "t": "trades",
    "n": "news",
}
_VALIDATABLE_FEEDS = frozenset({"bars", "quotes", "trades"})


class SequenceTracker:
    """Per-symbol, per-feed sequence counter for gap detection (PRD §Sequence Numbers).

    Clients see monotonically increasing integers starting at 1.
    A gap (new_seq != last_seq + 1) signals missed messages.
    All counters reset on gateway restart.
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


@dataclass
class ClientSubscription:
    """Tracks a client's subscriptions for a stream type."""

    client_id: str
    bars: set[str] = field(default_factory=set)
    quotes: set[str] = field(default_factory=set)
    trades: set[str] = field(default_factory=set)
    news: set[str] = field(default_factory=set)


class SubscriptionManager:
    """Manages subscription aggregation across clients.

    Tracks which clients want which symbols and computes the union
    for upstream subscription.
    """

    def __init__(self) -> None:
        self._subscriptions: dict[str, ClientSubscription] = {}
        self._index: dict[str, dict[str, set[str]]] = {
            "bars": {},
            "quotes": {},
            "trades": {},
            "news": {},
        }

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
        new_bars: set[str] = set()
        new_quotes: set[str] = set()
        new_trades: set[str] = set()
        new_news: set[str] = set()

        if bars:
            for symbol in bars:
                if symbol in sub.bars:
                    continue
                sub.bars.add(symbol)
                clients = self._index["bars"].get(symbol)
                if clients is None:
                    self._index["bars"][symbol] = {client_id}
                    new_bars.add(symbol)
                else:
                    clients.add(client_id)
        if quotes:
            for symbol in quotes:
                if symbol in sub.quotes:
                    continue
                sub.quotes.add(symbol)
                clients = self._index["quotes"].get(symbol)
                if clients is None:
                    self._index["quotes"][symbol] = {client_id}
                    new_quotes.add(symbol)
                else:
                    clients.add(client_id)
        if trades:
            for symbol in trades:
                if symbol in sub.trades:
                    continue
                sub.trades.add(symbol)
                clients = self._index["trades"].get(symbol)
                if clients is None:
                    self._index["trades"][symbol] = {client_id}
                    new_trades.add(symbol)
                else:
                    clients.add(client_id)
        if news:
            for symbol in news:
                if symbol in sub.news:
                    continue
                sub.news.add(symbol)
                clients = self._index["news"].get(symbol)
                if clients is None:
                    self._index["news"][symbol] = {client_id}
                    new_news.add(symbol)
                else:
                    clients.add(client_id)

        return new_bars, new_quotes, new_trades, new_news

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
        removed_bars: set[str] = set()
        removed_quotes: set[str] = set()
        removed_trades: set[str] = set()
        removed_news: set[str] = set()

        if bars:
            for symbol in bars:
                if symbol not in sub.bars:
                    continue
                sub.bars.discard(symbol)
                clients = self._index["bars"].get(symbol)
                if clients:
                    clients.discard(client_id)
                    if not clients:
                        self._index["bars"].pop(symbol, None)
                        removed_bars.add(symbol)
        if quotes:
            for symbol in quotes:
                if symbol not in sub.quotes:
                    continue
                sub.quotes.discard(symbol)
                clients = self._index["quotes"].get(symbol)
                if clients:
                    clients.discard(client_id)
                    if not clients:
                        self._index["quotes"].pop(symbol, None)
                        removed_quotes.add(symbol)
        if trades:
            for symbol in trades:
                if symbol not in sub.trades:
                    continue
                sub.trades.discard(symbol)
                clients = self._index["trades"].get(symbol)
                if clients:
                    clients.discard(client_id)
                    if not clients:
                        self._index["trades"].pop(symbol, None)
                        removed_trades.add(symbol)
        if news:
            for symbol in news:
                if symbol not in sub.news:
                    continue
                sub.news.discard(symbol)
                clients = self._index["news"].get(symbol)
                if clients:
                    clients.discard(client_id)
                    if not clients:
                        self._index["news"].pop(symbol, None)
                        removed_news.add(symbol)

        return removed_bars, removed_quotes, removed_trades, removed_news

    def remove_client(self, client_id: str) -> tuple[set[str], set[str], set[str], set[str]]:
        """Remove all subscriptions for a client."""
        if client_id not in self._subscriptions:
            return set(), set(), set(), set()

        sub = self._subscriptions.pop(client_id)
        removed_bars: set[str] = set()
        removed_quotes: set[str] = set()
        removed_trades: set[str] = set()
        removed_news: set[str] = set()

        for symbol in sub.bars:
            clients = self._index["bars"].get(symbol)
            if clients:
                clients.discard(client_id)
                if not clients:
                    self._index["bars"].pop(symbol, None)
                    removed_bars.add(symbol)

        for symbol in sub.quotes:
            clients = self._index["quotes"].get(symbol)
            if clients:
                clients.discard(client_id)
                if not clients:
                    self._index["quotes"].pop(symbol, None)
                    removed_quotes.add(symbol)

        for symbol in sub.trades:
            clients = self._index["trades"].get(symbol)
            if clients:
                clients.discard(client_id)
                if not clients:
                    self._index["trades"].pop(symbol, None)
                    removed_trades.add(symbol)

        for symbol in sub.news:
            clients = self._index["news"].get(symbol)
            if clients:
                clients.discard(client_id)
                if not clients:
                    self._index["news"].pop(symbol, None)
                    removed_news.add(symbol)

        return removed_bars, removed_quotes, removed_trades, removed_news

    def get_clients_for_symbol(self, symbol: str, data_type: str) -> list[str]:
        """Get all clients subscribed to a symbol for a data type (returns copy)."""
        return list(self.get_clients_for_symbol_view(symbol, data_type))

    def get_clients_for_symbol_view(self, symbol: str, data_type: str) -> set[str]:
        """Get raw set of clients subscribed to a symbol (zero-copy view)."""
        return self._index.get(data_type, {}).get(symbol, set())

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
        base_delay: float = 1.0,
        max_delay: float = 16.0,
        max_retries: int = 10,
    ) -> None:
        self.stream_type = stream_type
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_message = on_message
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries

        self._ws: WebSocketClientProtocol | None = None
        self._authenticated = False
        self._running = False
        self._receive_task: asyncio.Task | None = None
        self._subscriptions = SubscriptionManager()

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

        endpoint = self.stream_type.endpoint
        logger.info("connecting_upstream", stream=self.stream_type.value, endpoint=endpoint)

        self._ws = await websockets.connect(
            endpoint,
            ping_interval=30,
            ping_timeout=10,
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
        return json.dumps(data)

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
    ) -> None:
        """Subscribe to symbols upstream."""
        if not self.is_connected:
            logger.warning("subscribe_not_connected", stream=self.stream_type.value)
            return

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
            except Exception:
                pass
        except Exception as e:
            logger.warning(
                "websocket_close_error",
                stream=self.stream_type.value,
                error=str(e),
            )

    async def _connect_and_run(self) -> None:
        """Main connection loop with reconnection."""
        while self._running:
            try:
                await self.connect()
                await self.authenticate()

                # Resubscribe to all symbols
                bars, quotes, trades, news = self._subscriptions.get_all_subscriptions()
                if bars or quotes or trades or news:
                    await self.subscribe(bars, quotes, trades, news)

                # Start receive loop
                await self._receive_loop()

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(
                    "connection_closed",
                    stream=self.stream_type.value,
                    code=e.code,
                    reason=e.reason,
                )
            except Exception:
                logger.exception("connection_error", stream=self.stream_type.value)

            self._authenticated = False
            # Close the WebSocket to release Alpaca's connection slot before reconnecting
            await self._close_ws()

            if self._running:
                await self._reconnect_with_backoff()
                if not self._running:
                    # Reconnect gave up (max retries or non-recoverable error)
                    logger.info(
                        "stream_dormant",
                        stream=self.stream_type.value,
                        hint="Will restart on next client subscription",
                    )
                    return

    async def _receive_loop(self) -> None:
        """Receive and dispatch messages.

        Handles both JSON (stocks, crypto, news) and MessagePack (OPRA options) formats.
        """
        if not self._ws:
            return

        async for message in self._ws:
            try:
                data = self._decode_message(message)

                # Alpaca sends arrays of messages
                if isinstance(data, list):
                    for msg in data:
                        await self.on_message(msg)
                else:
                    await self.on_message(data)

            except Exception as e:
                logger.error("message_parse_error", error=str(e))

    # Auth error messages that indicate account-level issues, not transient failures
    _NON_RECOVERABLE_ERRORS = ("connection limit exceeded",)

    async def _reconnect_with_backoff(self) -> None:
        """Reconnect with exponential backoff per PRD.

        Sets self._running = False if retries are exhausted or a non-recoverable
        error is detected, signaling the outer loop to stop.
        """
        for attempt in range(self.max_retries):
            delay = min(self.base_delay * (2**attempt), self.max_delay)
            jitter = delay * 0.2 * (random.random() * 2 - 1)  # ±20%

            logger.info(
                "reconnecting",
                stream=self.stream_type.value,
                attempt=attempt + 1,
                delay=delay + jitter,
            )

            await asyncio.sleep(delay + jitter)

            if not self._running:
                return

            try:
                await self.connect()
                await self.authenticate()

                bars, quotes, trades, news = self._subscriptions.get_all_subscriptions()
                if bars or quotes or trades or news:
                    await self.subscribe(bars, quotes, trades, news)

                logger.info("reconnected", stream=self.stream_type.value, attempt=attempt + 1)
                return

            except Exception as e:
                error_msg = str(e)
                # Detect non-recoverable auth errors — stop immediately
                if any(msg in error_msg for msg in self._NON_RECOVERABLE_ERRORS):
                    logger.error(
                        "non_recoverable_error",
                        stream=self.stream_type.value,
                        error=error_msg,
                        hint="Check for stale connections or competing applications using the same Alpaca credentials.",
                    )
                    self._running = False
                    return

                logger.warning(
                    "reconnect_failed",
                    stream=self.stream_type.value,
                    attempt=attempt + 1,
                    error=error_msg,
                )

        logger.error("max_retries_exceeded", stream=self.stream_type.value)
        self._running = False


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
        lazy_connect: bool = True,
        fanout_max_inflight: int = DEFAULT_FANOUT_MAX_INFLIGHT,
        fanout_batch_size: int = DEFAULT_FANOUT_BATCH_SIZE,
        on_broadcast: Callable[[dict[str, Any] | str | bytes, list[str]], Awaitable[int]] | None = None,
    ) -> None:
        """Initialize multiplexer.

        Args:
            api_key: Alpaca API key
            api_secret: Alpaca API secret
            on_data: Callback for data messages (client_id, data_type, message)
            use_iex: Use IEX feed instead of SIP for stocks
            lazy_connect: If True, only connect to streams when first client subscribes
            fanout_max_inflight: Max concurrent callback deliveries per fanout batch.
            fanout_batch_size: Number of clients to dispatch per gather batch.
            on_broadcast: Optional callback for efficient broadcast (message, client_ids) -> count
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_data = on_data
        self._lazy_connect = lazy_connect
        self._fanout_max_inflight = max(1, fanout_max_inflight)
        self._fanout_client_batch_size = max(1, fanout_batch_size)
        self._on_broadcast = on_broadcast
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

        If lazy_connect is True (default), connections are only established
        when the first client subscribes to a stream. This works with Alpaca's
        Basic plan which only allows 1 concurrent WebSocket connection.
        """
        self._running = True
        logger.info("multiplexer_starting", lazy_connect=self._lazy_connect)

        if not self._lazy_connect:
            # Eager mode: start all connections immediately (requires multi-connection plan)
            for stream_type, conn in self._connections.items():
                task = asyncio.create_task(conn.start())
                self._tasks.append(task)
                logger.info("stream_started", stream=stream_type.value)
        else:
            logger.info("lazy_connect_enabled", message="Streams will connect on first subscription")

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

        # Connection task is actively running (connecting or retrying)
        if conn._running:
            return True

        # Stream is dormant (gave up after max retries) — restart it
        logger.info("restarting_dormant_stream", stream=stream_type.value)

        # Start connection for this stream
        logger.info("lazy_connect_starting", stream=stream_type.value)
        task = asyncio.create_task(conn.start())
        self._tasks.append(task)

        # Wait briefly for connection to establish
        for _ in range(50):  # 5 second timeout
            await asyncio.sleep(0.1)
            if conn.is_connected:
                logger.info("lazy_connect_established", stream=stream_type.value)
                return True

        logger.warning("lazy_connect_timeout", stream=stream_type.value)
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
        if new_bars or new_quotes or new_trades or new_news:
            await conn.subscribe(new_bars, new_quotes, new_trades, new_news)

        subscribed = list(set((bars or []) + (quotes or []) + (trades or []) + (news or [])))
        return {
            "type": "subscription_ack",
            "status": "ok",
            "subscribed": subscribed,
            "failed": [],
        }

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
        for sym in set(symbols):
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
                logger.warning(
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
        from datetime import UTC, datetime

        ts_ingest = datetime.now(UTC)
        seq = self._seq_tracker.next_seq(symbol_for_log, data_type)

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
        envelope_json = json.dumps(envelope)

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
                    event_id=envelope.get("event_id", "unknown"),
                    error=str(e),
                )
            return

        # Fallback to manual fanout if no broadcast callback (e.g. tests)
        async def _send(client_id: str) -> None:
            try:
                async with self._fanout_semaphore:
                    # Send pre-serialized JSON string if possible, or envelope dict
                    # on_data signature might need update or flexible handling
                    # Current on_data is Callable[[str, str, dict[str, Any]], Awaitable[None]]
                    # We should check if on_data supports string payload, but standard Multiplexer uses on_data from main.py
                    # which calls connection_manager.send_personal_message which expects JSON-serializable
                    # For safety in fallback mode, we pass the dict envelope
                    await self.on_data(client_id, data_type, envelope)
                record_stream_fanout_dispatch_event("delivered")
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
