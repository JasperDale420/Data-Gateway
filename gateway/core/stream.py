"""WebSocket multiplexer for upstream Alpaca connections.

Maintains single upstream WebSocket connections per stream type (stocks, options,
crypto, news) and fans out received data to subscribed downstream clients.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog
import websockets
from websockets.client import WebSocketClientProtocol

from gateway.core.envelope import wrap_event

logger = structlog.get_logger()


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


@dataclass
class ClientSubscription:
    """Tracks a client's subscriptions for a stream type."""

    client_id: str
    bars: set[str] = field(default_factory=set)
    quotes: set[str] = field(default_factory=set)
    trades: set[str] = field(default_factory=set)


class SubscriptionManager:
    """Manages subscription aggregation across clients.

    Tracks which clients want which symbols and computes the union
    for upstream subscription.
    """

    def __init__(self) -> None:
        self._subscriptions: dict[str, ClientSubscription] = {}

    def subscribe(
        self,
        client_id: str,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
    ) -> tuple[set[str], set[str], set[str]]:
        """Add subscriptions for a client. Returns newly added symbols."""
        if client_id not in self._subscriptions:
            self._subscriptions[client_id] = ClientSubscription(client_id=client_id)

        sub = self._subscriptions[client_id]
        old_aggregate = self._aggregate()

        if bars:
            sub.bars.update(bars)
        if quotes:
            sub.quotes.update(quotes)
        if trades:
            sub.trades.update(trades)

        new_aggregate = self._aggregate()

        # Return only newly added symbols (need upstream subscription)
        new_bars = new_aggregate[0] - old_aggregate[0]
        new_quotes = new_aggregate[1] - old_aggregate[1]
        new_trades = new_aggregate[2] - old_aggregate[2]

        return new_bars, new_quotes, new_trades

    def unsubscribe(
        self,
        client_id: str,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
    ) -> tuple[set[str], set[str], set[str]]:
        """Remove subscriptions for a client. Returns symbols to unsubscribe upstream."""
        if client_id not in self._subscriptions:
            return set(), set(), set()

        sub = self._subscriptions[client_id]
        old_aggregate = self._aggregate()

        if bars:
            sub.bars -= set(bars)
        if quotes:
            sub.quotes -= set(quotes)
        if trades:
            sub.trades -= set(trades)

        new_aggregate = self._aggregate()

        # Return symbols no longer needed by any client
        removed_bars = old_aggregate[0] - new_aggregate[0]
        removed_quotes = old_aggregate[1] - new_aggregate[1]
        removed_trades = old_aggregate[2] - new_aggregate[2]

        return removed_bars, removed_quotes, removed_trades

    def remove_client(self, client_id: str) -> tuple[set[str], set[str], set[str]]:
        """Remove all subscriptions for a client."""
        if client_id not in self._subscriptions:
            return set(), set(), set()

        old_aggregate = self._aggregate()
        del self._subscriptions[client_id]
        new_aggregate = self._aggregate()

        removed_bars = old_aggregate[0] - new_aggregate[0]
        removed_quotes = old_aggregate[1] - new_aggregate[1]
        removed_trades = old_aggregate[2] - new_aggregate[2]

        return removed_bars, removed_quotes, removed_trades

    def get_clients_for_symbol(self, symbol: str, data_type: str) -> list[str]:
        """Get all clients subscribed to a symbol for a data type."""
        clients = []
        for client_id, sub in self._subscriptions.items():
            symbol_set = getattr(sub, data_type, set())
            if symbol in symbol_set:
                clients.append(client_id)
        return clients

    def _aggregate(self) -> tuple[set[str], set[str], set[str]]:
        """Compute union of all client subscriptions."""
        all_bars: set[str] = set()
        all_quotes: set[str] = set()
        all_trades: set[str] = set()

        for sub in self._subscriptions.values():
            all_bars.update(sub.bars)
            all_quotes.update(sub.quotes)
            all_trades.update(sub.trades)

        return all_bars, all_quotes, all_trades

    def get_all_subscriptions(self) -> tuple[set[str], set[str], set[str]]:
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
        return self._ws is not None and self._ws.open and self._authenticated

    @property
    def subscriptions(self) -> SubscriptionManager:
        return self._subscriptions

    async def connect(self) -> None:
        """Establish WebSocket connection."""
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
        import json

        import msgpack

        if isinstance(message, bytes):
            return msgpack.unpackb(message, raw=False)
        return json.loads(message)

    def _encode_message(self, data: dict[str, Any]) -> bytes | str:
        """Encode a message to either JSON or MessagePack format."""
        import json

        import msgpack

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

        if len(msg) > 1:  # Has at least one subscription type
            await self._ws.send(self._encode_message(msg))
            logger.info(
                "subscribed_upstream",
                stream=self.stream_type.value,
                bars=list(bars) if bars else [],
                quotes=list(quotes) if quotes else [],
            )

    async def unsubscribe(
        self,
        bars: set[str] | None = None,
        quotes: set[str] | None = None,
        trades: set[str] | None = None,
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

        if len(msg) > 1:
            await self._ws.send(self._encode_message(msg))
            logger.info(
                "unsubscribed_upstream",
                stream=self.stream_type.value,
                bars=list(bars) if bars else [],
            )

    async def start(self) -> None:
        """Start the connection and receive loop."""
        self._running = True
        await self._connect_and_run()

    async def stop(self) -> None:
        """Stop the connection gracefully."""
        self._running = False
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                logger.debug("receive_task_cancelled", stream=self.stream_type.value)
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _connect_and_run(self) -> None:
        """Main connection loop with reconnection."""
        while self._running:
            try:
                await self.connect()
                await self.authenticate()

                # Resubscribe to all symbols
                bars, quotes, trades = self._subscriptions.get_all_subscriptions()
                if bars or quotes or trades:
                    await self.subscribe(bars, quotes, trades)

                # Start receive loop
                await self._receive_loop()

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(
                    "connection_closed",
                    stream=self.stream_type.value,
                    code=e.code,
                    reason=e.reason,
                )
            except Exception as e:
                logger.error("connection_error", stream=self.stream_type.value, error=str(e))

            self._authenticated = False
            self._ws = None

            if self._running:
                await self._reconnect_with_backoff()

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

    async def _reconnect_with_backoff(self) -> None:
        """Reconnect with exponential backoff per PRD."""
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

                bars, quotes, trades = self._subscriptions.get_all_subscriptions()
                if bars or quotes or trades:
                    await self.subscribe(bars, quotes, trades)

                logger.info("reconnected", stream=self.stream_type.value, attempt=attempt + 1)
                return

            except Exception as e:
                logger.warning(
                    "reconnect_failed",
                    stream=self.stream_type.value,
                    attempt=attempt + 1,
                    error=str(e),
                )

        logger.error("max_retries_exceeded", stream=self.stream_type.value)


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
    ) -> None:
        """Initialize multiplexer.

        Args:
            api_key: Alpaca API key
            api_secret: Alpaca API secret
            on_data: Callback for data messages (client_id, data_type, message)
            use_iex: Use IEX feed instead of SIP for stocks
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_data = on_data

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

    async def start(self) -> None:
        """Start all upstream connections."""
        self._running = True
        logger.info("multiplexer_starting")

        for stream_type, conn in self._connections.items():
            task = asyncio.create_task(conn.start())
            self._tasks.append(task)
            logger.info("stream_started", stream=stream_type.value)

    async def stop(self) -> None:
        """Stop all upstream connections."""
        self._running = False
        logger.info("multiplexer_stopping")

        for conn in self._connections.values():
            await conn.stop()

        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.debug("stream_task_cancelled")

        self._tasks.clear()
        logger.info("multiplexer_stopped")

    async def client_subscribe(
        self,
        client_id: str,
        stream_type: AlpacaStreamType,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
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

        # Track client subscription
        new_bars, new_quotes, new_trades = conn.subscriptions.subscribe(
            client_id, bars, quotes, trades
        )

        # Subscribe upstream only for new symbols
        if new_bars or new_quotes or new_trades:
            await conn.subscribe(new_bars, new_quotes, new_trades)

        subscribed = list(set((bars or []) + (quotes or []) + (trades or [])))
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
    ) -> dict[str, Any]:
        """Unsubscribe a client from symbols."""
        conn = self._get_connection(stream_type)
        if not conn:
            return {"type": "unsubscription_ack", "status": "ok", "unsubscribed": []}

        removed_bars, removed_quotes, removed_trades = conn.subscriptions.unsubscribe(
            client_id, bars, quotes, trades
        )

        # Unsubscribe upstream only for symbols no client wants
        if removed_bars or removed_quotes or removed_trades:
            await conn.unsubscribe(removed_bars, removed_quotes, removed_trades)

        unsubscribed = list(set((bars or []) + (quotes or []) + (trades or [])))
        return {
            "type": "unsubscription_ack",
            "status": "ok",
            "unsubscribed": unsubscribed,
        }

    async def client_disconnect(self, client_id: str) -> None:
        """Remove all subscriptions for a disconnecting client."""
        for conn in self._connections.values():
            removed_bars, removed_quotes, removed_trades = conn.subscriptions.remove_client(
                client_id
            )
            if removed_bars or removed_quotes or removed_trades:
                await conn.unsubscribe(removed_bars, removed_quotes, removed_trades)

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
        symbol = message.get("S", "")

        # Map Alpaca message types to our data types
        data_type_map = {
            "b": "bars",
            "q": "quotes",
            "t": "trades",
            "n": "news",
        }

        data_type = data_type_map.get(msg_type)
        if not data_type or not symbol:
            # System message, heartbeat, etc.
            return

        # Find connection and get subscribed clients
        conn = self._get_connection(stream_type)
        if not conn:
            return

        clients = conn.subscriptions.get_clients_for_symbol(symbol, data_type)

        # Wrap event in EventEnvelope for downstream consumers
        envelope = wrap_event(
            event=message,
            provider="alpaca",
            feed=data_type,
            source="websocket",
            stream_type=stream_type.value if stream_type else None,
        )

        # Fan out envelope to each subscribed client
        for client_id in clients:
            try:
                await self.on_data(client_id, data_type, envelope)
            except Exception as e:
                logger.error(
                    "fanout_error",
                    client_id=client_id,
                    symbol=symbol,
                    event_id=envelope.get("event_id", "unknown"),
                    error=str(e),
                )
