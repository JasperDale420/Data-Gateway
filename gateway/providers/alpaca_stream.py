"""Alpaca WebSocket stream handler."""

import asyncio
import json
import time
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Any

import structlog
import websockets
from websockets.legacy.client import WebSocketClientProtocol

from gateway.core.multiplexer import SubscriptionManager
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade

logger = structlog.get_logger()

ALPACA_STREAM_URL = "wss://stream.data.alpaca.markets/v2"
HEARTBEAT_TIMEOUT_SECONDS = 30.0
HEARTBEAT_CHECK_INTERVAL = 5.0


class AlpacaStreamHandler:
    """Manages Alpaca WebSocket connection with reconnection logic."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        feed: str = "sip",
        subscription_manager: SubscriptionManager | None = None,
    ):
        self._api_key = api_key
        self._secret_key = secret_key
        self._feed = feed
        self._ws: WebSocketClientProtocol | None = None
        self._running = False
        self._subscription_manager = subscription_manager or SubscriptionManager()
        self._message_queue: asyncio.Queue[dict] = asyncio.Queue()
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0

        # Heartbeat monitoring
        self._last_message_time: float = 0.0
        self._heartbeat_task: asyncio.Task | None = None

        # Set callbacks for upstream subscription changes
        self._subscription_manager.set_callbacks(
            on_subscribe=self._send_subscribe,
            on_unsubscribe=self._send_unsubscribe,
        )

    async def connect(self) -> None:
        """Connect to Alpaca WebSocket and authenticate."""
        url = f"{ALPACA_STREAM_URL}/{self._feed}"

        try:
            self._ws = await websockets.connect(url)
            self._running = True
            self._last_message_time = time.time()

            # Start heartbeat monitor
            if self._heartbeat_task is None or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

            # Wait for welcome message
            welcome = await self._ws.recv()
            welcome_data = json.loads(welcome)
            self._last_message_time = time.time()
            logger.info("alpaca_ws_connected", message=welcome_data)

            # Authenticate
            auth_msg = {
                "action": "auth",
                "key": self._api_key,
                "secret": self._secret_key,
            }
            await self._ws.send(json.dumps(auth_msg))

            auth_response = await self._ws.recv()
            auth_data = json.loads(auth_response)
            self._last_message_time = time.time()

            if auth_data[0].get("msg") == "authenticated":
                logger.info("alpaca_ws_authenticated")
                self._reconnect_delay = 1.0
            else:
                logger.error("alpaca_ws_auth_failed", response=auth_data)
                raise RuntimeError(f"Authentication failed: {auth_data}")

        except Exception as e:
            logger.error("alpaca_ws_connect_failed", error=str(e))
            raise

    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self._running = False

        # Cancel heartbeat task
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._ws:
            await self._ws.close()
            self._ws = None
        logger.info("alpaca_ws_disconnected")

    async def _heartbeat_monitor(self) -> None:
        """Monitor for stale connections and trigger reconnect."""
        while self._running:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)

            if not self._running:
                break

            elapsed = time.time() - self._last_message_time
            if elapsed > HEARTBEAT_TIMEOUT_SECONDS:
                logger.warning(
                    "alpaca_ws_heartbeat_timeout",
                    last_message_ago_seconds=elapsed,
                    timeout=HEARTBEAT_TIMEOUT_SECONDS,
                )
                # Trigger reconnect
                if self._ws:
                    await self._ws.close()
                    self._ws = None

    async def _send_subscribe(self, symbols: list[str], feed: str) -> None:
        """Send subscribe message to upstream."""
        if not self._ws:
            return

        msg = {"action": "subscribe"}
        if feed == "bars":
            msg["bars"] = symbols
        elif feed == "quotes":
            msg["quotes"] = symbols
        elif feed == "trades":
            msg["trades"] = symbols

        await self._ws.send(json.dumps(msg))
        logger.info("alpaca_ws_subscribe_sent", symbols=len(symbols), feed=feed)

    async def _send_unsubscribe(self, symbols: list[str], feed: str) -> None:
        """Send unsubscribe message to upstream."""
        if not self._ws:
            return

        msg = {"action": "unsubscribe"}
        if feed == "bars":
            msg["bars"] = symbols
        elif feed == "quotes":
            msg["quotes"] = symbols
        elif feed == "trades":
            msg["trades"] = symbols

        await self._ws.send(json.dumps(msg))
        logger.info("alpaca_ws_unsubscribe_sent", symbols=len(symbols), feed=feed)

    async def subscribe(self, client_id: str, symbols: list[str], feeds: list[str]) -> None:
        """Subscribe a client to symbols."""
        for feed in feeds:
            await self._subscription_manager.subscribe(client_id, symbols, feed)

    async def unsubscribe(self, client_id: str, symbols: list[str], feeds: list[str]) -> None:
        """Unsubscribe a client from symbols."""
        for feed in feeds:
            await self._subscription_manager.unsubscribe(client_id, symbols, feed)

    async def remove_client(self, client_id: str) -> None:
        """Remove all subscriptions for a client."""
        await self._subscription_manager.remove_client(client_id)

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Stream normalized messages from Alpaca."""
        while self._running:
            try:
                if not self._ws:
                    await self.connect()

                message = await self._ws.recv()
                self._last_message_time = time.time()
                data = json.loads(message)

                # Alpaca sends arrays of messages
                for item in data:
                    msg_type = item.get("T")

                    if msg_type == "b":
                        yield self._normalize_bar_message(item)
                    elif msg_type == "q":
                        yield self._normalize_quote_message(item)
                    elif msg_type == "t":
                        yield self._normalize_trade_message(item)
                    elif msg_type == "subscription":
                        logger.info("alpaca_subscription_ack", data=item)
                    elif msg_type == "error":
                        logger.error("alpaca_stream_error", data=item)

            except websockets.ConnectionClosed:
                if not self._running:
                    break
                logger.warning("alpaca_ws_disconnected_unexpectedly")
                await self._reconnect()

            except Exception as e:
                logger.error("alpaca_stream_error", error=str(e))
                if self._running:
                    await self._reconnect()

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        logger.info("alpaca_ws_reconnecting", delay=self._reconnect_delay)
        await asyncio.sleep(self._reconnect_delay)
        self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

        try:
            await self.connect()

            # Resubscribe to all active symbols
            for feed in ["bars", "quotes", "trades"]:
                symbols = self._subscription_manager.get_all_symbols(feed)
                if symbols:
                    await self._send_subscribe(symbols, feed)

        except Exception as e:
            logger.error("alpaca_ws_reconnect_failed", error=str(e))

    def _normalize_bar_message(self, raw: dict) -> dict:
        """Convert Alpaca bar to normalized format."""
        return {
            "type": "bar",
            "symbol": raw["S"],
            "data": NormalizedBar(
                symbol=raw["S"],
                timestamp=datetime.fromisoformat(raw["t"].replace("Z", "+00:00")),
                open=Decimal(str(raw["o"])),
                high=Decimal(str(raw["h"])),
                low=Decimal(str(raw["l"])),
                close=Decimal(str(raw["c"])),
                volume=int(raw["v"]),
                vwap=Decimal(str(raw["vw"])) if raw.get("vw") else None,
                trade_count=raw.get("n"),
                provider="alpaca",
            ).model_dump(mode="json"),
            "subscribers": list(self._subscription_manager.get_subscribers(raw["S"], "bars")),
        }

    def _normalize_quote_message(self, raw: dict) -> dict:
        """Convert Alpaca quote to normalized format."""
        return {
            "type": "quote",
            "symbol": raw["S"],
            "data": NormalizedQuote(
                symbol=raw["S"],
                timestamp=datetime.fromisoformat(raw["t"].replace("Z", "+00:00")),
                bid_price=Decimal(str(raw["bp"])),
                bid_size=int(raw["bs"]),
                ask_price=Decimal(str(raw["ap"])),
                ask_size=int(raw["as"]),
                provider="alpaca",
            ).model_dump(mode="json"),
            "subscribers": list(self._subscription_manager.get_subscribers(raw["S"], "quotes")),
        }

    def _normalize_trade_message(self, raw: dict) -> dict:
        """Convert Alpaca trade to normalized format."""
        return {
            "type": "trade",
            "symbol": raw["S"],
            "data": NormalizedTrade(
                symbol=raw["S"],
                timestamp=datetime.fromisoformat(raw["t"].replace("Z", "+00:00")),
                price=Decimal(str(raw["p"])),
                size=int(raw["s"]),
                exchange=raw.get("x"),
                conditions=raw.get("c", []),
                provider="alpaca",
            ).model_dump(mode="json"),
            "subscribers": list(self._subscription_manager.get_subscribers(raw["S"], "trades")),
        }

    def get_stats(self) -> dict:
        """Get stream handler statistics."""
        return {
            "connected": self._ws is not None,
            "running": self._running,
            "subscriptions": self._subscription_manager.get_stats(),
        }
