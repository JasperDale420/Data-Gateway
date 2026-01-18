"""Alpaca News WebSocket stream handler."""

import asyncio
import json
import time
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import structlog
import websockets

from gateway.core.multiplexer import SubscriptionManager

logger = structlog.get_logger()

ALPACA_NEWS_STREAM_URL = "wss://stream.data.alpaca.markets/v1beta1/news"
HEARTBEAT_TIMEOUT_SECONDS = 30.0
HEARTBEAT_CHECK_INTERVAL = 5.0


class AlpacaNewsStreamHandler:
    """Manages Alpaca News WebSocket connection with reconnection logic."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        subscription_manager: SubscriptionManager | None = None,
    ):
        self._api_key = api_key
        self._secret_key = secret_key
        self._subscription_manager = subscription_manager

        self._ws: Any | None = None
        self._connected = False
        self._authenticated = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10
        self._subscriptions: set[str] = set()

        # Stats
        self._messages_received = 0
        self._last_message_time: float = 0.0
        self._connect_time: datetime | None = None

        # Heartbeat monitoring
        self._heartbeat_task: asyncio.Task | None = None

    async def connect(self) -> bool:
        """Connect to Alpaca News WebSocket and authenticate."""
        try:
            logger.info("alpaca_news_stream_connecting")

            self._ws = await websockets.connect(ALPACA_NEWS_STREAM_URL)
            self._connected = True
            self._connect_time = datetime.now()
            self._last_message_time = time.time()

            # Wait for welcome message
            welcome = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            welcome_data = json.loads(welcome)
            logger.debug("alpaca_news_welcome", data=welcome_data)

            # Authenticate
            auth_msg = {
                "action": "auth",
                "key": self._api_key,
                "secret": self._secret_key,
            }
            await self._ws.send(json.dumps(auth_msg))

            auth_response = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            auth_data = json.loads(auth_response)

            if auth_data[0].get("T") == "success":
                self._authenticated = True
                self._reconnect_attempts = 0
                logger.info("alpaca_news_stream_authenticated")

                # Start heartbeat monitor
                self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

                return True
            else:
                logger.error("alpaca_news_auth_failed", response=auth_data)
                await self.disconnect()
                return False

        except Exception as e:
            logger.error("alpaca_news_connect_error", error=str(e))
            self._connected = False
            return False

    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self._connected = False
        self._authenticated = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        logger.info("alpaca_news_stream_disconnected")

    async def _heartbeat_monitor(self) -> None:
        """Monitor for stale connections and trigger reconnect."""
        while self._connected:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)

            if not self._connected:
                break

            time_since_last = time.time() - self._last_message_time
            if time_since_last > HEARTBEAT_TIMEOUT_SECONDS:
                logger.warning(
                    "alpaca_news_heartbeat_timeout",
                    seconds_since_last=time_since_last,
                )
                await self._reconnect()
                break

    async def _send_subscribe(self, symbols: list[str]) -> None:
        """Send subscribe message to upstream.

        Note: Use ["*"] to subscribe to all symbols.
        """
        if not self._ws or not self._authenticated:
            return

        subscribe_msg = {"action": "subscribe", "news": symbols}

        await self._ws.send(json.dumps(subscribe_msg))
        self._subscriptions.update(symbols)
        logger.info("alpaca_news_subscribe_sent", symbols=len(symbols))

    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        """Send unsubscribe message to upstream."""
        if not self._ws or not self._authenticated:
            return

        unsubscribe_msg = {"action": "unsubscribe", "news": symbols}

        await self._ws.send(json.dumps(unsubscribe_msg))
        self._subscriptions -= set(symbols)
        logger.info("alpaca_news_unsubscribe_sent", symbols=len(symbols))

    async def subscribe(self, client_id: str, symbols: list[str]) -> None:
        """Subscribe a client to news for symbols.

        Use ["*"] to subscribe to all symbols.
        """
        if self._subscription_manager:
            await self._subscription_manager.subscribe(client_id, symbols, "news")

    async def unsubscribe(self, client_id: str, symbols: list[str]) -> None:
        """Unsubscribe a client from news."""
        if self._subscription_manager:
            await self._subscription_manager.unsubscribe(client_id, symbols, "news")

    async def remove_client(self, client_id: str) -> None:
        """Remove all subscriptions for a client."""
        if self._subscription_manager:
            await self._subscription_manager.remove_client(client_id)

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Stream normalized news articles from Alpaca."""
        if not self._ws or not self._connected:
            logger.warning("alpaca_news_stream_not_connected")
            return

        try:
            async for raw_message in self._ws:
                self._last_message_time = time.time()
                self._messages_received += 1

                try:
                    messages = json.loads(raw_message)

                    for msg in messages:
                        msg_type = msg.get("T")

                        if msg_type == "n":  # News article
                            yield self._normalize_news(msg)
                        elif msg_type in ("success", "subscription", "error"):
                            logger.debug("alpaca_news_control_msg", msg=msg)
                        else:
                            logger.debug("alpaca_news_unknown_msg", type=msg_type)

                except json.JSONDecodeError as e:
                    logger.warning("alpaca_news_json_error", error=str(e))

        except websockets.ConnectionClosed:
            logger.warning("alpaca_news_connection_closed")
            await self._reconnect()
        except Exception as e:
            logger.error("alpaca_news_stream_error", error=str(e))
            await self._reconnect()

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        self._reconnect_attempts += 1
        if self._reconnect_attempts > self._max_reconnect_attempts:
            logger.error("alpaca_news_max_reconnects_exceeded")
            return

        delay = min(2**self._reconnect_attempts, 16)
        logger.info(
            "alpaca_news_reconnecting",
            attempt=self._reconnect_attempts,
            delay=delay,
        )

        await asyncio.sleep(delay)
        await self.disconnect()

        if await self.connect():
            # Resubscribe
            if self._subscriptions:
                await self._send_subscribe(list(self._subscriptions))

    def _normalize_news(self, raw: dict) -> dict[str, Any]:
        """Convert Alpaca news message to normalized format.

        Alpaca news message format:
        {
            "T": "n",
            "id": 26928472,
            "headline": "...",
            "summary": "...",
            "author": "...",
            "created_at": "2021-01-01T12:00:00Z",
            "updated_at": "2021-01-01T12:00:00Z",
            "url": "https://...",
            "content": "...",
            "symbols": ["AAPL", "TSLA"],
            "source": "benzinga"
        }
        """
        return {
            "type": "news",
            "id": str(raw.get("id", "")),
            "headline": raw.get("headline", ""),
            "summary": raw.get("summary"),
            "author": raw.get("author"),
            "created_at": raw.get("created_at"),
            "updated_at": raw.get("updated_at"),
            "url": raw.get("url"),
            "content": raw.get("content"),
            "symbols": raw.get("symbols", []),
            "source": raw.get("source"),
            "provider": "alpaca",
        }

    def get_stats(self) -> dict[str, Any]:
        """Get stream handler statistics."""
        return {
            "connected": self._connected,
            "authenticated": self._authenticated,
            "messages_received": self._messages_received,
            "subscriptions": len(self._subscriptions),
            "reconnect_attempts": self._reconnect_attempts,
            "uptime_seconds": (
                (datetime.now() - self._connect_time).total_seconds() if self._connect_time else 0
            ),
        }
