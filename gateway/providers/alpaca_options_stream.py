"""Alpaca Options WebSocket stream handler."""

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

ALPACA_OPTIONS_STREAM_URL = "wss://stream.data.alpaca.markets/v1beta1/options"
HEARTBEAT_TIMEOUT_SECONDS = 30.0
HEARTBEAT_CHECK_INTERVAL = 5.0


class AlpacaOptionsStreamHandler:
    """Manages Alpaca Options WebSocket connection with reconnection logic."""

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
        """Connect to Alpaca Options WebSocket and authenticate."""
        try:
            logger.info("alpaca_options_stream_connecting")

            self._ws = await websockets.connect(ALPACA_OPTIONS_STREAM_URL)
            self._connected = True
            self._connect_time = datetime.now()
            self._last_message_time = time.time()

            # Wait for welcome message
            welcome = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            welcome_data = json.loads(welcome)
            logger.debug("alpaca_options_welcome", data=welcome_data)

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
                logger.info("alpaca_options_stream_authenticated")

                # Start heartbeat monitor
                self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

                return True
            else:
                logger.error("alpaca_options_auth_failed", response=auth_data)
                await self.disconnect()
                return False

        except Exception as e:
            logger.error("alpaca_options_connect_error", error=str(e))
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

        logger.info("alpaca_options_stream_disconnected")

    async def _heartbeat_monitor(self) -> None:
        """Monitor for stale connections and trigger reconnect."""
        while self._connected:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)

            if not self._connected:
                break

            time_since_last = time.time() - self._last_message_time
            if time_since_last > HEARTBEAT_TIMEOUT_SECONDS:
                logger.warning(
                    "alpaca_options_heartbeat_timeout",
                    seconds_since_last=time_since_last,
                )
                await self._reconnect()
                break

    async def _send_subscribe(self, contracts: list[str], feeds: list[str]) -> None:
        """Send subscribe message to upstream."""
        if not self._ws or not self._authenticated:
            return

        subscribe_msg: dict[str, Any] = {"action": "subscribe"}

        for feed in feeds:
            if feed in ("quotes", "trades", "bars"):
                subscribe_msg[feed] = contracts

        await self._ws.send(json.dumps(subscribe_msg))
        logger.info(
            "alpaca_options_subscribe_sent",
            contracts=len(contracts),
            feeds=feeds,
        )

    async def _send_unsubscribe(self, contracts: list[str], feeds: list[str]) -> None:
        """Send unsubscribe message to upstream."""
        if not self._ws or not self._authenticated:
            return

        unsubscribe_msg: dict[str, Any] = {"action": "unsubscribe"}

        for feed in feeds:
            if feed in ("quotes", "trades", "bars"):
                unsubscribe_msg[feed] = contracts

        await self._ws.send(json.dumps(unsubscribe_msg))
        logger.info(
            "alpaca_options_unsubscribe_sent",
            contracts=len(contracts),
            feeds=feeds,
        )

    async def subscribe(self, client_id: str, contracts: list[str], feeds: list[str]) -> None:
        """Subscribe a client to option contracts."""
        if self._subscription_manager:
            for feed in feeds:
                await self._subscription_manager.subscribe(client_id, contracts, feed)

    async def unsubscribe(self, client_id: str, contracts: list[str], feeds: list[str]) -> None:
        """Unsubscribe a client from option contracts."""
        if self._subscription_manager:
            for feed in feeds:
                await self._subscription_manager.unsubscribe(client_id, contracts, feed)

    async def remove_client(self, client_id: str) -> None:
        """Remove all subscriptions for a client."""
        if self._subscription_manager:
            await self._subscription_manager.remove_client(client_id)

    async def stream(
        self,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream normalized messages from Alpaca Options."""
        if not self._ws or not self._connected:
            logger.warning("alpaca_options_stream_not_connected")
            return

        try:
            async for raw_message in self._ws:
                self._last_message_time = time.time()
                self._messages_received += 1

                try:
                    messages = json.loads(raw_message)

                    for msg in messages:
                        msg_type = msg.get("T")

                        if msg_type == "q":  # Quote
                            yield self._normalize_option_quote(msg)
                        elif msg_type == "t":  # Trade
                            yield self._normalize_option_trade(msg)
                        elif msg_type == "b":  # Bar
                            yield self._normalize_option_bar(msg)
                        elif msg_type in ("success", "subscription", "error"):
                            logger.debug("alpaca_options_control_msg", msg=msg)
                        else:
                            logger.debug("alpaca_options_unknown_msg", type=msg_type)

                except json.JSONDecodeError as e:
                    logger.warning("alpaca_options_json_error", error=str(e))

        except websockets.ConnectionClosed:
            logger.warning("alpaca_options_connection_closed")
            await self._reconnect()
        except Exception as e:
            logger.error("alpaca_options_stream_error", error=str(e))
            await self._reconnect()

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        self._reconnect_attempts += 1
        if self._reconnect_attempts > self._max_reconnect_attempts:
            logger.error("alpaca_options_max_reconnects_exceeded")
            return

        delay = min(2**self._reconnect_attempts, 16)
        logger.info(
            "alpaca_options_reconnecting",
            attempt=self._reconnect_attempts,
            delay=delay,
        )

        await asyncio.sleep(delay)
        await self.disconnect()

        if await self.connect():
            # Resubscribe
            if self._subscriptions:
                await self._send_subscribe(list(self._subscriptions), ["quotes", "trades"])

    def _normalize_option_quote(self, raw: dict) -> dict[str, Any]:
        """Convert Alpaca option quote to normalized format."""
        return {
            "type": "option_quote",
            "contract": raw.get("S", ""),
            "timestamp": raw.get("t", ""),
            "bid_price": raw.get("bp"),
            "bid_size": raw.get("bs"),
            "ask_price": raw.get("ap"),
            "ask_size": raw.get("as"),
            "bid_exchange": raw.get("bx"),
            "ask_exchange": raw.get("ax"),
            "provider": "alpaca",
        }

    def _normalize_option_trade(self, raw: dict) -> dict[str, Any]:
        """Convert Alpaca option trade to normalized format."""
        return {
            "type": "option_trade",
            "contract": raw.get("S", ""),
            "timestamp": raw.get("t", ""),
            "price": raw.get("p"),
            "size": raw.get("s"),
            "exchange": raw.get("x"),
            "conditions": raw.get("c", []),
            "provider": "alpaca",
        }

    def _normalize_option_bar(self, raw: dict) -> dict[str, Any]:
        """Convert Alpaca option bar to normalized format."""
        return {
            "type": "option_bar",
            "contract": raw.get("S", ""),
            "timestamp": raw.get("t", ""),
            "open": raw.get("o"),
            "high": raw.get("h"),
            "low": raw.get("l"),
            "close": raw.get("c"),
            "volume": raw.get("v"),
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
