"""Legacy WebSocket subscription manager (deprecated).

Superseded by gateway.core.stream.StreamMultiplexer.
Retained for backward compatibility only.
"""

import asyncio
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class Subscription:
    """Tracks a symbol subscription and its subscribers."""

    symbol: str
    feed: str
    client_ids: set[str] = field(default_factory=set)
    subscribed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def ref_count(self) -> int:
        return len(self.client_ids)


class SubscriptionManager:
    """Manages symbol subscriptions with reference counting."""

    def __init__(self, grace_period_seconds: int = 30):
        self._subscriptions: dict[str, Subscription] = {}
        self._pending_unsubscribes: dict[str, asyncio.Task] = {}
        self._grace_period = grace_period_seconds
        self._lock = asyncio.Lock()

        # Callbacks for upstream subscription changes
        self._on_subscribe: Callable[[list[str], str], Any] | None = None
        self._on_unsubscribe: Callable[[list[str], str], Any] | None = None

    def set_callbacks(
        self,
        on_subscribe: Callable[[list[str], str], Any],
        on_unsubscribe: Callable[[list[str], str], Any],
    ) -> None:
        """Set callbacks for upstream subscription changes."""
        self._on_subscribe = on_subscribe
        self._on_unsubscribe = on_unsubscribe

    async def subscribe(
        self,
        client_id: str,
        symbols: list[str],
        feed: str,
    ) -> list[str]:
        """Subscribe client to symbols. Returns newly subscribed symbols."""
        newly_subscribed = []

        async with self._lock:
            for symbol in symbols:
                key = f"{feed}:{symbol}"

                # Cancel pending unsubscribe if exists
                if key in self._pending_unsubscribes:
                    self._pending_unsubscribes[key].cancel()
                    del self._pending_unsubscribes[key]

                if key not in self._subscriptions:
                    # New subscription
                    self._subscriptions[key] = Subscription(
                        symbol=symbol,
                        feed=feed,
                        client_ids={client_id},
                    )
                    newly_subscribed.append(symbol)
                else:
                    # Add client to existing subscription
                    self._subscriptions[key].client_ids.add(client_id)

        if newly_subscribed and self._on_subscribe:
            await self._on_subscribe(newly_subscribed, feed)

        logger.info(
            "subscription_added",
            client_id=client_id,
            symbols=len(symbols),
            newly_subscribed=len(newly_subscribed),
            feed=feed,
        )

        return newly_subscribed

    async def unsubscribe(
        self,
        client_id: str,
        symbols: list[str],
        feed: str,
    ) -> list[str]:
        """Unsubscribe client from symbols. Returns symbols pending unsubscribe."""
        pending_removal = []

        async with self._lock:
            for symbol in symbols:
                key = f"{feed}:{symbol}"

                if key not in self._subscriptions:
                    continue

                sub = self._subscriptions[key]
                sub.client_ids.discard(client_id)

                if sub.ref_count == 0:
                    # Schedule delayed unsubscribe
                    pending_removal.append(symbol)
                    task = asyncio.create_task(self._delayed_unsubscribe(key, symbol, feed))
                    self._pending_unsubscribes[key] = task

        logger.info(
            "subscription_removed",
            client_id=client_id,
            symbols=len(symbols),
            pending_removal=len(pending_removal),
        )

        return pending_removal

    async def _delayed_unsubscribe(self, key: str, symbol: str, feed: str) -> None:
        """Wait grace period then unsubscribe if still zero refs."""
        try:
            await asyncio.sleep(self._grace_period)

            async with self._lock:
                if key in self._subscriptions:
                    sub = self._subscriptions[key]
                    if sub.ref_count == 0:
                        del self._subscriptions[key]
                        if key in self._pending_unsubscribes:
                            del self._pending_unsubscribes[key]

                        if self._on_unsubscribe:
                            await self._on_unsubscribe([symbol], feed)

                        logger.info(
                            "subscription_expired",
                            symbol=symbol,
                            feed=feed,
                        )
        except asyncio.CancelledError:
            pass

    async def remove_client(self, client_id: str) -> None:
        """Remove client from all subscriptions."""
        async with self._lock:
            for key, sub in list(self._subscriptions.items()):
                sub.client_ids.discard(client_id)

                if sub.ref_count == 0 and key not in self._pending_unsubscribes:
                    task = asyncio.create_task(self._delayed_unsubscribe(key, sub.symbol, sub.feed))
                    self._pending_unsubscribes[key] = task

        logger.info("client_subscriptions_removed", client_id=client_id)

    def get_subscribers(self, symbol: str, feed: str) -> set[str]:
        """Get client IDs subscribed to a symbol."""
        key = f"{feed}:{symbol}"
        sub = self._subscriptions.get(key)
        return sub.client_ids.copy() if sub else set()

    def get_all_symbols(self, feed: str) -> list[str]:
        """Get all subscribed symbols for a feed."""
        return [
            sub.symbol
            for key, sub in self._subscriptions.items()
            if sub.feed == feed and sub.ref_count > 0
        ]

    def get_stats(self) -> dict[str, Any]:
        """Get subscription statistics."""
        by_feed: dict[str, int] = defaultdict(int)
        for sub in self._subscriptions.values():
            if sub.ref_count > 0:
                by_feed[sub.feed] += 1

        return {
            "total_subscriptions": len(self._subscriptions),
            "active_subscriptions": sum(1 for s in self._subscriptions.values() if s.ref_count > 0),
            "pending_unsubscribes": len(self._pending_unsubscribes),
            "by_feed": dict(by_feed),
        }
