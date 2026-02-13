"""Request deduplication for coalescing identical in-flight requests."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

logger = structlog.get_logger()

T = TypeVar("T")


class RequestDeduplicator:
    """Coalesce identical in-flight requests.

    When multiple clients request the same data simultaneously,
    only one upstream request is made. All callers receive the
    same result.

    Example:
        dedup = RequestDeduplicator()

        # These two calls share the same upstream request
        result1 = await dedup.dedupe("bars:AAPL", fetch_bars)
        result2 = await dedup.dedupe("bars:AAPL", fetch_bars)
    """

    def __init__(self):
        self._pending: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._stats = {
            "total_requests": 0,
            "deduplicated": 0,
            "cache_hits": 0,
        }

    async def dedupe(
        self,
        key: str,
        fetcher: Callable[[], Awaitable[T]],
    ) -> T:
        """Execute a request with deduplication.

        Args:
            key: Unique key identifying the request
            fetcher: Async function that fetches the data

        Returns:
            The fetched data (shared if deduplicated)
        """
        self._stats["total_requests"] += 1

        async with self._lock:
            existing = self._pending.get(key)
            if existing:
                self._stats["deduplicated"] += 1
                logger.debug("request_deduplicated", key=key)
                future = existing
                is_new = False
            else:
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                self._pending[key] = future
                is_new = True

        if not is_new:
            return await future

        try:
            result = await fetcher()
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            async with self._lock:
                if key in self._pending:
                    del self._pending[key]

    def get_pending_count(self) -> int:
        """Get count of pending requests."""
        return len(self._pending)

    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            **self._stats,
            "pending_requests": len(self._pending),
            "dedup_rate": (
                self._stats["deduplicated"] / self._stats["total_requests"]
                if self._stats["total_requests"] > 0
                else 0.0
            ),
        }


# Singleton instance
_deduplicator: RequestDeduplicator = None


def get_deduplicator() -> RequestDeduplicator:
    """Get or create the singleton deduplicator."""
    global _deduplicator
    if _deduplicator is None:
        _deduplicator = RequestDeduplicator()
    return _deduplicator


# ─────────────────────────────────────────────────────────────────────────────
# Message Deduplicator (PRD lines 588-627)
# ─────────────────────────────────────────────────────────────────────────────

import hashlib
import json

from cachetools import LRUCache


class MessageDeduplicator:
    """Prevents duplicate message delivery per PRD spec.

    Uses per-symbol LRU cache of seen message hashes to detect
    and filter duplicate messages within a configurable window.

    Example:
        dedup = MessageDeduplicator(window_size=1000)

        if dedup.is_duplicate("AAPL", message):
            return  # Skip duplicate

        send_to_clients(message)
    """

    def __init__(self, window_size: int = 1000):
        """Initialize deduplicator.

        Args:
            window_size: Number of message hashes to remember per symbol
        """
        self._seen: dict[str, LRUCache] = {}
        self._window_size = window_size
        self._stats = {
            "total_messages": 0,
            "duplicates_detected": 0,
        }

    def _serialize_data(self, data: dict) -> str:
        """Serialize data for stable hashing."""
        try:
            return json.dumps(data, sort_keys=True, default=str)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "dedup_payload_unserializable",
                payload_type=type(data).__name__,
                error=str(exc),
                exc_info=True,
            )
            return repr(data)

    def _hash(self, symbol: str, timestamp: str, data: dict) -> str:
        """Create unique hash for deduplication.

        Args:
            symbol: Symbol name
            timestamp: Message timestamp
            data: Message data payload

        Returns:
            MD5 hash of the message content
        """
        content = f"{symbol}:{timestamp}:{self._serialize_data(data)}"
        return hashlib.md5(content.encode()).hexdigest()

    def is_duplicate(self, symbol: str, message: dict) -> bool:
        """Check if a message is a duplicate.

        Args:
            symbol: Symbol the message is for
            message: Message dict with 'timestamp' and 'data' keys

        Returns:
            True if this message was seen before, False if new
        """
        self._stats["total_messages"] += 1

        timestamp = message.get("timestamp", "")
        data = message.get("data", {})

        key = self._hash(symbol, str(timestamp), data)

        # Initialize LRU cache for symbol if needed
        if symbol not in self._seen:
            self._seen[symbol] = LRUCache(maxsize=self._window_size)

        # Check if seen
        if key in self._seen[symbol]:
            self._stats["duplicates_detected"] += 1
            logger.debug(
                "duplicate_message_detected",
                symbol=symbol,
                code="GW-I7010",
            )
            return True

        # Mark as seen
        self._seen[symbol][key] = True
        return False

    def clear_symbol(self, symbol: str) -> None:
        """Clear seen messages for a specific symbol."""
        if symbol in self._seen:
            del self._seen[symbol]

    def clear_all(self) -> None:
        """Clear all seen messages."""
        self._seen.clear()

    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            **self._stats,
            "symbols_tracked": len(self._seen),
            "duplicate_rate": (
                self._stats["duplicates_detected"] / self._stats["total_messages"]
                if self._stats["total_messages"] > 0
                else 0.0
            ),
        }


# Singleton for MessageDeduplicator
_message_deduplicator: MessageDeduplicator = None


def get_message_deduplicator() -> MessageDeduplicator:
    """Get or create the singleton message deduplicator."""
    global _message_deduplicator
    if _message_deduplicator is None:
        _message_deduplicator = MessageDeduplicator()
    return _message_deduplicator
