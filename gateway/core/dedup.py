"""Request deduplication for coalescing identical in-flight requests."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from gateway.core.logger import logger

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

    def __init__(self, lock_stripes: int = 32):
        self._pending: dict[str, asyncio.Future] = {}
        self._lock_stripe_count = max(1, int(lock_stripes))
        self._key_locks = [asyncio.Lock() for _ in range(self._lock_stripe_count)]
        self._stats = {
            "total_requests": 0,
            "deduplicated": 0,
            "cache_hits": 0,
        }

    def _lock_for_key(self, key: str) -> asyncio.Lock:
        """Return stripe lock for key to reduce unrelated-key contention."""
        return self._key_locks[hash(key) % self._lock_stripe_count]

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

        key_lock = self._lock_for_key(key)
        async with key_lock:
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
            # `asyncio.shield` so a cancelled follower does not poison the
            # shared future for other followers.  Without shield, cancelling
            # any follower task propagates the cancellation into `future`,
            # marking it cancelled — every other follower would observe
            # `CancelledError` even though the leader may still complete.
            return await asyncio.shield(future)

        try:
            result = await fetcher()
            future.set_result(result)
            return result
        except asyncio.CancelledError:
            # Leader was cancelled mid-fetch. If we don't propagate cancellation
            # into the shared future, every follower awaiting it will hang
            # forever — CancelledError is a BaseException and bypasses the
            # `except Exception` clause below.  Cancel the future so followers
            # receive CancelledError, then re-raise so cancellation propagates
            # to the leader's caller as well.
            if not future.done():
                future.cancel()
            raise
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            async with key_lock:
                if self._pending.get(key) is future:
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
_deduplicator: RequestDeduplicator | None = None


def get_deduplicator() -> RequestDeduplicator:
    """Get or create the singleton deduplicator."""
    global _deduplicator
    if _deduplicator is None:
        _deduplicator = RequestDeduplicator()
    return _deduplicator
