"""Base poller ABC for background polling tasks.

Extracts the common lifecycle skeleton shared by UWPoller,
AlpacaQuotesPoller, and TreasuryYieldPoller: ``_running`` flag,
``asyncio.Task`` management, ``start()`` / ``stop()`` / ``_poll_loop()``
with configurable interval, ``get_runtime_snapshot()``, and optional
dedup helpers (``_is_duplicate``, ``_mark_seen``, ``_cleanup_cache``).

Subclasses implement ``_poll_once()`` (the per-tick work) and optionally
``_on_start()`` for provider initialisation.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any

from gateway.core.logger import logger


class BasePoller(abc.ABC):
    """Abstract base for asyncio background pollers.

    Parameters
    ----------
    poll_interval_seconds:
        Seconds between ticks of the main loop.
    poller_name:
        Short label used in structured log events (e.g. ``"uw_poller"``).
    """

    def __init__(self, *, poll_interval_seconds: int, poller_name: str) -> None:
        self._poll_interval: int = poll_interval_seconds
        self._poller_name: str = poller_name
        self._running: bool = False
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def _on_start(self) -> bool:
        """Perform provider/resource initialisation before the loop starts.

        Return ``True`` if startup succeeded and the loop should begin,
        ``False`` to abort (e.g. missing provider).
        """
        ...

    @abc.abstractmethod
    async def _poll_once(self) -> None:
        """Execute a single polling iteration.

        Called inside the ``while self._running`` loop.  Exceptions are
        caught and logged by the base class so implementations may let
        transient errors propagate.
        """
        ...

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background polling task."""
        if self._running:
            logger.warning(f"{self._poller_name}_already_running")
            return

        ok = await self._on_start()
        if not ok:
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Cancel the background task and wait for it to finish."""
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _poll_loop(self) -> None:
        """Main loop — calls ``_poll_once`` every interval."""
        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error(f"{self._poller_name}_loop_error", error=str(e), exc_info=True)

            await asyncio.sleep(self._poll_interval)

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Base telemetry dict — subclasses extend via ``|`` merge."""
        return {
            "running": self._running,
            "poll_interval_seconds": self._poll_interval,
        }


class DedupMixin:
    """Optional mixin providing in-memory OrderedDict dedup helpers.

    Pollers that need deduplication can inherit from this alongside
    ``BasePoller``.  Call ``_init_dedup()`` from ``__init__`` to set up
    the cache.
    """

    _seen_ids: OrderedDict[str, datetime]
    _cache_ttl_seconds: int
    _poller_name: str  # provided by BasePoller

    # ponytail: hard LRU cap on the in-process dedup cache. TTL cleanup only runs
    # once per poll cycle, so between sweeps a high-volume burst (×24h TTL) could
    # grow this to hundreds of thousands of entries. Evicting an old id just means
    # a rare re-fetch re-publishes once, which Heber dedups anyway. Raise if a
    # single poller legitimately needs >200k unique ids within its TTL window.
    _MAX_SEEN_IDS = 200_000

    def _init_dedup(self, cache_ttl_seconds: int) -> None:
        self._seen_ids: OrderedDict[str, datetime] = OrderedDict()
        self._cache_ttl_seconds = cache_ttl_seconds

    def _is_duplicate(self, event_id: str) -> bool:
        return event_id in self._seen_ids

    def _mark_seen(self, event_id: str) -> None:
        self._seen_ids[event_id] = datetime.now(UTC)
        self._seen_ids.move_to_end(event_id)
        if len(self._seen_ids) > self._MAX_SEEN_IDS:
            self._seen_ids.popitem(last=False)  # evict least-recently-seen id

    def _cleanup_cache(self) -> None:
        """Remove expired entries via O(1) FIFO pops."""
        now = datetime.now(UTC)
        cutoff = self._cache_ttl_seconds
        removed = 0
        while self._seen_ids:
            eid, ts = next(iter(self._seen_ids.items()))
            if (now - ts).total_seconds() > cutoff:
                del self._seen_ids[eid]
                removed += 1
            else:
                break
        if removed:
            logger.debug(f"{self._poller_name}_cache_cleanup", removed=removed, remaining=len(self._seen_ids))
