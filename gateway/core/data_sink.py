"""Data sink abstraction for publishing data to downstream storage systems.

This module provides a pluggable architecture for publishing Gateway data
to external systems like Redis Streams, Kafka, or files for Heber ingestion.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog

from gateway.core.circuit_breaker import CircuitOpenError, CircuitState, get_circuit_breaker
from gateway.core.metrics import record_sink_publish

logger = structlog.get_logger()


class DataSink(ABC):
    """Abstract base class for data sinks.

    Each sink implementation handles publishing to a specific backend
    (Redis Streams, Kafka, file, etc.).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the sink name for logging/metrics."""
        ...

    @abstractmethod
    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Publish data to the sink.

        Args:
            topic: Topic/stream name (e.g., 'gateway.stream.bars')
            data: Message payload

        Returns:
            True if publish succeeded, False otherwise
        """
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the sink is healthy and connected."""
        ...

    @property
    def record_publish_metrics(self) -> bool:
        """Whether the sink records publish metrics internally."""
        return False

    async def close(self) -> None:  # noqa: B027
        """Close the sink connection. Override if cleanup is needed."""
        pass


class DataSinkRegistry:
    """Registry for managing multiple data sinks.

    Publishes to all registered sinks in parallel (fire-and-forget).
    Includes optional Redis-based deduplication to prevent duplicate events.
    """

    # Dedup cache TTL: 24 hours (events older than this are assumed unique)
    DEDUP_TTL_SECONDS = 86400

    def __init__(
        self,
        dedup_cache: Any | None = None,
        max_in_flight_per_sink: int = 256,
        slot_wait_timeout: float = 2.0,
    ) -> None:
        """Initialize registry.

        Args:
            dedup_cache: Optional Redis cache for deduplication.
                         If provided, duplicate events (same event_id) will be skipped.
            max_in_flight_per_sink: Max concurrent in-flight publish tasks per sink.
                                    Additional events are dropped with backpressure stats.
            slot_wait_timeout: Seconds to wait for an in-flight slot before dropping.
                               Set to 0.0 for immediate (non-blocking) drop behavior.
                               Default 2.0s tolerates short burst spikes.
        """
        self._sinks: list[DataSink] = []
        self._enabled = True
        self._background_tasks: set[asyncio.Task] = set()  # Prevent GC
        self._dedup_cache = dedup_cache
        self._max_in_flight_per_sink = max(1, max_in_flight_per_sink)
        self._slot_wait_timeout = max(0.0, slot_wait_timeout)
        self._dedup_stats = {"checked": 0, "deduplicated": 0}
        self._publish_stats = {"scheduled": 0, "dropped_backpressure": 0}
        self._sink_semaphores: dict[str, asyncio.Semaphore] = {}

    def set_dedup_cache(self, cache: Any) -> None:
        """Set dedup cache after initialization (for lazy setup)."""
        self._dedup_cache = cache
        logger.info("data_sink_dedup_enabled")

    def register(self, sink: DataSink) -> None:
        """Register a data sink."""
        self._sinks.append(sink)
        self._sink_semaphores.setdefault(sink.name, asyncio.Semaphore(self._max_in_flight_per_sink))
        logger.info("data_sink_registered", sink=sink.name)

    def disable(self) -> None:
        """Disable all publishing (for graceful shutdown)."""
        self._enabled = False

    def enable(self) -> None:
        """Enable publishing."""
        self._enabled = True

    @property
    def sink_count(self) -> int:
        """Return number of registered sinks."""
        return len(self._sinks)

    def get_dedup_stats(self) -> dict[str, int]:
        """Return deduplication statistics."""
        return self._dedup_stats.copy()

    def get_publish_stats(self) -> dict[str, int]:
        """Return publish scheduling/backpressure statistics."""
        return self._publish_stats.copy()

    async def publish_all(self, topic: str, data: dict[str, Any] | str | bytes) -> None:
        """Publish to all registered sinks (non-blocking).

        Uses fire-and-forget pattern to avoid blocking the caller.
        If dedup cache is configured, checks event_id before publishing.
        """
        if not self._enabled or not self._sinks:
            return

        # Dedup check: skip if event_id already published (only for dicts)
        if isinstance(data, dict):
            event_id = data.get("event_id")
            if event_id and self._dedup_cache:
                self._dedup_stats["checked"] += 1
                cache_key = f"dedup:publish:{event_id}"
                try:
                    # Atomic set-if-not-exists: first caller wins, no TOCTOU race.
                    # set_nx returns True if key was newly set, False if it existed.
                    is_new = await self._dedup_cache.set_nx(cache_key, "1", ttl=self.DEDUP_TTL_SECONDS)
                    if not is_new:
                        self._dedup_stats["deduplicated"] += 1
                        logger.debug(
                            "publish_deduplicated",
                            event_id=event_id,
                            topic=topic,
                        )
                        return  # Skip duplicate
                except Exception as e:
                    # On cache error, proceed with publish (fail open)
                    logger.warning(
                        "dedup_cache_error",
                        event_id=event_id,
                        error=str(e),
                    )

        for sink in self._sinks:
            # Check circuit state BEFORE creating fire-and-forget task.
            # Previously, the check happened inside _safe_publish, meaning
            # events queued during a burst would still spawn tasks that
            # immediately hit CircuitOpenError.  Checking here prevents
            # wasted task creation and noisy error logs.
            try:
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                if breaker.state == CircuitState.OPEN:
                    if hasattr(sink, "buffer_event"):
                        sink.buffer_event(topic, data)
                        logger.debug(
                            "data_sink_circuit_open_buffered",
                            sink=sink.name,
                            topic=topic,
                        )
                    else:
                        logger.debug(
                            "data_sink_circuit_open_skip",
                            sink=sink.name,
                            topic=topic,
                        )
                    if not sink.record_publish_metrics:
                        record_sink_publish(sink=sink.name, topic=topic, success=False)
                    continue
            except Exception:
                pass  # If breaker lookup fails, proceed with publish

            acquired = await self._try_acquire_sink_slot(sink.name)
            if not acquired:
                self._publish_stats["dropped_backpressure"] += 1
                logger.warning(
                    "data_sink_backpressure_drop",
                    sink=sink.name,
                    topic=topic,
                    max_in_flight=self._max_in_flight_per_sink,
                )
                if not sink.record_publish_metrics:
                    record_sink_publish(sink=sink.name, topic=topic, success=False)
                continue

            self._publish_stats["scheduled"] += 1
            task = asyncio.create_task(self._safe_publish_with_release(sink, topic, data))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def publish_all_batch(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> int:
        """Batch-publish multiple messages to all registered sinks.

        Sinks that implement ``publish_batch`` receive all messages in a single
        pipeline call. Other sinks fall back to individual ``publish`` calls.

        Args:
            messages: List of (topic, data) tuples.

        Returns:
            Total number of successfully published messages across all sinks.
        """
        if not self._enabled or not self._sinks or not messages:
            return 0

        total_published = 0

        for sink in self._sinks:
            # Check circuit state before any publish attempt
            try:
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                if breaker.state == CircuitState.OPEN:
                    if hasattr(sink, "buffer_event"):
                        for msg_topic, msg_data in messages:
                            sink.buffer_event(msg_topic, msg_data)
                        logger.warning(
                            "data_sink_batch_circuit_open_buffered",
                            sink=sink.name,
                            count=len(messages),
                        )
                    else:
                        logger.warning(
                            "data_sink_batch_circuit_open",
                            sink=sink.name,
                            count=len(messages),
                        )
                    continue
            except Exception:
                pass  # If breaker lookup fails, proceed with publish

            if hasattr(sink, "publish_batch"):
                try:
                    count = await sink.publish_batch(messages)
                    total_published += count
                except Exception:
                    logger.exception(
                        "data_sink_batch_publish_failed",
                        sink=sink.name,
                        count=len(messages),
                    )
            else:
                # Fallback: publish individually
                for topic, data in messages:
                    task = asyncio.create_task(self._safe_publish(sink, topic, data))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    total_published += 1  # Optimistic; errors logged in _safe_publish

        return total_published

    async def _try_acquire_sink_slot(self, sink_name: str) -> bool:
        """Try to reserve an in-flight publish slot.

        Waits up to ``slot_wait_timeout`` seconds for a slot to become
        available during burst publishing (e.g., batch backfill or EOD polls).
        When ``slot_wait_timeout`` is 0.0 the acquire is non-blocking: returns
        False immediately if no slot is available.
        """
        sem = self._sink_semaphores.get(sink_name)
        if sem is None:
            sem = asyncio.Semaphore(self._max_in_flight_per_sink)
            self._sink_semaphores[sink_name] = sem
        if self._slot_wait_timeout == 0.0:
            # Non-blocking: only acquire if a slot is immediately available.
            if sem.locked():
                return False
            await sem.acquire()
            return True
        try:
            await asyncio.wait_for(sem.acquire(), timeout=self._slot_wait_timeout)
            return True
        except TimeoutError:
            return False

    async def _safe_publish_with_release(
        self,
        sink: DataSink,
        topic: str,
        data: dict[str, Any] | str | bytes,
    ) -> None:
        """Publish and always release the per-sink in-flight slot."""
        try:
            await self._safe_publish(sink, topic, data)
        finally:
            sem = self._sink_semaphores.get(sink.name)
            if sem is not None:
                sem.release()

    async def _safe_publish(self, sink: DataSink, topic: str, data: dict[str, Any] | str | bytes) -> None:
        """Publish with error handling."""
        try:
            breaker = await get_circuit_breaker(f"data_sink:{sink.name}")

            async def _call():
                result = await sink.publish(topic, data)
                if result is False:
                    raise RuntimeError("sink_publish_failed")
                return result

            await breaker.call(_call)
            if not sink.record_publish_metrics:
                record_sink_publish(sink=sink.name, topic=topic, success=True)
        except CircuitOpenError as e:
            logger.warning(
                "data_sink_circuit_open",
                sink=sink.name,
                topic=topic,
                retry_after=round(e.retry_after, 2),
            )
            record_sink_publish(sink=sink.name, topic=topic, success=False)
        except Exception:
            logger.exception(
                "data_sink_publish_failed",
                sink=sink.name,
                topic=topic,
            )
            if not sink.record_publish_metrics:
                record_sink_publish(sink=sink.name, topic=topic, success=False)

    async def health_check_all(self) -> dict[str, bool]:
        """Check health of all sinks."""
        results = {}
        for sink in self._sinks:
            try:
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                if breaker.state == CircuitState.OPEN:
                    results[sink.name] = False
                    continue
                results[sink.name] = await sink.health_check()
            except Exception:
                results[sink.name] = False
        return results

    async def close_all(self) -> None:
        """Close all sinks."""
        self.disable()
        for sink in self._sinks:
            try:
                await sink.close()
            except Exception as e:
                logger.warning("data_sink_close_failed", sink=sink.name, error=str(e))


# Topic constants for Heber integration
class Topics:
    """Standard topic names for data publishing."""

    # REST API responses
    REST_BARS = "gateway.rest.bars"
    REST_QUOTES = "gateway.rest.quotes"
    REST_TRADES = "gateway.rest.trades"
    REST_OPTIONS = "gateway.rest.options"

    # WebSocket stream data
    STREAM_BARS = "gateway.stream.bars"
    STREAM_QUOTES = "gateway.stream.quotes"
    STREAM_TRADES = "gateway.stream.trades"
    STREAM_NEWS = "gateway.stream.news"
    STREAM_OPTIONS = "gateway.stream.options"

    @classmethod
    def from_message_type(cls, msg_type: str) -> str:
        """Map Alpaca message type to topic."""
        mapping = {
            "b": cls.STREAM_BARS,
            "q": cls.STREAM_QUOTES,
            "t": cls.STREAM_TRADES,
            "n": cls.STREAM_NEWS,
            "d": cls.STREAM_BARS,  # dailyBars
            "u": cls.STREAM_BARS,  # updatedBars
        }
        return mapping.get(msg_type, f"gateway.stream.{msg_type}")
