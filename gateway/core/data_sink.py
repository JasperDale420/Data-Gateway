"""Data sink abstraction for publishing data to downstream storage systems.

This module provides a pluggable architecture for publishing Gateway data
to external systems like Redis Streams, Kafka, or files for Heber ingestion.

Dispatch model
--------------

Each registered sink owns a bounded ``asyncio.Queue`` and a small worker
pool that drains the queue.  Producers call ``publish_all`` which routes
the event through the per-sink queue:

    producer ──put(timeout)──▶ Queue[topic, data] ──▶ worker ──▶ sink.publish

Backpressure is propagated by ``Queue.put`` blocking the producer until a
slot opens or ``data_sink_producer_block_timeout_seconds`` elapses.  When
the timeout fires the event is dropped and the *emergency-only*
``gateway_sink_producer_timeout_drops_total`` counter is incremented (see
``config/prometheus_alerts.yml``).  This replaces an earlier
drop-on-saturation semaphore that silently lost any event scheduled while
the in-flight cap was already reached — operators observed thousands of
silent drops per minute around opening bell with no recovery path.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from gateway.core.circuit_breaker import CircuitOpenError, CircuitState, get_circuit_breaker
from gateway.core.log_throttle import LogThrottle
from gateway.core.logger import logger
from gateway.core.metrics import (
    record_sink_producer_timeout_drop,
    record_sink_publish,
    set_sink_queue_capacity,
    set_sink_queue_size,
    set_sink_worker_count,
)

# A saturated sink drops every overflowing event and pages on each one. Throttle
# the CRITICAL page to one per minute per sink so the storm doesn't bury the
# alert; every drop is still counted in the metric and publish stats.
_PRODUCER_DROP_LOG_THROTTLE = LogThrottle(interval_seconds=60.0)


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


# Sentinel value pushed into a sink's queue to signal worker shutdown.
_SHUTDOWN_SENTINEL: Any = object()


class DataSinkRegistry:
    """Registry for managing multiple data sinks.

    Publishes route through per-sink bounded queues drained by a small
    worker pool.  Includes optional Redis-based deduplication to prevent
    duplicate events.
    """

    # Dedup cache TTL: 24 hours (events older than this are assumed unique)
    DEDUP_TTL_SECONDS = 86400

    def __init__(
        self,
        dedup_cache: Any | None = None,
        *,
        queue_size: int = 4096,
        worker_count: int = 8,
        producer_block_timeout_seconds: float = 0.1,
    ) -> None:
        """Initialize registry.

        Args:
            dedup_cache: Optional Redis cache for deduplication.
                         If provided, duplicate events (same event_id) will be skipped.
            queue_size: Bounded per-sink dispatch queue size.
            worker_count: Number of worker tasks draining each sink's queue.
            producer_block_timeout_seconds: Max time a producer blocks on a full
                                            queue before dropping an event.
        """
        self._sinks: list[DataSink] = []
        self._enabled = True
        self._background_tasks: set[asyncio.Task] = set()  # Prevent GC
        self._dedup_cache = dedup_cache
        self._queue_size = max(1, int(queue_size))
        self._worker_count = max(1, int(worker_count))
        self._producer_block_timeout_seconds = max(0.001, float(producer_block_timeout_seconds))
        self._dedup_stats = {"checked": 0, "deduplicated": 0}
        self._publish_stats = {
            "scheduled": 0,
            "queued": 0,
            "dropped_producer_timeout": 0,
        }
        self._sink_queues: dict[str, asyncio.Queue[tuple[str, Any]]] = {}
        self._sink_workers: dict[str, list[asyncio.Task[None]]] = {}
        self._sink_inflight: dict[str, int] = {}

    def set_dedup_cache(self, cache: Any) -> None:
        """Set dedup cache after initialization (for lazy setup)."""
        self._dedup_cache = cache
        logger.info("data_sink_dedup_enabled")

    def register(self, sink: DataSink) -> None:
        """Register a data sink."""
        self._sinks.append(sink)
        self._ensure_workers(sink)
        logger.info("data_sink_registered", sink=sink.name)

    def _ensure_workers(self, sink: DataSink) -> None:
        """Lazily create the per-sink queue and worker pool."""
        if sink.name in self._sink_queues:
            return
        queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(maxsize=self._queue_size)
        self._sink_queues[sink.name] = queue
        self._sink_inflight[sink.name] = 0
        set_sink_queue_capacity(sink.name, self._queue_size)
        set_sink_queue_size(sink.name, 0)
        set_sink_worker_count(sink.name, self._worker_count)
        workers: list[asyncio.Task[None]] = []
        for idx in range(self._worker_count):
            task = asyncio.create_task(
                self._sink_worker_loop(sink, queue),
                name=f"data_sink_worker:{sink.name}:{idx}",
            )
            workers.append(task)
        self._sink_workers[sink.name] = workers

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

    def get_queue_depth(self, sink_name: str) -> int:
        """Return current queue depth for ``sink_name`` (bounded-queue path only)."""
        queue = self._sink_queues.get(sink_name)
        return queue.qsize() if queue is not None else 0

    def get_queue_utilization(self, sink_name: str) -> float:
        """Return queued plus worker-in-flight utilization ratio for ``sink_name``."""
        queue = self._sink_queues.get(sink_name)
        if queue is None:
            return 0.0
        queue_capacity = queue.maxsize or self._queue_size
        worker_capacity = len(self._sink_workers.get(sink_name, []))
        total_capacity = queue_capacity + worker_capacity
        if total_capacity <= 0:
            return 0.0
        return (queue.qsize() + self._sink_inflight.get(sink_name, 0)) / total_capacity

    def can_accept_low_priority(self, sink_name: str, *, max_utilization: float) -> bool:
        """Return whether a low-priority publish may enter ``sink_name``'s queue."""
        if not self._enabled or not self._sinks or sink_name not in self._sink_queues:
            return False
        return self.get_queue_utilization(sink_name) < max_utilization

    async def publish_all(self, topic: str, data: dict[str, Any] | str | bytes) -> None:
        """Publish to all registered sinks.

        Under the bounded-queue path (default), this awaits a slot in each
        sink's queue with a short timeout. Under the legacy semaphore path,
        events are dispatched fire-and-forget and dropped on saturation.

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
                    # set_nx returns True if newly set, False if it already exists,
                    # None if the dedup backend was unavailable. Only False is a
                    # confirmed duplicate — None must fail open and publish, never
                    # be silently dropped.
                    is_new = await self._dedup_cache.set_nx(cache_key, "1", ttl=self.DEDUP_TTL_SECONDS)
                    if is_new is False:
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
            # Check circuit state BEFORE enqueueing. Otherwise events queued
            # during a burst would still be picked up by workers that
            # immediately hit CircuitOpenError, wasting queue capacity and
            # producing noisy error logs.
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

            await self._enqueue_for_sink(sink, topic, data)

    async def _enqueue_for_sink(
        self,
        sink: DataSink,
        topic: str,
        data: dict[str, Any] | str | bytes,
    ) -> None:
        """Enqueue a (topic, data) tuple for ``sink``'s worker pool.

        Blocks with ``producer_block_timeout_seconds`` on a full queue
        before dropping. Drops here mean the queue is full AND workers
        cannot drain it within the timeout — an emergency condition
        distinct from steady-state in-flight saturation.
        """
        self._ensure_workers(sink)
        queue = self._sink_queues[sink.name]
        try:
            await asyncio.wait_for(
                queue.put((topic, data)),
                timeout=self._producer_block_timeout_seconds,
            )
        except TimeoutError:
            self._publish_stats["dropped_producer_timeout"] += 1
            record_sink_producer_timeout_drop(sink.name)
            allowed, suppressed = _PRODUCER_DROP_LOG_THROTTLE.should_emit(sink.name)
            if allowed:
                logger.critical(
                    "data_sink_producer_timeout_drop",
                    sink=sink.name,
                    topic=topic,
                    queue_size=self._queue_size,
                    producer_block_timeout_seconds=self._producer_block_timeout_seconds,
                    suppressed_since_last=suppressed,
                )
            if not sink.record_publish_metrics:
                record_sink_publish(sink=sink.name, topic=topic, success=False)
            return

        self._publish_stats["queued"] += 1
        set_sink_queue_size(sink.name, queue.qsize())

    async def _sink_worker_loop(
        self,
        sink: DataSink,
        queue: asyncio.Queue[tuple[str, Any]],
    ) -> None:
        """Drain ``queue`` and publish entries via ``sink``.

        Exits when the shutdown sentinel is received. Exceptions from
        ``_safe_publish`` are swallowed inside that helper, so the worker
        never dies on a single bad event.

        Cancellation safety: if the worker is cancelled mid-publish (e.g.
        ``drain_queues`` timeout fires), the in-flight event is routed
        through ``sink.buffer_event`` *before* ``task_done`` acks the
        queue. Otherwise the event would be lost — ``task_done`` runs in
        ``finally`` regardless of how the publish exits, but
        ``CancelledError`` bypasses ``except Exception`` and never
        reached the retry/buffer branch.
        """
        while True:
            item = await queue.get()
            acked = False
            try:
                if item is _SHUTDOWN_SENTINEL:
                    queue.task_done()
                    acked = True
                    return
                topic, data = item
                self._publish_stats["scheduled"] += 1
                self._sink_inflight[sink.name] = self._sink_inflight.get(sink.name, 0) + 1
                try:
                    await self._safe_publish(sink, topic, data)
                except asyncio.CancelledError:
                    # Drain timeout / shutdown forced a cancel mid-publish.
                    # Salvage the event by handing it to the sink's failed
                    # buffer so the next reconnect/drain can retry it.
                    self._safe_buffer_event(sink, topic, data, reason="cancelled")
                    queue.task_done()
                    acked = True
                    set_sink_queue_size(sink.name, queue.qsize())
                    raise
                except Exception:
                    # _safe_publish handles its own logging; this is a
                    # defence-in-depth guard so a bug in the helper can't
                    # take the worker out. The retry/buffer logic inside
                    # the sink owns the routing decision.
                    logger.exception(
                        "data_sink_worker_unhandled_exception",
                        sink=sink.name,
                        topic=topic,
                    )
            finally:
                if item is not _SHUTDOWN_SENTINEL:
                    self._sink_inflight[sink.name] = max(0, self._sink_inflight.get(sink.name, 0) - 1)
                if not acked:
                    queue.task_done()
                    set_sink_queue_size(sink.name, queue.qsize())

    def _safe_buffer_event(
        self,
        sink: DataSink,
        topic: str,
        data: dict[str, Any] | str | bytes,
        *,
        reason: str,
    ) -> None:
        """Route an in-flight event to the sink's failed buffer if supported.

        Used when a worker is cancelled mid-publish, so events that have
        been pulled from the queue aren't silently dropped. Sinks without
        a ``buffer_event`` hook (rare in production — only test stubs)
        cannot rescue the event, but we still log so the loss is visible.
        """
        buffer = getattr(sink, "buffer_event", None)
        if callable(buffer):
            try:
                buffer(topic, data)
                logger.warning(
                    "data_sink_worker_buffered_inflight_event",
                    sink=sink.name,
                    topic=topic,
                    reason=reason,
                )
                return
            except Exception:
                logger.exception(
                    "data_sink_worker_buffer_failed",
                    sink=sink.name,
                    topic=topic,
                    reason=reason,
                )
        logger.warning(
            "data_sink_worker_inflight_event_lost",
            sink=sink.name,
            topic=topic,
            reason=reason,
        )

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

    async def publish_all_batch_indexed(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> set[int]:
        """Batch-publish and return the EXACT succeeded message indices.

        Like ``publish_all_batch`` but reports *which* messages landed (by
        position in ``messages``) instead of a count. An index is reported
        succeeded only if it landed in every batch-capable sink — for the
        single Redis stream sink in production this is exactly the set of
        events written to ``heber:events``. This lets per-event contracts
        (flow WS fan-out parity) fan out only the events Heber received,
        rather than the "first N" approximation a count forces.

        Non-batch sinks fall through to individual ``publish`` (optimistic,
        matching ``publish_all_batch``) and do not constrain the index set.
        """
        if not self._enabled or not self._sinks or not messages:
            return set()

        all_indices = set(range(len(messages)))
        succeeded: set[int] | None = None

        for sink in self._sinks:
            # Check circuit state before any publish attempt.
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
                    # Circuit-open buffering is not a confirmed Redis write, so
                    # nothing landed for this sink — intersect to the empty set.
                    succeeded = set() if succeeded is None else (succeeded & set())
                    continue
            except Exception:
                pass  # If breaker lookup fails, proceed with publish

            if hasattr(sink, "publish_batch_indexed"):
                try:
                    sink_indices = await sink.publish_batch_indexed(messages)
                except Exception:
                    logger.exception(
                        "data_sink_batch_publish_failed",
                        sink=sink.name,
                        count=len(messages),
                    )
                    sink_indices = set()
                succeeded = sink_indices if succeeded is None else (succeeded & sink_indices)
            elif hasattr(sink, "publish_batch"):
                # Count-only sink: it cannot say WHICH indices landed. A FULL
                # count (== len(messages)) confirms every index. A PARTIAL count
                # is ambiguous — we don't know which events made it — so those
                # indices must be reported NOT-fully-confirmed (intersect to the
                # empty set), never optimistically passed through as all_indices.
                # Marking/tapping an ambiguously-published event would break the
                # per-event Heber/WS parity the indexed contract guarantees.
                try:
                    count = await sink.publish_batch(messages)
                except Exception:
                    logger.exception(
                        "data_sink_batch_publish_failed",
                        sink=sink.name,
                        count=len(messages),
                    )
                    count = 0
                if count >= len(messages):
                    confirmed = all_indices
                else:
                    confirmed = set()
                    logger.warning(
                        "data_sink_count_only_partial_unconfirmed",
                        sink=sink.name,
                        published=count,
                        total=len(messages),
                    )
                succeeded = confirmed if succeeded is None else (succeeded & confirmed)
            else:
                for topic, data in messages:
                    task = asyncio.create_task(self._safe_publish(sink, topic, data))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)

        # If no sink reported per-index results (no batch-capable sink wired,
        # only fire-and-forget publish fallbacks), fall back to optimistic
        # all-indices so callers don't drop everything.
        return succeeded if succeeded is not None else all_indices

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
        except Exception as e:
            # Sinks that record their own metrics (e.g. RedisStreamsSink) already
            # log detailed errors internally.  Avoid duplicate ERROR+traceback spam
            # by using a quieter log for those sinks.
            if sink.record_publish_metrics:
                logger.debug(
                    "data_sink_publish_failed",
                    sink=sink.name,
                    topic=topic,
                    error=str(e),
                )
            else:
                logger.exception(
                    "data_sink_publish_failed",
                    sink=sink.name,
                    topic=topic,
                )
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

    async def drain_queues(self, timeout_seconds: float = 5.0) -> None:
        """Drain pending queue items and stop worker pools.

        Awaits ``queue.join()`` to flush every queued event, then sends one
        shutdown sentinel per worker so each worker exits cleanly. Bounded
        by ``timeout_seconds`` so a stuck sink can't block shutdown forever.
        """
        if not self._sink_queues:
            return

        # Phase 1: flush all queued events. queue.join() returns once every
        # item that's been put() has had task_done() called on it — i.e.
        # all events have been published.
        async def _flush_one(name: str, queue: asyncio.Queue) -> None:
            await queue.join()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *(_flush_one(name, q) for name, q in self._sink_queues.items()),
                    return_exceptions=True,
                ),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            logger.warning(
                "data_sink_flush_timeout",
                timeout_seconds=timeout_seconds,
                queue_depths={name: q.qsize() for name, q in self._sink_queues.items()},
            )

        # Phase 2: send sentinels to wind down workers. Use put() with the
        # remaining budget so a hammered queue (full of post-flush late
        # arrivals) doesn't lose its sentinel.
        for sink_name, workers in self._sink_workers.items():
            queue = self._sink_queues.get(sink_name)
            if queue is None:
                continue
            for _ in workers:
                try:
                    await asyncio.wait_for(
                        queue.put(_SHUTDOWN_SENTINEL),
                        timeout=max(0.1, timeout_seconds / 2),
                    )
                except TimeoutError:
                    # Hard force: cancel remaining workers if sentinel won't fit.
                    for worker in workers:
                        if not worker.done():
                            worker.cancel()
                    break

        pending: list[asyncio.Task[None]] = []
        for workers in self._sink_workers.values():
            pending.extend(workers)

        if not pending:
            return

        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            logger.warning(
                "data_sink_drain_timeout",
                timeout_seconds=timeout_seconds,
                pending_workers=sum(1 for w in pending if not w.done()),
            )
            for worker in pending:
                if not worker.done():
                    worker.cancel()
        finally:
            for sink_name in list(self._sink_queues.keys()):
                set_sink_queue_size(sink_name, 0)

    async def close_all(self) -> None:
        """Close all sinks and the dedup cache."""
        self.disable()

        # Drain bounded-queue workers before tearing down sinks so any
        # in-flight events get a final publish attempt.
        await self.drain_queues()

        # Close the dedup cache first to prevent new operations against a
        # closing Redis connection (avoids "Event loop is closed" errors).
        if self._dedup_cache is not None:
            try:
                await self._dedup_cache.close()
            except Exception as e:
                logger.warning("dedup_cache_close_failed", error=str(e))

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
