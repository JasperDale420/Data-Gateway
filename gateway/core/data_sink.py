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
from collections.abc import Awaitable, Callable
from typing import Any, cast

from gateway.core.circuit_breaker import CircuitOpenError, CircuitState, get_circuit_breaker
from gateway.core.log_throttle import LogThrottle
from gateway.core.logger import logger
from gateway.core.metrics import (
    record_low_priority_rest_shed,
    record_sink_producer_timeout_drop,
    record_sink_producer_timeout_loss,
    record_sink_publish,
    set_sink_queue_capacity,
    set_sink_queue_size,
    set_sink_queue_utilization,
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

    @property
    def durable_admission(self) -> bool:
        """Whether ``publish`` durably admits data before returning."""
        return False

    def is_durable_topic(self, topic: str) -> bool:
        """Whether this topic must commit before producer success."""
        del topic
        return self.durable_admission

    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Spill an undeliverable event into a retry buffer.

        Returns True if the event was buffered for later drain, False if this
        sink has no buffer (the event is lost). Default: no buffer.
        """
        return False

    def schedule_drain(self) -> None:
        """Best-effort: flush the retry buffer once transient backpressure clears.

        Default: no-op. Buffered sinks override this. A producer-timeout spill
        happens while the connection is healthy (queue full, not Redis down),
        so nothing else would drain it until the next reconnect.
        """
        return None

    async def close(self) -> None:  # noqa: B027
        """Close the sink connection. Override if cleanup is needed."""
        pass


# Sentinel value pushed into a sink's queue to signal worker shutdown.
_SHUTDOWN_SENTINEL: Any = object()


class _BatchProbeFailure(RuntimeError):
    """A breaker-admitted batch produced zero successes — a sink failure."""


def _batch_result_alive(raw: Any, n: int) -> bool:
    """True when the sink accepted at least one message of a non-empty batch.

    Judged on the RAW result, not normalized flags: a partial opaque count
    yields all-False flags (no message may be confirmed) but still proves the
    sink is alive — it must not be recorded as a breaker failure.
    """
    if n == 0:
        return True
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return raw > 0
    if isinstance(raw, list | tuple):
        return any(raw)
    if isinstance(raw, set | frozenset):
        return len(raw) > 0
    return False


def _flags_from_results(result: Any, n: int) -> list[bool]:
    """Normalize a per-message ``list[bool]`` batch result to exactly n flags."""
    flags = [bool(ok) for ok in result] if isinstance(result, list | tuple) else []
    return (flags + [False] * n)[:n]


def _flags_from_count(result: Any, n: int) -> list[bool]:
    """Normalize an opaque success count: only a FULL count confirms messages.

    A partial count cannot say WHICH messages landed, so no message may be
    confirmed (callers retry; sink-side event_id dedup drops duplicates).
    """
    count = result if isinstance(result, int) else 0
    return [True] * n if count >= n else [False] * n


def _flags_from_indices(result: Any, n: int) -> list[bool]:
    """Normalize a set of succeeded message indices to n flags."""
    flags = [False] * n
    if isinstance(result, set | frozenset):
        for index in result:
            if isinstance(index, int) and 0 <= index < n:
                flags[index] = True
    return flags


def _event_log_context(data: Any) -> dict[str, str]:
    """Extract high-signal event identifiers for drop logs."""
    if not isinstance(data, dict):
        return {}
    context: dict[str, str] = {}
    for key in ("event_id", "feed", "provider", "instrument_key"):
        value = data.get(key)
        if isinstance(value, str):
            context[key] = value
    return context


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
            "producer_timeout_loss": 0,
            "low_priority_shed": 0,
        }
        self._publish_stats_by_source_feed: dict[str, dict[str, int]] = {}
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
        if not self._is_durable(sink):
            self._ensure_workers(sink)
        logger.info("data_sink_registered", sink=sink.name)

    @staticmethod
    def _is_durable(sink: DataSink) -> bool:
        """Keep capability detection compatible with existing duck-typed sinks."""
        return bool(getattr(sink, "durable_admission", False))

    @classmethod
    def _is_durable_for(cls, sink: DataSink, topic: str) -> bool:
        checker = getattr(sink, "is_durable_topic", None)
        if callable(checker):
            return bool(checker(topic))
        return cls._is_durable(sink)

    def _ensure_workers(self, sink: DataSink) -> None:
        """Lazily create the per-sink queue and worker pool."""
        if sink.name in self._sink_queues:
            return
        queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(maxsize=self._queue_size)
        self._sink_queues[sink.name] = queue
        self._sink_inflight[sink.name] = 0
        set_sink_queue_capacity(sink.name, self._queue_size)
        set_sink_queue_size(sink.name, 0)
        set_sink_queue_utilization(sink.name, 0.0)
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

    @property
    def has_durable_admission(self) -> bool:
        """Return whether any registered sink commits before returning."""
        return any(self._is_durable(sink) for sink in self._sinks)

    def has_durable_admission_for(self, topic: str) -> bool:
        """Return whether ``topic`` commits before returning."""
        return any(self._is_durable_for(sink, topic) for sink in self._sinks)

    def get_dedup_stats(self) -> dict[str, int]:
        """Return deduplication statistics."""
        return self._dedup_stats.copy()

    def get_publish_stats(self) -> dict[str, int]:
        """Return publish scheduling/backpressure statistics."""
        return self._publish_stats.copy()

    def _source_feed_key(
        self,
        data: dict[str, Any] | str | bytes | None,
        *,
        source: str | None,
        feed: str | None,
    ) -> str:
        source_value = source if isinstance(source, str) and source.strip() else None
        if source_value is None and isinstance(data, dict):
            raw_source = data.get("source")
            if isinstance(raw_source, str) and raw_source.strip():
                source_value = raw_source
        if source_value in {"rest", "websocket"} and source is None:
            source_value = "poller"
        source_value = source_value or "unknown"
        feed_value = feed if isinstance(feed, str) and feed.strip() else None
        if feed_value is None and isinstance(data, dict):
            raw_feed = data.get("feed")
            if isinstance(raw_feed, str) and raw_feed.strip():
                feed_value = raw_feed
        return f"{source_value}:{feed_value or 'unknown'}"

    def _record_partitioned_publish_stat(
        self,
        status: str,
        data: dict[str, Any] | str | bytes | None = None,
        *,
        source: str | None,
        feed: str | None = None,
    ) -> None:
        partition = self._publish_stats_by_source_feed.setdefault(
            self._source_feed_key(data, source=source, feed=feed),
            {
                "queued": 0,
                "dropped_producer_timeout": 0,
                "low_priority_shed": 0,
            },
        )
        partition[status] = int(partition.get(status, 0)) + 1

    def record_low_priority_shed(self, *, feed: str, source: str = "rest") -> None:
        """Record a low-priority publish that was intentionally shed before enqueue."""
        self._publish_stats["low_priority_shed"] += 1
        self._record_partitioned_publish_stat(
            "low_priority_shed",
            None,
            source=source,
            feed=feed,
        )
        if source == "rest":
            record_low_priority_rest_shed(feed=feed)

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

    def _update_sink_queue_metrics(self, sink_name: str) -> None:
        queue = self._sink_queues.get(sink_name)
        if queue is None:
            return
        set_sink_queue_size(sink_name, queue.qsize())
        set_sink_queue_utilization(sink_name, self.get_queue_utilization(sink_name))

    def get_backpressure_snapshot(self) -> dict[str, Any]:
        """Return current bounded-queue and backpressure diagnostics."""
        return {
            "queue_size": self._queue_size,
            "worker_count": self._worker_count,
            "producer_block_timeout_seconds": self._producer_block_timeout_seconds,
            "sinks": {
                name: {
                    "queue_depth": queue.qsize(),
                    "queue_utilization": self.get_queue_utilization(name),
                    "inflight": self._sink_inflight.get(name, 0),
                }
                for name, queue in self._sink_queues.items()
            },
            "publish_stats": {
                **self._publish_stats.copy(),
                "by_source_feed": {key: value.copy() for key, value in self._publish_stats_by_source_feed.items()},
            },
        }

    def get_transport_status(self) -> dict[str, Any]:
        """Expose durable admission separately from asynchronous broker delivery."""
        statuses: dict[str, Any] = {}
        for sink in self._sinks:
            snapshot = getattr(sink, "transport_status", None)
            if callable(snapshot):
                statuses[sink.name] = snapshot()
        return statuses

    def can_accept_low_priority(
        self,
        sink_name: str,
        *,
        max_utilization: float,
        topic: str | None = None,
    ) -> bool:
        """Return whether a low-priority publish may enter ``sink_name``'s queue."""
        if not self._enabled or not self._sinks:
            return False
        durable_sink = next(
            (
                candidate
                for candidate in self._sinks
                if (self._is_durable_for(candidate, topic) if topic is not None else self._is_durable(candidate))
            ),
            None,
        )
        if durable_sink is not None:
            capacity_check = getattr(durable_sink, "can_accept_low_priority", None)
            if callable(capacity_check):
                return bool(capacity_check(max_utilization=max_utilization))
            return True
        if sink_name not in self._sink_queues:
            return False
        return self.get_queue_utilization(sink_name) < max_utilization

    async def publish_all(
        self,
        topic: str,
        data: dict[str, Any] | str | bytes,
        *,
        source: str | None = None,
        feed: str | None = None,
    ) -> None:
        """Publish to all registered sinks.

        Under the bounded-queue path (default), this awaits a slot in each
        sink's queue with a short timeout. Under the legacy semaphore path,
        events are dispatched fire-and-forget and dropped on saturation.

        If dedup cache is configured, checks event_id before publishing.
        """
        if not self._enabled or not self._sinks:
            return

        # Durable sinks own exact idempotency in their on-disk admission
        # store. Redis-backed best-effort dedup must never skip an event before
        # that durable boundary.
        has_durable_sink = any(self._is_durable_for(sink, topic) for sink in self._sinks)

        # Dedup check: skip if event_id already published (only for dicts)
        if isinstance(data, dict) and not has_durable_sink:
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
            if self._is_durable_for(sink, topic):
                accepted = await sink.publish(topic, data)
                if not accepted:
                    raise RuntimeError(f"durable_sink_admission_failed:{sink.name}")
                self._publish_stats["scheduled"] += 1
                self._publish_stats["queued"] += 1
                self._record_partitioned_publish_stat("queued", data, source=source, feed=feed)
                if not sink.record_publish_metrics:
                    record_sink_publish(sink=sink.name, topic=topic, success=True)
                continue

            # Check circuit state BEFORE enqueueing. Otherwise events queued
            # during a burst would still be picked up by workers that
            # immediately hit CircuitOpenError, wasting queue capacity and
            # producing noisy error logs.
            try:
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                # probe_due(): once the recovery window elapses, events must
                # flow to the worker again so breaker.call() can run the
                # half-open probe — buffering unconditionally on OPEN leaves
                # the breaker open forever (no other path probes it).
                if breaker.state == CircuitState.OPEN and not breaker.probe_due():
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
            except RuntimeError as e:
                # Breaker registry is pure in-memory asyncio; lookup only fails
                # on event-loop/lock teardown races (shutdown, cross-loop tests).
                # Fail open — publish rather than drop — but leave a trace.
                logger.debug("data_sink_breaker_precheck_failed", sink=sink.name, error=str(e))

            await self._enqueue_for_sink(sink, topic, data, source=source, feed=feed)

    async def _enqueue_for_sink(
        self,
        sink: DataSink,
        topic: str,
        data: dict[str, Any] | str | bytes,
        *,
        source: str | None,
        feed: str | None,
    ) -> None:
        """Enqueue a (topic, data) tuple for ``sink``'s worker pool.

        Blocks with ``producer_block_timeout_seconds`` on a full queue
        before dropping. Drops here mean the queue is full AND workers
        cannot drain it within the timeout — an emergency condition
        distinct from steady-state in-flight saturation.
        """
        self._ensure_workers(sink)
        queue = self._sink_queues[sink.name]
        partition_feed = feed
        if partition_feed is None and isinstance(data, dict):
            raw_feed = data.get("feed")
            if isinstance(raw_feed, str):
                partition_feed = raw_feed
        try:
            await asyncio.wait_for(
                queue.put((topic, data)),
                timeout=self._producer_block_timeout_seconds,
            )
        except TimeoutError:
            buffered = sink.buffer_event(topic, data)
            if buffered:
                sink.schedule_drain()
            self._publish_stats["dropped_producer_timeout"] += 1
            if not buffered:
                self._publish_stats["producer_timeout_loss"] = self._publish_stats.get("producer_timeout_loss", 0) + 1
                record_sink_producer_timeout_loss(sink.name, source=source, feed=partition_feed)
            self._record_partitioned_publish_stat(
                "dropped_producer_timeout",
                data,
                source=source,
                feed=partition_feed,
            )
            record_sink_producer_timeout_drop(sink.name, source=source, feed=partition_feed)
            allowed, suppressed = _PRODUCER_DROP_LOG_THROTTLE.should_emit(sink.name)
            if allowed:
                # Spilled = recoverable (warning); true loss = page-worthy (critical).
                log_fn = logger.warning if buffered else logger.critical
                log_fn(
                    "data_sink_producer_timeout_drop",
                    sink=sink.name,
                    topic=topic,
                    queue_size=self._queue_size,
                    producer_block_timeout_seconds=self._producer_block_timeout_seconds,
                    suppressed_since_last=suppressed,
                    spilled_to_buffer=buffered,
                    **_event_log_context(data),
                )
            if not sink.record_publish_metrics:
                record_sink_publish(sink=sink.name, topic=topic, success=False)
            return

        self._publish_stats["queued"] += 1
        self._record_partitioned_publish_stat("queued", data, source=source, feed=partition_feed)
        self._update_sink_queue_metrics(sink.name)

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
                # nosemgrep: empire-no-bare-exception -- worker boundary: logged via logger.exception below; a helper bug must not kill the worker
                try:
                    await self._safe_publish(sink, topic, data)
                except asyncio.CancelledError:
                    # Drain timeout / shutdown forced a cancel mid-publish.
                    # Salvage the event by handing it to the sink's failed
                    # buffer so the next reconnect/drain can retry it.
                    self._safe_buffer_event(sink, topic, data, reason="cancelled")
                    queue.task_done()
                    acked = True
                    self._update_sink_queue_metrics(sink.name)
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
                    self._update_sink_queue_metrics(sink.name)

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
            # nosemgrep: empire-no-bare-exception -- salvage path: failure logged via logger.exception; loss log below must still run
            try:
                # buffer_event returns True only if the event actually landed
                # in a retry buffer. The ABC default returns False (no buffer),
                # so a falsy return must fall through to the loss log rather
                # than being treated as a successful rescue.
                if buffer(topic, data):
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
            Total number of successful sink publishes summed across all
            sinks (legacy semantics): with N sinks and M messages where
            every sink accepts every message, returns N*M.

        Notes:
            The integer return value cannot identify *which* messages
            succeeded — under partial-batch failure (a Redis pipeline executed
            with ``transaction=False`` can fail at arbitrary indices) callers
            slicing the input as ``messages[:return_value]`` will mark the
            wrong events as published. New code should use
            :meth:`publish_all_batch_results`, which returns a parallel
            ``list[bool]``.
        """
        if not self._enabled or not self._sinks or not messages:
            return 0

        # Re-implement the legacy sum-across-sinks count directly. Going via
        # ``publish_all_batch_results`` would aggregate "any sink accepted"
        # into a single bool per message and lose the cross-sink sum that
        # existing callers (option_capture, backfill) rely on.
        total = 0
        for sink in self._sinks:
            indexed_messages = await self._batch_candidates(sink, messages)
            if not indexed_messages:
                continue
            sink_messages = [message for _, message in indexed_messages]
            if hasattr(sink, "publish_batch_results"):
                sink_flags = await self._batch_publish_flags(
                    sink, sink_messages, sink.publish_batch_results, _flags_from_results
                )
                total += sum(1 for ok in sink_flags if ok)
            elif hasattr(sink, "publish_batch"):
                # Count-only sink through the breaker: a full count adds
                # len(messages); a partial count adds 0 — an opaque partial
                # count cannot say which messages landed, and this legacy
                # counter's callers only use the total for logging/metrics.
                sink_flags = await self._batch_publish_flags(sink, sink_messages, sink.publish_batch, _flags_from_count)
                total += sum(1 for ok in sink_flags if ok)
            else:
                for _index, (topic, data) in indexed_messages:
                    task = asyncio.create_task(self._safe_publish(sink, topic, data))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    total += 1  # Optimistic
        return total

    async def _batch_candidates(
        self,
        sink: DataSink,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[tuple[int, tuple[str, dict[str, Any]]]]:
        """Filter only volatile messages when their shared circuit is open."""
        indexed = list(enumerate(messages))
        volatile = [item for item in indexed if not self._is_durable_for(sink, item[1][0])]
        if not volatile:
            return indexed
        try:
            breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
            if breaker.state != CircuitState.OPEN or breaker.probe_due():
                # Not open, or the recovery window elapsed — admit the batch so
                # the worker's breaker.call() can probe (see probe_due()).
                return indexed
        except RuntimeError as e:
            logger.debug("data_sink_breaker_precheck_failed", sink=sink.name, error=str(e))
            return indexed

        buffer = getattr(sink, "buffer_event", None)
        if callable(buffer):
            for _index, (topic, data) in volatile:
                buffer(topic, data)
        logger.warning(
            "data_sink_batch_circuit_open_buffered" if callable(buffer) else "data_sink_batch_circuit_open",
            sink=sink.name,
            count=len(volatile),
        )
        return [item for item in indexed if self._is_durable_for(sink, item[1][0])]

    def _buffer_batch(
        self,
        sink: DataSink,
        sink_messages: list[tuple[str, dict[str, Any]]],
        *,
        reason: str,
    ) -> None:
        """Buffer every message of a failed admitted batch for later drain."""
        for topic, data in sink_messages:
            self._safe_buffer_event(sink, topic, data, reason=reason)

    async def _batch_publish_flags(
        self,
        sink: DataSink,
        sink_messages: list[tuple[str, dict[str, Any]]],
        func: Any,
        to_flags: Callable[[Any, int], list[bool]],
    ) -> list[bool]:
        """Run one sink batch publish through its circuit breaker.

        Returns per-message success flags aligned with ``sink_messages``.
        Batch APIs bypass the per-event worker, so without this the breaker
        could never half-open, re-open, or close on batch traffic — a stuck
        OPEN breaker then starves batch pollers forever (2026-08-05 incident).

        Outcome handling:
        - A breaker-admitted call whose result has zero successes counts as a
          sink FAILURE for the breaker (Redis batch failures return all-False
          rather than raising) and buffers the whole admitted batch.
        - CircuitOpenError (open circuit, or a half-open probe already in
          flight): volatile messages are buffered — they must never bypass
          single-probe admission — while durable/backfill-lane messages are
          published directly, preserving the lane-routing contract that an
          open live-lane circuit does not block backfill. Direct-path
          outcomes deliberately do not feed breaker state (they ride an
          independent lane/connection from the one that tripped).
        """
        n = len(sink_messages)
        if n == 0:
            return []
        breaker = await get_circuit_breaker(f"data_sink:{sink.name}")

        async def _probe() -> list[bool]:
            raw = await func(sink_messages)
            if not _batch_result_alive(raw, n):
                raise _BatchProbeFailure(f"sink {sink.name} accepted 0 of {n} batch messages")
            return to_flags(raw, n)

        try:
            return await breaker.call(_probe)
        except CircuitOpenError:
            flags = [False] * n
            durable_indexed = [
                (index, message)
                for index, message in enumerate(sink_messages)
                if self._is_durable_for(sink, message[0])
            ]
            for index, (topic, data) in enumerate(sink_messages):
                if not self._is_durable_for(sink, topic):
                    self._safe_buffer_event(sink, topic, data, reason="circuit_open_batch")
            if durable_indexed:
                durable_messages = [message for _, message in durable_indexed]
                # nosemgrep: empire-no-bare-exception -- batch boundary: logged via logger.exception with explicit failed-result fallback
                try:
                    durable_flags = to_flags(await func(durable_messages), len(durable_messages))
                except Exception:
                    logger.exception(
                        "data_sink_batch_publish_failed",
                        sink=sink.name,
                        count=len(durable_messages),
                    )
                    durable_flags = [False] * len(durable_messages)
                for (index, _message), ok in zip(durable_indexed, durable_flags, strict=True):
                    flags[index] = ok
            return flags
        except Exception:
            # Breaker-admitted call failed (raised, or all-failed results
            # converted to _BatchProbeFailure): the breaker recorded the
            # failure; buffer the whole admitted batch so it drains once the
            # sink recovers instead of being lost.
            logger.exception(
                "data_sink_batch_publish_failed",
                sink=sink.name,
                count=n,
            )
            self._buffer_batch(sink, sink_messages, reason="batch_publish_failed")
            return [False] * n

    async def publish_all_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[bool]:
        """Batch-publish messages and return per-message success flags.

        Returns a ``list[bool]`` aligned 1:1 with ``messages``: each entry is
        ``True`` if *at least one* registered sink published that message
        successfully, ``False`` otherwise. This is the safe primitive for
        poller dedup state — callers can iterate ``zip(messages, results)``
        and mark only successful events as seen, instead of slicing the input
        by an aggregate count (which silently mis-marks events under partial
        pipeline failure).

        Args:
            messages: List of (topic, data) tuples.

        Returns:
            ``list[bool]`` of length ``len(messages)``.
        """
        if not messages:
            return []
        if not self._enabled or not self._sinks:
            return [False] * len(messages)

        # Aggregate per-message success across all sinks. A message is
        # "published" if any sink accepted it (matches semantics of the
        # int-returning publish_all_batch sum below). With the standard
        # single-sink configuration this is unambiguous.
        any_success = [False] * len(messages)

        for sink in self._sinks:
            indexed_messages = await self._batch_candidates(sink, messages)
            if not indexed_messages:
                continue
            sink_messages = [message for _, message in indexed_messages]

            if hasattr(sink, "publish_batch_results"):
                # Preferred path: sink returns per-message booleans, so
                # partial failure is observable. Runs through the circuit
                # breaker so batch traffic can probe/close/reopen it.
                sink_results = await self._batch_publish_flags(
                    sink, sink_messages, sink.publish_batch_results, _flags_from_results
                )
                for (original_index, _message), ok in zip(
                    indexed_messages,
                    sink_results,
                    strict=True,
                ):
                    if ok:
                        any_success[original_index] = True
            elif hasattr(sink, "publish_batch"):
                # Legacy path: only an aggregate count is available.
                # Approximating per-message success from a count is unsafe
                # (this is the original bug), but the only safe fallback that
                # preserves "no-event-lost" semantics is to mark every
                # message as failed when count < len(messages), so callers
                # can retry. When count == len(messages) all succeeded.
                # (_flags_from_count encodes exactly that full-count rule.)
                sink_flags = await self._batch_publish_flags(sink, sink_messages, sink.publish_batch, _flags_from_count)
                for (original_index, _message), ok in zip(indexed_messages, sink_flags, strict=True):
                    if ok:
                        any_success[original_index] = True
            else:
                # Fallback: publish individually. Each task is fire-and-forget
                # (background); the legacy semantics were "optimistic" --
                # preserve that for back-compat callers that explicitly
                # request per-message results from a non-batch sink.
                for original_index, (topic, data) in indexed_messages:
                    task = asyncio.create_task(self._safe_publish(sink, topic, data))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    any_success[original_index] = True

        return any_success

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
            indexed_messages = await self._batch_candidates(sink, messages)
            candidate_indices = {index for index, _message in indexed_messages}
            succeeded = candidate_indices if succeeded is None else (succeeded & candidate_indices)
            if not indexed_messages:
                continue
            sink_messages = [message for _, message in indexed_messages]

            if hasattr(sink, "publish_batch_indexed"):
                sink_flags = await self._batch_publish_flags(
                    sink, sink_messages, sink.publish_batch_indexed, _flags_from_indices
                )
                sink_indices = {indexed_messages[index][0] for index, ok in enumerate(sink_flags) if ok}
                succeeded = sink_indices if succeeded is None else (succeeded & sink_indices)
            elif hasattr(sink, "publish_batch"):
                # Count-only sink: it cannot say WHICH indices landed. A FULL
                # count (== len(messages)) confirms every index. A PARTIAL count
                # is ambiguous — we don't know which events made it — so those
                # indices must be reported NOT-fully-confirmed (intersect to the
                # empty set), never optimistically passed through as all_indices.
                # Marking/tapping an ambiguously-published event would break the
                # per-event Heber/WS parity the indexed contract guarantees.
                # (_flags_from_count confirms indices only on a full count.)
                sink_flags = await self._batch_publish_flags(sink, sink_messages, sink.publish_batch, _flags_from_count)
                if all(sink_flags):
                    confirmed = candidate_indices
                else:
                    confirmed = set()
                    logger.warning(
                        "data_sink_count_only_partial_unconfirmed",
                        sink=sink.name,
                        total=len(sink_messages),
                    )
                succeeded = confirmed if succeeded is None else (succeeded & confirmed)
            else:
                for _index, (topic, data) in indexed_messages:
                    task = asyncio.create_task(self._safe_publish(sink, topic, data))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)

        # If no sink reported per-index results (no batch-capable sink wired,
        # only fire-and-forget publish fallbacks), fall back to optimistic
        # all-indices so callers don't drop everything.
        return succeeded if succeeded is not None else all_indices

    async def publish_flow_with_watch_batch_indexed(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> set[int]:
        """Atomically admit flow writer and watch copies for durable live lanes."""
        if not self._enabled or not self._sinks or not messages:
            return set()
        succeeded: set[int] | None = None
        for sink in self._sinks:
            publish = getattr(sink, "publish_flow_with_watch_batch_results", None)
            if not callable(publish):
                continue
            publish_flow = cast(
                Callable[[list[tuple[str, dict[str, Any]]]], Awaitable[list[bool]]],
                publish,
            )
            results = await publish_flow(messages)
            if len(results) != len(messages):
                raise RuntimeError("flow writer/watch admission returned an invalid result length")
            indices = {index for index, accepted in enumerate(results) if accepted}
            succeeded = indices if succeeded is None else succeeded & indices
        return succeeded or set()

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
            # PR #32 established `_safe_buffer_event` as the single buffer
            # site for in-flight events. `publish_all` checks the circuit
            # state BEFORE enqueue and routes to `buffer_event` if OPEN —
            # but the breaker can trip AFTER enqueue and BEFORE this worker
            # call. Without this branch the event is silently lost, which
            # regressed PR #32's invariant. Use the same buffer routing as
            # the generic-exception path below.
            self._safe_buffer_event(sink, topic, data, reason="circuit_open_in_flight")
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
            # nosemgrep: empire-no-bare-exception -- any sink/breaker error means unhealthy; logged at debug, health.py logs degradation throttled
            try:
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                if breaker.state == CircuitState.OPEN:
                    results[sink.name] = False
                    continue
                results[sink.name] = await sink.health_check()
            except Exception:
                logger.debug("data_sink_health_check_error", sink=sink.name, exc_info=True)
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
        finally:
            for sink_name in self._sink_queues:
                self._update_sink_queue_metrics(sink_name)

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
                set_sink_queue_utilization(sink_name, 0.0)

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
