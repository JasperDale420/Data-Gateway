"""Redis Streams data sink for publishing to Heber.

This sink publishes Gateway data to Redis Streams, which Heber ingestors
can consume for storage in the Bronze layer.

Uses a connection pool for concurrent pipeline execution and processes
batch chunks in parallel with retry logic.
"""

import asyncio
import time
from collections import deque
from typing import Any

import orjson

from gateway.core.data_sink import DataSink
from gateway.core.logger import logger
from gateway.core.metrics import (
    record_sink_buffer_eviction,
    record_sink_publish,
    set_sink_buffer_size,
)

DEFAULT_OPERATION_TIMEOUT_SECONDS = 5.0
DEFAULT_POOL_SIZE = 64
BATCH_CHUNK_SIZE = 2_000
MAX_CONCURRENT_CHUNKS = 4
CHUNK_RETRY_ATTEMPTS = 1
INITIAL_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 5.0

# Retry settings for single publish operations.
# Previously, a single transient failure (timeout, connection reset) would
# immediately reset the connection and return False, causing the circuit
# breaker to count it as a failure.  With retries, transient blips are
# absorbed before they cascade into circuit-open events.
PUBLISH_RETRY_ATTEMPTS = 3
PUBLISH_RETRY_BACKOFF_BASE = 0.1  # seconds — doubles each attempt (0.1, 0.2, 0.4)

# Additional backoff when Redis reports it is still loading data into memory.
# This is a transient state during Redis startup/restart and warrants a
# longer pause before retrying.
REDIS_LOADING_BACKOFF_SECONDS = 2.0

# Substrings in error messages indicating Redis is still loading its dataset.
_REDIS_LOADING_INDICATORS = frozenset(
    {
        "loading the dataset",
        "redis is loading",
        "loading",
    }
)

# Failed event buffer: events that exhaust all retries are held here
# and drained on the next successful reconnect.  The deque maxlen caps
# memory usage — at ~1 KB/event, 10 000 events ≈ 10 MB.
FAILED_EVENT_BUFFER_CAPACITY = 10_000


class RedisStreamsSink(DataSink):
    """Redis Streams implementation of DataSink.

    Publishes messages to Redis Streams with automatic trimming
    to prevent unbounded growth. Uses a connection pool to support
    concurrent pipeline execution from multiple backfill coroutines.
    """

    def __init__(
        self,
        redis_url: str,
        max_len: int = 100_000,
        approximate_trim: bool = True,
        operation_timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
        pool_size: int = DEFAULT_POOL_SIZE,
        worker_count: int | None = None,
        failed_buffer_capacity: int = FAILED_EVENT_BUFFER_CAPACITY,
    ) -> None:
        """Initialize Redis Streams sink.

        Args:
            redis_url: Redis connection URL
            max_len: Maximum stream length (oldest entries trimmed)
            approximate_trim: Use ~ for more efficient trimming
            operation_timeout_seconds: Timeout for Redis operations
            pool_size: Max connections in the Redis connection pool
            worker_count: Number of data_sink registry workers that publish
                through this sink concurrently. When set, ``pool_size`` is
                raised to at least this value so the pool can never be smaller
                than the concurrency drawing from it.
            failed_buffer_capacity: Max events held in the in-memory retry
                buffer before the oldest are evicted (metered via
                ``gateway_sink_buffer_evictions_total``). Size to ride out a
                Redis blip at peak event rate; ~1 KB/event.

        Pool sizing vs. worker count:
            The data_sink registry runs ``worker_count`` coroutines that each
            call ``publish``/``publish_batch`` against this single sink, so up
            to ``worker_count`` connections are checked out at once. A
            ``pool_size`` below ``worker_count`` makes the BlockingConnectionPool
            serialize workers and, with the dedup cache fanning out its own
            connections on the same Redis, contributed to the "Too many
            connections" flood. Passing ``worker_count`` ties them together so
            they cannot silently drift apart.
        """
        self._redis_url = redis_url
        self._max_len = max_len
        self._approximate = approximate_trim
        self._operation_timeout_seconds = max(0.5, float(operation_timeout_seconds))
        # Settings.data_sink_redis_pool_size is already validated to [1, 128]
        # via field constraints; this is a defense-in-depth lower bound for
        # callers constructing the sink directly (tests, scripts).
        pool_size = max(1, int(pool_size))
        if worker_count is not None and pool_size < worker_count:
            logger.warning(
                "redis_sink_pool_smaller_than_workers",
                pool_size=pool_size,
                worker_count=worker_count,
                msg="Raising pool size to worker_count so workers can't starve on connections.",
            )
            pool_size = int(worker_count)
        self._pool_size = pool_size
        self._redis: Any = None
        self._connected = False
        self._connect_lock = asyncio.Lock()
        self._reconnect_cooldown_until: float = 0.0
        self._backoff_seconds: float = INITIAL_BACKOFF_SECONDS

        self._closed = False

        # Failed event buffer — holds (topic, payload_bytes) for events that
        # exhausted all retries.  Drained automatically on reconnect. The
        # capacity is operator-tunable (data_sink_failed_buffer_capacity): at
        # OPRA quote volume the old fixed 10k filled in under a second during a
        # circuit-breaker-open window, silently evicting the bulk of an outage.
        self._failed_buffer: deque[tuple[str, bytes]] = deque(maxlen=max(1, int(failed_buffer_capacity)))
        self._drain_lock = asyncio.Lock()
        # Hold a strong reference to the drain task so it cannot be GC'd
        # mid-flight while the buffer drain is in progress.
        self._drain_tasks: set[asyncio.Task[None]] = set()
        self._buffer_stats = {"buffered": 0, "drained": 0, "evicted": 0}

    @property
    def name(self) -> str:
        return "redis_streams"

    @property
    def record_publish_metrics(self) -> bool:
        return True

    async def _ensure_connected(self) -> None:
        """Lazy connection to Redis with connection pool.

        Respects a reconnect cooldown set by ``_reset_connection`` to avoid
        tight reconnect-fail loops that saturate Redis with connections.
        Refuses to reconnect after ``close()`` has been called.
        """
        if self._closed:
            return
        if self._redis is not None:
            return

        now = time.monotonic()
        if now < self._reconnect_cooldown_until:
            remaining = self._reconnect_cooldown_until - now
            logger.debug(
                "redis_sink_reconnect_cooldown",
                wait_seconds=round(remaining, 2),
            )
            await asyncio.sleep(remaining)

        is_reconnect = False
        async with self._connect_lock:
            if self._redis is None:
                try:
                    is_reconnect = self._reconnect_cooldown_until > 0
                    self._redis = self._create_client()
                    self._connected = True
                    self._backoff_seconds = INITIAL_BACKOFF_SECONDS
                    logger.info(
                        "redis_sink_connected",
                        url=self._redis_url[:20] + "...",
                        pool_size=self._pool_size,
                        timeout_seconds=self._operation_timeout_seconds,
                        buffered_events=len(self._failed_buffer),
                    )
                except ImportError:
                    logger.error("redis_sink_import_error", msg="redis package not installed")
                    raise

        # After reconnect, drain any buffered events (outside the lock).
        # Hold a strong reference so the task cannot be GC'd mid-drain —
        # asyncio keeps only weak refs to tasks, so a discarded task may
        # vanish before completing, silently losing buffered events.
        if is_reconnect and self._failed_buffer:
            task = asyncio.create_task(self._drain_buffer())
            self._drain_tasks.add(task)
            task.add_done_callback(self._drain_tasks.discard)

    @staticmethod
    async def _close_stale_client(client: Any) -> None:
        """Close a stale Redis client and disconnect the underlying pool.

        Calling ``client.close()`` releases the client handle but the
        ``ConnectionPool`` may keep idle TCP sockets open.  Explicitly
        disconnecting the pool ensures every socket is closed immediately.
        """
        try:
            if hasattr(client, "aclose"):
                await client.aclose()
            else:
                await client.close()
        except Exception as e:
            logger.debug("redis_sink_close_client_error", error=str(e))

        try:
            pool = getattr(client, "connection_pool", None)
            if pool is not None:
                await pool.disconnect()
            logger.debug("redis_sink_stale_client_closed")
        except Exception:
            pass

    def _create_client(self) -> Any:
        import redis.asyncio as aioredis

        pool = aioredis.BlockingConnectionPool.from_url(
            self._redis_url,
            max_connections=self._pool_size,
            timeout=self._operation_timeout_seconds,
            decode_responses=False,
            socket_connect_timeout=self._operation_timeout_seconds,
            socket_timeout=self._operation_timeout_seconds,
        )
        return aioredis.Redis(connection_pool=pool)

    @staticmethod
    def _is_redis_loading(error: Exception) -> bool:
        """Check if the error indicates Redis is still loading its dataset."""
        msg = str(error).lower()
        return any(indicator in msg for indicator in _REDIS_LOADING_INDICATORS)

    async def _reset_connection(
        self,
        operation: str,
        error: Exception,
        *,
        failed_client: Any = None,
        log_level: str = "warning",
    ) -> None:
        """Force reconnect on the next call after a transport/protocol failure.

        Awaits closing the old client pool to prevent connection leaks,
        then sets an exponential backoff cooldown to avoid tight
        reconnect-fail loops.

        If ``failed_client`` is provided, the reset is only performed when
        ``self._redis`` is still that exact object.  This prevents multiple
        concurrent chunks from each triggering independent resets when only
        one is needed.

        Acquires ``_connect_lock`` to serialize against ``_ensure_connected``
        and other concurrent ``_reset_connection`` calls, preventing multiple
        independent pool creations which would leak connections.

        Args:
            log_level: Log level for the reset message. Use "debug" for
                intermediate retry resets to reduce noise; "warning" (default)
                for final/standalone resets.
        """
        if failed_client is not None and self._redis is not failed_client:
            return

        async with self._connect_lock:
            # Re-check after acquiring the lock — another coroutine may have
            # already reset a different (or the same) client while we waited.
            if failed_client is not None and self._redis is not failed_client:
                return

            old_client = self._redis
            self._redis = None
            self._connected = False
            if old_client is not None:
                await self._close_stale_client(old_client)

            # Use a longer backoff when Redis is still loading its dataset
            if self._is_redis_loading(error):
                effective_backoff = max(self._backoff_seconds, REDIS_LOADING_BACKOFF_SECONDS)
            else:
                effective_backoff = self._backoff_seconds

            self._reconnect_cooldown_until = time.monotonic() + effective_backoff
            error_desc = str(error) if str(error) else type(error).__name__
            log_fn = logger.debug if log_level == "debug" else logger.warning
            log_fn(
                "redis_sink_connection_reset",
                operation=operation,
                error=error_desc,
                backoff_seconds=round(effective_backoff, 2),
            )
            self._backoff_seconds = min(self._backoff_seconds * 2, MAX_BACKOFF_SECONDS)

    def _buffer_failed_event(self, topic: str, payload: bytes) -> None:
        """Buffer a failed event for retry on reconnect.

        Uses a bounded deque so oldest events are evicted if the buffer fills.
        Updates the Prometheus ``gateway_sink_buffer_size`` gauge on every
        append and increments ``gateway_sink_buffer_evictions_total`` when
        the deque was already at capacity — those metrics drive the
        ``SinkBufferEvictionsActive`` alert that turns silent data loss
        into a paging signal (RCA: 2026-05-05 32-hour outage).
        """
        was_full = len(self._failed_buffer) == (self._failed_buffer.maxlen or FAILED_EVENT_BUFFER_CAPACITY)
        self._failed_buffer.append((topic, payload))
        self._buffer_stats["buffered"] += 1
        set_sink_buffer_size(self.name, len(self._failed_buffer))
        if was_full:
            self._buffer_stats["evicted"] += 1
            record_sink_buffer_eviction(self.name)
            logger.warning(
                "redis_sink_buffer_eviction",
                buffer_size=len(self._failed_buffer),
                total_evicted=self._buffer_stats["evicted"],
            )

    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> None:
        """Buffer an event from the registry when circuit breaker is OPEN.

        Serializes the data and delegates to _buffer_failed_event so it
        participates in the same drain logic on reconnect.
        """
        if isinstance(data, str):
            payload = data.encode()
        elif isinstance(data, bytes):
            payload = data
        else:
            payload = orjson.dumps(data, default=str)
        self._buffer_failed_event(topic, payload)

    async def _drain_buffer(self) -> None:
        """Drain buffered failed events after a successful reconnect.

        Called from _ensure_connected after re-establishing a connection.
        Uses a pipeline to send all buffered events efficiently.
        Events that fail during drain are re-buffered (up to one attempt).
        """
        if not self._failed_buffer:
            return

        async with self._drain_lock:
            await self._do_drain()

    async def _do_drain(self) -> None:
        """Execute the actual drain.  Separated for lock clarity."""
        if not self._failed_buffer:
            return

        client = self._redis
        if client is None:
            return

        # Snapshot and clear — events that fail drain get re-appended
        events = list(self._failed_buffer)
        self._failed_buffer.clear()

        logger.info(
            "redis_sink_buffer_drain_start",
            count=len(events),
        )

        drained = 0
        re_buffer: list[tuple[str, bytes]] = []

        # Drain in pipeline chunks for efficiency
        for i in range(0, len(events), BATCH_CHUNK_SIZE):
            chunk = events[i : i + BATCH_CHUNK_SIZE]
            try:
                async with client.pipeline(transaction=False) as pipe:
                    for topic, payload_bytes in chunk:
                        pipe.xadd(
                            topic,
                            {b"data": payload_bytes},
                            maxlen=self._max_len,
                            approximate=self._approximate,
                        )
                    timeout = self._operation_timeout_seconds + (len(chunk) / 500) * 0.5
                    results = await asyncio.wait_for(pipe.execute(), timeout=timeout)

                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        re_buffer.append(chunk[j])
                    else:
                        drained += 1
            except Exception as e:
                error_desc = str(e) if str(e) else type(e).__name__
                logger.warning(
                    "redis_sink_buffer_drain_chunk_error",
                    chunk_size=len(chunk),
                    error=error_desc,
                )
                re_buffer.extend(chunk)

        # Re-buffer events that failed during drain
        for item in re_buffer:
            self._failed_buffer.append(item)

        self._buffer_stats["drained"] += drained
        set_sink_buffer_size(self.name, len(self._failed_buffer))
        logger.info(
            "redis_sink_buffer_drain_complete",
            drained=drained,
            re_buffered=len(re_buffer),
            buffer_remaining=len(self._failed_buffer),
        )

    def get_buffer_stats(self) -> dict[str, int]:
        """Return failed event buffer statistics."""
        return {
            **self._buffer_stats,
            "buffer_size": len(self._failed_buffer),
            "buffer_capacity": self._failed_buffer.maxlen or FAILED_EVENT_BUFFER_CAPACITY,
        }

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Publish data to a Redis Stream with automatic retry.

        Retries up to PUBLISH_RETRY_ATTEMPTS times with exponential backoff
        before declaring failure.  This absorbs transient connection blips
        and prevents them from cascading into circuit breaker trips.

        Args:
            topic: Stream name (e.g., 'gateway.stream.bars')
            data: Message payload (will be JSON serialized)

        Returns:
            True if successful
        """
        if self._closed:
            return False
        if isinstance(data, str):
            payload = {b"data": data.encode()}
        elif isinstance(data, bytes):
            payload = {b"data": data}
        else:
            payload = {b"data": orjson.dumps(data, default=str)}

        last_error: Exception | None = None

        for attempt in range(PUBLISH_RETRY_ATTEMPTS):
            await self._ensure_connected()
            client = self._redis
            if client is None:
                # Could not reconnect — count as a failed attempt
                last_error = last_error or ConnectionError("redis_not_connected")
                if attempt < PUBLISH_RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(PUBLISH_RETRY_BACKOFF_BASE * (2**attempt))
                continue

            try:
                message_id = await asyncio.wait_for(
                    client.xadd(
                        topic,
                        payload,
                        maxlen=self._max_len,
                        approximate=self._approximate,
                    ),
                    timeout=self._operation_timeout_seconds,
                )

                if attempt > 0:
                    logger.info(
                        "redis_sink_publish_retry_success",
                        topic=topic,
                        attempt=attempt + 1,
                    )

                logger.debug(
                    "redis_sink_published",
                    topic=topic,
                    message_id=message_id.decode() if isinstance(message_id, bytes) else str(message_id),
                    event_id=data.get("event_id", "unknown") if isinstance(data, dict) else "unknown",
                )

                record_sink_publish(sink=self.name, topic=topic, success=True)
                return True

            # NB: do NOT catch asyncio.CancelledError here. The registry
            # worker loop (gateway/core/data_sink.py) catches CancelledError
            # at the publish-call boundary and routes the in-flight event
            # through sink.buffer_event. Catching+buffering here too would
            # double-buffer the same event into _failed_buffer and replay
            # it twice on next drain.
            except Exception as e:
                last_error = e
                is_last_attempt = attempt >= PUBLISH_RETRY_ATTEMPTS - 1
                # Log intermediate retry resets at DEBUG to reduce noise;
                # the final WARNING is emitted below after all retries exhaust.
                await self._reset_connection(
                    operation="publish",
                    error=e,
                    failed_client=client,
                    log_level="warning" if is_last_attempt else "debug",
                )

                if not is_last_attempt:
                    # Use a longer backoff when Redis is loading
                    if self._is_redis_loading(e):
                        backoff = REDIS_LOADING_BACKOFF_SECONDS
                    else:
                        backoff = PUBLISH_RETRY_BACKOFF_BASE * (2**attempt)
                    error_desc = str(e) if str(e) else type(e).__name__
                    logger.debug(
                        "redis_sink_publish_retrying",
                        topic=topic,
                        attempt=attempt + 1,
                        max_attempts=PUBLISH_RETRY_ATTEMPTS,
                        backoff_seconds=round(backoff, 3),
                        error=error_desc,
                    )
                    # Backoff sleep — CancelledError propagates up to the
                    # registry worker which handles the buffer routing (see
                    # comment above the outer try block).
                    await asyncio.sleep(backoff)

        # All retries exhausted — buffer the event for drain on reconnect
        error_desc = (
            str(last_error)
            if last_error and str(last_error)
            else (type(last_error).__name__ if last_error else "unknown")
        )
        self._buffer_failed_event(topic, payload[b"data"])
        logger.warning(
            "redis_sink_publish_error",
            topic=topic,
            error=error_desc,
            attempts=PUBLISH_RETRY_ATTEMPTS,
            buffered=True,
            buffer_size=len(self._failed_buffer),
        )
        record_sink_publish(sink=self.name, topic=topic, success=False)
        return False

    async def publish_batch(
        self,
        messages: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> int:
        """Publish multiple messages via concurrent chunked Redis pipelines.

        Large batches are split into chunks of BATCH_CHUNK_SIZE and processed
        concurrently (up to MAX_CONCURRENT_CHUNKS at once) for higher throughput.

        Args:
            messages: List of (topic, data) tuples to publish.

        Returns:
            Number of successfully published messages.

        Notes:
            The aggregate count cannot identify *which* messages succeeded
            under partial-pipeline failure. New callers should use
            :meth:`publish_batch_results`, which returns ``list[bool]``
            aligned 1:1 with the input.
        """
        results = await self.publish_batch_results(messages)
        return sum(1 for ok in results if ok)

    async def publish_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> list[bool]:
        """Publish messages and return per-message success flags.

        Each entry in the returned list is aligned with ``messages`` and is
        ``True`` if the matching XADD pipeline reply was a non-exception
        stream id, ``False`` otherwise. This lets callers tell exactly which
        messages landed when a ``transaction=False`` pipeline fails at
        arbitrary indices.

        Args:
            messages: List of (topic, data) tuples to publish.

        Returns:
            ``list[bool]`` of length ``len(messages)``.
        """
        if not messages:
            return []

        await self._ensure_connected()

        chunks = [messages[i : i + BATCH_CHUNK_SIZE] for i in range(0, len(messages), BATCH_CHUNK_SIZE)]

        sem = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)

        async def _process_chunk_with_retry(chunk: list) -> list[bool]:
            async with sem:
                chunk_results = await self._publish_chunk(chunk)
                if not any(chunk_results) and len(chunk) > 0:
                    # Retry once on total failure
                    await self._ensure_connected()
                    chunk_results = await self._publish_chunk(chunk)
                    if any(chunk_results):
                        logger.info(
                            "redis_sink_chunk_retry_success",
                            chunk_size=len(chunk),
                            published=sum(1 for ok in chunk_results if ok),
                        )
                return chunk_results

        chunk_outcomes = await asyncio.gather(
            *[_process_chunk_with_retry(chunk) for chunk in chunks],
            return_exceptions=True,
        )

        results: list[bool] = []
        for i, outcome in enumerate(chunk_outcomes):
            # gather(return_exceptions=True) can yield CancelledError, a
            # BaseException (not Exception); treat any BaseException as a
            # failed chunk rather than trying to iterate it as list[bool].
            if isinstance(outcome, BaseException):
                logger.warning(
                    "redis_sink_chunk_exception",
                    chunk_index=i,
                    error=str(outcome) or type(outcome).__name__,
                )
                for topic, _ in chunks[i]:
                    record_sink_publish(sink=self.name, topic=topic, success=False)
                results.extend([False] * len(chunks[i]))
            else:
                # outcome is list[bool] aligned with chunks[i]
                results.extend(outcome)

        return results

    async def publish_batch_indexed(
        self,
        messages: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> set[int]:
        """Publish multiple messages and return the EXACT succeeded indices.

        Identical wire behavior to ``publish_batch`` but reports *which*
        messages landed (by position in ``messages``) rather than only a count.
        On a partial Redis failure (e.g. item 0 fails, item 1 succeeds) the
        returned set is ``{1}`` — never ``{0}`` ("first N"). Callers that must
        preserve a per-event contract (flow WS fan-out parity with the Redis
        stream) use this so they fan out exactly the events Heber received.
        """
        if not messages:
            return set()

        await self._ensure_connected()

        chunks = [messages[i : i + BATCH_CHUNK_SIZE] for i in range(0, len(messages), BATCH_CHUNK_SIZE)]
        chunk_offsets = list(range(0, len(messages), BATCH_CHUNK_SIZE))

        sem = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)

        async def _process_chunk_with_retry(chunk: list) -> set[int]:
            async with sem:
                succeeded = await self._publish_chunk_indexed(chunk)
                if not succeeded and len(chunk) > 0:
                    # Retry once on total failure (mirror publish_batch).
                    await self._ensure_connected()
                    succeeded = await self._publish_chunk_indexed(chunk)
                    if succeeded:
                        logger.info(
                            "redis_sink_chunk_retry_success",
                            chunk_size=len(chunk),
                            published=len(succeeded),
                        )
                return succeeded

        results = await asyncio.gather(
            *[_process_chunk_with_retry(chunk) for chunk in chunks],
            return_exceptions=True,
        )

        published_indices: set[int] = set()
        for chunk_idx, result in enumerate(results):
            offset = chunk_offsets[chunk_idx]
            if isinstance(result, BaseException):
                logger.warning(
                    "redis_sink_chunk_exception",
                    chunk_index=chunk_idx,
                    error=str(result) or type(result).__name__,
                )
                for topic, _ in chunks[chunk_idx]:
                    record_sink_publish(sink=self.name, topic=topic, success=False)
            else:
                for local_idx in result:
                    published_indices.add(offset + local_idx)

        return published_indices

    async def _publish_chunk(
        self,
        chunk: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> list[bool]:
        """Execute a single pipeline chunk.

        Returns:
            ``list[bool]`` aligned with ``chunk``: ``True`` per message whose
            XADD reply was not an exception. A pipeline-level failure (e.g.
            connection reset before ``pipe.execute`` returns) marks all
            messages in the chunk as failed.
        """
        client = self._redis
        if client is None:
            for topic, _ in chunk:
                record_sink_publish(sink=self.name, topic=topic, success=False)
            return [False] * len(chunk)

        try:
            async with client.pipeline(transaction=False) as pipe:
                for topic, data in chunk:
                    if isinstance(data, str):
                        payload = {b"data": data.encode()}
                    elif isinstance(data, bytes):
                        payload = {b"data": data}
                    else:
                        payload = {b"data": orjson.dumps(data, default=str)}
                    pipe.xadd(
                        topic,
                        payload,
                        maxlen=self._max_len,
                        approximate=self._approximate,
                    )

                # Timeout scales with chunk size: base + proportional
                timeout = self._operation_timeout_seconds + (len(chunk) / 500) * 0.5
                results = await asyncio.wait_for(pipe.execute(), timeout=timeout)

            outcomes: list[bool] = []
            for i, result in enumerate(results):
                topic = chunk[i][0]
                if isinstance(result, Exception):
                    logger.warning(
                        "redis_sink_batch_item_error",
                        topic=topic,
                        error=str(result),
                    )
                    record_sink_publish(sink=self.name, topic=topic, success=False)
                    outcomes.append(False)
                else:
                    record_sink_publish(sink=self.name, topic=topic, success=True)
                    outcomes.append(True)

            logger.debug(
                "redis_sink_chunk_published",
                chunk_size=len(chunk),
                published=sum(outcomes),
            )
            return outcomes

        except Exception as e:
            error_msg = str(e) if str(e) else type(e).__name__
            await self._reset_connection(
                operation="publish_batch",
                error=e,
                failed_client=client,
            )
            logger.warning(
                "redis_sink_batch_error",
                count=len(chunk),
                error=error_msg,
            )
            for topic, _ in chunk:
                record_sink_publish(sink=self.name, topic=topic, success=False)
            return [False] * len(chunk)

    async def _publish_chunk_indexed(
        self,
        chunk: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> set[int]:
        """Execute a single pipeline chunk. Returns succeeded local indices.

        Thin adapter over :meth:`_publish_chunk` so the indexed batch API
        (flow fan-out parity) shares a single publish implementation.
        """
        return {i for i, ok in enumerate(await self._publish_chunk(chunk)) if ok}

    async def health_check(self) -> bool:
        """Check Redis connection health."""
        client = None
        try:
            await self._ensure_connected()
            client = self._redis
            if client is None:
                return False
            await asyncio.wait_for(client.ping(), timeout=self._operation_timeout_seconds)
            return True
        except Exception as e:
            await self._reset_connection(
                operation="health_check",
                error=e,
                failed_client=client,
            )
            return False

    async def close(self) -> None:
        """Close Redis connection pool.

        Sets ``_closed`` so that no further operations attempt to reconnect,
        which would fail with "Event loop is closed" during shutdown.
        Awaits any in-flight drain tasks (with a short timeout) so they don't
        keep running against a torn-down client/pool, then explicitly
        disconnects the underlying connection pool to release all TCP sockets
        immediately.

        Before closing, makes one final attempt to drain ``_failed_buffer``
        through the live connection so buffered events aren't lost on graceful
        shutdown. Events still in the buffer after this attempt are logged so
        operators have a count for post-mortem.
        """
        # Final attempt to drain the failed-event buffer through the live
        # connection. We do this BEFORE setting _closed so the drain path
        # can publish, but AFTER any in-flight tasks would have finished.
        # If Redis is unreachable at this point, the buffer simply remains
        # in memory and we log the count.
        if self._failed_buffer and self._redis is not None:
            try:
                await asyncio.wait_for(self._do_drain(), timeout=5.0)
            except (TimeoutError, Exception) as exc:  # noqa: BLE001 — best-effort
                logger.warning(
                    "redis_sink_close_drain_failed",
                    remaining=len(self._failed_buffer),
                    error=str(exc),
                )

        if self._failed_buffer:
            logger.warning(
                "redis_sink_close_buffer_nonempty",
                lost_events=len(self._failed_buffer),
                msg="Events were buffered when shutdown completed; they are NOT persisted to disk and are lost.",
            )

        self._closed = True

        # Drain background tasks first so they don't try to publish through
        # the client we're about to close. _drain_buffer holds _drain_lock
        # while running; cancel any that are still pending and give them a
        # brief window to finish gracefully (most chunks are sub-second).
        pending = list(self._drain_tasks)
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=2.0,
                )
            except TimeoutError:
                logger.warning(
                    "redis_sink_drain_close_timeout",
                    pending=len(pending),
                )
                for task in pending:
                    if not task.done():
                        task.cancel()
                # Best-effort wait for cancellations to settle
                await asyncio.gather(*pending, return_exceptions=True)
            self._drain_tasks.clear()

        client = self._redis
        self._redis = None
        self._connected = False
        if client is not None:
            await self._close_stale_client(client)
            logger.info("redis_sink_closed")


class LogSink(DataSink):
    """Simple logging sink for development/debugging.

    Logs all published messages at DEBUG level.
    """

    @property
    def name(self) -> str:
        return "log"

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Log the message."""
        if isinstance(data, dict):
            keys = list(data.keys())
        else:
            keys = ["<serialized>"]
        logger.debug("data_sink_log", topic=topic, data_keys=keys)
        return True

    async def health_check(self) -> bool:
        return True
