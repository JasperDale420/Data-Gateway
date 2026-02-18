"""Redis Streams data sink for publishing to Heber.

This sink publishes Gateway data to Redis Streams, which Heber ingestors
can consume for storage in the Bronze layer.

Uses a connection pool for concurrent pipeline execution and processes
batch chunks in parallel with retry logic.
"""

import asyncio
from typing import Any

import orjson
import structlog

from gateway.core.data_sink import DataSink
from gateway.core.metrics import record_sink_publish

logger = structlog.get_logger()

DEFAULT_OPERATION_TIMEOUT_SECONDS = 5.0
DEFAULT_POOL_SIZE = 8
BATCH_CHUNK_SIZE = 2_000
MAX_CONCURRENT_CHUNKS = 4
CHUNK_RETRY_ATTEMPTS = 1


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
    ) -> None:
        """Initialize Redis Streams sink.

        Args:
            redis_url: Redis connection URL
            max_len: Maximum stream length (oldest entries trimmed)
            approximate_trim: Use ~ for more efficient trimming
            operation_timeout_seconds: Timeout for Redis operations
            pool_size: Max connections in the Redis connection pool
        """
        self._redis_url = redis_url
        self._max_len = max_len
        self._approximate = approximate_trim
        self._operation_timeout_seconds = max(0.5, float(operation_timeout_seconds))
        self._pool_size = max(1, min(32, int(pool_size)))
        self._redis: Any = None
        self._connected = False
        self._connect_lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return "redis_streams"

    @property
    def record_publish_metrics(self) -> bool:
        return True

    async def _ensure_connected(self) -> None:
        """Lazy connection to Redis with connection pool."""
        if self._redis is not None:
            return

        async with self._connect_lock:
            if self._redis is None:
                try:
                    self._redis = self._create_client()
                    self._connected = True
                    logger.info(
                        "redis_sink_connected",
                        url=self._redis_url[:20] + "...",
                        pool_size=self._pool_size,
                        timeout_seconds=self._operation_timeout_seconds,
                    )
                except ImportError:
                    logger.error("redis_sink_import_error", msg="redis package not installed")
                    raise

    @staticmethod
    async def _close_stale_client(client: Any) -> None:
        """Close a stale Redis client to release pooled connections."""
        try:
            await client.close()
            logger.debug("redis_sink_stale_client_closed")
        except Exception:
            pass

    def _create_client(self) -> Any:
        import redis.asyncio as aioredis

        pool = aioredis.ConnectionPool.from_url(
            self._redis_url,
            max_connections=self._pool_size,
            decode_responses=False,
            socket_connect_timeout=self._operation_timeout_seconds,
            socket_timeout=self._operation_timeout_seconds,
        )
        return aioredis.Redis(connection_pool=pool)

    def _reset_connection(self, operation: str, error: Exception) -> None:
        """Force reconnect on the next call after a transport/protocol failure.

        Closes the old client pool to prevent connection leaks before
        discarding the reference.
        """
        old_client = self._redis
        self._redis = None
        self._connected = False
        if old_client is not None:
            try:
                asyncio.get_event_loop().create_task(self._close_stale_client(old_client))
            except RuntimeError:
                pass
        logger.warning(
            "redis_sink_connection_reset",
            operation=operation,
            error=str(error) or type(error).__name__,
        )

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Publish data to a Redis Stream.

        Args:
            topic: Stream name (e.g., 'gateway.stream.bars')
            data: Message payload (will be JSON serialized)

        Returns:
            True if successful
        """
        await self._ensure_connected()

        try:
            if isinstance(data, str):
                payload = {b"data": data.encode()}
            elif isinstance(data, bytes):
                payload = {b"data": data}
            else:
                payload = {b"data": orjson.dumps(data, default=str)}

            message_id = await asyncio.wait_for(
                self._redis.xadd(
                    topic,
                    payload,
                    maxlen=self._max_len,
                    approximate=self._approximate,
                ),
                timeout=self._operation_timeout_seconds,
            )

            logger.debug(
                "redis_sink_published",
                topic=topic,
                message_id=message_id.decode() if isinstance(message_id, bytes) else str(message_id),
                event_id=data.get("event_id", "unknown") if isinstance(data, dict) else "unknown",
            )

            record_sink_publish(sink=self.name, topic=topic, success=True)
            return True

        except Exception as e:
            self._reset_connection(operation="publish", error=e)
            logger.warning("redis_sink_publish_error", topic=topic, error=str(e))
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
        """
        if not messages:
            return 0

        await self._ensure_connected()

        chunks = [messages[i : i + BATCH_CHUNK_SIZE] for i in range(0, len(messages), BATCH_CHUNK_SIZE)]

        sem = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)

        async def _process_chunk_with_retry(chunk: list) -> int:
            async with sem:
                published = await self._publish_chunk(chunk)
                if published == 0 and len(chunk) > 0:
                    # Retry once on total failure
                    await self._ensure_connected()
                    published = await self._publish_chunk(chunk)
                    if published > 0:
                        logger.info(
                            "redis_sink_chunk_retry_success",
                            chunk_size=len(chunk),
                            published=published,
                        )
                return published

        results = await asyncio.gather(
            *[_process_chunk_with_retry(chunk) for chunk in chunks],
            return_exceptions=True,
        )

        total_published = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(
                    "redis_sink_chunk_exception",
                    chunk_index=i,
                    error=str(result),
                )
                for topic, _ in chunks[i]:
                    record_sink_publish(sink=self.name, topic=topic, success=False)
            else:
                total_published += result

        return total_published

    async def _publish_chunk(
        self,
        chunk: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> int:
        """Execute a single pipeline chunk. Returns number published."""
        try:
            pipe = self._redis.pipeline(transaction=False)
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

            published = 0
            for i, result in enumerate(results):
                topic = chunk[i][0]
                if isinstance(result, Exception):
                    logger.warning(
                        "redis_sink_batch_item_error",
                        topic=topic,
                        error=str(result),
                    )
                    record_sink_publish(sink=self.name, topic=topic, success=False)
                else:
                    published += 1
                    record_sink_publish(sink=self.name, topic=topic, success=True)

            logger.debug(
                "redis_sink_chunk_published",
                chunk_size=len(chunk),
                published=published,
            )
            return published

        except Exception as e:
            error_msg = str(e) or type(e).__name__
            self._reset_connection(operation="publish_batch", error=e)
            logger.warning(
                "redis_sink_batch_error",
                count=len(chunk),
                error=error_msg,
            )
            for topic, _ in chunk:
                record_sink_publish(sink=self.name, topic=topic, success=False)
            return 0

    async def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            await self._ensure_connected()
            await asyncio.wait_for(self._redis.ping(), timeout=self._operation_timeout_seconds)
            return True
        except Exception as e:
            self._reset_connection(operation="health_check", error=e)
            return False

    async def close(self) -> None:
        """Close Redis connection pool."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            self._connected = False
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
