"""Redis Streams data sink for publishing to Heber.

This sink publishes Gateway data to Redis Streams, which Heber ingestors
can consume for storage in the Bronze layer.
"""

import asyncio
from typing import Any

import orjson
import structlog

from gateway.core.data_sink import DataSink
from gateway.core.metrics import record_sink_publish

logger = structlog.get_logger()

DEFAULT_OPERATION_TIMEOUT_SECONDS = 1.0


class RedisStreamsSink(DataSink):
    """Redis Streams implementation of DataSink.

    Publishes messages to Redis Streams with automatic trimming
    to prevent unbounded growth.
    """

    def __init__(
        self,
        redis_url: str,
        max_len: int = 100_000,
        approximate_trim: bool = True,
        operation_timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize Redis Streams sink.

        Args:
            redis_url: Redis connection URL
            max_len: Maximum stream length (oldest entries trimmed)
            approximate_trim: Use ~ for more efficient trimming
        """
        self._redis_url = redis_url
        self._max_len = max_len
        self._approximate = approximate_trim
        self._operation_timeout_seconds = max(0.1, float(operation_timeout_seconds))
        self._redis: Any = None
        self._connected = False

    @property
    def name(self) -> str:
        return "redis_streams"

    @property
    def record_publish_metrics(self) -> bool:
        return True

    async def _ensure_connected(self) -> None:
        """Lazy connection to Redis."""
        if self._redis is None:
            try:
                self._redis = self._create_client()
                self._connected = True
                logger.info("redis_sink_connected", url=self._redis_url[:20] + "...")
            except ImportError:
                logger.error("redis_sink_import_error", msg="redis package not installed")
                raise

    def _create_client(self) -> Any:
        import redis.asyncio as aioredis

        return aioredis.from_url(
            self._redis_url,
            decode_responses=True,
            socket_connect_timeout=self._operation_timeout_seconds,
            socket_timeout=self._operation_timeout_seconds,
        )

    def _reset_connection(self, operation: str, error: Exception) -> None:
        """Force reconnect on the next call after a transport/protocol failure."""
        self._redis = None
        self._connected = False
        logger.warning(
            "redis_sink_connection_reset",
            operation=operation,
            error=str(error),
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
            # Serialize payload if not already serialized
            if isinstance(data, str | bytes):
                payload = {"data": data}
            else:
                # orjson returns bytes, which Redis handles natively
                payload = {"data": orjson.dumps(data, default=str)}

            # Add to stream with automatic trimming
            message_id = await asyncio.wait_for(
                self._redis.xadd(
                    topic,
                    payload,
                    maxlen=self._max_len,
                    approximate=self._approximate,
                ),
                timeout=self._operation_timeout_seconds,
            )

            # Log successful publish at debug level for tracing
            logger.debug(
                "redis_sink_published",
                topic=topic,
                message_id=str(message_id),
                event_id=data.get("event_id", "unknown") if isinstance(data, dict) else "unknown",
            )

            # Record metrics
            record_sink_publish(sink=self.name, topic=topic, success=True)

            return True

        except Exception as e:
            self._reset_connection(operation="publish", error=e)
            logger.warning("redis_sink_publish_error", topic=topic, error=str(e))
            # Record error metrics
            record_sink_publish(sink=self.name, topic=topic, success=False)
            return False

    async def publish_batch(
        self,
        messages: list[tuple[str, dict[str, Any] | str | bytes]],
    ) -> int:
        """Publish multiple messages via a Redis pipeline (single round trip).

        Args:
            messages: List of (topic, data) tuples to publish.

        Returns:
            Number of successfully published messages.
        """
        if not messages:
            return 0

        await self._ensure_connected()

        try:
            pipe = self._redis.pipeline(transaction=False)
            for topic, data in messages:
                if isinstance(data, str | bytes):
                    payload = {"data": data}
                else:
                    payload = {"data": orjson.dumps(data, default=str)}
                pipe.xadd(
                    topic,
                    payload,
                    maxlen=self._max_len,
                    approximate=self._approximate,
                )

            results = await asyncio.wait_for(
                pipe.execute(),
                timeout=self._operation_timeout_seconds * 2,
            )

            published = 0
            for i, result in enumerate(results):
                topic = messages[i][0]
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
                "redis_sink_batch_published",
                total=len(messages),
                published=published,
            )
            return published

        except Exception as e:
            self._reset_connection(operation="publish_batch", error=e)
            logger.warning(
                "redis_sink_batch_error",
                count=len(messages),
                error=str(e),
            )
            for topic, _ in messages:
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
        """Close Redis connection."""
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
