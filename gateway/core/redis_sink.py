"""Redis Streams data sink for publishing to Heber.

This sink publishes Gateway data to Redis Streams, which Heber ingestors
can consume for storage in the Bronze layer.
"""

import json
from typing import Any

import structlog

from gateway.core.data_sink import DataSink

logger = structlog.get_logger()


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
                import redis.asyncio as aioredis

                self._redis = aioredis.from_url(
                    self._redis_url,
                    decode_responses=True,
                )
                self._connected = True
                logger.info("redis_sink_connected", url=self._redis_url[:20] + "...")
            except ImportError:
                logger.error("redis_sink_import_error", msg="redis package not installed")
                raise

    async def publish(self, topic: str, data: dict[str, Any]) -> bool:
        """Publish data to a Redis Stream.

        Args:
            topic: Stream name (e.g., 'gateway.stream.bars')
            data: Message payload (will be JSON serialized)

        Returns:
            True if successful
        """
        await self._ensure_connected()

        try:
            # Serialize payload
            payload = {"data": json.dumps(data, default=str)}

            # Add to stream with automatic trimming
            message_id = await self._redis.xadd(
                topic,
                payload,
                maxlen=self._max_len,
                approximate=self._approximate,
            )

            # Log successful publish at debug level for tracing
            logger.debug(
                "redis_sink_published",
                topic=topic,
                message_id=str(message_id),
                event_id=data.get("event_id", "unknown"),
            )

            # Record metrics
            try:
                from gateway.core.metrics import record_sink_publish

                record_sink_publish(sink=self.name, topic=topic, success=True)
            except ImportError:
                pass

            return True

        except Exception as e:
            logger.warning("redis_sink_publish_error", topic=topic, error=str(e))
            # Record error metrics
            try:
                from gateway.core.metrics import record_sink_publish

                record_sink_publish(sink=self.name, topic=topic, success=False)
            except ImportError:
                pass
            return False

    async def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            await self._ensure_connected()
            await self._redis.ping()
            return True
        except Exception:
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

    async def publish(self, topic: str, data: dict[str, Any]) -> bool:
        """Log the message."""
        logger.debug("data_sink_log", topic=topic, data_keys=list(data.keys()))
        return True

    async def health_check(self) -> bool:
        return True
