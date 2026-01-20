"""Data sink abstraction for publishing data to downstream storage systems.

This module provides a pluggable architecture for publishing Gateway data
to external systems like Redis Streams, Kafka, or files for Heber ingestion.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog

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
    async def publish(self, topic: str, data: dict[str, Any]) -> bool:
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

    async def close(self) -> None:
        """Close the sink connection. Override if cleanup is needed."""
        pass


class DataSinkRegistry:
    """Registry for managing multiple data sinks.

    Publishes to all registered sinks in parallel (fire-and-forget).
    """

    def __init__(self) -> None:
        self._sinks: list[DataSink] = []
        self._enabled = True
        self._background_tasks: set[asyncio.Task] = set()  # Prevent GC

    def register(self, sink: DataSink) -> None:
        """Register a data sink."""
        self._sinks.append(sink)
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

    async def publish_all(self, topic: str, data: dict[str, Any]) -> None:
        """Publish to all registered sinks (non-blocking).

        Uses fire-and-forget pattern to avoid blocking the caller.
        """
        if not self._enabled or not self._sinks:
            return

        for sink in self._sinks:
            task = asyncio.create_task(self._safe_publish(sink, topic, data))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _safe_publish(self, sink: DataSink, topic: str, data: dict[str, Any]) -> None:
        """Publish with error handling."""
        try:
            await sink.publish(topic, data)
        except Exception as e:
            logger.warning(
                "data_sink_publish_failed",
                sink=sink.name,
                topic=topic,
                error=str(e),
            )

    async def health_check_all(self) -> dict[str, bool]:
        """Check health of all sinks."""
        results = {}
        for sink in self._sinks:
            try:
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
