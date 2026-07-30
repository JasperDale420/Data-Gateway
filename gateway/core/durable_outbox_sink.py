"""Durable producer admission with one ordered broker drain."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from gateway.core.data_sink import DataSink
from gateway.core.durable_outbox import OutboxEntry, SQLiteOutbox
from gateway.core.logger import logger
from gateway.core.metrics import set_durable_outbox_utilization

OutboxPublisher = Callable[[OutboxEntry], Awaitable[bool]]


class DurableOutboxSink(DataSink):
    """Admit to SQLite before asynchronously publishing in FIFO order."""

    def __init__(
        self,
        outbox: SQLiteOutbox,
        publisher: OutboxPublisher,
        *,
        retry_delay_seconds: float = 1.0,
        name: str = "durable_outbox",
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._retry_delay_seconds = max(0.01, retry_delay_seconds)
        self._name = name
        self._storage_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._drain_task: asyncio.Task[None] | None = None
        self._closed = False
        self._admission_error: str | None = None
        self._utilization = outbox.capacity_utilization()

    @property
    def name(self) -> str:
        return self._name

    @property
    def durable_admission(self) -> bool:
        return True

    async def start(self) -> None:
        """Start draining rows left by this or a previous process."""
        async with self._storage_lock:
            self._record_utilization()
            self._start_drain()

    def _start_drain(self) -> None:
        if self._closed:
            raise RuntimeError("durable outbox sink is closed")
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(
                self._drain(),
                name=f"durable_outbox_drain:{self.name}",
            )
        self._wake.set()

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Durably admit one event without blocking the event loop."""
        async with self._storage_lock:
            if self._closed:
                raise RuntimeError("durable outbox sink is closed")
            self._raise_if_admission_failed()
            try:
                await asyncio.to_thread(self._outbox.admit, topic, data)
            except Exception as exc:
                self._admission_error = f"{type(exc).__name__}: {exc}"
                raise
            self._record_utilization()
            self._start_drain()
        return True

    async def publish_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[bool]:
        """Durably admit a batch in one SQLite transaction."""
        if not messages:
            return []
        async with self._storage_lock:
            if self._closed:
                raise RuntimeError("durable outbox sink is closed")
            self._raise_if_admission_failed()
            try:
                await asyncio.to_thread(self._outbox.admit_many, messages)
            except Exception as exc:
                self._admission_error = f"{type(exc).__name__}: {exc}"
                raise
            self._record_utilization()
            self._start_drain()
        return [True] * len(messages)

    async def publish_batch_indexed(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> set[int]:
        """Durably admit a batch and return every accepted input index."""
        results = await self.publish_batch_results(messages)
        return {index for index, accepted in enumerate(results) if accepted}

    async def publish_batch(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> int:
        """Durably admit a batch and return its accepted count."""
        return sum(await self.publish_batch_results(messages))

    async def health_check(self) -> bool:
        return not self._closed and self._admission_error is None and await asyncio.to_thread(self._outbox.has_capacity)

    def can_accept_low_priority(self, *, max_utilization: float) -> bool:
        return self._admission_error is None and self._utilization < max_utilization

    def _raise_if_admission_failed(self) -> None:
        if self._admission_error is not None:
            raise RuntimeError(f"durable outbox admission failed: {self._admission_error}")

    def schedule_drain(self) -> None:
        self._wake.set()

    async def _drain(self) -> None:
        while True:
            self._wake.clear()
            async with self._storage_lock:
                entries = await asyncio.to_thread(self._outbox.pending, limit=1)
            if not entries:
                await self._wake.wait()
                continue

            entry = entries[0]
            error: str | None = None
            try:
                acknowledged = await self._publisher(entry)
                if not acknowledged:
                    error = "publisher returned False"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                acknowledged = False
                error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "durable_outbox_publish_failed",
                    sink=self.name,
                    event_id=entry.event_id,
                    sequence=entry.sequence,
                    error=error,
                    exc_info=True,
                )
            if error is not None and not acknowledged:
                logger.warning(
                    "durable_outbox_event_retained",
                    sink=self.name,
                    event_id=entry.event_id,
                    sequence=entry.sequence,
                    error=error,
                )

            async with self._storage_lock:
                if acknowledged:
                    await asyncio.to_thread(self._outbox.acknowledge, entry.sequence)
                else:
                    await asyncio.to_thread(
                        self._outbox.record_failure,
                        entry.sequence,
                        error or "publisher rejected event",
                    )
                self._record_utilization()
            if not acknowledged:
                await asyncio.sleep(self._retry_delay_seconds)

    async def close(self) -> None:
        """Stop publication and close SQLite after active admission finishes."""
        if self._closed:
            return
        async with self._storage_lock:
            self._closed = True
        if self._drain_task is not None:
            self._drain_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._drain_task
            self._drain_task = None
        async with self._storage_lock:
            await asyncio.to_thread(self._outbox.close)

    def _record_utilization(self) -> None:
        self._utilization = self._outbox.capacity_utilization()
        set_durable_outbox_utilization(self._utilization)


class LaneRoutedSink(DataSink):
    """Route each ingest lane to exactly one configured transport."""

    def __init__(
        self,
        durable: DurableOutboxSink,
        redis: DataSink,
        *,
        lanes: str,
    ) -> None:
        self._durable = durable
        self._redis = redis
        self._durable_lanes = {"live", "backfill"} if lanes == "both" else {lanes}

    @property
    def name(self) -> str:
        if self._durable_lanes == {"live", "backfill"}:
            return self._durable.name
        return self._redis.name

    @property
    def durable_admission(self) -> bool:
        return self._durable_lanes == {"live", "backfill"}

    def is_durable_topic(self, topic: str) -> bool:
        return self._delegate(topic) is self._durable

    def _delegate(self, topic: str) -> DataSink:
        lane = "backfill" if topic == "heber:events:backfill" else "live"
        return self._durable if lane in self._durable_lanes else self._redis

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        return await self._delegate(topic).publish(topic, data)

    async def publish_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[bool]:
        results = [False] * len(messages)
        for delegate in (self._durable, self._redis):
            indexed = [
                (index, message) for index, message in enumerate(messages) if self._delegate(message[0]) is delegate
            ]
            if not indexed:
                continue
            # nosemgrep: empire-no-bare-exception -- transport boundary: one lane failure must not erase exact results from the other lane
            try:
                delegate_results = await delegate.publish_batch_results(  # type: ignore[attr-defined]
                    [message for _, message in indexed]
                )
            except Exception:
                logger.exception(
                    "lane_routed_sink_batch_failed",
                    delegate=delegate.name,
                    count=len(indexed),
                )
                continue
            if len(delegate_results) != len(indexed):
                logger.error(
                    "lane_routed_sink_batch_results_length_mismatch",
                    delegate=delegate.name,
                    expected=len(indexed),
                    actual=len(delegate_results),
                )
                continue
            for (index, _), accepted in zip(indexed, delegate_results, strict=True):
                results[index] = accepted
        return results

    async def publish_batch_indexed(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> set[int]:
        return {index for index, accepted in enumerate(await self.publish_batch_results(messages)) if accepted}

    async def publish_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> int:
        return sum(await self.publish_batch_results(messages))

    async def health_check(self) -> bool:
        durable_healthy = await self._durable.health_check()
        if self._durable_lanes == {"live", "backfill"}:
            return durable_healthy
        return durable_healthy and await self._redis.health_check()

    def can_accept_low_priority(self, *, max_utilization: float) -> bool:
        if "backfill" not in self._durable_lanes:
            return True
        return self._durable.can_accept_low_priority(max_utilization=max_utilization)

    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        return self._delegate(topic).buffer_event(topic, data)

    def schedule_drain(self) -> None:
        self._redis.schedule_drain()

    async def close(self) -> None:
        await asyncio.gather(self._durable.close(), self._redis.close())
