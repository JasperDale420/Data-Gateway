"""Durable producer admission with one ordered broker drain."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, cast

from gateway.core.data_sink import DataSink
from gateway.core.durable_outbox import OutboxCapacityError, OutboxEntry, SQLiteOutbox
from gateway.core.logger import logger
from gateway.core.metrics import (
    record_durable_outbox_retained,
    set_durable_outbox_delivery_health,
    set_durable_outbox_summary,
    set_durable_outbox_utilization,
)

OutboxPublisher = Callable[[OutboxEntry], Awaitable[bool]]
_MAX_ADMISSION_TRANSACTION_EVENTS = 256
_MAX_ADMISSION_TRANSACTION_BYTES = 16 * 1024**2
_MAX_ADMISSION_GROUP_WAIT_SECONDS = 0.005
_MAX_ADMISSION_QUEUE_EVENTS = 8_192
_MAX_ADMISSION_QUEUE_BYTES = _MAX_ADMISSION_TRANSACTION_BYTES
_MAX_CONCURRENT_PUBACKS = 32
_OUTBOX_METRICS_REFRESH_SECONDS = 5.0
_DRAIN_SHUTDOWN_TIMEOUT_SECONDS = 20.0


@dataclass(slots=True)
class _AdmissionRequest:
    topic: str
    data: dict[str, Any] | str | bytes
    size_bytes: int
    result: asyncio.Future[bool]


class DurableOutboxSink(DataSink):
    """Admit to SQLite before asynchronously publishing in FIFO order."""

    def __init__(
        self,
        outbox: SQLiteOutbox,
        publisher: OutboxPublisher,
        *,
        retry_delay_seconds: float = 1.0,
        name: str = "durable_outbox",
        admission_queue_max_events: int = _MAX_ADMISSION_QUEUE_EVENTS,
        admission_queue_max_bytes: int = _MAX_ADMISSION_QUEUE_BYTES,
        drain_shutdown_timeout_seconds: float = _DRAIN_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        if admission_queue_max_events < 1:
            raise ValueError("admission queue event limit must be positive")
        if admission_queue_max_bytes < 1:
            raise ValueError("admission queue byte limit must be positive")
        self._outbox = outbox
        self._publisher = publisher
        self._retry_delay_seconds = max(0.01, retry_delay_seconds)
        self._drain_shutdown_timeout_seconds = max(0.01, drain_shutdown_timeout_seconds)
        self._name = name
        self._storage_lock = asyncio.Lock()
        self._admission_requests: deque[_AdmissionRequest] = deque()
        self._admission_queued_bytes = 0
        self._admission_queue_max_events = admission_queue_max_events
        self._admission_queue_max_bytes = admission_queue_max_bytes
        self._admission_task: asyncio.Task[None] | None = None
        self._metrics_task: asyncio.Task[None] | None = None
        self._wake = {lane: asyncio.Event() for lane in ("live", "backfill", "watch")}
        # Set whenever a lane holds no publisher acknowledgement that isn't yet
        # durably settled in SQLite; cleared the instant one arrives. close()
        # waits on this before cancelling a drain task, so an acknowledgement
        # already given by the publisher can never be abandoned mid-settle.
        # It does not gate on "a publish is in flight" (a stuck publisher that
        # never returns must still cancel promptly on shutdown).
        self._settle_safe = {lane: asyncio.Event() for lane in ("live", "backfill", "watch")}
        for event in self._settle_safe.values():
            event.set()
        self._drain_tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False
        self._admission_error: str | None = None
        self._delivery_errors: dict[str, str] = {}
        self._failed_delivery_sequences = {lane: set[int]() for lane in ("live", "backfill", "watch")}
        self._puback_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_PUBACKS)
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
            await asyncio.to_thread(self._refresh_outbox_metrics)
            self._start_drain()
            if self._metrics_task is None or self._metrics_task.done():
                self._metrics_task = asyncio.create_task(
                    self._refresh_outbox_metrics_periodically(),
                    name=f"durable_outbox_metrics:{self.name}",
                )

    @staticmethod
    def _lane(topic: str) -> str:
        if topic == "heber:events:backfill":
            return "backfill"
        if topic == "heber:watch":
            return "watch"
        return "live"

    def _start_drain(self, *lanes: str) -> None:
        if self._closed:
            raise RuntimeError("durable outbox sink is closed")
        for lane in lanes or ("live", "backfill", "watch"):
            task = self._drain_tasks.get(lane)
            if task is None or task.done():
                self._drain_tasks[lane] = asyncio.create_task(
                    self._drain(lane),
                    name=f"durable_outbox_drain:{self.name}:{lane}",
                )
            self._wake[lane].set()

    @staticmethod
    def _admission_size(data: dict[str, Any] | str | bytes) -> int:
        """Return the same conservative byte budget SQLite uses per row."""
        return len(SQLiteOutbox._serialize(data)) + 512

    @classmethod
    def _validate_transaction(cls, messages: list[tuple[str, dict[str, Any]]]) -> None:
        """Reject a batch that cannot be one bounded all-or-none SQLite transaction."""
        if len(messages) > _MAX_ADMISSION_TRANSACTION_EVENTS:
            raise OutboxCapacityError("durable outbox admission transaction event limit reached")
        if sum(cls._admission_size(data) for _topic, data in messages) > _MAX_ADMISSION_TRANSACTION_BYTES:
            raise OutboxCapacityError("durable outbox admission transaction byte limit reached")

    @classmethod
    def admission_batches(
        cls,
        messages: list[tuple[str, dict[str, Any]]],
        *,
        copies_per_message: int = 1,
    ) -> list[list[tuple[str, dict[str, Any]]]]:
        """Partition caller batches into transactions without splitting an input record's copies."""
        if copies_per_message < 1:
            raise ValueError("copies per message must be positive")
        batches: list[list[tuple[str, dict[str, Any]]]] = []
        batch: list[tuple[str, dict[str, Any]]] = []
        batch_events = 0
        batch_bytes = 0
        for message in messages:
            message_bytes = cls._admission_size(message[1]) * copies_per_message
            if (
                copies_per_message > _MAX_ADMISSION_TRANSACTION_EVENTS
                or message_bytes > _MAX_ADMISSION_TRANSACTION_BYTES
            ):
                raise OutboxCapacityError("durable outbox admission transaction byte limit reached")
            if batch and (
                batch_events + copies_per_message > _MAX_ADMISSION_TRANSACTION_EVENTS
                or batch_bytes + message_bytes > _MAX_ADMISSION_TRANSACTION_BYTES
            ):
                batches.append(batch)
                batch = []
                batch_events = 0
                batch_bytes = 0
            batch.append(message)
            batch_events += copies_per_message
            batch_bytes += message_bytes
        if batch:
            batches.append(batch)
        return batches

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Durably admit one event through a bounded, fully synchronous group commit."""
        result = asyncio.get_running_loop().create_future()
        size_bytes = self._admission_size(data)
        if size_bytes > _MAX_ADMISSION_TRANSACTION_BYTES:
            raise OutboxCapacityError("durable outbox admission transaction byte limit reached")
        async with self._storage_lock:
            if self._closed:
                raise RuntimeError("durable outbox sink is closed")
            self._raise_if_admission_failed()
            if len(self._admission_requests) >= self._admission_queue_max_events:
                raise OutboxCapacityError("durable outbox admission queue event limit reached")
            if self._admission_queued_bytes + size_bytes > self._admission_queue_max_bytes:
                raise OutboxCapacityError("durable outbox admission queue byte limit reached")
            self._admission_requests.append(
                _AdmissionRequest(topic=topic, data=data, size_bytes=size_bytes, result=result)
            )
            self._admission_queued_bytes += size_bytes
            if self._admission_task is None or self._admission_task.done():
                self._admission_task = asyncio.create_task(
                    self._commit_admission_groups(),
                    name=f"durable_outbox_admit:{self.name}",
                )
        return await result

    async def _commit_admission_groups(self) -> None:
        """Commit at most 256 single-event requests after a five-millisecond coalesce window."""
        wait_before_commit = True
        while True:
            if wait_before_commit:
                await asyncio.sleep(_MAX_ADMISSION_GROUP_WAIT_SECONDS)
                wait_before_commit = False
            async with self._storage_lock:
                requests: list[_AdmissionRequest] = []
                transaction_bytes = 0
                while (
                    self._admission_requests
                    and len(requests) < _MAX_ADMISSION_TRANSACTION_EVENTS
                    and transaction_bytes + self._admission_requests[0].size_bytes <= _MAX_ADMISSION_TRANSACTION_BYTES
                ):
                    request = self._admission_requests.popleft()
                    requests.append(request)
                    transaction_bytes += request.size_bytes
                self._admission_queued_bytes -= transaction_bytes
                if not requests:
                    self._admission_task = None
                    return
                if self._closed:
                    error: Exception | None = RuntimeError("durable outbox sink is closed")
                elif self._admission_error is not None:
                    error = RuntimeError(f"durable outbox admission failed: {self._admission_error}")
                else:
                    error = None
                    try:
                        await asyncio.to_thread(
                            self._outbox.admit_many,
                            [(request.topic, request.data) for request in requests],
                        )
                    except Exception as exc:
                        self._poison_admission(exc)
                        error = exc
                    else:
                        self._start_drain(*(self._lane(request.topic) for request in requests))
            for request in requests:
                if request.result.done():
                    continue
                if error is None:
                    request.result.set_result(True)
                else:
                    request.result.set_exception(error)
            if error is not None:
                return

    def _poison_admission(self, error: Exception) -> None:
        """Fail every uncommitted caller after a terminal SQLite admission error."""
        self._admission_error = f"{type(error).__name__}: {error}"
        while self._admission_requests:
            request = self._admission_requests.popleft()
            if not request.result.done():
                request.result.set_exception(error)
        self._admission_queued_bytes = 0

    async def publish_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[bool]:
        """Durably admit a batch in one SQLite transaction."""
        if not messages:
            return []
        self._validate_transaction(messages)
        async with self._storage_lock:
            if self._closed:
                raise RuntimeError("durable outbox sink is closed")
            self._raise_if_admission_failed()
            try:
                await asyncio.to_thread(self._outbox.admit_many, messages)
            except Exception as exc:
                self._poison_admission(exc)
                raise
            self._start_drain(*(self._lane(topic) for topic, _data in messages))
        return [True] * len(messages)

    async def publish_flow_with_watch(self, writer_topic: str, data: dict[str, Any]) -> bool:
        """Atomically admit a flow record to the writer and watch work queues."""
        return bool(await self.publish_flow_with_watch_batch([(writer_topic, data)]))

    async def publish_flow_with_watch_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> list[bool]:
        """Atomically admit flow records to their writer and watch work queues."""
        if not messages:
            return []
        paired = list(messages)
        paired.extend(("heber:watch", data) for _topic, data in messages)
        self._validate_transaction(paired)
        async with self._storage_lock:
            if self._closed:
                raise RuntimeError("durable outbox sink is closed")
            self._raise_if_admission_failed()
            try:
                await asyncio.to_thread(self._outbox.admit_many, paired)
            except Exception as exc:
                self._poison_admission(exc)
                raise
            self._start_drain(*(self._lane(topic) for topic, _data in messages), "watch")
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

    def transport_status(self) -> dict[str, Any]:
        """Report admission and broker-delivery health without conflating them."""
        summaries = self._outbox.lane_summaries()
        publisher_status = getattr(self._publisher, "transport_status", None)
        broker = cast(dict[str, Any], publisher_status()) if callable(publisher_status) else {}
        broker_connection = broker.get("broker_connection", "unknown")
        admission = "ok" if not self._closed and self._admission_error is None else "failed"
        lanes: dict[str, dict[str, Any]] = {}
        for lane, summary in summaries.items():
            delivery_degraded = lane in self._delivery_errors or broker_connection == "disconnected"
            lanes[lane] = {
                "status": "degraded_durable_buffering" if admission == "ok" and delivery_degraded else admission,
                "admission": admission,
                "delivery": "degraded" if delivery_degraded else "ok",
                "delivery_error": self._delivery_errors.get(lane),
                "pending_count": summary.pending_count,
                "oldest_event_age_seconds": summary.oldest_event_age_seconds,
                "publish_failure_count": summary.publish_failure_count,
            }
        delivery_degraded = any(lane["delivery"] == "degraded" for lane in lanes.values())
        pending_count = sum(summary.pending_count for summary in summaries.values())
        oldest_event_age_seconds = max(summary.oldest_event_age_seconds for summary in summaries.values())
        publish_failure_count = sum(summary.publish_failure_count for summary in summaries.values())
        return {
            "status": "degraded_durable_buffering" if admission == "ok" and delivery_degraded else admission,
            "admission": admission,
            "delivery": "degraded" if delivery_degraded else "ok",
            "delivery_errors": self._delivery_errors.copy(),
            "outbox_utilization": self._utilization,
            "pending_count": pending_count,
            "oldest_event_age_seconds": oldest_event_age_seconds,
            "publish_failure_count": publish_failure_count,
            "broker_connection": broker_connection,
            "broker_last_connection_error": broker.get("last_connection_error"),
            "lanes": lanes,
        }

    def can_accept_low_priority(self, *, max_utilization: float) -> bool:
        return self._admission_error is None and self._utilization < max_utilization

    def _raise_if_admission_failed(self) -> None:
        if self._admission_error is not None:
            raise RuntimeError(f"durable outbox admission failed: {self._admission_error}")

    def schedule_drain(self) -> None:
        self._start_drain()

    async def _drain(self, lane: str) -> None:
        while True:
            self._wake[lane].clear()
            async with self._storage_lock:
                entries = await asyncio.to_thread(
                    self._outbox.pending,
                    limit=_MAX_ADMISSION_TRANSACTION_EVENTS,
                    lane=lane,
                )
            if not entries:
                if self._closed:
                    return
                await self._wake[lane].wait()
                continue

            outcomes = await asyncio.gather(*(self._publish_with_puback(lane, entry) for entry in entries))
            acknowledged_sequences = [
                entry.sequence for entry, (acknowledged, _error) in zip(entries, outcomes, strict=True) if acknowledged
            ]
            failures = [
                (entry.sequence, error or "publisher rejected event")
                for entry, (acknowledged, error) in zip(entries, outcomes, strict=True)
                if not acknowledged
            ]
            try:
                async with self._storage_lock:
                    await asyncio.to_thread(
                        self._outbox.settle_publications,
                        acknowledged_sequences,
                        failures,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                self._failed_delivery_sequences[lane].update(entry.sequence for entry in entries)
                self._delivery_errors[lane] = error
                set_durable_outbox_delivery_health(lane, False)
                logger.error(
                    "durable_outbox_settlement_failed",
                    sink=self.name,
                    lane=lane,
                    sequences=[entry.sequence for entry in entries],
                    error=error,
                    exc_info=True,
                )
                # The batch's fate is fully logged and tracked even though the
                # settlement write itself failed, so a shutdown may proceed —
                # this failure mode already accepts possible redelivery on
                # restart, deduped downstream by event_id.
                self._settle_safe[lane].set()
                await asyncio.sleep(self._retry_delay_seconds)
                continue

            self._settle_safe[lane].set()
            for entry, (acknowledged, _error) in zip(entries, outcomes, strict=True):
                if acknowledged:
                    self._failed_delivery_sequences[lane].discard(entry.sequence)
                else:
                    record_durable_outbox_retained(lane)
            if not self._failed_delivery_sequences[lane]:
                self._delivery_errors.pop(lane, None)
                set_durable_outbox_delivery_health(lane, True)
            if failures:
                await asyncio.sleep(self._retry_delay_seconds)

    async def _publish_with_puback(self, lane: str, entry: OutboxEntry) -> tuple[bool, str | None]:
        """Publish one admitted row under the global PubAck concurrency bound."""
        error: str | None = None
        try:
            async with self._puback_semaphore:
                acknowledged = await self._publisher(entry)
            if acknowledged:
                # The publisher has committed to this event. Until it is durably
                # settled below, a shutdown must not cancel this lane's drain.
                self._settle_safe[lane].clear()
            else:
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
            # This is event-loop-local state. Mark it before waiting for the
            # SQLite bookkeeping lock so a failed broker acknowledgement is
            # immediately visible to readiness and alerting.
            failure = error or "publisher rejected event"
            self._failed_delivery_sequences[lane].add(entry.sequence)
            self._delivery_errors[lane] = failure
            set_durable_outbox_delivery_health(lane, False)

        return acknowledged, error

    async def close(self) -> None:
        """Stop publication and close SQLite after active admission finishes."""
        if self._closed:
            return
        async with self._storage_lock:
            self._closed = True
            pending_admissions = list(self._admission_requests)
            self._admission_requests.clear()
            self._admission_queued_bytes = 0
        for request in pending_admissions:
            if not request.result.done():
                request.result.set_exception(RuntimeError("durable outbox sink is closed"))
        if self._admission_task is not None:
            await self._admission_task
            self._admission_task = None
        if self._metrics_task is not None:
            self._metrics_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._metrics_task
            self._metrics_task = None
        # Wake any drain parked with an empty queue so it observes self._closed
        # and returns instead of waiting forever.
        for wake in self._wake.values():
            wake.set()
        drain_tasks = list(self._drain_tasks.values())
        if drain_tasks:
            # Cancelling a drain while it holds a publisher acknowledgement that
            # is not yet durably settled would abandon that ack: the row stays
            # pending and gets redelivered on restart (bounded redundant
            # delivery, deduped downstream by event_id). Wait for each lane to
            # reach a safe point before cancelling anything. This is bounded,
            # not "wait for the drain to finish" — a lane with no unsettled ack
            # (idle, retrying, or a publish that is simply slow/stuck and has
            # not yet returned) is already safe and does not block this wait.
            #
            # This narrows the race rather than eliminating it: it is
            # deliberately NOT a guarantee. If a broker never acks a sibling
            # publish in the same batch within the timeout (e.g. a broker
            # outage spanning the whole shutdown window — the JetStream client
            # puts no timeout of its own on a PubAck), this still falls back to
            # cancelling — the same behavior this file had before this fix
            # existed at all. That residual window only opens during an
            # already-exceptional outage, versus the pre-fix bug which
            # reproduced on any close() racing an ordinary in-flight publish.
            # See test_close_falls_back_to_cancelling_past_the_shutdown_timeout.
            await self._wait_until_safe_to_cancel_drains()
            # No `await` between the safety check above and cancelling below.
            # A lane's event can only be cleared from a synchronous line inside
            # _publish_with_puback, which can only run when this coroutine
            # yields control — so once every lane is observed set with no
            # intervening yield, none can have gone unsafe again before these
            # cancellations land. An `await` here (even just to reap the helper
            # wait tasks) would reopen exactly that gap.
            for task in drain_tasks:
                task.cancel()
            for task in drain_tasks:
                with suppress(asyncio.CancelledError):
                    await task
        self._drain_tasks.clear()
        async with self._storage_lock:
            await asyncio.to_thread(self._outbox.close)

    async def _wait_until_safe_to_cancel_drains(self) -> None:
        """Block until no lane holds an unsettled acknowledgement, or the timeout elapses.

        Returns only once a synchronous `is_set()` check confirms every lane is
        safe, with that check as the function's last action. A version of this
        that instead waited on `event.wait()` futures and then cleaned them up
        before returning left `await` points between "observed safe" and the
        caller's cancellation — during which a publish that was already in
        flight could complete and clear a lane's event again, reopening the
        exact abandoned-acknowledgement race this exists to close.
        """
        deadline = asyncio.get_running_loop().time() + self._drain_shutdown_timeout_seconds
        waiters: list[asyncio.Task[bool]] = []
        try:
            while not all(event.is_set() for event in self._settle_safe.values()):
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    logger.warning(
                        "durable_outbox_shutdown_unsettled_ack_timeout",
                        sink=self.name,
                        timeout_seconds=self._drain_shutdown_timeout_seconds,
                        unsettled_lanes=sorted(lane for lane, event in self._settle_safe.items() if not event.is_set()),
                    )
                    return
                pending = [
                    asyncio.ensure_future(event.wait()) for event in self._settle_safe.values() if not event.is_set()
                ]
                waiters.extend(pending)
                with suppress(TimeoutError):
                    await asyncio.wait_for(asyncio.wait(pending), timeout=remaining)
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()

    async def _refresh_outbox_metrics_periodically(self) -> None:
        """Bound aggregate SQLite status reads away from the admission and ACK paths."""
        while True:
            await asyncio.sleep(_OUTBOX_METRICS_REFRESH_SECONDS)
            async with self._storage_lock:
                if self._closed:
                    return
                await asyncio.to_thread(self._refresh_outbox_metrics)

    def _refresh_outbox_metrics(self) -> None:
        self._utilization = self._outbox.capacity_utilization()
        set_durable_outbox_utilization(self._utilization)
        broker_status = getattr(self._publisher, "transport_status", None)
        broker = cast(dict[str, Any], broker_status()) if callable(broker_status) else {}
        broker_disconnected = broker.get("broker_connection") == "disconnected"
        for lane, summary in self._outbox.lane_summaries().items():
            set_durable_outbox_summary(
                lane=lane,
                pending_count=summary.pending_count,
                oldest_event_age_seconds=summary.oldest_event_age_seconds,
                publish_failure_count=summary.publish_failure_count,
            )
            set_durable_outbox_delivery_health(
                lane,
                healthy=not broker_disconnected and lane not in self._delivery_errors,
            )


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
        self._lane_health = {"live": "unknown", "backfill": "unknown"}

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

    def transport_status(self) -> dict[str, Any]:
        """Expose durable admission and broker delivery for the routed lanes."""
        snapshot = getattr(self._durable, "transport_status", None)
        status = cast(dict[str, Any], snapshot()) if callable(snapshot) else {}
        durable_lane_status = cast(dict[str, dict[str, Any]], status.pop("lanes", {}))
        status["durable_lanes"] = sorted(self._durable_lanes)
        status["lanes"] = {}
        for lane, topic in (("live", "heber:events"), ("backfill", "heber:events:backfill")):
            if self._delegate(topic) is self._durable:
                status["lanes"][lane] = {
                    "transport": "durable",
                    "health": self._lane_health[lane],
                    **durable_lane_status.get(lane, {}),
                }
            else:
                status["lanes"][lane] = {"transport": "redis", "health": self._lane_health[lane]}
        if "live" in self._durable_lanes:
            status["lanes"]["watch"] = {
                "transport": "durable",
                "health": self._lane_health["live"],
                **durable_lane_status.get("watch", {}),
            }
        selected_durable = [
            lane_status for lane_status in status["lanes"].values() if lane_status["transport"] == "durable"
        ]
        admission_failed = any(lane.get("admission") == "failed" for lane in selected_durable)
        delivery_degraded = any(lane.get("delivery") == "degraded" for lane in selected_durable)
        status["admission"] = "failed" if admission_failed else "ok"
        status["delivery"] = "degraded" if delivery_degraded else "ok"
        status["status"] = "failed" if admission_failed else "degraded_durable_buffering" if delivery_degraded else "ok"
        status["delivery_errors"] = {
            lane: error
            for lane, error in cast(dict[str, str], status.get("delivery_errors", {})).items()
            if lane in status["lanes"] and status["lanes"][lane]["transport"] == "durable"
        }
        return status

    def _delegate(self, topic: str) -> DataSink:
        lane = "backfill" if topic == "heber:events:backfill" else "live"
        return self._durable if lane in self._durable_lanes else self._redis

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        return await self._delegate(topic).publish(topic, data)

    async def _publish_durable_chunks(
        self,
        messages: list[tuple[str, dict[str, Any]]],
        *,
        copies_per_message: int,
        publish: Callable[[list[tuple[str, dict[str, Any]]]], Awaitable[list[bool]]],
    ) -> list[bool]:
        """Publish bounded durable transactions while preserving result positions."""
        batches = self._durable.admission_batches(messages, copies_per_message=copies_per_message)
        results: list[bool] = []
        for batch in batches:
            # nosemgrep: empire-no-bare-exception -- a failed transaction must report only its own inputs false
            try:
                batch_results = await publish(batch)
            except Exception:
                logger.exception(
                    "lane_routed_sink_bounded_batch_failed",
                    delegate=self._durable.name,
                    count=len(batch),
                    copies_per_message=copies_per_message,
                )
                results.extend([False] * len(batch))
                continue
            if len(batch_results) != len(batch):
                logger.error(
                    "lane_routed_sink_batch_results_length_mismatch",
                    delegate=self._durable.name,
                    expected=len(batch),
                    actual=len(batch_results),
                )
                results.extend([False] * len(batch))
                continue
            results.extend(batch_results)
        return results

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
            delegate_messages = [message for _, message in indexed]
            # nosemgrep: empire-no-bare-exception -- transport boundary: one lane failure must not erase exact results from the other lane
            try:
                if isinstance(delegate, DurableOutboxSink):
                    delegate_results = await self._publish_durable_chunks(
                        delegate_messages,
                        copies_per_message=1,
                        publish=self._durable.publish_batch_results,
                    )
                else:
                    delegate_results = await delegate.publish_batch_results(delegate_messages)  # type: ignore[attr-defined]
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

    async def publish_flow_with_watch_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[bool]:
        """Atomically admit durable flow writer and watch copies."""
        if not all(self._delegate(topic) is self._durable for topic, _data in messages):
            raise RuntimeError("flow watch requires the live lane to use durable admission")
        if not isinstance(self._durable, DurableOutboxSink):
            publish_flow = cast(
                Callable[[list[tuple[str, dict[str, Any]]]], Awaitable[list[bool]]],
                self._durable.publish_flow_with_watch_batch,
            )
            return await publish_flow(messages)
        return await self._publish_durable_chunks(
            messages,
            copies_per_message=2,
            publish=self._durable.publish_flow_with_watch_batch,
        )

    async def publish_batch_indexed(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> set[int]:
        return {index for index, accepted in enumerate(await self.publish_batch_results(messages)) if accepted}

    async def publish_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> int:
        return sum(await self.publish_batch_results(messages))

    async def health_check(self) -> bool:
        durable_healthy = await self._durable.health_check()
        redis_healthy = await self._redis.health_check() if self._durable_lanes != {"live", "backfill"} else True
        self._lane_health["live"] = (
            "ok" if (durable_healthy if "live" in self._durable_lanes else redis_healthy) else "degraded"
        )
        self._lane_health["backfill"] = (
            "ok" if (durable_healthy if "backfill" in self._durable_lanes else redis_healthy) else "degraded"
        )
        return durable_healthy and redis_healthy

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
