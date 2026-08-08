from __future__ import annotations

import asyncio
import multiprocessing
import threading
from contextlib import suppress
from pathlib import Path

import pytest

from gateway.core.durable_outbox import OutboxCapacityError, OutboxEntry, SQLiteOutbox
from gateway.core.durable_outbox_sink import DurableOutboxSink, LaneRoutedSink


def _event(event_id: str) -> dict[str, str]:
    return {"event_id": event_id, "feed": "trades"}


def _admit_in_child(path: str) -> None:
    with SQLiteOutbox(path) as outbox:
        outbox.admit("heber.live.trades", _event("evt-child"))


def test_child_process_commit_survives_process_exit(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    process = multiprocessing.get_context("spawn").Process(
        target=_admit_in_child,
        args=(str(path),),
    )

    process.start()
    process.join(timeout=10)

    assert process.exitcode == 0
    with SQLiteOutbox(path) as reopened:
        assert [entry.event_id for entry in reopened.pending()] == ["evt-child"]


@pytest.mark.asyncio
async def test_publish_offloads_durable_admission_from_event_loop(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    entered = threading.Event()
    release = threading.Event()
    original_admit_many = outbox.admit_many

    def blocking_admit_many(*args: object, **kwargs: object) -> list[bool]:
        entered.set()
        assert release.wait(timeout=5)
        return original_admit_many(*args, **kwargs)

    outbox.admit_many = blocking_admit_many  # type: ignore[method-assign]

    async def publish_ack(_entry: OutboxEntry) -> bool:
        return True

    sink = DurableOutboxSink(outbox, publish_ack)
    task = asyncio.create_task(sink.publish("heber.live.trades", _event("evt-1")))
    assert await asyncio.to_thread(entered.wait, 2)

    loop_was_responsive = False

    async def mark_responsive() -> None:
        nonlocal loop_was_responsive
        loop_was_responsive = True

    await mark_responsive()
    assert loop_was_responsive is True
    release.set()
    assert await task is True
    await sink.close()


@pytest.mark.asyncio
async def test_concurrent_single_event_publishes_share_one_bounded_group_commit(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    admitted_batch_sizes: list[int] = []
    original_admit_many = outbox.admit_many

    def admit_many(messages: list[tuple[str, dict[str, str]]]) -> list[bool]:
        admitted_batch_sizes.append(len(messages))
        return original_admit_many(messages)

    outbox.admit_many = admit_many  # type: ignore[method-assign]

    sink = DurableOutboxSink(outbox, lambda _entry: asyncio.sleep(0, result=True))
    await asyncio.gather(
        sink.publish("heber:events", _event("evt-1")),
        sink.publish("heber:events", _event("evt-2")),
    )

    assert admitted_batch_sizes == [2]
    await sink.close()


@pytest.mark.asyncio
async def test_group_commit_never_admits_more_than_256_events_in_one_transaction(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    admitted_batch_sizes: list[int] = []
    original_admit_many = outbox.admit_many

    def admit_many(messages: list[tuple[str, dict[str, str]]]) -> list[bool]:
        admitted_batch_sizes.append(len(messages))
        return original_admit_many(messages)

    outbox.admit_many = admit_many  # type: ignore[method-assign]
    sink = DurableOutboxSink(outbox, lambda _entry: asyncio.sleep(0, result=True))

    await asyncio.gather(*(sink.publish("heber:events", _event(f"evt-{index}")) for index in range(257)))

    assert admitted_batch_sizes == [256, 1]
    await sink.close()


@pytest.mark.asyncio
async def test_terminal_group_failure_rejects_later_queued_callers_without_hanging(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")

    def fail_admission(_messages: list[tuple[str, dict[str, str]]]) -> list[bool]:
        raise OutboxCapacityError("simulated disk budget exhaustion")

    outbox.admit_many = fail_admission  # type: ignore[method-assign]
    sink = DurableOutboxSink(outbox, lambda _entry: asyncio.sleep(0, result=True))
    requests = [sink.publish("heber:events", _event(f"evt-{index}")) for index in range(257)]

    results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=2)

    assert len(results) == 257
    assert all(isinstance(result, OutboxCapacityError) for result in results)
    await sink.close()


@pytest.mark.asyncio
async def test_admission_queue_rejects_count_and_byte_overflow_before_commit(tmp_path: Path) -> None:
    count_sink = DurableOutboxSink(
        SQLiteOutbox(tmp_path / "count.sqlite3"),
        lambda _entry: asyncio.sleep(0, result=True),
        admission_queue_max_events=1,
    )
    first = asyncio.create_task(count_sink.publish("heber:events", _event("count-1")))
    await asyncio.sleep(0)
    with pytest.raises(OutboxCapacityError, match="queue event limit"):
        await count_sink.publish("heber:events", _event("count-2"))
    await first
    await count_sink.close()

    byte_sink = DurableOutboxSink(
        SQLiteOutbox(tmp_path / "bytes.sqlite3"),
        lambda _entry: asyncio.sleep(0, result=True),
        admission_queue_max_bytes=1_600,
    )
    payload = {"event_id": "bytes-1", "payload": "x" * 700}
    first = asyncio.create_task(byte_sink.publish("heber:events", payload))
    await asyncio.sleep(0)
    with pytest.raises(OutboxCapacityError, match="queue byte limit"):
        await byte_sink.publish("heber:events", {"event_id": "bytes-2", "payload": "x" * 700})
    await first
    await byte_sink.close()


@pytest.mark.asyncio
async def test_direct_batch_admission_rejects_oversized_transactions_without_partial_writes(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    sink = DurableOutboxSink(outbox, lambda _entry: asyncio.sleep(0, result=True))

    with pytest.raises(OutboxCapacityError, match="transaction event limit"):
        await sink.publish_batch_results([("heber:events", _event(f"evt-{index}")) for index in range(257)])
    assert outbox.pending() == []

    large_payload = "x" * (8 * 1024**2)
    with pytest.raises(OutboxCapacityError, match="transaction byte limit"):
        await sink.publish_batch_results(
            [
                ("heber:events", {"event_id": "large-1", "payload": large_payload}),
                ("heber:events", {"event_id": "large-2", "payload": large_payload}),
            ]
        )
    assert outbox.pending() == []
    await sink.close()


@pytest.mark.asyncio
async def test_flow_writer_watch_batch_rejects_oversized_atomic_transactions(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    sink = DurableOutboxSink(outbox, lambda _entry: asyncio.sleep(0, result=True))

    with pytest.raises(OutboxCapacityError, match="transaction event limit"):
        await sink.publish_flow_with_watch_batch([("heber:events", _event(f"flow-{index}")) for index in range(129)])
    assert outbox.pending() == []

    large_payload = "x" * (8 * 1024**2)
    with pytest.raises(OutboxCapacityError, match="transaction byte limit"):
        await sink.publish_flow_with_watch(
            "heber:events",
            {"event_id": "large-flow", "payload": large_payload},
        )
    assert outbox.pending() == []
    await sink.close()


@pytest.mark.asyncio
async def test_lane_router_chunks_configured_backfill_batch_into_bounded_transactions(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    transaction_sizes: list[int] = []
    original_admit_many = outbox.admit_many

    def admit_many(messages: list[tuple[str, dict[str, str]]]) -> list[bool]:
        transaction_sizes.append(len(messages))
        return original_admit_many(messages)

    class _UnusedRedis:
        name = "redis"

        async def publish(self, _topic: str, _data: dict[str, str]) -> bool:
            return True

        async def health_check(self) -> bool:
            return True

        async def close(self) -> None:
            return None

    async def blocked_publisher(_entry: OutboxEntry) -> bool:
        await asyncio.Event().wait()
        return True

    outbox.admit_many = admit_many  # type: ignore[method-assign]
    durable = DurableOutboxSink(outbox, blocked_publisher)
    sink = LaneRoutedSink(durable, _UnusedRedis(), lanes="backfill")
    messages = [("heber:events:backfill", _event(f"backfill-{index}")) for index in range(5_000)]

    assert await sink.publish_batch_results(messages) == [True] * len(messages)
    assert sum(transaction_sizes) == len(messages)
    assert all(size <= 256 for size in transaction_sizes)
    assert outbox.summary().pending_count == len(messages)
    await sink.close()


@pytest.mark.asyncio
async def test_lane_router_keeps_each_flow_writer_watch_pair_in_one_bounded_transaction(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    transaction_sizes: list[int] = []
    original_admit_many = outbox.admit_many

    def admit_many(messages: list[tuple[str, dict[str, str]]]) -> list[bool]:
        transaction_sizes.append(len(messages))
        assert all(
            sum(1 for _topic, data in messages if data["event_id"] == event_id) == 2
            for event_id in {data["event_id"] for _topic, data in messages}
        )
        return original_admit_many(messages)

    class _UnusedRedis:
        name = "redis"

        async def publish(self, _topic: str, _data: dict[str, str]) -> bool:
            return True

        async def health_check(self) -> bool:
            return True

        async def close(self) -> None:
            return None

    async def blocked_publisher(_entry: OutboxEntry) -> bool:
        await asyncio.Event().wait()
        return True

    outbox.admit_many = admit_many  # type: ignore[method-assign]
    durable = DurableOutboxSink(outbox, blocked_publisher)
    sink = LaneRoutedSink(durable, _UnusedRedis(), lanes="live")
    messages = [("heber:events", _event(f"flow-{index}")) for index in range(129)]

    assert await sink.publish_flow_with_watch_batch_results(messages) == [True] * len(messages)
    assert transaction_sizes == [256, 2]
    assert outbox.summary().pending_count == 258
    await sink.close()


@pytest.mark.asyncio
async def test_close_rejects_queued_admission_before_it_commits(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    sink = DurableOutboxSink(SQLiteOutbox(path), lambda _entry: asyncio.sleep(0, result=True))
    queued = asyncio.create_task(sink.publish("heber:events", _event("queued")))

    await asyncio.sleep(0)
    await sink.close()

    with pytest.raises(RuntimeError, match="closed"):
        await queued
    with SQLiteOutbox(path) as reopened:
        assert reopened.pending() == []


@pytest.mark.asyncio
async def test_drain_allows_at_most_32_concurrent_pubacks(tmp_path: Path) -> None:
    active = 0
    peak_active = 0
    release = asyncio.Event()
    published = 0
    finished = asyncio.Event()
    ramped = asyncio.Event()
    lock = asyncio.Lock()

    async def publish_ack(_entry: OutboxEntry) -> bool:
        nonlocal active, peak_active, published
        async with lock:
            active += 1
            peak_active = max(peak_active, active)
            if peak_active >= 32:
                ramped.set()
        await release.wait()
        async with lock:
            active -= 1
            published += 1
            if published == 33:
                finished.set()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(tmp_path / "outbox.sqlite3"), publish_ack)
    await sink.publish_batch_results([("heber:events", _event(f"evt-{index}")) for index in range(33)])
    # Wait on the ramp-up signal rather than a fixed number of event-loop yields:
    # how many yields the drain needs to reach its ceiling depends on machine
    # speed, so a fixed budget passes locally and fails on a loaded CI runner.
    # A too-low ceiling still fails the assertion below via the timeout, and a
    # missing ceiling still fails it by overshooting 32.
    with suppress(TimeoutError):
        await asyncio.wait_for(ramped.wait(), timeout=10)

    assert peak_active == 32
    release.set()
    await asyncio.wait_for(finished.wait(), timeout=2)
    await sink.close()


@pytest.mark.asyncio
async def test_concurrent_puback_failure_retains_only_the_unacknowledged_row(tmp_path: Path) -> None:
    both_started = asyncio.Event()
    release = asyncio.Event()
    started = 0

    async def publish_ack(entry: OutboxEntry) -> bool:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await release.wait()
        return entry.event_id != "fail"

    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    sink = DurableOutboxSink(outbox, publish_ack, retry_delay_seconds=60)
    await sink.publish_batch_results([("heber:events", _event("fail")), ("heber:events", _event("succeed"))])
    await asyncio.wait_for(both_started.wait(), timeout=2)
    release.set()
    for _ in range(100):
        pending = outbox.pending()
        if [entry.event_id for entry in pending] == ["fail"] and pending[0].attempts == 1:
            break
        await asyncio.sleep(0)

    assert [entry.event_id for entry in outbox.pending()] == ["fail"]
    assert outbox.pending()[0].attempts == 1
    status = sink.transport_status()
    assert status["lanes"]["live"]["delivery"] == "degraded"
    assert status["lanes"]["live"]["delivery_error"] == "publisher returned False"
    await sink.close()


@pytest.mark.asyncio
async def test_one_sqlite_settlement_covers_each_256_row_drain_group(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    capacity_calls = 0
    summary_calls = 0
    acknowledged = asyncio.Event()
    original_capacity = outbox.capacity_utilization
    original_lane_summaries = outbox.lane_summaries
    original_settle_publications = outbox.settle_publications
    loop = asyncio.get_running_loop()
    settlement_calls: list[tuple[list[int], list[tuple[int, str]]]] = []

    def capacity_utilization() -> float:
        nonlocal capacity_calls
        capacity_calls += 1
        return original_capacity()

    def lane_summaries():
        nonlocal summary_calls
        summary_calls += 1
        return original_lane_summaries()

    def settle_publications(
        acknowledged_sequences: list[int],
        failures: list[tuple[int, str]],
    ) -> tuple[int, int]:
        settlement_calls.append((acknowledged_sequences, failures))
        result = original_settle_publications(acknowledged_sequences, failures)
        if result[0] == 256:
            loop.call_soon_threadsafe(acknowledged.set)
        return result

    outbox.capacity_utilization = capacity_utilization  # type: ignore[method-assign]
    outbox.lane_summaries = lane_summaries  # type: ignore[method-assign]
    outbox.settle_publications = settle_publications  # type: ignore[method-assign]
    sink = DurableOutboxSink(outbox, lambda _entry: asyncio.sleep(0, result=True))

    await sink.start()
    await sink.publish_batch_results([("heber:events", _event(f"evt-{index}")) for index in range(256)])
    await asyncio.wait_for(acknowledged.wait(), timeout=2)

    assert len(settlement_calls) == 1
    assert len(settlement_calls[0][0]) == 256
    assert settlement_calls[0][1] == []
    assert capacity_calls == 2  # construction plus the initial bounded refresh
    assert summary_calls == 1
    await sink.close()


@pytest.mark.asyncio
async def test_settlement_failure_retains_pubacknowledged_rows_for_idempotent_retry(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    cleanup_attempted = threading.Event()

    def fail_settlement(
        _acknowledged_sequences: list[int],
        _failures: list[tuple[int, str]],
    ) -> tuple[int, int]:
        cleanup_attempted.set()
        raise RuntimeError("simulated settlement failure")

    outbox.settle_publications = fail_settlement  # type: ignore[method-assign]
    sink = DurableOutboxSink(
        outbox,
        lambda _entry: asyncio.sleep(0, result=True),
        retry_delay_seconds=60,
    )

    await sink.publish_batch_results(
        [
            ("heber:events", _event("evt-1")),
            ("heber:events", _event("evt-2")),
        ]
    )
    assert await asyncio.to_thread(cleanup_attempted.wait, 2)

    assert [entry.event_id for entry in outbox.pending()] == ["evt-1", "evt-2"]
    await sink.close()


@pytest.mark.asyncio
async def test_batch_admission_returns_per_message_results_and_drains_in_order(tmp_path: Path) -> None:
    published: list[str] = []
    all_published = asyncio.Event()

    async def publish_ack(entry: OutboxEntry) -> bool:
        published.append(entry.event_id)
        if len(published) == 3:
            all_published.set()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(tmp_path / "outbox.sqlite3"), publish_ack)
    results = await sink.publish_batch_results(
        [
            ("heber.live.trades", _event("evt-1")),
            ("heber.live.trades", _event("evt-2")),
            ("heber.live.trades", _event("evt-3")),
        ]
    )

    assert results == [True, True, True]
    await asyncio.wait_for(all_published.wait(), timeout=2)
    assert published == ["evt-1", "evt-2", "evt-3"]
    await sink.close()


@pytest.mark.asyncio
async def test_batch_contracts_count_exact_duplicates_as_durably_accepted(tmp_path: Path) -> None:
    release_publisher = asyncio.Event()

    async def publish_ack(_entry: OutboxEntry) -> bool:
        await release_publisher.wait()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(tmp_path / "outbox.sqlite3"), publish_ack)
    messages = [
        ("heber.live.trades", _event("evt-duplicate")),
        ("heber.live.trades", _event("evt-duplicate")),
    ]

    assert await sink.publish_batch_results(messages) == [True, True]
    assert await sink.publish_batch_indexed(messages) == {0, 1}
    assert await sink.publish_batch(messages) == 2

    release_publisher.set()
    await sink.close()


@pytest.mark.asyncio
async def test_start_drains_rows_admitted_by_previous_process(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path) as original:
        original.admit("heber.live.trades", _event("evt-before-restart"))

    published = asyncio.Event()

    async def publish_ack(entry: OutboxEntry) -> bool:
        assert entry.event_id == "evt-before-restart"
        published.set()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(path), publish_ack)
    await sink.start()
    await asyncio.wait_for(published.wait(), timeout=2)
    await sink.close()

    with SQLiteOutbox(path) as reopened:
        assert reopened.pending() == []


@pytest.mark.asyncio
async def test_publisher_failure_waits_for_group_then_retains_only_failed_entry(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    first_failed = asyncio.Event()
    release_retry = asyncio.Event()
    published: list[str] = []

    async def publish_ack(entry: OutboxEntry) -> bool:
        published.append(entry.event_id)
        if entry.event_id == "evt-1" and not first_failed.is_set():
            first_failed.set()
            return False
        await release_retry.wait()
        return True

    sink = DurableOutboxSink(outbox, publish_ack, retry_delay_seconds=60)
    await sink.publish_batch_results(
        [
            ("heber.live.trades", _event("evt-1")),
            ("heber.live.trades", _event("evt-2")),
        ]
    )
    await asyncio.wait_for(first_failed.wait(), timeout=2)

    pending = await asyncio.to_thread(outbox.pending)
    assert [entry.event_id for entry in pending] == ["evt-1", "evt-2"]
    assert pending[0].attempts == 0
    assert published == ["evt-1", "evt-2"]

    release_retry.set()
    for _ in range(100):
        pending = await asyncio.to_thread(outbox.pending)
        if [entry.event_id for entry in pending] == ["evt-1"] and pending[0].attempts == 1:
            break
        await asyncio.sleep(0)
    assert [entry.event_id for entry in pending] == ["evt-1"]
    assert pending[0].attempts == 1
    await sink.close()


@pytest.mark.asyncio
async def test_failed_backfill_does_not_block_live_or_watch_delivery(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    backfill_failed = asyncio.Event()
    delivered = asyncio.Event()
    published: list[str] = []

    async def publish_ack(entry: OutboxEntry) -> bool:
        published.append(entry.event_id)
        if entry.topic == "heber:events:backfill":
            backfill_failed.set()
            return False
        if {"evt-live", "evt-watch"}.issubset(published):
            delivered.set()
        return True

    sink = DurableOutboxSink(outbox, publish_ack, retry_delay_seconds=60)
    await sink.publish_batch_results(
        [
            ("heber:events:backfill", _event("evt-backfill")),
            ("heber:events", _event("evt-live")),
            ("heber:watch", _event("evt-watch")),
        ]
    )

    await asyncio.wait_for(backfill_failed.wait(), timeout=2)
    await asyncio.wait_for(delivered.wait(), timeout=2)
    assert published[:3] == ["evt-backfill", "evt-live", "evt-watch"]
    assert sink.transport_status()["admission"] == "ok"
    assert sink.transport_status()["delivery"] == "degraded"
    assert "backfill" in sink.transport_status()["delivery_errors"]
    await sink.close()


@pytest.mark.asyncio
async def test_flow_writer_and_watch_are_admitted_atomically_before_drain(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    release = asyncio.Event()

    async def publish_ack(_entry: OutboxEntry) -> bool:
        await release.wait()
        return True

    sink = DurableOutboxSink(outbox, publish_ack)
    flow = _event("evt-flow")

    assert await sink.publish_flow_with_watch("heber:events", flow) is True
    assert [(entry.topic, entry.event_id) for entry in await asyncio.to_thread(outbox.pending)] == [
        ("heber:events", "evt-flow"),
        ("heber:watch", "evt-flow"),
    ]
    release.set()
    await sink.close()


@pytest.mark.asyncio
async def test_failed_watch_does_not_block_authoritative_live_delivery(tmp_path: Path) -> None:
    live_delivered = asyncio.Event()
    watch_failed = asyncio.Event()

    async def publish_ack(entry: OutboxEntry) -> bool:
        if entry.topic == "heber:watch":
            watch_failed.set()
            return False
        live_delivered.set()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(tmp_path / "outbox.sqlite3"), publish_ack, retry_delay_seconds=60)
    await sink.publish_flow_with_watch("heber:events", _event("evt-flow"))

    await asyncio.wait_for(watch_failed.wait(), timeout=2)
    await asyncio.wait_for(live_delivered.wait(), timeout=2)
    status = sink.transport_status()
    assert status["delivery_errors"] == {"watch": "publisher returned False"}
    assert status["lanes"]["watch"]["delivery"] == "degraded"
    assert status["lanes"]["live"]["delivery"] == "ok"
    await sink.close()


@pytest.mark.asyncio
async def test_failed_live_delivery_does_not_degrade_watch_lane(tmp_path: Path) -> None:
    live_failed = asyncio.Event()
    watch_delivered = asyncio.Event()

    async def publish_ack(entry: OutboxEntry) -> bool:
        if entry.topic == "heber:events":
            live_failed.set()
            return False
        watch_delivered.set()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(tmp_path / "outbox.sqlite3"), publish_ack, retry_delay_seconds=60)
    await sink.publish_flow_with_watch("heber:events", _event("evt-flow"))

    await asyncio.wait_for(live_failed.wait(), timeout=2)
    await asyncio.wait_for(watch_delivered.wait(), timeout=2)
    status = sink.transport_status()
    assert status["lanes"]["live"]["delivery"] == "degraded"
    assert status["lanes"]["watch"]["delivery"] == "ok"
    await sink.close()


@pytest.mark.asyncio
async def test_transport_status_exposes_pending_age_failures_and_durable_buffering(tmp_path: Path) -> None:
    failed = asyncio.Event()
    recorded = asyncio.Event()
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    original_settle_publications = outbox.settle_publications
    loop = asyncio.get_running_loop()

    def settle_publications(
        acknowledged_sequences: list[int],
        failures: list[tuple[int, str]],
    ) -> tuple[int, int]:
        result = original_settle_publications(acknowledged_sequences, failures)
        loop.call_soon_threadsafe(recorded.set)
        return result

    outbox.settle_publications = settle_publications  # type: ignore[method-assign]

    class _DisconnectedPublisher:
        def transport_status(self) -> dict[str, str]:
            return {"broker_connection": "disconnected"}

        async def __call__(self, _entry: OutboxEntry) -> bool:
            failed.set()
            return False

    sink = DurableOutboxSink(
        outbox,
        _DisconnectedPublisher(),
        retry_delay_seconds=60,
    )
    await sink.publish("heber:events", _event("evt-1"))
    await asyncio.wait_for(failed.wait(), timeout=2)
    await asyncio.wait_for(recorded.wait(), timeout=2)

    status = sink.transport_status()

    assert status["status"] == "degraded_durable_buffering"
    assert status["broker_connection"] == "disconnected"
    assert status["pending_count"] == 1
    assert status["publish_failure_count"] == 1
    assert status["oldest_event_age_seconds"] >= 0
    await sink.close()


@pytest.mark.asyncio
async def test_close_cancels_blocked_publish_and_leaves_event_for_restart(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    publisher_started = asyncio.Event()
    publisher_cancelled = asyncio.Event()

    async def blocked_publisher(_entry: OutboxEntry) -> bool:
        publisher_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            publisher_cancelled.set()
            raise

    sink = DurableOutboxSink(SQLiteOutbox(path), blocked_publisher)
    await sink.publish("heber.live.trades", _event("evt-1"))
    await asyncio.wait_for(publisher_started.wait(), timeout=2)
    await sink.close()

    assert publisher_cancelled.is_set()
    with SQLiteOutbox(path) as reopened:
        assert [entry.event_id for entry in reopened.pending()] == ["evt-1"]


@pytest.mark.asyncio
async def test_health_fails_when_outbox_disk_budget_is_exhausted(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    sink = DurableOutboxSink(
        SQLiteOutbox(path, max_bytes=1),
        lambda _entry: asyncio.sleep(0, result=True),
    )

    assert await sink.health_check() is False
    await sink.close()


@pytest.mark.asyncio
async def test_admission_failure_poisons_sink_until_restart(tmp_path: Path) -> None:
    sink = DurableOutboxSink(
        SQLiteOutbox(tmp_path / "outbox.sqlite3", max_bytes=1),
        lambda _entry: asyncio.sleep(0, result=True),
    )

    with pytest.raises(OutboxCapacityError):
        await sink.publish("heber.live.trades", _event("evt-1"))
    with pytest.raises(RuntimeError, match="admission failed"):
        await sink.publish("heber.live.trades", _event("evt-2"))

    assert await sink.health_check() is False
    await sink.close()


@pytest.mark.asyncio
async def test_close_settles_entries_the_publisher_already_acknowledged(tmp_path: Path) -> None:
    """close() must not discard acknowledgements the publisher already gave.

    The drain suspends at the `asyncio.gather` over a batch's publishes, so a
    close() arriving while some entries are acknowledged and others are still in
    flight used to cancel the drain before `settle_publications` ran. Those rows
    stayed pending despite the broker having accepted them, so the next process
    republished them. Shutdown must stop the drain at a safe point instead.
    """
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path) as original:
        for index in range(3):
            original.admit("heber.live.trades", _event(f"evt-{index}"))

    first_published = asyncio.Event()
    release = asyncio.Event()
    acknowledged: list[str] = []

    async def publish_ack(entry: OutboxEntry) -> bool:
        if entry.event_id == "evt-0":
            acknowledged.append(entry.event_id)
            first_published.set()
            return True
        await release.wait()
        acknowledged.append(entry.event_id)
        return True

    sink = DurableOutboxSink(SQLiteOutbox(path), publish_ack)
    await sink.start()
    await asyncio.wait_for(first_published.wait(), timeout=5)

    # Close while evt-0 is acknowledged but the rest of the batch is in flight.
    # The delay only has to be long enough for a shutdown that abandons the
    # drain to have already done so; a shutdown that stops it at a safe point is
    # still waiting here, so a longer delay would not change the outcome.
    close_task = asyncio.create_task(sink.close())
    await asyncio.sleep(0.2)
    release.set()
    await asyncio.wait_for(close_task, timeout=10)

    assert sorted(acknowledged) == ["evt-0", "evt-1", "evt-2"]
    with SQLiteOutbox(path) as reopened:
        assert reopened.pending() == []


@pytest.mark.asyncio
async def test_close_settles_simultaneous_unsettled_acks_across_lanes(tmp_path: Path) -> None:
    """close() must wait for every lane, not just the first one it samples safe.

    Regression coverage for a narrower version of the same abandoned-
    acknowledgement race: an earlier draft of the shutdown fix waited on each
    lane's "safe to cancel" signal via `event.wait()` tasks and then reaped
    them before cancelling the drains, which put `await` points between
    "observed every lane safe" and "cancel the drains" -- during which a
    lane that looked safe a moment earlier could take on a new unsettled
    acknowledgement. Each lane here gets two entries in one batch: the first
    acknowledges immediately (clearing that lane's safe-to-cancel signal) while
    the second stays in flight, holding the batch's `gather()` -- and so its
    settlement -- open. That keeps both lanes unsettled at once for close() to
    race against.
    """
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path) as original:
        original.admit("heber.live.trades", _event("evt-live-0"))
        original.admit("heber.live.trades", _event("evt-live-1"))
        original.admit("heber:events:backfill", _event("evt-backfill-0"))
        original.admit("heber:events:backfill", _event("evt-backfill-1"))

    both_holding = asyncio.Event()
    release = asyncio.Event()
    holding: set[str] = set()
    acknowledged: list[str] = []

    async def publish_ack(entry: OutboxEntry) -> bool:
        if entry.event_id.endswith("-1"):
            holding.add(entry.event_id)
            if len(holding) == 2:
                both_holding.set()
            await release.wait()
        acknowledged.append(entry.event_id)
        return True

    sink = DurableOutboxSink(SQLiteOutbox(path), publish_ack)
    await sink.start()
    await asyncio.wait_for(both_holding.wait(), timeout=5)

    # Both lanes' "-0" entries have acknowledged by now, each clearing its
    # lane's safe-to-cancel signal; each lane's "-1" entry is what is holding
    # the gather (and thus settlement) open, so both lanes are unsettled.
    close_task = asyncio.create_task(sink.close())
    await asyncio.sleep(0.2)
    release.set()
    await asyncio.wait_for(close_task, timeout=10)

    assert sorted(acknowledged) == ["evt-backfill-0", "evt-backfill-1", "evt-live-0", "evt-live-1"]
    with SQLiteOutbox(path) as reopened:
        assert reopened.pending() == []


@pytest.mark.asyncio
async def test_close_falls_back_to_cancelling_past_the_shutdown_timeout(tmp_path: Path) -> None:
    """The safe-shutdown wait is bounded, not a guarantee -- and that is deliberate.

    If a publish in the same batch as an already-acknowledged entry never
    resolves within `drain_shutdown_timeout_seconds` (e.g. a broker outage
    spanning the whole shutdown window -- the JetStream client puts no timeout
    of its own on a PubAck), close() falls back to cancelling rather than
    hanging shutdown indefinitely. The already-acknowledged entry in that batch
    was never durably settled (the batch's `gather()` never completed), so it
    stays pending and is redelivered on restart -- the same outcome this file
    already accepts elsewhere (settlement failures, a publisher that never
    starts). This is the file's one bounded escape hatch, not a regression: it
    only opens once the configured timeout is exhausted, versus the original
    bug which reproduced on any close() racing an ordinary in-flight publish.
    """
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path) as original:
        original.admit("heber.live.trades", _event("evt-acked"))
        original.admit("heber.live.trades", _event("evt-never-acks"))

    acked_first = asyncio.Event()
    never_release = asyncio.Event()

    async def publish_ack(entry: OutboxEntry) -> bool:
        if entry.event_id == "evt-acked":
            acked_first.set()
            return True
        await never_release.wait()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(path), publish_ack, drain_shutdown_timeout_seconds=0.2)
    await sink.start()
    await asyncio.wait_for(acked_first.wait(), timeout=5)

    start = asyncio.get_running_loop().time()
    await asyncio.wait_for(sink.close(), timeout=5)
    elapsed = asyncio.get_running_loop().time() - start

    # A lower bound close to the configured 0.2s proves close() genuinely spent
    # the wait budget on the unsettled lane rather than cancelling immediately
    # (which would also leave both rows pending, so that alone doesn't
    # distinguish the fallback path from the pre-fix immediate-cancel bug).
    # The upper bound proves it did not hang past that budget.
    assert 0.15 <= elapsed < 2.0
    with SQLiteOutbox(path) as reopened:
        assert sorted(entry.event_id for entry in reopened.pending()) == ["evt-acked", "evt-never-acks"]
