from __future__ import annotations

import asyncio
import multiprocessing
import threading
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
    lock = asyncio.Lock()

    async def publish_ack(_entry: OutboxEntry) -> bool:
        nonlocal active, peak_active, published
        async with lock:
            active += 1
            peak_active = max(peak_active, active)
        await release.wait()
        async with lock:
            active -= 1
            published += 1
            if published == 33:
                finished.set()
        return True

    sink = DurableOutboxSink(SQLiteOutbox(tmp_path / "outbox.sqlite3"), publish_ack)
    await sink.publish_batch_results([("heber:events", _event(f"evt-{index}")) for index in range(33)])
    for _ in range(100):
        if peak_active == 32:
            break
        await asyncio.sleep(0)

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
