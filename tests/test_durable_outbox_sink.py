from __future__ import annotations

import asyncio
import multiprocessing
import threading
from pathlib import Path

import pytest

from gateway.core.durable_outbox import OutboxCapacityError, OutboxEntry, SQLiteOutbox
from gateway.core.durable_outbox_sink import DurableOutboxSink


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
    original_admit = outbox.admit

    def blocking_admit(*args: object, **kwargs: object) -> bool:
        entered.set()
        assert release.wait(timeout=5)
        return original_admit(*args, **kwargs)

    outbox.admit = blocking_admit  # type: ignore[method-assign]

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
async def test_publisher_failure_retains_oldest_entry_and_blocks_later_entries(tmp_path: Path) -> None:
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
    assert pending[0].attempts == 1
    assert published == ["evt-1"]

    release_retry.set()
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
