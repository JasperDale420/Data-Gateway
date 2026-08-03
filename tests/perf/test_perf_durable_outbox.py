"""Local durable-outbox throughput harnesses; not a broker or Heber E2E test."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from gateway.core.durable_outbox import SQLiteOutbox
from gateway.core.durable_outbox_sink import DurableOutboxSink

pytestmark = pytest.mark.perf


def _envelope(index: int) -> dict[str, object]:
    return {
        "event_id": f"benchmark-{index}",
        "feed": "flow_alerts",
        "payload": "x" * 900,
    }


@pytest.mark.asyncio
async def test_outbox_admission_rate_for_one_kib_envelopes(tmp_path: Path) -> None:
    """Measure the production outbox path shape with bounded FULL SQLite commits."""
    event_count = 5_000
    path = tmp_path / "state" / "outbox" / "events.sqlite3"
    outbox = SQLiteOutbox(path)

    async def blocked_publisher(_entry: object) -> bool:
        await asyncio.Event().wait()
        return True

    sink = DurableOutboxSink(outbox, blocked_publisher)
    started = time.perf_counter()
    await asyncio.gather(*(sink.publish("heber:events", _envelope(index)) for index in range(event_count)))
    elapsed = time.perf_counter() - started
    rate = event_count / elapsed

    assert outbox.summary().pending_count == event_count
    assert rate >= 7_500
    await sink.close()
    print(f"durable_outbox_admission_events_per_second={rate:.0f}")


@pytest.mark.asyncio
async def test_outbox_drain_rate_with_local_ack_harness(tmp_path: Path) -> None:
    """Measure SQLite-to-publisher draining only; it excludes broker and Heber."""
    event_count = 1_000
    path = tmp_path / "state" / "outbox" / "events.sqlite3"
    outbox = SQLiteOutbox(path)
    outbox.admit_many([("heber:events", _envelope(index)) for index in range(event_count)])

    async def acknowledge(_entry: object) -> bool:
        return True

    sink = DurableOutboxSink(outbox, acknowledge)
    started = time.perf_counter()
    await sink.start()
    while outbox.summary().pending_count:
        await asyncio.sleep(0.001)
    elapsed = time.perf_counter() - started
    await sink.close()

    print(f"durable_outbox_local_drain_events_per_second={event_count / elapsed:.0f}")
