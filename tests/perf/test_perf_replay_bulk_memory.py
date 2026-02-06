"""Perf coverage for replay and bulk memory-sensitive paths.

Run explicitly with:
    pytest -m perf tests/perf -q
"""

import gc
import time
import tracemalloc
from datetime import UTC, datetime

import pytest

from gateway.core.bulk import BulkBarsRequest, BulkJob, BulkJobManager, BulkJobStatus
from gateway.core.replay import (
    ReplayConfig,
    ReplayMessage,
    ReplaySession,
    ReplaySessionManager,
    ReplayState,
)

pytestmark = pytest.mark.perf


@pytest.mark.asyncio
async def test_replay_run_large_message_batch_memory_profile() -> None:
    """Profile replay loop allocations on a large in-memory message batch."""
    manager = ReplaySessionManager()
    config = ReplayConfig(
        name="perf-replay",
        symbols=["AAPL"],
        feeds=["bars"],
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 2, tzinfo=UTC),
        speed=1000.0,
    )

    messages = [
        ReplayMessage(
            feed="stock_bars",
            symbol="AAPL",
            data={
                "open": 150.0,
                "high": 151.0,
                "low": 149.0,
                "close": 150.5,
                "volume": 1000 + i,
            },
            market_timestamp=config.start,
        )
        for i in range(3000)
    ]

    async def loader(_session: ReplaySession) -> list[ReplayMessage]:
        return messages

    manager.set_data_loader(loader)

    session = ReplaySession(
        session_id="perf-replay",
        config=config,
        client_id="perf-client",
        state=ReplayState.RUNNING,
    )
    session.started_at = datetime.now(UTC)
    session.current_timestamp = config.start

    delivered = 0

    async def callback(_message: dict) -> None:
        nonlocal delivered
        delivered += 1

    gc.collect()
    tracemalloc.start()
    start = time.perf_counter()
    await manager._run_replay(session, callback)
    duration = time.perf_counter() - start
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert session.state == ReplayState.COMPLETED
    assert delivered == len(messages)
    assert duration < 0.5
    assert peak_bytes < 1_000_000


@pytest.mark.asyncio
async def test_bulk_results_stream_has_lower_peak_memory_than_jsonl_materialization() -> None:
    """Verify streaming results has materially lower peak allocation than JSONL materialization."""
    manager = BulkJobManager()
    request = BulkBarsRequest(symbols=["AAPL"], start="2025-01-01", end="2025-01-02")
    job = BulkJob(job_id="perf-bulk", job_type="bars", request=request, client_id="perf-client")
    job.status = BulkJobStatus.COMPLETE
    job.results = [
        {
            "symbol": "AAPL",
            "timestamp": f"2025-01-01T00:{i % 60:02d}:00Z",
            "close": 100.0 + (i % 10),
            "volume": 1000 + i,
        }
        for i in range(20000)
    ]
    manager._jobs[job.job_id] = job

    gc.collect()
    tracemalloc.start()
    async for _ in manager.get_results_stream(job.job_id):
        pass
    _, stream_peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    gc.collect()
    tracemalloc.start()
    jsonl_output = manager.get_results_jsonl(job.job_id)
    _, jsonl_peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(jsonl_output) > 1_000_000
    assert stream_peak_bytes < 100_000
    assert jsonl_peak_bytes > stream_peak_bytes * 10
