from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gateway.core.bulk import BulkBarsRequest, BulkJob, BulkJobManager, BulkJobStatus


def _make_complete_job(job_id: str) -> BulkJob:
    request = BulkBarsRequest(symbols=["AAPL"], start="2025-01-01", end="2025-01-02")
    job = BulkJob(job_id=job_id, job_type="bars", request=request, client_id="test-client")
    job.status = BulkJobStatus.COMPLETE
    job.results = [
        {"symbol": "AAPL", "timestamp": "2025-01-01T00:00:00Z", "close": 100.0},
        {"symbol": "AAPL", "timestamp": "2025-01-01T00:01:00Z", "close": 101.0},
        {"symbol": "AAPL", "timestamp": "2025-01-01T00:02:00Z", "close": 102.0},
        {"symbol": "AAPL", "timestamp": "2025-01-01T00:03:00Z", "close": 103.0},
        {"symbol": "AAPL", "timestamp": "2025-01-01T00:04:00Z", "close": 104.0},
    ]
    return job


def test_iter_results_jsonl_chunks_matches_legacy_jsonl_output() -> None:
    manager = BulkJobManager()
    job = _make_complete_job("bulk-jsonl")
    manager._jobs[job.job_id] = job

    chunks = list(manager.iter_results_jsonl_chunks(job.job_id, records_per_chunk=2))
    reconstructed = b"".join(chunks).decode("utf-8")
    expected = "\n".join(json.dumps(record) for record in job.results)

    assert len(chunks) == 3
    assert reconstructed == expected
    assert manager.get_results_jsonl(job.job_id) == expected


def test_iter_results_jsonl_chunks_returns_empty_for_incomplete_job() -> None:
    manager = BulkJobManager()
    request = BulkBarsRequest(symbols=["AAPL"], start="2025-01-01", end="2025-01-02")
    job = BulkJob(job_id="bulk-pending", job_type="bars", request=request, client_id="test-client")
    job.status = BulkJobStatus.RUNNING
    manager._jobs[job.job_id] = job

    assert list(manager.iter_results_jsonl_chunks(job.job_id)) == []


def test_spooled_results_are_streamed_and_jsonl_encoded() -> None:
    manager = BulkJobManager()
    manager._max_results_in_memory = 2
    manager._spill_results_to_disk = True

    request = BulkBarsRequest(symbols=["AAPL"], start="2025-01-01", end="2025-01-02")
    job = BulkJob(job_id="bulk-spooled", job_type="bars", request=request, client_id="test-client")
    manager._append_job_results(
        job,
        [
            {"symbol": "AAPL", "timestamp": "2025-01-01T00:00:00Z", "close": 100.0},
            {"symbol": "AAPL", "timestamp": "2025-01-01T00:01:00Z", "close": 101.0},
            {"symbol": "AAPL", "timestamp": "2025-01-01T00:02:00Z", "close": 102.0},
        ],
    )
    job.status = BulkJobStatus.COMPLETE
    manager._jobs[job.job_id] = job

    assert job.results == []
    assert job.results_spool_path

    streamed = list(manager.iter_results_jsonl_chunks(job.job_id, records_per_chunk=2))
    reconstructed = b"".join(streamed).decode("utf-8")

    expected_records = manager.get_results_json(job.job_id)
    expected_jsonl = "\n".join(json.dumps(record) for record in expected_records)
    assert reconstructed == expected_jsonl
    assert expected_records[0]["close"] == 100.0
    assert len(expected_records) == 3


@pytest.mark.asyncio
async def test_cleanup_expired_jobs_removes_spool_file() -> None:
    manager = BulkJobManager()
    manager._max_results_in_memory = 1
    manager._spill_results_to_disk = True

    request = BulkBarsRequest(symbols=["AAPL"], start="2025-01-01", end="2025-01-02")
    job = BulkJob(job_id="bulk-expired", job_type="bars", request=request, client_id="test-client")
    manager._append_job_results(
        job,
        [
            {"symbol": "AAPL", "timestamp": "2025-01-01T00:00:00Z", "close": 100.0},
            {"symbol": "AAPL", "timestamp": "2025-01-01T00:01:00Z", "close": 101.0},
        ],
    )
    job.status = BulkJobStatus.COMPLETE
    job.created_at = datetime.now(UTC) - timedelta(seconds=manager.JOB_TTL_SECONDS + 1)
    manager._jobs[job.job_id] = job

    spool_path = Path(job.results_spool_path or "")
    assert spool_path.exists()

    await manager._cleanup_expired_jobs()

    assert job.job_id not in manager._jobs
    assert not spool_path.exists()
