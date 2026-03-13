"""Tests for the backfill engine and API router."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.core.backfill import (
    HEAVYWEIGHT_FEEDS,
    BackfillEngine,
    BackfillJob,
    BackfillRequest,
    BackfillStatus,
    SymbolProgress,
    _date_chunks,
    _normalize_results,
    get_backfill_engine,
)

# ── date chunking ──────────────────────────────────────────────────────


def test_date_chunks_single_day() -> None:
    chunks = _date_chunks(date(2025, 1, 1), date(2025, 1, 1), chunk_days=1)
    assert len(chunks) == 1
    start, end = chunks[0]
    assert start == datetime(2025, 1, 1, tzinfo=UTC)
    assert end == datetime(2025, 1, 1, 23, 59, 59, tzinfo=UTC)


def test_date_chunks_multi_day() -> None:
    chunks = _date_chunks(date(2025, 1, 1), date(2025, 1, 5), chunk_days=2)
    assert len(chunks) == 3
    assert chunks[0][0].date() == date(2025, 1, 1)
    assert chunks[0][1].date() == date(2025, 1, 2)
    assert chunks[1][0].date() == date(2025, 1, 3)
    assert chunks[1][1].date() == date(2025, 1, 4)
    assert chunks[2][0].date() == date(2025, 1, 5)
    assert chunks[2][1].date() == date(2025, 1, 5)


def test_date_chunks_exact_division() -> None:
    chunks = _date_chunks(date(2025, 1, 1), date(2025, 1, 4), chunk_days=2)
    assert len(chunks) == 2


# ── result normalization ───────────────────────────────────────────────


def test_normalize_results_list_of_dicts() -> None:
    items = [{"a": 1}, {"b": 2}]
    assert _normalize_results(items) == [{"a": 1}, {"b": 2}]


def test_normalize_results_empty() -> None:
    assert _normalize_results([]) == []
    assert _normalize_results(None) == []


def test_normalize_results_single_dict() -> None:
    assert _normalize_results({"key": "val"}) == [{"key": "val"}]


def test_normalize_results_nested_items() -> None:
    nested = {"items": [{"x": 1}, {"y": 2}], "count": 2}
    assert _normalize_results(nested) == [{"x": 1}, {"y": 2}]


def test_normalize_results_pydantic_model() -> None:
    class FakeModel:
        def model_dump(self, mode: str = "python") -> dict:
            return {"field": "value"}

    assert _normalize_results([FakeModel()]) == [{"field": "value"}]


# ── BackfillJob properties ────────────────────────────────────────────


def test_job_progress_zero_chunks() -> None:
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    job = BackfillJob(request=req, symbols_progress={})
    assert job.progress == pytest.approx(0.0)
    assert job.symbols_total == 0
    assert job.symbols_complete == 0


def test_job_progress_tracking() -> None:
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL", "MSFT"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    sp1 = SymbolProgress(symbol="AAPL", chunks_total=4, chunks_complete=2, status="running")
    sp2 = SymbolProgress(symbol="MSFT", chunks_total=4, chunks_complete=4, status="complete")
    job = BackfillJob(
        request=req,
        symbols_progress={"AAPL": sp1, "MSFT": sp2},
    )
    assert job.symbols_total == 2
    assert job.symbols_complete == 1  # only MSFT is complete
    assert job.progress == pytest.approx(0.75)  # 6/8 chunks


def test_job_eta_returns_none_before_start() -> None:
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    job = BackfillJob(request=req)
    assert job.eta_seconds is None


# ── BackfillEngine ─────────────────────────────────────────────────────


def _make_engine() -> BackfillEngine:
    """Create a BackfillEngine with mock registries."""
    engine = BackfillEngine()
    provider_registry = MagicMock()
    provider_registry.get.return_value = MagicMock()
    sink_registry = MagicMock()
    sink_registry.publish_all = AsyncMock()
    engine.configure(provider_registry=provider_registry, sink_registry=sink_registry)
    return engine


def test_submit_validates_provider() -> None:
    engine = BackfillEngine()
    engine.configure(provider_registry=MagicMock(), sink_registry=MagicMock())
    engine._provider_registry.get.return_value = None

    req = BackfillRequest(
        provider="nonexistent",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    with pytest.raises(ValueError, match="Unknown provider"):
        engine.submit(req)


def test_submit_validates_feed() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="nonexistent_feed",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    with pytest.raises(ValueError, match="Unsupported feed"):
        engine.submit(req)


def test_submit_validates_date_range() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 5),
        end=date(2025, 1, 1),
    )
    with pytest.raises(ValueError, match="start must be <= end"):
        engine.submit(req)


def test_submit_rejects_wildcard_symbol() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["*"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    fake_task = MagicMock()
    with (
        patch("gateway.core.backfill.asyncio.create_task", return_value=fake_task),
        pytest.raises(ValueError, match="Wildcard symbol"),
    ):
        engine.submit(req)


def test_submit_rejects_stock_symbols_for_alpaca_crypto_feed() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="crypto_bars",
        symbols=["AAPL", "MSFT"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )

    fake_task = MagicMock()

    def _fake_create_task(coro):
        coro.close()
        return fake_task

    with (
        patch("gateway.core.backfill.asyncio.create_task", side_effect=_fake_create_task),
        pytest.raises(ValueError, match="Invalid symbols for alpaca feed 'crypto_bars'"),
    ):
        engine.submit(req)


def test_submit_accepts_valid_crypto_symbols_for_alpaca_crypto_feed() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="crypto_bars",
        symbols=["btc/usd", "eth/usdt"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )

    fake_task = MagicMock()

    def _fake_create_task(coro):
        coro.close()
        return fake_task

    with patch("gateway.core.backfill.asyncio.create_task", side_effect=_fake_create_task):
        job = engine.submit(req)
    assert job.request.symbols == ["BTC/USD", "ETH/USDT"]

    # Cleanup: cancel the background task
    engine.cancel(job.job_id)


@pytest.mark.asyncio
async def test_submit_creates_job_with_correct_structure() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL", "MSFT"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 3),
        chunk_days=1,
    )
    job = engine.submit(req)

    assert job.job_id.startswith("bf-")
    assert job.status == BackfillStatus.QUEUED
    assert "AAPL" in job.symbols_progress
    assert "MSFT" in job.symbols_progress
    assert job.symbols_progress["AAPL"].chunks_total == 3

    # Cleanup: cancel the background task
    engine.cancel(job.job_id)


def test_get_job_returns_none_for_unknown() -> None:
    engine = _make_engine()
    assert engine.get_job("nonexistent") is None


@pytest.mark.asyncio
async def test_list_jobs() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    job = engine.submit(req)
    assert len(engine.list_jobs()) == 1
    engine.cancel(job.job_id)


@pytest.mark.asyncio
async def test_cancel_running_job() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    job = engine.submit(req)
    assert engine.cancel(job.job_id)
    assert job.status == BackfillStatus.CANCELLED


def test_cancel_nonexistent_job_returns_false() -> None:
    engine = _make_engine()
    assert engine.cancel("nonexistent") is False


def test_supported_feeds() -> None:
    engine = _make_engine()
    feeds = engine.supported_feeds
    assert isinstance(feeds, list)
    assert len(feeds) > 0
    assert all("provider" in f and "feed" in f for f in feeds)
    # Verify alpaca bars is in the list
    assert {"provider": "alpaca", "feed": "bars"} in feeds


def test_submit_without_config_raises() -> None:
    engine = BackfillEngine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    with pytest.raises(ValueError, match="not configured"):
        engine.submit(req)


# ── Full job execution ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_executes_and_publishes() -> None:
    engine = _make_engine()

    # Mock the dispatch function to return data
    mock_data = [{"symbol": "AAPL", "close": 150.0, "timestamp": "2025-01-01T00:00:00Z"}]

    with (
        patch.dict(
            "gateway.core.backfill.BACKFILL_DISPATCH",
            {("alpaca", "bars"): AsyncMock(return_value=mock_data)},
        ),
        patch(
            "gateway.core.backfill.get_rate_limiter",
            return_value=MagicMock(acquire=AsyncMock()),
        ),
        patch(
            "gateway.core.backfill.wrap_event",
            return_value={"event_id": "test", "payload": mock_data[0]},
        ),
    ):
        req = BackfillRequest(
            provider="alpaca",
            feed="bars",
            symbols=["AAPL"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 1),
        )
        job = engine.submit(req)

        # Wait for background task
        await asyncio.sleep(0.5)

        assert job.status == BackfillStatus.COMPLETED
        assert job.records_published == 1
        assert job.symbols_progress["AAPL"].status == "complete"
        engine._sink_registry.publish_all.assert_called()


@pytest.mark.asyncio
async def test_job_handles_provider_error_gracefully() -> None:
    engine = _make_engine()

    with (
        patch.dict(
            "gateway.core.backfill.BACKFILL_DISPATCH",
            {("alpaca", "bars"): AsyncMock(side_effect=RuntimeError("API error"))},
        ),
        patch(
            "gateway.core.backfill.get_rate_limiter",
            return_value=MagicMock(acquire=AsyncMock()),
        ),
    ):
        req = BackfillRequest(
            provider="alpaca",
            feed="bars",
            symbols=["AAPL"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 1),
        )
        job = engine.submit(req)

        await asyncio.sleep(0.5)

        # Job completes despite errors (partial failure tracking)
        assert job.status in (BackfillStatus.COMPLETED, BackfillStatus.FAILED)
        assert len(job.errors) > 0
        assert job.symbols_progress["AAPL"].status == "failed"


@pytest.mark.asyncio
async def test_engine_shutdown_cancels_running_tasks() -> None:
    engine = _make_engine()

    # Create a slow dispatch function
    async def slow_dispatch(*args, **kwargs):
        await asyncio.sleep(10)
        return [{"data": "never"}]

    with (
        patch.dict(
            "gateway.core.backfill.BACKFILL_DISPATCH",
            {("alpaca", "bars"): slow_dispatch},
        ),
        patch(
            "gateway.core.backfill.get_rate_limiter",
            return_value=MagicMock(acquire=AsyncMock()),
        ),
    ):
        req = BackfillRequest(
            provider="alpaca",
            feed="bars",
            symbols=["AAPL"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 1),
        )
        engine.submit(req)
        await asyncio.sleep(0.1)

        await engine.shutdown()
        assert len(engine._running_tasks) == 0


# ── API Router Tests ───────────────────────────────────────────────────


def test_backfill_api_submit(client, test_api_key) -> None:
    mock_job = BackfillJob(
        request=BackfillRequest(
            provider="alpaca",
            feed="bars",
            symbols=["AAPL"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 1),
        ),
    )
    mock_engine = MagicMock()
    mock_engine.submit.return_value = mock_job

    with patch("gateway.api.backfill.get_backfill_engine", return_value=mock_engine):
        response = client.post(
            "/api/v1/backfill",
            json={
                "provider": "alpaca",
                "feed": "bars",
                "symbols": ["AAPL"],
                "start": "2025-01-01",
                "end": "2025-01-01",
            },
            headers={"X-Gateway-Key": test_api_key},
        )

    assert response.status_code == 202
    data = response.json()
    assert data["success"] is True
    assert "job_id" in data["data"]


def test_backfill_api_get_status_not_found(client, test_api_key) -> None:
    mock_engine = MagicMock()
    mock_engine.get_job.return_value = None

    with patch("gateway.api.backfill.get_backfill_engine", return_value=mock_engine):
        response = client.get(
            "/api/v1/backfill/nonexistent",
            headers={"X-Gateway-Key": test_api_key},
        )
    assert response.status_code == 404


# ── Singleton ──────────────────────────────────────────────────────────


def test_get_backfill_engine_returns_singleton() -> None:
    with patch("gateway.core.backfill._engine", None):
        e1 = get_backfill_engine()
        e2 = get_backfill_engine()
        assert e1 is e2


# ── Feed Weight Classification Tests ──────────────────────────────────


def test_feed_weight_classification() -> None:
    """Trades-like feeds should be classified as heavyweight."""
    assert "trades" in HEAVYWEIGHT_FEEDS
    assert "option_trades" in HEAVYWEIGHT_FEEDS
    assert "crypto_trades" in HEAVYWEIGHT_FEEDS
    # Lightweight feeds should not be in HEAVYWEIGHT_FEEDS
    assert "bars" not in HEAVYWEIGHT_FEEDS
    assert "quotes" not in HEAVYWEIGHT_FEEDS
    assert "news" not in HEAVYWEIGHT_FEEDS


def test_feed_weighted_semaphores() -> None:
    """Engine should create separate semaphores for lightweight and heavyweight feeds."""
    engine = BackfillEngine(
        lightweight_concurrency=5,
        heavyweight_concurrency=2,
    )
    # Get semaphores for the same provider, different feed weights
    bars_sem = engine._get_semaphore("alpaca", "bars")
    trades_sem = engine._get_semaphore("alpaca", "trades")

    # They should be different semaphore objects
    assert bars_sem is not trades_sem

    # Same weight class should return the same semaphore
    bars_sem2 = engine._get_semaphore("alpaca", "quotes")
    assert bars_sem is bars_sem2

    trades_sem2 = engine._get_semaphore("alpaca", "option_trades")
    assert trades_sem is trades_sem2


@pytest.mark.asyncio
async def test_lightweight_not_blocked_by_heavyweight() -> None:
    """A running heavyweight job should not block lightweight jobs from starting."""
    engine = BackfillEngine(
        lightweight_concurrency=3,
        heavyweight_concurrency=1,
    )
    provider_registry = MagicMock()
    provider_registry.get.return_value = MagicMock()
    sink_registry = MagicMock()
    sink_registry.publish_all = AsyncMock()
    engine.configure(provider_registry=provider_registry, sink_registry=sink_registry)

    # Acquire the heavyweight semaphore to simulate a running trades job
    heavyweight_sem = engine._get_semaphore("alpaca", "trades")
    await heavyweight_sem.acquire()

    # Lightweight semaphore should still be available
    lightweight_sem = engine._get_semaphore("alpaca", "bars")
    acquired = lightweight_sem._value > 0
    assert acquired, "Lightweight semaphore should not be blocked by heavyweight jobs"

    # Release to clean up
    heavyweight_sem.release()


# ── cancel_all / flush / expiry ────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_all_cancels_running_jobs() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    job1 = engine.submit(req)
    job2 = engine.submit(req)

    cancelled = engine.cancel_all()
    assert cancelled == 2
    assert job1.status == BackfillStatus.CANCELLED
    assert job2.status == BackfillStatus.CANCELLED


@pytest.mark.asyncio
async def test_flush_purges_job_history() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    engine.submit(req)
    engine.submit(req)
    assert len(engine.list_jobs()) == 2

    purged = engine.flush()
    assert purged == 2
    assert len(engine.list_jobs()) == 0


def test_stale_job_auto_expiry() -> None:
    engine = _make_engine()
    req = BackfillRequest(
        provider="alpaca",
        feed="bars",
        symbols=["AAPL"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
    )
    # Manually add a stale completed job (completed 2 hours ago)
    from gateway.core.backfill import JOB_EXPIRY_SECONDS

    job = BackfillJob(request=req)
    job.status = BackfillStatus.COMPLETED
    job.completed_at = datetime.now(UTC) - timedelta(seconds=JOB_EXPIRY_SECONDS + 60)
    engine._jobs[job.job_id] = job

    # Add a fresh completed job (should NOT be pruned)
    fresh_job = BackfillJob(request=req)
    fresh_job.status = BackfillStatus.COMPLETED
    fresh_job.completed_at = datetime.now(UTC)
    engine._jobs[fresh_job.job_id] = fresh_job

    assert len(engine.list_jobs()) == 2

    # Trigger prune via submit
    engine._prune_stale_jobs()

    assert len(engine.list_jobs()) == 1
    assert engine.get_job(fresh_job.job_id) is not None
    assert engine.get_job(job.job_id) is None


@pytest.mark.asyncio
async def test_concurrent_symbol_processing() -> None:
    """Symbols within a job should be processed concurrently (up to semaphore limit)."""
    engine = BackfillEngine(symbol_concurrency=3)
    provider_registry = MagicMock()
    provider_registry.get.return_value = MagicMock()
    sink_registry = MagicMock()
    sink_registry.publish_all = AsyncMock()
    engine.configure(provider_registry=provider_registry, sink_registry=sink_registry)

    # Track concurrent execution
    active_count = 0
    max_concurrent = 0
    lock = asyncio.Lock()

    async def tracking_dispatch(*args, **kwargs):
        nonlocal active_count, max_concurrent
        async with lock:
            active_count += 1
            max_concurrent = max(max_concurrent, active_count)
        await asyncio.sleep(0.1)
        async with lock:
            active_count -= 1
        return [{"data": "value"}]

    with (
        patch.dict(
            "gateway.core.backfill.BACKFILL_DISPATCH",
            {("alpaca", "bars"): tracking_dispatch},
        ),
        patch(
            "gateway.core.backfill.get_rate_limiter",
            return_value=MagicMock(acquire=AsyncMock()),
        ),
        patch(
            "gateway.core.backfill.wrap_event",
            return_value={"event_id": "test", "payload": {}},
        ),
    ):
        req = BackfillRequest(
            provider="alpaca",
            feed="bars",
            symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "META"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 1),
        )
        job = engine.submit(req)
        await asyncio.sleep(1.5)

        assert job.status == BackfillStatus.COMPLETED
        # With concurrency=3, we should see at most 3 symbols running at once
        assert max_concurrent <= 3
        # But more than 1 (proving concurrency works)
        assert max_concurrent > 1


# ── New API Endpoint Tests ─────────────────────────────────────────────


def test_backfill_api_cancel_all(client, test_api_key) -> None:
    mock_engine = MagicMock()
    mock_engine.cancel_all.return_value = 3

    with patch("gateway.api.backfill.get_backfill_engine", return_value=mock_engine):
        response = client.post(
            "/api/v1/backfill/cancel-all",
            headers={"X-Gateway-Key": test_api_key},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["meta"]["cancelled"] == 3


def test_backfill_api_flush(client, test_api_key) -> None:
    mock_engine = MagicMock()
    mock_engine.flush.return_value = 5

    with patch("gateway.api.backfill.get_backfill_engine", return_value=mock_engine):
        response = client.delete(
            "/api/v1/backfill",
            headers={"X-Gateway-Key": test_api_key},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["meta"]["purged"] == 5
