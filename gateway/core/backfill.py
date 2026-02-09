"""Backfill engine for historical data fetching and sink publishing.

Manages long-running jobs that fetch historical data from providers
and publish results through the DataSinkRegistry -> Redis Streams -> Heber pipeline.
"""

import asyncio
import uuid
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

from gateway.core.envelope import wrap_event
from gateway.core.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

HEBER_EVENTS_TOPIC = "heber:events"

# Default chunk size in days for splitting date ranges
DEFAULT_CHUNK_DAYS = 1

# Per-provider delay between chunks to avoid API throttling
PROVIDER_CHUNK_DELAY_MS: dict[str, int] = {
    "alpaca": 200,
    "unusual_whales": 500,
}
DEFAULT_CHUNK_DELAY_MS = 300


class BackfillStatus(str, Enum):
    """Lifecycle states for a backfill job."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BackfillRequest(BaseModel):
    """Incoming request to start a backfill job."""

    provider: str = Field(description="Provider name: alpaca, unusual_whales")
    feed: str = Field(description="Feed type: bars, trades, quotes, etc.")
    symbols: list[str] = Field(description="Symbols to backfill")
    start: date = Field(description="Start date (inclusive)")
    end: date = Field(description="End date (inclusive)")
    timeframe: str = Field(
        default="1Day",
        description="Bar timeframe (only for bars feed)",
    )
    priority: str = Field(default="normal", description="Job priority: low, normal, high")
    chunk_days: int = Field(
        default=DEFAULT_CHUNK_DAYS,
        ge=1,
        le=30,
        description="Days per fetch chunk",
    )


class SymbolProgress(BaseModel):
    """Progress tracking for a single symbol within a job."""

    symbol: str
    status: str = "pending"
    chunks_total: int = 0
    chunks_complete: int = 0
    records_published: int = 0
    errors: list[str] = Field(default_factory=list)


class BackfillJob(BaseModel):
    """Tracks state and progress of a backfill job."""

    job_id: str = Field(default_factory=lambda: f"bf-{uuid.uuid4().hex[:12]}")
    request: BackfillRequest
    status: BackfillStatus = BackfillStatus.QUEUED
    symbols_progress: dict[str, SymbolProgress] = Field(default_factory=dict)
    records_published: int = 0
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def symbols_complete(self) -> int:
        return sum(
            1 for sp in self.symbols_progress.values() if sp.status in ("complete", "failed")
        )

    @property
    def symbols_total(self) -> int:
        return len(self.symbols_progress)

    @property
    def progress(self) -> float:
        total_chunks = sum(sp.chunks_total for sp in self.symbols_progress.values())
        done_chunks = sum(sp.chunks_complete for sp in self.symbols_progress.values())
        if total_chunks == 0:
            return 0.0
        return round(done_chunks / total_chunks, 4)

    @property
    def eta_seconds(self) -> float | None:
        if not self.started_at or self.progress <= 0:
            return None
        elapsed = (datetime.now(UTC) - self.started_at).total_seconds()
        if self.progress >= 1.0:
            return 0
        return round(elapsed / self.progress * (1.0 - self.progress), 1)


def _date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[datetime, datetime]]:
    """Split a date range into (start_dt, end_dt) chunks."""
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        dt_start = datetime(cursor.year, cursor.month, cursor.day, tzinfo=UTC)
        dt_end = datetime(
            chunk_end.year,
            chunk_end.month,
            chunk_end.day,
            23,
            59,
            59,
            tzinfo=UTC,
        )
        chunks.append((dt_start, dt_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


# Dispatch table: (provider, feed) -> async callable(provider_instance, symbol, start, end, **kwargs) -> list[dict|BaseModel]
# Each function must return a list of normalized event dicts/models.


async def _alpaca_bars(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_bars([sym], kw.get("timeframe", "1Day"), s, e)


async def _alpaca_trades(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_trades([sym], s, e)


async def _alpaca_quotes(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_historical_quotes([sym], s, e)


async def _alpaca_option_trades(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_option_trades([sym], s, e)


async def _alpaca_crypto_bars(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_crypto_bars(sym, kw.get("timeframe", "1Day"), s, e)


async def _alpaca_crypto_trades(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_crypto_trades(sym, s, e)


async def _uw_flow(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_flow_alerts(limit=200)


async def _uw_ticker_flow(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_ticker_flow(sym, date_str=s.strftime("%Y-%m-%d"), limit=200)


async def _uw_darkpool(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_darkpool_recent(limit=200)


async def _uw_darkpool_ticker(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_darkpool_ticker(sym, date_str=s.strftime("%Y-%m-%d"), limit=200)


async def _uw_congress(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_congress_trades(symbol=sym, limit=200)


async def _uw_insiders(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_insiders(symbol=sym, limit=200)


async def _uw_institutions(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_institutions(sym, limit=200)


async def _uw_greeks(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_greek_exposure(sym, date_str=s.strftime("%Y-%m-%d"))


async def _uw_earnings(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_earnings(sym)


BACKFILL_DISPATCH: dict[tuple[str, str], Any] = {
    # Alpaca
    ("alpaca", "bars"): _alpaca_bars,
    ("alpaca", "trades"): _alpaca_trades,
    ("alpaca", "quotes"): _alpaca_quotes,
    ("alpaca", "option_trades"): _alpaca_option_trades,
    ("alpaca", "crypto_bars"): _alpaca_crypto_bars,
    ("alpaca", "crypto_trades"): _alpaca_crypto_trades,
    # Unusual Whales
    ("unusual_whales", "flow_alerts"): _uw_flow,
    ("unusual_whales", "ticker_flow"): _uw_ticker_flow,
    ("unusual_whales", "darkpool"): _uw_darkpool,
    ("unusual_whales", "darkpool_ticker"): _uw_darkpool_ticker,
    ("unusual_whales", "congress_trades"): _uw_congress,
    ("unusual_whales", "insider_trades"): _uw_insiders,
    ("unusual_whales", "institutions"): _uw_institutions,
    ("unusual_whales", "greek_exposure"): _uw_greeks,
    ("unusual_whales", "earnings"): _uw_earnings,
}


class BackfillEngine:
    """Manages backfill jobs: queuing, execution, rate limiting, progress tracking.

    Enforces one running job per provider to prevent API bans.
    Jobs for different providers can run concurrently.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, BackfillJob] = {}
        self._running_tasks: dict[str, asyncio.Task[None]] = {}
        # Lock per provider: at most one backfill job runs per provider
        self._provider_locks: dict[str, asyncio.Lock] = {}
        self._sink_registry: Any = None
        self._provider_registry: Any = None

    def configure(
        self,
        provider_registry: Any,
        sink_registry: Any,
    ) -> None:
        """Wire in registries during app startup."""
        self._provider_registry = provider_registry
        self._sink_registry = sink_registry

    def _get_lock(self, provider: str) -> asyncio.Lock:
        if provider not in self._provider_locks:
            self._provider_locks[provider] = asyncio.Lock()
        return self._provider_locks[provider]

    @property
    def supported_feeds(self) -> list[dict[str, str]]:
        """List supported (provider, feed) pairs."""
        return [{"provider": p, "feed": f} for p, f in BACKFILL_DISPATCH]

    def submit(self, request: BackfillRequest) -> BackfillJob:
        """Validate and queue a new backfill job, then start it in the background."""
        # Validate provider
        if not self._provider_registry:
            raise ValueError("BackfillEngine not configured: missing provider registry")
        provider = self._provider_registry.get(request.provider)
        if provider is None:
            raise ValueError(f"Unknown provider: {request.provider}")

        # Validate dispatch key
        dispatch_key = (request.provider, request.feed)
        if dispatch_key not in BACKFILL_DISPATCH:
            supported = [f for (p, f) in BACKFILL_DISPATCH if p == request.provider]
            raise ValueError(
                f"Unsupported feed '{request.feed}' for provider '{request.provider}'. "
                f"Supported: {supported}"
            )

        # Validate date range
        if request.start > request.end:
            raise ValueError("start must be <= end")

        # Build job
        chunks = _date_chunks(request.start, request.end, request.chunk_days)
        symbols_progress = {
            sym: SymbolProgress(
                symbol=sym,
                chunks_total=len(chunks),
            )
            for sym in request.symbols
        }

        job = BackfillJob(request=request, symbols_progress=symbols_progress)
        self._jobs[job.job_id] = job

        # Kick off in background
        task = asyncio.create_task(self._run_job(job))
        self._running_tasks[job.job_id] = task
        task.add_done_callback(lambda t: self._running_tasks.pop(job.job_id, None))

        logger.info(
            "backfill_job_submitted",
            job_id=job.job_id,
            provider=request.provider,
            feed=request.feed,
            symbols=len(request.symbols),
            chunks=len(chunks),
        )

        return job

    def get_job(self, job_id: str) -> BackfillJob | None:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[BackfillJob]:
        return list(self._jobs.values())

    def cancel(self, job_id: str) -> bool:
        """Cancel a running job. Returns True if cancellation was successful."""
        job = self._jobs.get(job_id)
        if not job or job.status not in (BackfillStatus.QUEUED, BackfillStatus.RUNNING):
            return False

        task = self._running_tasks.get(job_id)
        if task and not task.done():
            task.cancel()

        job.status = BackfillStatus.CANCELLED
        job.completed_at = datetime.now(UTC)
        logger.info("backfill_job_cancelled", job_id=job_id)
        return True

    async def _run_job(self, job: BackfillJob) -> None:
        """Execute a backfill job, respecting per-provider concurrency locks."""
        lock = self._get_lock(job.request.provider)

        async with lock:
            job.status = BackfillStatus.RUNNING
            job.started_at = datetime.now(UTC)

            logger.info(
                "backfill_job_started",
                job_id=job.job_id,
                provider=job.request.provider,
                feed=job.request.feed,
            )

            try:
                await self._execute_job(job)
            except asyncio.CancelledError:
                job.status = BackfillStatus.CANCELLED
                logger.info("backfill_job_cancelled_during_execution", job_id=job.job_id)
                return
            except Exception as e:
                job.status = BackfillStatus.FAILED
                job.errors.append(str(e))
                logger.error(
                    "backfill_job_failed",
                    job_id=job.job_id,
                    error=str(e),
                    exc_info=True,
                )
                return

            # Determine final status
            failed_symbols = [
                sp.symbol for sp in job.symbols_progress.values() if sp.status == "failed"
            ]

            if failed_symbols:
                if len(failed_symbols) == len(job.symbols_progress):
                    job.status = BackfillStatus.FAILED
                else:
                    # Partial success still marked as completed; errors are trackable
                    job.status = BackfillStatus.COMPLETED
            else:
                job.status = BackfillStatus.COMPLETED

            job.completed_at = datetime.now(UTC)
            logger.info(
                "backfill_job_completed",
                job_id=job.job_id,
                status=job.status.value,
                records_published=job.records_published,
                errors=len(job.errors),
            )

    async def _execute_job(self, job: BackfillJob) -> None:
        """Iterate over symbols and date chunks, fetching and publishing data."""
        provider_instance = self._provider_registry.get(job.request.provider)
        if provider_instance is None:
            raise RuntimeError(f"Provider '{job.request.provider}' not available")

        dispatch_fn = BACKFILL_DISPATCH[(job.request.provider, job.request.feed)]
        chunks = _date_chunks(job.request.start, job.request.end, job.request.chunk_days)
        rate_limiter = get_rate_limiter()

        delay_ms = PROVIDER_CHUNK_DELAY_MS.get(job.request.provider, DEFAULT_CHUNK_DELAY_MS)

        for sym in job.request.symbols:
            if job.status == BackfillStatus.CANCELLED:
                break

            sp = job.symbols_progress[sym]
            sp.status = "running"

            for chunk_start, chunk_end in chunks:
                if job.status == BackfillStatus.CANCELLED:
                    break

                try:
                    # Respect rate limits
                    await rate_limiter.acquire(job.request.provider, block=True)

                    # Fetch data from provider
                    results = await dispatch_fn(
                        provider_instance,
                        sym,
                        chunk_start,
                        chunk_end,
                        timeframe=job.request.timeframe,
                    )

                    if not results:
                        sp.chunks_complete += 1
                        continue

                    # Normalize results to list of dicts
                    items = _normalize_results(results)

                    # Wrap each item in an envelope and publish
                    published = await self._publish_items(
                        items=items,
                        provider=job.request.provider,
                        feed=job.request.feed,
                        job_id=job.job_id,
                    )

                    sp.records_published += published
                    job.records_published += published
                    sp.chunks_complete += 1

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    error_msg = f"{sym} chunk {chunk_start.date()}-{chunk_end.date()}: {e}"
                    sp.errors.append(error_msg)
                    job.errors.append(error_msg)
                    sp.chunks_complete += 1  # Move on even on failure
                    logger.warning(
                        "backfill_chunk_failed",
                        job_id=job.job_id,
                        symbol=sym,
                        chunk_start=str(chunk_start.date()),
                        chunk_end=str(chunk_end.date()),
                        error=str(e),
                        exc_info=True,
                    )

                # Inter-chunk delay to stay under rate limits
                await asyncio.sleep(delay_ms / 1000)

            # Mark symbol status
            if sp.errors:
                sp.status = "failed"
            else:
                sp.status = "complete"

    async def _publish_items(
        self,
        items: list[dict[str, Any]],
        provider: str,
        feed: str,
        job_id: str,
    ) -> int:
        """Wrap items in envelopes and publish to sink. Returns count published."""
        if not self._sink_registry:
            logger.warning("backfill_publish_skipped", reason="no sink registry", job_id=job_id)
            return 0

        published = 0
        for item in items:
            envelope = wrap_event(
                event=item,
                provider=provider,
                feed=feed,
                source="backfill",
            )
            await self._sink_registry.publish_all(HEBER_EVENTS_TOPIC, envelope)
            published += 1

        return published

    async def shutdown(self) -> None:
        """Cancel all running jobs on engine shutdown."""
        for job_id, task in list(self._running_tasks.items()):
            if not task.done():
                task.cancel()
                logger.info("backfill_job_cancelled_on_shutdown", job_id=job_id)

        # Wait for tasks to finish
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)
        self._running_tasks.clear()


def _normalize_results(results: Any) -> list[dict[str, Any]]:
    """Convert provider results to a flat list of dicts."""
    if not results:
        return []

    # Already a list
    if isinstance(results, list):
        out = []
        for item in results:
            if hasattr(item, "model_dump"):
                out.append(item.model_dump(mode="json"))
            elif hasattr(item, "__dict__"):
                out.append(vars(item))
            elif isinstance(item, dict):
                out.append(item)
            else:
                out.append({"value": item})
        return out

    # Single dict/model
    if hasattr(results, "model_dump"):
        return [results.model_dump(mode="json")]
    if isinstance(results, dict):
        # May be a nested response like {"items": [...]}
        if "items" in results and isinstance(results["items"], list):
            return _normalize_results(results["items"])
        return [results]

    return []


# Module-level singleton
_engine: BackfillEngine | None = None


def get_backfill_engine() -> BackfillEngine:
    """Get or create the singleton BackfillEngine."""
    global _engine
    if _engine is None:
        _engine = BackfillEngine()
    return _engine
