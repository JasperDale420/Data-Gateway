"""Backfill engine for historical data fetching and sink publishing.

Manages long-running jobs that fetch historical data from providers
and publish results through the DataSinkRegistry -> Redis Streams -> Heber pipeline.
"""

import asyncio
import uuid
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from gateway.core.envelope import wrap_event
from gateway.core.logger import logger
from gateway.core.rate_limiter import ProviderRateLimitManager, get_rate_limiter
from gateway.core.security import InputValidator

_INPUT_VALIDATOR = InputValidator()

HEBER_EVENTS_TOPIC = "heber:events"

# Default chunk size in days for splitting date ranges
DEFAULT_CHUNK_DAYS = 1

# Per-provider delay between chunks to avoid API throttling
PROVIDER_CHUNK_DELAY_MS: dict[str, int] = {
    "alpaca": 50,
    "unusual_whales": 500,
}
DEFAULT_CHUNK_DELAY_MS = 300

# Max concurrent symbol fetches within a single backfill job
DEFAULT_SYMBOL_CONCURRENCY = 10

# Feed weight classification for concurrency scheduling
# Lightweight feeds complete quickly and can run many in parallel.
# Heavyweight feeds transfer large volumes and need limited concurrency.
HEAVYWEIGHT_FEEDS = frozenset({"trades", "option_trades", "crypto_trades"})

# Default concurrency per weight class per provider
DEFAULT_LIGHTWEIGHT_CONCURRENCY = 5
DEFAULT_HEAVYWEIGHT_CONCURRENCY = 2
ALPACA_CRYPTO_FEEDS = frozenset({"crypto_bars", "crypto_trades"})

# Auto-expire completed/failed/cancelled jobs after this duration (seconds)
JOB_EXPIRY_SECONDS = 3600


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
        return sum(1 for sp in self.symbols_progress.values() if sp.status in ("complete", "failed"))

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
    return await p.get_bars([sym], kw.get("timeframe", "1Day"), s, e, adjustment=kw.get("adjustment", "split"))


async def _alpaca_trades(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_trades([sym], s, e)


async def _alpaca_quotes(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_historical_quotes([sym], s, e)


async def _alpaca_option_trades(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_option_trades([sym], s, e)


async def _alpaca_option_bars(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_option_bars(contracts=[sym], timeframe=kw.get("timeframe", "1Day"), start=s, end=e)


async def _alpaca_option_quotes(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_historical_option_quotes(contracts=[sym], start=s, end=e)


async def _alpaca_crypto_quotes(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    return await p.get_historical_crypto_quotes(pair=sym, start=s, end=e)


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


async def _alpaca_news(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # Alpaca news endpoint accepts multiple symbols, but backfill engine drives per-symbol
    return await p.get_news(symbols=[sym], start=s, end=e, include_content=True)


async def _uw_market_tide(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # sym is ignored — market tide is market-wide. Date filters by chunk start.
    return await p.get_market_tide(date_str=s.strftime("%Y-%m-%d"))


async def _uw_sector_tide(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # sym is the GICS sector name (e.g. "Technology"). Date filters by chunk start.
    return await p.get_sector_tide(sector=sym, date_str=s.strftime("%Y-%m-%d"))


BACKFILL_DISPATCH: dict[tuple[str, str], Any] = {
    # Alpaca
    ("alpaca", "bars"): _alpaca_bars,
    ("alpaca", "trades"): _alpaca_trades,
    ("alpaca", "quotes"): _alpaca_quotes,
    ("alpaca", "option_trades"): _alpaca_option_trades,
    ("alpaca", "option_bars"): _alpaca_option_bars,
    ("alpaca", "option_quotes"): _alpaca_option_quotes,
    ("alpaca", "crypto_bars"): _alpaca_crypto_bars,
    ("alpaca", "crypto_trades"): _alpaca_crypto_trades,
    ("alpaca", "crypto_quotes"): _alpaca_crypto_quotes,
    ("alpaca", "news"): _alpaca_news,
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
    ("unusual_whales", "market_tide"): _uw_market_tide,
    ("unusual_whales", "sector_tide"): _uw_sector_tide,
}


class BackfillEngine:
    """Manages backfill jobs: queuing, execution, rate limiting, progress tracking.

    Uses feed-weighted concurrency: lightweight feeds (bars, quotes, news) get
    higher concurrent slots than heavyweight feeds (trades) to prevent starvation.
    Jobs for different providers can run concurrently.
    """

    def __init__(
        self,
        symbol_concurrency: int = DEFAULT_SYMBOL_CONCURRENCY,
        lightweight_concurrency: int = DEFAULT_LIGHTWEIGHT_CONCURRENCY,
        heavyweight_concurrency: int = DEFAULT_HEAVYWEIGHT_CONCURRENCY,
    ) -> None:
        self._jobs: dict[str, BackfillJob] = {}
        self._running_tasks: dict[str, asyncio.Task[None]] = {}
        # Separate semaphores per provider per weight class
        self._lightweight_semaphores: dict[str, asyncio.Semaphore] = {}
        self._heavyweight_semaphores: dict[str, asyncio.Semaphore] = {}
        self._sink_registry: Any = None
        self._provider_registry: Any = None
        self._symbol_concurrency = symbol_concurrency
        self._lightweight_concurrency = max(1, lightweight_concurrency)
        self._heavyweight_concurrency = max(1, heavyweight_concurrency)
        self._instance_id = uuid.uuid4().hex[:8]
        logger.info("backfill_engine_init", instance_id=self._instance_id)

    def configure(
        self,
        provider_registry: Any,
        sink_registry: Any,
    ) -> None:
        """Wire in registries during app startup."""
        self._provider_registry = provider_registry
        self._sink_registry = sink_registry

    def _get_semaphore(self, provider: str, feed: str) -> asyncio.Semaphore:
        """Get the appropriate semaphore based on feed weight."""
        if feed in HEAVYWEIGHT_FEEDS:
            if provider not in self._heavyweight_semaphores:
                self._heavyweight_semaphores[provider] = asyncio.Semaphore(self._heavyweight_concurrency)
            return self._heavyweight_semaphores[provider]
        if provider not in self._lightweight_semaphores:
            self._lightweight_semaphores[provider] = asyncio.Semaphore(self._lightweight_concurrency)
        return self._lightweight_semaphores[provider]

    @property
    def supported_feeds(self) -> list[dict[str, str]]:
        """List supported (provider, feed) pairs."""
        return [{"provider": p, "feed": f} for p, f in BACKFILL_DISPATCH]

    def submit(self, request: BackfillRequest) -> BackfillJob:
        """Validate and queue a new backfill job, then start it in the background."""
        self._prune_stale_jobs()

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
                f"Unsupported feed '{request.feed}' for provider '{request.provider}'. Supported: {supported}"
            )

        # Validate date range
        if request.start > request.end:
            raise ValueError("start must be <= end")

        # Validate and normalize symbols
        normalized_symbols: list[str] = []
        invalid_symbols: list[str] = []

        for raw_symbol in request.symbols:
            symbol = raw_symbol.strip().upper()
            if not symbol:
                invalid_symbols.append(raw_symbol)
                continue
            if "*" in symbol:
                invalid_symbols.append(raw_symbol)
                continue
            normalized_symbols.append(symbol)

        if invalid_symbols:
            logger.warning(
                "backfill_invalid_symbols",
                provider=request.provider,
                feed=request.feed,
                invalid_symbols=invalid_symbols,
            )
            raise ValueError(
                "Wildcard symbol '*' is not supported for backfill. Provide explicit symbols like ['AAPL', 'MSFT']."
            )

        if not normalized_symbols:
            raise ValueError("symbols must include at least one valid symbol")

        if request.provider == "alpaca" and request.feed in ALPACA_CRYPTO_FEEDS:
            invalid_crypto_symbols = [
                symbol
                for symbol in normalized_symbols
                if _INPUT_VALIDATOR.validate_symbol(symbol, symbol_type="crypto") is not None
            ]
            if invalid_crypto_symbols:
                logger.warning(
                    "backfill_invalid_crypto_symbols",
                    provider=request.provider,
                    feed=request.feed,
                    invalid_symbols=invalid_crypto_symbols,
                    total_symbols=len(normalized_symbols),
                )
                raise ValueError(
                    f"Invalid symbols for alpaca feed '{request.feed}': {invalid_crypto_symbols}. "
                    "Expected crypto pairs like ['BTC/USD', 'ETH/USD']."
                )

        request.symbols = normalized_symbols

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
        logger.info("backfill_job_created", instance_id=self._instance_id, job_id=job.job_id, job_obj_id=id(job))

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
        job = self._jobs.get(job_id)
        if job:
            logger.debug(
                "backfill_job_read",
                instance_id=self._instance_id,
                job_id=job_id,
                job_obj_id=id(job),
                records=job.records_published,
            )
        else:
            logger.warning("backfill_job_not_found", instance_id=self._instance_id, job_id=job_id)
        return job

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

    def cancel_all(self) -> int:
        """Cancel all running and queued jobs. Returns count of cancelled jobs."""
        cancelled = 0
        for job_id in self._jobs:
            if self.cancel(job_id):
                cancelled += 1
        logger.info("backfill_cancel_all", cancelled=cancelled)
        return cancelled

    def flush(self) -> int:
        """Cancel all jobs and purge job history. Returns count of removed jobs."""
        self.cancel_all()
        count = len(self._jobs)
        self._jobs.clear()
        self._running_tasks.clear()
        logger.info("backfill_flushed", purged=count)
        return count

    def _prune_stale_jobs(self) -> None:
        """Remove completed/failed/cancelled jobs older than JOB_EXPIRY_SECONDS."""
        now = datetime.now(UTC)
        terminal_statuses = {BackfillStatus.COMPLETED, BackfillStatus.FAILED, BackfillStatus.CANCELLED}
        stale_ids = [
            jid
            for jid, job in self._jobs.items()
            if job.status in terminal_statuses
            and job.completed_at
            and (now - job.completed_at).total_seconds() > JOB_EXPIRY_SECONDS
        ]
        for jid in stale_ids:
            del self._jobs[jid]
        if stale_ids:
            logger.info("backfill_stale_jobs_pruned", count=len(stale_ids))

    async def _run_job(self, job: BackfillJob) -> None:
        """Execute a backfill job, respecting feed-weighted concurrency limits."""
        semaphore = self._get_semaphore(job.request.provider, job.request.feed)

        async with semaphore:
            job.status = BackfillStatus.RUNNING
            job.started_at = datetime.now(UTC)

            logger.info(
                "backfill_job_started",
                instance_id=self._instance_id,
                job_obj_id=id(job),
                job_id=job.job_id,
                provider=job.request.provider,
                feed=job.request.feed,
            )

            try:
                await self._execute_job(job)
            except asyncio.CancelledError:
                job.status = BackfillStatus.CANCELLED
                logger.info("backfill_job_cancelled_during_execution", job_id=job.job_id)
                raise
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
            failed_symbols = [sp.symbol for sp in job.symbols_progress.values() if sp.status == "failed"]

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
        """Fetch and publish data for all symbols, iterating date-first.

        Iterates date chunks in the outer loop and symbols concurrently in the
        inner loop. This ensures all events for a given date range arrive
        together in the Redis stream, giving the downstream Heber consumer
        better partition locality and fewer tiny parquet files.
        """
        provider_instance = self._provider_registry.get(job.request.provider)
        if provider_instance is None:
            raise RuntimeError(f"Provider '{job.request.provider}' not available")

        dispatch_fn = BACKFILL_DISPATCH[(job.request.provider, job.request.feed)]
        chunks = _date_chunks(job.request.start, job.request.end, job.request.chunk_days)
        rate_limiter = get_rate_limiter()
        delay_ms = PROVIDER_CHUNK_DELAY_MS.get(job.request.provider, DEFAULT_CHUNK_DELAY_MS)

        sem = asyncio.Semaphore(self._symbol_concurrency)

        # Mark all symbols as running
        for sym in job.request.symbols:
            job.symbols_progress[sym].status = "running"

        for chunk_start, chunk_end in chunks:
            if job.status == BackfillStatus.CANCELLED:
                break

            async def _bounded_chunk(
                sym: str,
                _chunk_start: datetime = chunk_start,
                _chunk_end: datetime = chunk_end,
            ) -> None:
                async with sem:
                    if job.status == BackfillStatus.CANCELLED:
                        return
                    sp = job.symbols_progress[sym]
                    await self._process_chunk(
                        job,
                        sp,
                        sym,
                        _chunk_start,
                        _chunk_end,
                        dispatch_fn,
                        provider_instance,
                        rate_limiter,
                    )

            tasks = [asyncio.create_task(_bounded_chunk(sym)) for sym in job.request.symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, result in zip(job.request.symbols, results, strict=True):
                if isinstance(result, Exception):
                    error_msg = f"{sym}: unhandled error in chunk {chunk_start.date()}: {result}"
                    job.errors.append(error_msg)
                    logger.error(
                        "backfill_symbol_unhandled_error",
                        job_id=job.job_id,
                        symbol=sym,
                        error=str(result),
                    )

            await asyncio.sleep(delay_ms / 1000)

        # Finalize symbol statuses
        for sym, sp in job.symbols_progress.items():
            if sp.status == "running":
                sp.status = "failed" if sp.errors else "complete"

    async def _process_symbol(
        self,
        job: BackfillJob,
        sym: str,
        chunks: list[tuple[datetime, datetime]],
        dispatch_fn: Any,
        provider_instance: Any,
        rate_limiter: ProviderRateLimitManager,
        delay_ms: int,
    ) -> None:
        """Fetch and publish all chunks for a single symbol.

        Retained for callers outside the main ``_execute_job`` path.
        The primary backfill path now iterates date-first in ``_execute_job``.
        """
        sp = job.symbols_progress[sym]
        sp.status = "running"

        for chunk_start, chunk_end in chunks:
            if job.status == BackfillStatus.CANCELLED:
                break
            await self._process_chunk(
                job,
                sp,
                sym,
                chunk_start,
                chunk_end,
                dispatch_fn,
                provider_instance,
                rate_limiter,
            )
            await asyncio.sleep(delay_ms / 1000)

        sp.status = "failed" if sp.errors else "complete"

    async def _process_chunk(
        self,
        job: BackfillJob,
        sp: SymbolProgress,
        sym: str,
        chunk_start: datetime,
        chunk_end: datetime,
        dispatch_fn: Any,
        provider_instance: Any,
        rate_limiter: ProviderRateLimitManager,
    ) -> None:
        """Fetch and publish a single date chunk for a symbol."""
        try:
            await rate_limiter.acquire(job.request.provider, block=True)
            results = await dispatch_fn(
                provider_instance,
                sym,
                chunk_start,
                chunk_end,
                timeframe=job.request.timeframe,
            )
            if not results:
                sp.chunks_complete += 1
                return

            items = await asyncio.to_thread(_normalize_results, results)
            published = await self._publish_items(
                items=items,
                provider=job.request.provider,
                feed=job.request.feed,
                job_id=job.job_id,
            )
            # Surface a sink shortfall in the job result so a partial publish is
            # a detectable, re-runnable gap rather than a silent undercount.
            if published < len(items):
                shortfall = (
                    f"{sym} chunk {chunk_start.date()}-{chunk_end.date()}: "
                    f"sink published {published}/{len(items)} (partial failure)"
                )
                sp.errors.append(shortfall)
                job.errors.append(shortfall)
            sp.records_published += published
            job.records_published += published
            sp.chunks_complete += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            error_msg = f"{sym} chunk {chunk_start.date()}-{chunk_end.date()}: {e}"
            sp.errors.append(error_msg)
            job.errors.append(error_msg)
            sp.chunks_complete += 1
            logger.warning(
                "backfill_chunk_failed",
                job_id=job.job_id,
                symbol=sym,
                chunk_start=str(chunk_start.date()),
                chunk_end=str(chunk_end.date()),
                error=str(e),
                exc_info=True,
            )

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

        # Sort by timestamp for downstream partition locality — Heber's consumer
        # partitions Silver by date, so grouping events by date in the stream
        # means fewer, larger partition flushes instead of many tiny files.
        items.sort(key=lambda x: x.get("timestamp") or x.get("t") or "")

        messages: list[tuple[str, dict[str, Any]]] = []
        for item in items:
            envelope = wrap_event(
                event=item,
                provider=provider,
                feed=feed,
                source="backfill",
            )
            messages.append((HEBER_EVENTS_TOPIC, envelope))

        # Prefer the per-message results API: publish_all_batch returns only an
        # aggregate count, which under a partial Redis pipeline failure can't
        # tell which events landed — so the chunk was marked complete with an
        # undercount and the missing rows became an undetectable hole in Heber.
        if hasattr(self._sink_registry, "publish_all_batch_results"):
            results = await self._sink_registry.publish_all_batch_results(messages)
            published = sum(1 for ok in results if ok)
            if published < len(messages):
                logger.warning(
                    "backfill_publish_partial",
                    job_id=job_id,
                    provider=provider,
                    feed=feed,
                    attempted=len(messages),
                    published=published,
                    failed=len(messages) - published,
                    hint="events that did not confirm are not retried by backfill; "
                    "re-run this chunk to fill the gap (unless the sink was circuit-open, "
                    "in which case buffered events drain on reconnect)",
                )
            return published

        # Fallback: individual publishes for registries without batch support
        published = 0
        for topic, envelope in messages:
            await self._sink_registry.publish_all(topic, envelope)
            published += 1
        return published

    async def shutdown(self) -> None:
        """Cancel all running jobs on engine shutdown."""
        for job_id, task in self._running_tasks.items():
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


def get_backfill_engine(
    lightweight_concurrency: int = DEFAULT_LIGHTWEIGHT_CONCURRENCY,
    heavyweight_concurrency: int = DEFAULT_HEAVYWEIGHT_CONCURRENCY,
) -> BackfillEngine:
    """Get or create the singleton BackfillEngine.

    If the engine hasn't been created yet, initializes it with the
    provided concurrency settings. Subsequent calls return the
    existing instance regardless of arguments.
    """
    global _engine
    if _engine is None:
        _engine = BackfillEngine(
            lightweight_concurrency=lightweight_concurrency,
            heavyweight_concurrency=heavyweight_concurrency,
        )
    return _engine
