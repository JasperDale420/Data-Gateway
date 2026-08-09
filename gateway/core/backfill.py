"""Backfill engine for historical data fetching and sink publishing.

Manages long-running jobs that fetch historical data from providers
and publish results through the DataSinkRegistry -> Redis Streams -> Heber pipeline.
"""

import asyncio
import hashlib
import json
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from gateway.core.backfill_manifest import (
    HeberChunkAcknowledgement,
    HeberReadinessBinding,
)
from gateway.core.calendar import US_HOLIDAYS, TradingCalendar
from gateway.core.envelope import wrap_event
from gateway.core.logger import logger
from gateway.core.metrics import set_replay_verification_state
from gateway.core.rate_limiter import ProviderRateLimitManager, get_rate_limiter
from gateway.core.security import InputValidator

_INPUT_VALIDATOR = InputValidator()

# Bulk backfill publishes to the dedicated stream consumed by
# heber-backfill-consumer, isolating it from the live feed so a large job can
# never MAXLEN-evict un-consumed live events (same design as uw_backfill_driver).
HEBER_EVENTS_TOPIC = os.environ.get("GATEWAY_BACKFILL_STREAM", "heber:events:backfill")

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

DEFAULT_ACK_WAIT_SECONDS = 60.0
ACK_POLL_SECONDS = 1.0
DEFAULT_MAX_CHUNK_RECORDS = 5_000


def expected_heber_backfill_binding(settings: Any) -> HeberReadinessBinding:
    """Return the exact Heber binding required by the selected backfill lane."""
    if settings.jetstream_enabled and settings.jetstream_lanes in {"backfill", "both"}:
        return HeberReadinessBinding(
            transport="jetstream",
            lane="backfill",
            stream="HEBER_BACKFILL",
            durable_consumer=settings.jetstream_backfill_durable_name,
        )
    return HeberReadinessBinding(
        transport="redis",
        lane="backfill",
        stream=HEBER_EVENTS_TOPIC,
        durable_consumer=settings.redis_backfill_consumer_group,
    )


# Per-underlying UW analytics feeds whose rows carry an ``expiry`` field. Without
# an equity override, ``_infer_instrument_type`` sees ``expiry`` and tags every
# row as ``instrument_type=option`` with a malformed ``option:{symbol}`` key (no
# OCC suffix), which ``wrap_event`` rejects -- failing the whole chunk. The EOD
# poller applies the same override; see ``_poll_eod_iv_term_structure``.
UW_EQUITY_ANALYTICS_FEEDS = frozenset({"iv_term_structure", "historic_option_volume"})


class BackfillStatus(str, Enum):
    """Lifecycle states for a backfill job."""

    QUEUED = "queued"
    RUNNING = "running"
    AWAITING_ACK = "awaiting_ack"
    PARTIAL = "partial"
    FAILED = "failed"
    VERIFIED = "verified"
    UNRECOVERABLE = "unrecoverable"
    CANCELLED = "cancelled"


class ReplayCapability(str, Enum):
    """Historical truth supported by one provider/feed pair."""

    DATE_BOUNDED = "date_bounded"
    UNPROVEN = "unproven"
    UNRECOVERABLE = "unrecoverable"


class ReplayFeedCapability(BaseModel):
    capability: ReplayCapability
    reason: str | None = None


class ReplayValidationError(ValueError):
    """Provider or acknowledgement evidence violated the replay contract."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


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
    canary: bool = Field(
        default=False,
        description="Require exactly one symbol and one completed market day",
    )


class SymbolProgress(BaseModel):
    """Progress tracking for a single symbol within a job."""

    symbol: str
    status: str = "pending"
    chunks_total: int = 0
    chunks_complete: int = 0
    records_published: int = 0
    errors: list[str] = Field(default_factory=list)


class ChunkResult(BaseModel):
    """Durable evidence for one symbol/date chunk."""

    chunk_id: str
    symbol: str
    requested_start: datetime
    requested_end: datetime
    status: str = "pending"
    returned_start: datetime | None = None
    returned_end: datetime | None = None
    record_count: int = 0
    records_published: int = 0
    event_ids_sha256: str | None = None
    records_sha256: str | None = None
    published_at: datetime | None = None
    acknowledged: bool = False
    heber_commit_id: str | None = None
    error: str | None = None


class BackfillJob(BaseModel):
    """Tracks state and progress of a backfill job."""

    job_id: str = Field(default_factory=lambda: f"bf-{uuid.uuid4().hex[:12]}")
    manifest_hash: str = ""
    request: BackfillRequest
    status: BackfillStatus = BackfillStatus.QUEUED
    symbols_progress: dict[str, SymbolProgress] = Field(default_factory=dict)
    chunks: dict[str, ChunkResult] = Field(default_factory=dict)
    records_published: int = 0
    errors: list[str] = Field(default_factory=list)
    blocked_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    gateway_completed_at: datetime | None = None
    ingestion_verified_at: datetime | None = None

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


def _chunk_id(
    job_id: str,
    symbol: str,
    chunk_start: datetime,
    chunk_end: datetime,
) -> str:
    raw = f"{job_id}|{symbol}|{chunk_start.isoformat()}|{chunk_end.isoformat()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def _manifest_hash(request: BackfillRequest) -> str:
    canonical = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _backfill_chunk_proof(envelopes: list[dict[str, Any]]) -> tuple[str, str]:
    """Return order-independent event and payload proofs for one replay chunk."""
    records = sorted(
        (
            str(envelope["event_id"]),
            hashlib.sha256(
                json.dumps(
                    envelope["payload"],
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
            ).hexdigest(),
        )
        for envelope in envelopes
    )
    if len({event_id for event_id, _payload_hash in records}) != len(records):
        raise ReplayValidationError(
            "provider_duplicate_event_identity",
            "backfill chunk contains duplicate event_id values",
        )
    event_ids_sha256 = hashlib.sha256("\n".join(event_id for event_id, _ in records).encode()).hexdigest()
    records_sha256 = hashlib.sha256(
        "\n".join(f"{event_id}:{payload_hash}" for event_id, payload_hash in records).encode()
    ).hexdigest()
    return event_ids_sha256, records_sha256


def _manifest_structure_valid(job: BackfillJob) -> bool:
    manifest_hash = _manifest_hash(job.request)
    expected_chunks = {
        _chunk_id(job.job_id, symbol, chunk_start, chunk_end): (
            symbol,
            chunk_start,
            chunk_end,
        )
        for symbol in job.request.symbols
        for chunk_start, chunk_end in _date_chunks(
            job.request.start,
            job.request.end,
            job.request.chunk_days,
        )
    }
    return (
        bool(expected_chunks)
        and job.manifest_hash == manifest_hash
        and job.job_id == f"bf-{manifest_hash[:32]}"
        and set(job.chunks) == set(expected_chunks)
        and all(
            chunk.chunk_id == chunk_id
            and (chunk.symbol, chunk.requested_start, chunk.requested_end) == expected_chunks[chunk_id]
            for chunk_id, chunk in job.chunks.items()
        )
    )


def _publication_evidence_complete(job: BackfillJob) -> bool:
    return bool(job.chunks) and all(
        chunk.status in {"published", "verified"}
        and chunk.record_count > 0
        and chunk.records_published == chunk.record_count
        and bool(chunk.event_ids_sha256)
        and bool(chunk.records_sha256)
        and chunk.published_at is not None
        for chunk in job.chunks.values()
    )


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


async def _uw_oi_change(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # Daily EOD feed, one snapshot per trading day. Fetch the chunk's start date.
    return await p.get_oi_change(sym, date_str=s.strftime("%Y-%m-%d"))


async def _uw_iv_rank(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # Daily EOD as-of value. ``get_iv_rank`` returns a single model (or None); wrap
    # it in a list so the publish path handles it like every other fetcher.
    result = await p.get_iv_rank(sym, date_str=s.strftime("%Y-%m-%d"))
    return [result] if result else []


async def _uw_iv_term_structure(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # Per-underlying analytics: one row per expiry of the SAME ticker. The provider
    # method is snapshot-only (no date param), so a backfill returns the current
    # term structure regardless of [start, end] — see module note in CHANGELOG.
    return await p.get_iv_term_structure(sym)


async def _uw_historic_option_volume(p: Any, sym: str, s: datetime, e: datetime, **kw: Any) -> list:
    # Daily EOD feed, volume/premium bucketed per expiry. Fetch the chunk's start date.
    return await p.get_historic_option_volume(sym, date_str=s.strftime("%Y-%m-%d"))


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
    ("unusual_whales", "oi_change"): _uw_oi_change,
    ("unusual_whales", "iv_rank"): _uw_iv_rank,
    ("unusual_whales", "iv_term_structure"): _uw_iv_term_structure,
    ("unusual_whales", "historic_option_volume"): _uw_historic_option_volume,
    ("unusual_whales", "earnings"): _uw_earnings,
    ("unusual_whales", "market_tide"): _uw_market_tide,
    ("unusual_whales", "sector_tide"): _uw_sector_tide,
}

_UNPROVEN_COVERAGE = ReplayFeedCapability(
    capability=ReplayCapability.UNPROVEN,
    reason="provider_complete_coverage_not_proven",
)
BACKFILL_CAPABILITIES: dict[tuple[str, str], ReplayFeedCapability] = {
    **dict.fromkeys(BACKFILL_DISPATCH, _UNPROVEN_COVERAGE),
    ("unusual_whales", "flow_alerts"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="provider_has_no_date_bounded_flow_alerts_contract",
    ),
    ("unusual_whales", "ticker_flow"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="provider_date_fallback_can_drop_bounds",
    ),
    ("unusual_whales", "darkpool"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="provider_returns_recent_snapshot_without_date_bounds",
    ),
    ("unusual_whales", "darkpool_ticker"): ReplayFeedCapability(
        capability=ReplayCapability.UNPROVEN,
        reason="provider_historical_bounds_not_proven",
    ),
    ("unusual_whales", "congress_trades"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="snapshot_only_source",
    ),
    ("unusual_whales", "insider_trades"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="snapshot_only_source",
    ),
    ("unusual_whales", "institutions"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="snapshot_only_source",
    ),
    ("unusual_whales", "iv_term_structure"): ReplayFeedCapability(
        capability=ReplayCapability.UNRECOVERABLE,
        reason="snapshot_only_source",
    ),
    **{
        ("unusual_whales", feed): ReplayFeedCapability(
            capability=ReplayCapability.UNRECOVERABLE,
            reason="snapshot_only_source",
        )
        for feed in ("short_interest", "short_volume", "ftds")
    },
    ("unusual_whales", "earnings"): ReplayFeedCapability(
        capability=ReplayCapability.UNPROVEN,
        reason="provider_historical_bounds_not_proven",
    ),
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
        ack_wait_seconds: float = DEFAULT_ACK_WAIT_SECONDS,
        max_chunk_records: int = DEFAULT_MAX_CHUNK_RECORDS,
        capabilities: dict[tuple[str, str], ReplayFeedCapability] | None = None,
    ) -> None:
        self._jobs: dict[str, BackfillJob] = {}
        self._running_tasks: dict[str, asyncio.Task[None]] = {}
        self._submission_locks: dict[str, asyncio.Lock] = {}
        # Separate semaphores per provider per weight class
        self._lightweight_semaphores: dict[str, asyncio.Semaphore] = {}
        self._heavyweight_semaphores: dict[str, asyncio.Semaphore] = {}
        self._sink_registry: Any = None
        self._provider_registry: Any = None
        self._manifest_store: Any = None
        self._symbol_concurrency = symbol_concurrency
        self._lightweight_concurrency = max(1, lightweight_concurrency)
        self._heavyweight_concurrency = max(1, heavyweight_concurrency)
        self._ack_wait_seconds = max(0.0, ack_wait_seconds)
        self._max_chunk_records = max(1, int(max_chunk_records))
        self._capabilities = capabilities or BACKFILL_CAPABILITIES
        self._expected_heber_binding = HeberReadinessBinding(
            transport="redis",
            lane="backfill",
            stream=HEBER_EVENTS_TOPIC,
            durable_consumer="heber-backfill-writers",
        )
        self._instance_id = uuid.uuid4().hex[:8]
        logger.info("backfill_engine_init", instance_id=self._instance_id)

    def configure(
        self,
        provider_registry: Any,
        sink_registry: Any,
        manifest_store: Any,
        expected_heber_binding: HeberReadinessBinding | None = None,
    ) -> None:
        """Wire in registries during app startup."""
        self._provider_registry = provider_registry
        self._sink_registry = sink_registry
        self._manifest_store = manifest_store
        if expected_heber_binding is not None:
            self._expected_heber_binding = expected_heber_binding

    async def start(self) -> None:
        """Restore durable blocked state and automatic acknowledgement checks."""
        if self._manifest_store is None:
            return
        for payload in await self._manifest_store.load_jobs():
            job = BackfillJob.model_validate(payload)
            self._jobs[job.job_id] = job
            if not _manifest_structure_valid(job):
                job.status = BackfillStatus.FAILED
                job.blocked_reason = "durable_manifest_invalid"
                job.completed_at = datetime.now(UTC)
                await self._save_job(job)
                self._set_verification_alert(job)
                continue
            if job.status == BackfillStatus.VERIFIED:
                if not _publication_evidence_complete(job):
                    job.status = BackfillStatus.PARTIAL
                    job.blocked_reason = "durable_manifest_publication_evidence_incomplete"
                    job.ingestion_verified_at = None
                    job.completed_at = datetime.now(UTC)
                    await self._save_job(job)
                else:
                    job.status = BackfillStatus.AWAITING_ACK
                    job.blocked_reason = "missing_heber_acknowledgement"
                    job.ingestion_verified_at = None
                    job.completed_at = None
                    await self._save_job(job)
            elif job.status in {BackfillStatus.QUEUED, BackfillStatus.RUNNING}:
                job.status = BackfillStatus.FAILED
                job.blocked_reason = "gateway_restart_interrupted_before_publication"
                job.completed_at = datetime.now(UTC)
                await self._save_job(job)
            elif job.status == BackfillStatus.AWAITING_ACK and job.blocked_reason is None:
                job.blocked_reason = "missing_heber_acknowledgement"
                await self._save_job(job)
            self._set_verification_alert(job)
            if (
                job.gateway_completed_at is not None
                and job.status in {BackfillStatus.AWAITING_ACK, BackfillStatus.PARTIAL}
                and job.blocked_reason
                in {
                    None,
                    "missing_heber_acknowledgement",
                    "heber_acknowledgement_invalid",
                    "heber_acknowledgement_mismatch",
                }
            ):
                if not _publication_evidence_complete(job):
                    job.status = BackfillStatus.PARTIAL
                    job.blocked_reason = "durable_manifest_publication_evidence_incomplete"
                    job.completed_at = datetime.now(UTC)
                    await self._save_job(job)
                    self._set_verification_alert(job)
                    continue
                job.status = BackfillStatus.AWAITING_ACK
                self._start_task(job, self._verify_heber_acknowledgements(job))

    def _start_task(self, job: BackfillJob, coroutine: Any) -> None:
        task = asyncio.create_task(coroutine)
        self._running_tasks[job.job_id] = task

        def remove_completed(completed: asyncio.Task[None]) -> None:
            if self._running_tasks.get(job.job_id) is completed:
                self._running_tasks.pop(job.job_id, None)

        task.add_done_callback(remove_completed)

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
    def supported_feeds(self) -> list[dict[str, str | None]]:
        """List provider/feed capability truth, including blocked feeds."""
        return [
            {
                "provider": provider,
                "feed": feed,
                "capability": capability.capability.value,
                "reason": capability.reason,
            }
            for (provider, feed), capability in self._capabilities.items()
        ]

    async def submit(self, request: BackfillRequest) -> BackfillJob:
        """Persist and queue a replay job only after capability validation."""
        if self._manifest_store is None:
            raise ValueError("BackfillEngine not configured: missing durable manifest store")

        # Validate provider
        if not self._provider_registry:
            raise ValueError("BackfillEngine not configured: missing provider registry")
        provider = self._provider_registry.get(request.provider)
        if provider is None:
            raise ValueError(f"Unknown provider: {request.provider}")

        dispatch_key = (request.provider, request.feed)
        capability = self._capabilities.get(dispatch_key)
        if dispatch_key not in BACKFILL_DISPATCH and capability is None:
            supported = [f for (p, f) in BACKFILL_DISPATCH if p == request.provider]
            raise ValueError(
                f"Unsupported feed '{request.feed}' for provider '{request.provider}'. Supported: {supported}"
            )
        if capability is None:
            capability = ReplayFeedCapability(
                capability=ReplayCapability.UNPROVEN,
                reason="provider_historical_bounds_not_proven",
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

        if request.canary:
            if len(normalized_symbols) != 1:
                raise ValueError("canary replay requires exactly one symbol")
            if request.start != request.end:
                raise ValueError("canary replay requires exactly one completed market day")
            if not _is_completed_market_day(request.end):
                raise ValueError("canary replay requires one completed market day")

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

        manifest_hash = _manifest_hash(request)
        job_id = f"bf-{manifest_hash[:32]}"

        lock = self._submission_locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            return await self._submit_locked(
                request=request,
                capability=capability,
                manifest_hash=manifest_hash,
                job_id=job_id,
            )

    async def _submit_locked(
        self,
        *,
        request: BackfillRequest,
        capability: ReplayFeedCapability,
        manifest_hash: str,
        job_id: str,
    ) -> BackfillJob:
        running_task = self._running_tasks.get(job_id)
        if running_task is not None and not running_task.done():
            return self._jobs[job_id]

        persisted = await self._manifest_store.load_job(job_id)
        if persisted:
            job = BackfillJob.model_validate(persisted)
            if job.manifest_hash != manifest_hash or job.request != request:
                raise ValueError(f"Durable manifest mismatch for {job_id}")
            self._jobs[job_id] = job
            if not _manifest_structure_valid(job):
                await self._block_job(job, BackfillStatus.FAILED, "durable_manifest_invalid")
                return job
            if job.status == BackfillStatus.UNRECOVERABLE:
                return job
            if job.status == BackfillStatus.VERIFIED:
                if not _publication_evidence_complete(job):
                    await self._block_job(
                        job,
                        BackfillStatus.PARTIAL,
                        "durable_manifest_publication_evidence_incomplete",
                    )
                    return job
                job.status = BackfillStatus.AWAITING_ACK
                job.blocked_reason = "missing_heber_acknowledgement"
                job.ingestion_verified_at = None
                job.completed_at = None
                await self._save_job(job)
                self._set_verification_alert(job)
                self._start_task(job, self._verify_heber_acknowledgements(job))
                return job
            job.status = BackfillStatus.QUEUED
            job.blocked_reason = None
            job.started_at = None
            job.completed_at = None
            job.gateway_completed_at = None
            job.ingestion_verified_at = None
            job.errors.clear()
            job.records_published = 0
            for progress in job.symbols_progress.values():
                progress.status = "pending"
                progress.chunks_complete = 0
                progress.records_published = 0
                progress.errors.clear()
        else:
            job = BackfillJob(
                job_id=job_id,
                manifest_hash=manifest_hash,
                request=request,
            )

        chunks = _date_chunks(request.start, request.end, request.chunk_days)
        if not job.symbols_progress:
            job.symbols_progress = {
                sym: SymbolProgress(symbol=sym, chunks_total=len(chunks)) for sym in request.symbols
            }
        for sym in request.symbols:
            for chunk_start, chunk_end in chunks:
                chunk_id = _chunk_id(job_id, sym, chunk_start, chunk_end)
                job.chunks.setdefault(
                    chunk_id,
                    ChunkResult(
                        chunk_id=chunk_id,
                        symbol=sym,
                        requested_start=chunk_start,
                        requested_end=chunk_end,
                    ),
                )

        self._jobs[job.job_id] = job
        if capability.capability != ReplayCapability.DATE_BOUNDED:
            job.status = BackfillStatus.UNRECOVERABLE
            job.blocked_reason = capability.reason or "provider_historical_bounds_not_proven"
            job.completed_at = datetime.now(UTC)
            await self._save_job(job)
            self._set_verification_alert(job)
            logger.warning(
                "backfill_unrecoverable",
                job_id=job.job_id,
                provider=request.provider,
                feed=request.feed,
                scope=self._scope(job),
                reason=job.blocked_reason,
                kairos_intake_blocked=True,
                safe_next_action="add and prove a genuinely date-bounded provider contract",
            )
            return job

        await self._save_job(job)
        logger.info("backfill_job_created", instance_id=self._instance_id, job_id=job.job_id, job_obj_id=id(job))

        # Kick off in background
        self._start_task(job, self._run_job(job))

        logger.info(
            "backfill_job_submitted",
            job_id=job.job_id,
            provider=request.provider,
            feed=request.feed,
            symbols=len(request.symbols),
            chunks=len(chunks),
        )

        return job

    async def wait(self, job_id: str) -> BackfillJob | None:
        """Wait for the current automatic fetch/verification attempt."""
        task = self._running_tasks.get(job_id)
        if task is not None:
            await task
        return self._jobs.get(job_id)

    async def _save_job(self, job: BackfillJob) -> None:
        await self._manifest_store.save_job(
            job_id=job.job_id,
            payload=job.model_dump(mode="json"),
            status=job.status.value,
            created_at=job.created_at,
        )

    @staticmethod
    def _scope(job: BackfillJob) -> str:
        symbols = ",".join(job.request.symbols)
        return f"{job.job_id}:{symbols}:{job.request.start.isoformat()}:{job.request.end.isoformat()}"

    def _set_verification_alert(self, job: BackfillJob) -> None:
        set_replay_verification_state(
            verified=job.status == BackfillStatus.VERIFIED,
            provider=job.request.provider,
            feed=job.request.feed,
            scope=self._scope(job),
            reason=job.blocked_reason or "none",
        )

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

    async def cancel(self, job_id: str) -> bool:
        """Cancel a running job. Returns True if cancellation was successful."""
        job = self._jobs.get(job_id)
        if not job or job.status not in (BackfillStatus.QUEUED, BackfillStatus.RUNNING):
            return False

        task = self._running_tasks.get(job_id)
        if task and not task.done():
            task.cancel()

        job.status = BackfillStatus.CANCELLED
        job.completed_at = datetime.now(UTC)
        job.blocked_reason = "cancelled"
        await self._save_job(job)
        self._set_verification_alert(job)
        logger.info("backfill_job_cancelled", job_id=job_id)
        return True

    async def cancel_all(self) -> int:
        """Cancel all running and queued jobs. Returns count of cancelled jobs."""
        cancelled = 0
        for job_id in list(self._jobs):
            if await self.cancel(job_id):
                cancelled += 1
        logger.info("backfill_cancel_all", cancelled=cancelled)
        return cancelled

    async def flush(self) -> int:
        """Refuse unbounded deletion of durable replay evidence."""
        logger.warning(
            "backfill_flush_refused",
            reason="durable replay evidence requires bounded retention policy",
        )
        return 0

    async def _run_job(self, job: BackfillJob) -> None:
        """Execute a backfill job, respecting feed-weighted concurrency limits."""
        semaphore = self._get_semaphore(job.request.provider, job.request.feed)

        async with semaphore:
            # nosemgrep: empire-no-bare-exception -- fail-closed integrity gate: any read failure (redis or manifest validation) logs exc_info and blocks the job FAILED rather than backfilling unverified
            try:
                readiness = await self._manifest_store.read_heber_readiness()
            except Exception:
                logger.error(
                    "backfill_heber_readiness_invalid",
                    job_id=job.job_id,
                    exc_info=True,
                )
                await self._block_job(job, BackfillStatus.FAILED, "heber_readiness_invalid")
                return
            if readiness is None:
                await self._block_job(job, BackfillStatus.FAILED, "heber_readiness_missing")
                return
            readiness_failure = readiness.failure_reason(expected=self._expected_heber_binding)
            if readiness_failure:
                await self._block_job(job, BackfillStatus.FAILED, readiness_failure)
                return

            job.status = BackfillStatus.RUNNING
            job.started_at = datetime.now(UTC)
            await self._save_job(job)

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
                job.blocked_reason = "cancelled"
                job.completed_at = datetime.now(UTC)
                await self._save_job(job)
                self._set_verification_alert(job)
                logger.info("backfill_job_cancelled_during_execution", job_id=job.job_id)
                raise
            except Exception as e:
                job.errors.append(str(e))
                await self._block_job(job, BackfillStatus.FAILED, "backfill_execution_failed")
                logger.error(
                    "backfill_job_failed",
                    job_id=job.job_id,
                    error=str(e),
                    exc_info=True,
                )
                return

            failed_symbols = [sp.symbol for sp in job.symbols_progress.values() if sp.status == "failed"]
            if failed_symbols:
                status = (
                    BackfillStatus.FAILED
                    if len(failed_symbols) == len(job.symbols_progress)
                    else BackfillStatus.PARTIAL
                )
                await self._block_job(
                    job,
                    status,
                    job.blocked_reason or "partial_chunk_failure",
                )
                return

            job.gateway_completed_at = datetime.now(UTC)
            job.status = BackfillStatus.AWAITING_ACK
            await self._save_job(job)
            logger.info(
                "backfill_gateway_publication_completed",
                job_id=job.job_id,
                status=job.status.value,
                records_published=job.records_published,
                errors=len(job.errors),
            )
            await self._verify_heber_acknowledgements(job)

    async def _block_job(
        self,
        job: BackfillJob,
        status: BackfillStatus,
        reason: str,
    ) -> None:
        job.status = status
        job.blocked_reason = reason
        job.completed_at = datetime.now(UTC)
        await self._save_job(job)
        self._set_verification_alert(job)
        logger.warning(
            "backfill_verification_blocked",
            job_id=job.job_id,
            status=status.value,
            reason=reason,
            provider=job.request.provider,
            feed=job.request.feed,
            scope=self._scope(job),
            kairos_intake_blocked=True,
            safe_next_action=_safe_next_action(reason),
        )

    async def _verify_heber_acknowledgements(self, job: BackfillJob) -> None:
        if not _manifest_structure_valid(job) or not _publication_evidence_complete(job):
            await self._block_job(
                job,
                BackfillStatus.PARTIAL,
                "durable_manifest_publication_evidence_incomplete",
            )
            return
        deadline = asyncio.get_running_loop().time() + self._ack_wait_seconds
        reason = "missing_heber_acknowledgement"
        alerted = job.status == BackfillStatus.PARTIAL
        while True:
            all_verified = True
            for chunk in job.chunks.values():
                # nosemgrep: empire-no-bare-exception -- fail-closed ack verification: any read failure logs exc_info and leaves the chunk unverified rather than certifying it
                try:
                    acknowledgement = await self._manifest_store.read_ack(
                        job.job_id,
                        chunk.chunk_id,
                    )
                except Exception:
                    logger.error(
                        "backfill_heber_acknowledgement_invalid",
                        job_id=job.job_id,
                        chunk_id=chunk.chunk_id,
                        exc_info=True,
                    )
                    reason = "heber_acknowledgement_invalid"
                    all_verified = False
                    acknowledgement = None
                mismatch = _ack_mismatch(job, chunk, acknowledgement)
                if mismatch:
                    all_verified = False
                    if reason != "heber_acknowledgement_invalid":
                        reason = mismatch
                    continue
                assert acknowledgement is not None
                chunk.acknowledged = True
                chunk.heber_commit_id = acknowledgement.commit_id
                chunk.status = "verified"

            if all_verified:
                job.status = BackfillStatus.VERIFIED
                job.blocked_reason = None
                job.ingestion_verified_at = datetime.now(UTC)
                job.completed_at = job.ingestion_verified_at
                await self._save_job(job)
                self._set_verification_alert(job)
                logger.info(
                    "backfill_ingestion_verified",
                    job_id=job.job_id,
                    provider=job.request.provider,
                    feed=job.request.feed,
                    scope=self._scope(job),
                    chunks=len(job.chunks),
                    records=job.records_published,
                    kairos_intake_blocked=False,
                )
                return
            if not alerted and asyncio.get_running_loop().time() >= deadline:
                await self._block_job(job, BackfillStatus.PARTIAL, reason)
                alerted = True
                if self._ack_wait_seconds == 0:
                    return
            await asyncio.sleep(ACK_POLL_SECONDS)

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
                    job.symbols_progress[sym].errors.append(error_msg)
                    job.blocked_reason = "backfill_execution_failed"
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
        chunk_id = _chunk_id(job.job_id, sym, chunk_start, chunk_end)
        chunk = job.chunks[chunk_id]
        if chunk.acknowledged:
            sp.chunks_complete += 1
            sp.records_published += chunk.records_published
            job.records_published += chunk.records_published
            return
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
                raise ReplayValidationError(
                    "provider_empty_coverage",
                    f"{sym} returned no records for required chunk",
                )

            items = await asyncio.to_thread(_normalize_results, results)
            if len(items) > self._max_chunk_records:
                raise ReplayValidationError(
                    "provider_chunk_too_large",
                    f"{sym} returned {len(items)} records; maximum is {self._max_chunk_records}",
                )
            timestamps = _validate_returned_items(
                items,
                symbol=sym,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                feed=job.request.feed,
            )
            messages = await asyncio.to_thread(
                self._wrap_items,
                items,
                job.request.provider,
                job.request.feed,
                job.job_id,
                chunk_id,
                job.manifest_hash,
            )
            if len(messages) > self._max_chunk_records:
                raise ReplayValidationError(
                    "provider_chunk_too_large",
                    f"{sym} produced {len(messages)} envelopes; maximum is {self._max_chunk_records}",
                )
            envelopes = [envelope for _topic, envelope in messages]
            event_ids_sha256, records_sha256 = _backfill_chunk_proof(envelopes)
            for envelope in envelopes:
                envelope["lineage"].update(
                    {
                        "backfill_expected_record_count": len(envelopes),
                        "backfill_expected_event_ids_sha256": event_ids_sha256,
                        "backfill_expected_records_sha256": records_sha256,
                    }
                )
            if chunk.event_ids_sha256 and (
                chunk.event_ids_sha256 != event_ids_sha256 or chunk.records_sha256 != records_sha256
            ):
                raise ReplayValidationError(
                    "provider_record_identity_changed",
                    f"{sym} returned different stable record identities on retry",
                )

            chunk.returned_start = min(timestamps)
            chunk.returned_end = max(timestamps)
            chunk.record_count = len(messages)
            chunk.event_ids_sha256 = event_ids_sha256
            chunk.records_sha256 = records_sha256
            chunk.status = "validated"
            await self._save_job(job)

            chunk.published_at = datetime.now(UTC)
            results_by_message = await self._publish_messages(
                messages=messages,
                provider=job.request.provider,
                feed=job.request.feed,
                job_id=job.job_id,
            )
            published = sum(1 for published_ok in results_by_message if published_ok)
            if published < len(messages):
                shortfall = (
                    f"{sym} chunk {chunk_start.date()}-{chunk_end.date()}: "
                    f"sink published {published}/{len(messages)} (partial failure)"
                )
                sp.errors.append(shortfall)
                job.errors.append(shortfall)
                job.blocked_reason = "stream_publication_partial"
                chunk.error = shortfall
                chunk.status = "partial"
            else:
                chunk.status = "published"
            sp.records_published += published
            job.records_published += published
            chunk.records_published = published
            sp.chunks_complete += 1
            await self._save_job(job)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            error_msg = f"{sym} chunk {chunk_start.date()}-{chunk_end.date()}: {e}"
            sp.errors.append(error_msg)
            job.errors.append(error_msg)
            reason = e.reason if isinstance(e, ReplayValidationError) else _exception_reason(e)
            job.blocked_reason = reason
            chunk.error = error_msg
            chunk.status = "failed"
            sp.chunks_complete += 1
            await self._save_job(job)
            logger.warning(
                "backfill_chunk_failed",
                job_id=job.job_id,
                symbol=sym,
                chunk_start=str(chunk_start.date()),
                chunk_end=str(chunk_end.date()),
                error=str(e),
                reason=reason,
                exc_info=True,
            )

    @staticmethod
    def _equity_overrides(item: dict[str, Any], provider: str, feed: str) -> dict[str, Any]:
        """Force the equity instrument key for per-underlying analytics feeds.

        ``iv_term_structure`` / ``historic_option_volume`` rows carry an
        ``expiry`` field, so ``_infer_instrument_type`` would otherwise tag them
        as option contracts with a malformed ``option:{symbol}`` key and fail the
        chunk. Mirrors the EOD poller's per-feed override.
        """
        if provider != "unusual_whales" or feed not in UW_EQUITY_ANALYTICS_FEEDS:
            return {}
        symbol = str(item.get("symbol") or item.get("ticker") or "").upper()
        if not symbol:
            return {}
        return {
            "instrument_type_override": "equity",
            "instrument_key_override": f"equity:{symbol}",
            "symbol_override": symbol,
        }

    @staticmethod
    def _wrap_items(
        items: list[dict[str, Any]],
        provider: str,
        feed: str,
        job_id: str = "",
        chunk_id: str = "",
        manifest_hash: str = "",
    ) -> list[tuple[str, dict[str, Any]]]:
        """Sort items by timestamp and wrap each in an envelope.

        Pure/synchronous so it can run via ``asyncio.to_thread`` off the loop.
        Sorting groups events by date for downstream partition locality — Heber
        partitions Silver by date, so this means fewer, larger partition flushes.
        """
        items.sort(key=lambda x: x.get("timestamp") or x.get("t") or "")
        messages: list[tuple[str, dict[str, Any]]] = []
        for item in items:
            envelope = wrap_event(
                event=item,
                provider=provider,
                feed=feed,
                source="backfill",
                **BackfillEngine._equity_overrides(item, provider, feed),
            )
            if job_id:
                envelope["lineage"].update(
                    {
                        "backfill_job_id": job_id,
                        "backfill_chunk_id": chunk_id,
                        "backfill_manifest_hash": manifest_hash,
                    }
                )
            messages.append((HEBER_EVENTS_TOPIC, envelope))
        return messages

    async def _publish_messages(
        self,
        messages: list[tuple[str, dict[str, Any]]],
        provider: str,
        feed: str,
        job_id: str,
    ) -> list[bool]:
        """Publish already-validated envelopes and return exact Redis acceptance."""
        if not self._sink_registry:
            logger.warning("backfill_publish_skipped", reason="no sink registry", job_id=job_id)
            return [False] * len(messages)

        durable_for_topic = getattr(self._sink_registry, "has_durable_admission_for", None)
        is_durable = (
            bool(durable_for_topic(HEBER_EVENTS_TOPIC))
            if callable(durable_for_topic)
            else bool(getattr(self._sink_registry, "has_durable_admission", False))
        )
        if is_durable:
            can_accept = getattr(self._sink_registry, "can_accept_low_priority", None)
            admission_kwargs: dict[str, Any] = {"max_utilization": 0.70}
            if callable(durable_for_topic):
                admission_kwargs["topic"] = HEBER_EVENTS_TOPIC
            if callable(can_accept) and not can_accept("redis_streams", **admission_kwargs):
                logger.warning(
                    "backfill_paused_durable_outbox_pressure",
                    job_id=job_id,
                    provider=provider,
                    feed=feed,
                )
                raise ReplayValidationError(
                    "durable_outbox_pressure",
                    "durable outbox is above the backfill admission threshold",
                )

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
            return results

        logger.error(
            "backfill_exact_publish_results_unavailable",
            job_id=job_id,
            provider=provider,
            feed=feed,
        )
        return [False] * len(messages)

    async def _publish_items(
        self,
        items: list[dict[str, Any]],
        provider: str,
        feed: str,
        job_id: str,
    ) -> int:
        """Compatibility helper for tests; durable jobs use ``_publish_messages``."""
        messages = await asyncio.to_thread(self._wrap_items, items, provider, feed)
        results = await self._publish_messages(messages, provider, feed, job_id)
        return sum(1 for ok in results if ok)

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
        if self._manifest_store is not None:
            await self._manifest_store.close()


def _validate_returned_items(
    items: list[dict[str, Any]],
    *,
    symbol: str,
    chunk_start: datetime,
    chunk_end: datetime,
    feed: str,
) -> list[datetime]:
    """Reject the entire provider response if scope or time evidence is incomplete."""
    timestamps: list[datetime] = []
    market_wide = feed in {"market_tide", "sector_tide"}
    for item in items:
        raw_timestamp = next(
            (
                item.get(field)
                for field in (
                    "timestamp",
                    "t",
                    "published_at",
                    "datetime",
                    "date",
                    "transaction_date",
                    "filing_date",
                    "report_date",
                    "effective_date",
                    "period_end_date",
                    "fiscal_date_ending",
                )
                if item.get(field) is not None
            ),
            None,
        )
        timestamp = _strict_timestamp(raw_timestamp)
        if timestamp is None:
            raise ReplayValidationError(
                "provider_timestamp_missing",
                "provider record has no parseable source timestamp",
            )
        if timestamp < chunk_start or timestamp > chunk_end:
            raise ReplayValidationError(
                "provider_date_out_of_range",
                f"provider returned {timestamp.isoformat()} outside requested chunk",
            )
        if not market_wide:
            returned_symbol = str(
                item.get("symbol") or item.get("ticker") or item.get("underlying") or item.get("S") or ""
            ).upper()
            if returned_symbol != symbol.upper():
                raise ReplayValidationError(
                    "provider_symbol_mismatch",
                    f"provider returned symbol {returned_symbol or '<missing>'} for {symbol}",
                )
        timestamps.append(timestamp)
    return timestamps


def _strict_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, date):
        timestamp = datetime(value.year, value.month, value.day, tzinfo=UTC)
    elif isinstance(value, int | float):
        seconds = float(value)
        if seconds > 10_000_000_000:
            seconds /= 1000
        try:
            timestamp = datetime.fromtimestamp(seconds, tz=UTC)
        except (OSError, OverflowError, ValueError):
            return None
    elif isinstance(value, str) and value.strip():
        normalized = value.strip().replace("Z", "+00:00")
        # Date-only is tried first because datetime.fromisoformat also accepts
        # those strings, and returns them naive — which the zone check below
        # then rejects. A date names no instant of its own, so it reads as UTC
        # midnight, the same reading the `isinstance(value, date)` branch above
        # already gives a real date object. Feeds whose only time evidence is a
        # date (filing_date, report_date, period_end_date) depend on this.
        try:
            parsed_date = date.fromisoformat(normalized)
        except ValueError:
            # nosemgrep: empire-no-return-none-for-failure -- parse predicate, not a failure signal: None means "not a timestamp", which every caller checks
            try:
                timestamp = datetime.fromisoformat(normalized)
            except ValueError:
                return None
        else:
            timestamp = datetime(
                parsed_date.year,
                parsed_date.month,
                parsed_date.day,
                tzinfo=UTC,
            )
    else:
        return None
    if timestamp.tzinfo is None:
        return None
    return timestamp.astimezone(UTC)


def _is_completed_market_day(day: date) -> bool:
    if day >= date.today() or day.weekday() >= 5:
        return False
    if 2024 <= day.year <= 2026:
        return day not in US_HOLIDAYS
    # nosemgrep: empire-no-bare-exception -- fail-closed calendar fallback: an unavailable calendar logs exc_info and reports the day incomplete rather than canarying against unknown data
    try:
        return TradingCalendar().is_trading_day(day)
    except Exception:
        logger.warning(
            "backfill_canary_calendar_unavailable",
            day=day.isoformat(),
            exc_info=True,
        )
        return False


def _ack_mismatch(
    job: BackfillJob,
    chunk: ChunkResult,
    acknowledgement: HeberChunkAcknowledgement | None,
) -> str | None:
    if acknowledgement is None:
        return "missing_heber_acknowledgement"
    if (
        acknowledgement.status != "committed"
        or acknowledgement.job_id != job.job_id
        or acknowledgement.chunk_id != chunk.chunk_id
        or acknowledgement.manifest_hash != job.manifest_hash
        or acknowledgement.record_count != chunk.record_count
        or acknowledgement.event_ids_sha256 != chunk.event_ids_sha256
        or acknowledgement.records_sha256 != chunk.records_sha256
        or not acknowledgement.commit_id
        or acknowledgement.committed_at.tzinfo is None
        or chunk.published_at is None
        or acknowledgement.committed_at < chunk.published_at
    ):
        return "heber_acknowledgement_mismatch"
    return None


def _exception_reason(error: Exception) -> str:
    if type(error).__name__ == "RateLimitExceeded":
        return "provider_rate_limit"
    return "provider_request_failed"


def _safe_next_action(reason: str) -> str:
    if reason.startswith("heber_") or reason == "missing_heber_acknowledgement":
        return "restore Heber consumer/writer health and durable post-commit acknowledgements, then retry"
    if reason.startswith("provider_"):
        return "fix or prove the provider date-bounded contract, then retry the same manifest"
    if reason == "stream_publication_partial":
        return "restore the Redis backfill stream and retry the same manifest"
    return "inspect the durable manifest; keep Kairos blocked and retry only after the cause is fixed"


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
    max_chunk_records: int = DEFAULT_MAX_CHUNK_RECORDS,
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
            max_chunk_records=max_chunk_records,
        )
    return _engine
