"""Bulk data job management for large dataset retrieval.

Implements async job-based bulk data fetching as specified in PRD.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Job Status
# ─────────────────────────────────────────────────────────────────────────────


class BulkJobStatus(str, Enum):
    """Status of a bulk data job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


# ─────────────────────────────────────────────────────────────────────────────
# Job Request Models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BulkBarsRequest:
    """Request for bulk historical bars."""

    symbols: list[str]
    start: str
    end: str
    timeframe: str = "1Day"
    adjusted: bool = False
    format: str = "jsonl"  # jsonl or parquet

    def validate(self) -> list[str]:
        """Validate request. Returns list of error messages."""
        errors = []
        if not self.symbols:
            errors.append("symbols list cannot be empty")
        if len(self.symbols) > 500:
            errors.append(f"Maximum 500 symbols allowed, got {len(self.symbols)}")
        if not self.start:
            errors.append("start date is required")
        if not self.end:
            errors.append("end date is required")
        if self.format not in ("jsonl", "parquet", "json"):
            errors.append(f"Invalid format: {self.format}, must be jsonl, json, or parquet")
        return errors


@dataclass
class BulkOptionsRequest:
    """Request for bulk options chains."""

    underlyings: list[str]
    date: str
    expiration_range: dict[str, int] | None = None
    moneyness_range: dict[str, float] | None = None
    format: str = "jsonl"


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Job
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BulkJob:
    """Represents a bulk data job."""

    job_id: str
    job_type: str  # "bars" or "options"
    request: Any
    status: BulkJobStatus = BulkJobStatus.PENDING

    # Progress tracking
    symbols_total: int = 0
    symbols_complete: int = 0
    records_fetched: int = 0

    # Timing
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None

    # Results storage
    results: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    # Background task reference
    _task: asyncio.Task[None] | None = field(default=None, repr=False)

    @property
    def progress(self) -> float:
        """Calculate progress percentage (0.0 to 1.0)."""
        if self.symbols_total == 0:
            return 0.0
        return self.symbols_complete / self.symbols_total

    @property
    def eta_seconds(self) -> int | None:
        """Estimate remaining time in seconds."""
        if not self.started_at or self.symbols_complete == 0:
            return None

        elapsed = (datetime.now(UTC) - self.started_at).total_seconds()
        rate = self.symbols_complete / elapsed
        remaining = self.symbols_total - self.symbols_complete

        if rate > 0:
            return int(remaining / rate)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to response dict."""
        result = {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status.value,
            "progress": round(self.progress, 3),
            "symbols_total": self.symbols_total,
            "symbols_complete": self.symbols_complete,
            "records_fetched": self.records_fetched,
            "created_at": self.created_at.isoformat(),
        }

        if self.started_at:
            result["started_at"] = self.started_at.isoformat()
        if self.completed_at:
            result["completed_at"] = self.completed_at.isoformat()
        if self.eta_seconds is not None:
            result["eta_seconds"] = self.eta_seconds
        if self.error:
            result["error"] = self.error

        return result


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Job Manager
# ─────────────────────────────────────────────────────────────────────────────


class BulkJobManager:
    """Manages bulk data jobs.

    Jobs are processed asynchronously and results stored in memory.
    For production, this should be backed by Redis or a database.
    """

    MAX_JOBS = 100  # Maximum concurrent jobs
    JOB_TTL_SECONDS = 3600  # Jobs expire after 1 hour

    def __init__(self) -> None:
        self._jobs: dict[str, BulkJob] = {}
        self._lock = asyncio.Lock()
        self._fetch_bars_func: Any | None = None

    def set_bars_fetcher(self, func: Any) -> None:
        """Set the function used to fetch bars for a symbol."""
        self._fetch_bars_func = func

    async def create_bars_job(self, request: BulkBarsRequest) -> BulkJob:
        """Create a new bulk bars job.

        Args:
            request: Bulk bars request

        Returns:
            Created job
        """
        job_id = f"bulk-{uuid.uuid4().hex[:12]}"

        job = BulkJob(
            job_id=job_id,
            job_type="bars",
            request=request,
            symbols_total=len(request.symbols),
        )

        async with self._lock:
            # Clean up old jobs
            await self._cleanup_expired_jobs()

            if len(self._jobs) >= self.MAX_JOBS:
                raise ValueError("Maximum concurrent jobs reached")

            self._jobs[job_id] = job

        # Start processing in background (store reference to prevent GC)
        job._task = asyncio.create_task(self._process_bars_job(job))

        logger.info(
            "bulk_job_created",
            job_id=job_id,
            symbols=len(request.symbols),
            timeframe=request.timeframe,
        )

        return job

    def get_job_sync(self, job_id: str) -> BulkJob | None:
        """Get a job by ID (sync)."""
        return self._jobs.get(job_id)

    async def get_job(self, job_id: str) -> BulkJob | None:
        """Get a job by ID."""
        return self.get_job_sync(job_id)

    async def list_jobs(self) -> list[BulkJob]:
        """List all active jobs."""
        return list(self._jobs.values())

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job if it's still running."""
        job = self._jobs.get(job_id)
        if job and job.status in (BulkJobStatus.PENDING, BulkJobStatus.RUNNING):
            job.status = BulkJobStatus.FAILED
            job.error = "Cancelled by user"
            return True
        return False

    async def get_results_stream(self, job_id: str) -> AsyncIterator[dict[str, Any]]:
        """Stream results from a completed job.

        Args:
            job_id: Job ID

        Yields:
            Individual result records
        """
        job = self._jobs.get(job_id)
        if not job:
            return

        for record in job.results:
            yield record

    def get_results_jsonl(self, job_id: str) -> str:
        """Get results as JSONL string."""
        job = self._jobs.get(job_id)
        if not job or job.status != BulkJobStatus.COMPLETE:
            return ""

        lines = [json.dumps(r) for r in job.results]
        return "\n".join(lines)

    async def _process_bars_job(self, job: BulkJob) -> None:
        """Process a bulk bars job."""
        job.status = BulkJobStatus.RUNNING
        job.started_at = datetime.now(UTC)

        request: BulkBarsRequest = job.request

        try:
            for symbol in request.symbols:
                if job.status == BulkJobStatus.FAILED:
                    # Job was cancelled
                    break

                try:
                    # Fetch bars for this symbol
                    bars = await self._fetch_symbol_bars(
                        symbol=symbol,
                        start=request.start,
                        end=request.end,
                        timeframe=request.timeframe,
                        adjusted=request.adjusted,
                    )

                    job.results.extend(bars)
                    job.records_fetched += len(bars)

                except Exception as e:
                    logger.warning(
                        "bulk_symbol_fetch_failed",
                        job_id=job.job_id,
                        symbol=symbol,
                        error=str(e),
                    )

                job.symbols_complete += 1

            if job.status == BulkJobStatus.RUNNING:
                job.status = BulkJobStatus.COMPLETE
                job.completed_at = datetime.now(UTC)

                logger.info(
                    "bulk_job_complete",
                    job_id=job.job_id,
                    records=job.records_fetched,
                    duration_seconds=(job.completed_at - job.started_at).total_seconds(),
                )

        except Exception as e:
            job.status = BulkJobStatus.FAILED
            job.error = str(e)
            job.completed_at = datetime.now(UTC)

            logger.error(
                "bulk_job_failed",
                job_id=job.job_id,
                error=str(e),
            )

    async def _fetch_symbol_bars(
        self,
        symbol: str,
        start: str,
        end: str,
        timeframe: str,
        adjusted: bool,
    ) -> list[dict[str, Any]]:
        """Fetch bars for a single symbol.

        Override this method or use set_bars_fetcher to customize.
        """
        if self._fetch_bars_func:
            return await self._fetch_bars_func(
                symbol=symbol,
                start=start,
                end=end,
                timeframe=timeframe,
                adjusted=adjusted,
            )

        # Default: return empty (provider integration pending)
        return []

    async def _cleanup_expired_jobs(self) -> None:
        """Remove expired jobs."""
        now = datetime.now(UTC)
        expired = []

        for job_id, job in self._jobs.items():
            age = (now - job.created_at).total_seconds()
            if age > self.JOB_TTL_SECONDS and job.status in (
                BulkJobStatus.COMPLETE,
                BulkJobStatus.FAILED,
            ):
                expired.append(job_id)

        for job_id in expired:
            del self._jobs[job_id]
            logger.debug("bulk_job_expired", job_id=job_id)


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_manager: BulkJobManager | None = None


def get_bulk_manager() -> BulkJobManager:
    """Get the singleton bulk job manager."""
    global _manager
    if _manager is None:
        _manager = BulkJobManager()
    return _manager
