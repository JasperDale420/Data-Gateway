"""Bulk data job management for large dataset retrieval.

Implements async job-based bulk data fetching as specified in PRD.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any

import structlog

from gateway.config import get_settings

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
        if self.start and self.end:
            try:
                start_dt = datetime.fromisoformat(self.start)
            except ValueError:
                try:
                    start_dt = datetime.fromisoformat(f"{self.start}T00:00:00")
                except ValueError:
                    start_dt = None
                    errors.append(f"Invalid start date: {self.start}. Use YYYY-MM-DD or ISO8601")

            try:
                end_dt = datetime.fromisoformat(self.end)
            except ValueError:
                try:
                    end_dt = datetime.fromisoformat(f"{self.end}T00:00:00")
                except ValueError:
                    end_dt = None
                    errors.append(f"Invalid end date: {self.end}. Use YYYY-MM-DD or ISO8601")

            if start_dt and end_dt and end_dt < start_dt:
                errors.append("end date must be after start date")
        return errors


@dataclass
class BulkOptionsRequest:
    """Request for bulk options chains."""

    underlyings: list[str]
    date: str
    expiration_range: dict[str, int] | None = None
    moneyness_range: dict[str, float] | None = None
    format: str = "jsonl"

    def validate(self) -> list[str]:
        """Validate request. Returns list of error messages."""
        errors = []
        if not self.underlyings:
            errors.append("underlyings list cannot be empty")
        if len(self.underlyings) > 50:
            errors.append(f"Maximum 50 underlyings allowed, got {len(self.underlyings)}")
        if not self.date:
            errors.append("date is required")
        else:
            try:
                date.fromisoformat(self.date)
            except ValueError:
                errors.append(f"Invalid date: {self.date}. Use YYYY-MM-DD")

        if self.format not in ("jsonl", "json"):
            errors.append(f"Invalid format: {self.format}, must be jsonl or json")

        if self.expiration_range:
            min_dte = self.expiration_range.get("min_dte")
            max_dte = self.expiration_range.get("max_dte")
            if min_dte is not None and min_dte < 0:
                errors.append("expiration_range.min_dte must be >= 0")
            if max_dte is not None and max_dte < 0:
                errors.append("expiration_range.max_dte must be >= 0")
            if min_dte is not None and max_dte is not None and int(min_dte) > int(max_dte):
                errors.append("expiration_range.min_dte must be <= max_dte")

        if self.moneyness_range:
            min_delta = self.moneyness_range.get("min_delta")
            max_delta = self.moneyness_range.get("max_delta")
            if min_delta is not None and not (0 <= float(min_delta) <= 1):
                errors.append("moneyness_range.min_delta must be between 0 and 1")
            if max_delta is not None and not (0 <= float(max_delta) <= 1):
                errors.append("moneyness_range.max_delta must be between 0 and 1")
            if (
                min_delta is not None
                and max_delta is not None
                and float(min_delta) > float(max_delta)
            ):
                errors.append("moneyness_range.min_delta must be <= max_delta")

        return errors


@dataclass
class BulkAdjustmentFactorsRequest:
    """Request for bulk adjustment factors."""

    symbols: list[str]
    start: str
    end: str | None = None
    format: str = "jsonl"

    def validate(self) -> list[str]:
        """Validate request. Returns list of error messages."""
        errors = []
        if not self.symbols:
            errors.append("symbols list cannot be empty")
        if len(self.symbols) > 500:
            errors.append(f"Maximum 500 symbols allowed, got {len(self.symbols)}")
        if not self.start:
            errors.append("start date is required")
        else:
            try:
                date.fromisoformat(self.start)
            except ValueError:
                errors.append(f"Invalid start date: {self.start}. Use YYYY-MM-DD")
        if self.end:
            try:
                date.fromisoformat(self.end)
            except ValueError:
                errors.append(f"Invalid end date: {self.end}. Use YYYY-MM-DD")
        if self.format not in ("jsonl", "json"):
            errors.append(f"Invalid format: {self.format}, must be jsonl or json")
        return errors


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Job
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BulkJob:
    """Represents a bulk data job."""

    job_id: str
    job_type: str  # "bars" or "options"
    request: Any
    client_id: str
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
        self._fetch_options_func: Any | None = None
        self._fetch_adjustments_func: Any | None = None
        self._max_concurrency = 10

    def set_bars_fetcher(self, func: Any) -> None:
        """Set the function used to fetch bars for a symbol."""
        self._fetch_bars_func = func

    def has_bars_fetcher(self) -> bool:
        """Check whether a bars fetcher is configured."""
        return self._fetch_bars_func is not None

    def set_options_fetcher(self, func: Any) -> None:
        """Set the function used to fetch options chains for an underlying."""
        self._fetch_options_func = func

    def has_options_fetcher(self) -> bool:
        """Check whether an options fetcher is configured."""
        return self._fetch_options_func is not None

    def set_adjustments_fetcher(self, func: Any) -> None:
        """Set the function used to fetch adjustment factors for a symbol."""
        self._fetch_adjustments_func = func

    def has_adjustments_fetcher(self) -> bool:
        """Check whether an adjustments fetcher is configured."""
        return self._fetch_adjustments_func is not None

    async def create_bars_job(self, request: BulkBarsRequest, client_id: str) -> BulkJob:
        """Create a new bulk bars job.

        Args:
            request: Bulk bars request
            client_id: Authenticated client ID

        Returns:
            Created job
        """
        job_id = f"bulk-{uuid.uuid4().hex[:12]}"

        job = BulkJob(
            job_id=job_id,
            job_type="bars",
            request=request,
            client_id=client_id,
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

    async def create_options_job(
        self,
        request: BulkOptionsRequest,
        client_id: str,
    ) -> BulkJob:
        """Create a new bulk options chains job."""
        job_id = f"bulk-opt-{uuid.uuid4().hex[:12]}"

        job = BulkJob(
            job_id=job_id,
            job_type="options",
            request=request,
            client_id=client_id,
            symbols_total=len(request.underlyings),
        )

        async with self._lock:
            await self._cleanup_expired_jobs()

            if len(self._jobs) >= self.MAX_JOBS:
                raise ValueError("Maximum concurrent jobs reached")

            self._jobs[job_id] = job

        job._task = asyncio.create_task(self._process_options_job(job))

        logger.info(
            "bulk_options_job_created",
            job_id=job_id,
            underlyings=len(request.underlyings),
        )

        return job

    async def create_adjustments_job(
        self,
        request: BulkAdjustmentFactorsRequest,
        client_id: str,
    ) -> BulkJob:
        """Create a new bulk adjustment factors job."""
        job_id = f"bulk-adj-{uuid.uuid4().hex[:12]}"

        job = BulkJob(
            job_id=job_id,
            job_type="adjustment_factors",
            request=request,
            client_id=client_id,
            symbols_total=len(request.symbols),
        )

        async with self._lock:
            await self._cleanup_expired_jobs()

            if len(self._jobs) >= self.MAX_JOBS:
                raise ValueError("Maximum concurrent jobs reached")

            self._jobs[job_id] = job

        job._task = asyncio.create_task(self._process_adjustments_job(job))

        logger.info(
            "bulk_adjustments_job_created",
            job_id=job_id,
            symbols=len(request.symbols),
        )

        return job

    def get_job_sync(self, job_id: str) -> BulkJob | None:
        """Get a job by ID (sync)."""
        return self._jobs.get(job_id)

    async def get_job(self, job_id: str) -> BulkJob | None:
        """Get a job by ID."""
        return self.get_job_sync(job_id)

    async def list_jobs(self, client_id: str | None = None) -> list[BulkJob]:
        """List active jobs, optionally filtered by client."""
        jobs = list(self._jobs.values())
        if client_id is None:
            return jobs
        return [job for job in jobs if job.client_id == client_id]

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
            sem = asyncio.Semaphore(self._max_concurrency)

            async def _fetch_symbol(symbol: str) -> list[dict[str, Any]] | None:
                if job.status == BulkJobStatus.FAILED:
                    return None
                async with sem:
                    try:
                        return await self._fetch_symbol_bars(
                            symbol=symbol,
                            start=request.start,
                            end=request.end,
                            timeframe=request.timeframe,
                            adjusted=request.adjusted,
                        )
                    except Exception as e:
                        logger.warning(
                            "bulk_symbol_fetch_failed",
                            job_id=job.job_id,
                            symbol=symbol,
                            error=str(e),
                        )
                        return None

            tasks = [asyncio.create_task(_fetch_symbol(symbol)) for symbol in request.symbols]
            for task in asyncio.as_completed(tasks):
                try:
                    bars = await task
                except Exception as e:
                    logger.warning(
                        "bulk_symbol_task_failed",
                        job_id=job.job_id,
                        error=str(e),
                    )
                    bars = None

                if bars:
                    job.results.extend(bars)
                    job.records_fetched += len(bars)

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

        settings = get_settings()
        if settings.allow_stub_data:
            # Default: return empty (provider integration pending)
            return []
        raise RuntimeError("Bulk bars fetcher not configured")

    async def _process_options_job(self, job: BulkJob) -> None:
        """Process a bulk options chains job."""
        job.status = BulkJobStatus.RUNNING
        job.started_at = datetime.now(UTC)

        request: BulkOptionsRequest = job.request

        try:
            as_of_date = date.fromisoformat(request.date)
        except ValueError as e:
            job.status = BulkJobStatus.FAILED
            job.error = f"Invalid date: {request.date}"
            job.completed_at = datetime.now(UTC)
            logger.error("bulk_options_invalid_date", job_id=job.job_id, error=str(e))
            return

        expiration_gte = None
        expiration_lte = None
        expiration_gte_date: date | None = None
        expiration_lte_date: date | None = None
        if request.expiration_range:
            min_dte = request.expiration_range.get("min_dte")
            max_dte = request.expiration_range.get("max_dte")
            if min_dte is not None:
                expiration_gte_date = as_of_date + timedelta(days=int(min_dte))
                expiration_gte = expiration_gte_date.isoformat()
            if max_dte is not None:
                expiration_lte_date = as_of_date + timedelta(days=int(max_dte))
                expiration_lte = expiration_lte_date.isoformat()

        min_delta = None
        max_delta = None
        if request.moneyness_range:
            min_delta = request.moneyness_range.get("min_delta")
            max_delta = request.moneyness_range.get("max_delta")

        try:
            sem = asyncio.Semaphore(self._max_concurrency)

            async def _fetch_underlying(underlying: str) -> list[Any] | None:
                if job.status == BulkJobStatus.FAILED:
                    return None
                async with sem:
                    try:
                        return await self._fetch_option_chain(
                            underlying=underlying,
                            expiration_gte=expiration_gte,
                            expiration_lte=expiration_lte,
                        )
                    except Exception as e:
                        logger.warning(
                            "bulk_options_fetch_failed",
                            job_id=job.job_id,
                            underlying=underlying,
                            error=str(e),
                        )
                        return None

            async def _fetch_underlying_with_key(underlying: str) -> tuple[str, list[Any] | None]:
                return underlying, await _fetch_underlying(underlying)

            tasks = [
                asyncio.create_task(_fetch_underlying_with_key(underlying))
                for underlying in request.underlyings
            ]
            for task in asyncio.as_completed(tasks):
                try:
                    underlying, contracts = await task
                except Exception as e:
                    logger.warning(
                        "bulk_options_task_failed",
                        job_id=job.job_id,
                        underlying="unknown",
                        error=str(e),
                    )
                    contracts = None

                if contracts:
                    filtered = self._filter_option_contracts(
                        contracts,
                        expiration_gte=expiration_gte_date,
                        expiration_lte=expiration_lte_date,
                        min_delta=min_delta,
                        max_delta=max_delta,
                    )
                    job.results.extend(filtered)
                    job.records_fetched += len(filtered)

                job.symbols_complete += 1

            if job.status == BulkJobStatus.RUNNING:
                job.status = BulkJobStatus.COMPLETE
                job.completed_at = datetime.now(UTC)
                logger.info(
                    "bulk_options_job_complete",
                    job_id=job.job_id,
                    records=job.records_fetched,
                    duration_seconds=(job.completed_at - job.started_at).total_seconds(),
                )

        except Exception as e:
            job.status = BulkJobStatus.FAILED
            job.error = str(e)
            job.completed_at = datetime.now(UTC)
            logger.error("bulk_options_job_failed", job_id=job.job_id, error=str(e))

    def _filter_option_contracts(
        self,
        contracts: list[Any],
        expiration_gte: date | None,
        expiration_lte: date | None,
        min_delta: float | None,
        max_delta: float | None,
    ) -> list[dict[str, Any]]:
        """Filter and normalize option contracts."""
        filtered: list[dict[str, Any]] = []

        for contract in contracts:
            record: dict[str, Any]
            if hasattr(contract, "model_dump"):
                record = contract.model_dump(mode="json")
            elif hasattr(contract, "to_dict"):
                record = contract.to_dict()
            elif isinstance(contract, dict):
                record = contract
            else:
                continue

            if min_delta is not None or max_delta is not None:
                delta = record.get("delta")
                if delta is None:
                    continue
                try:
                    delta_val = abs(float(delta))
                except (TypeError, ValueError):
                    continue
                if min_delta is not None and delta_val < float(min_delta):
                    continue
                if max_delta is not None and delta_val > float(max_delta):
                    continue

            if expiration_gte or expiration_lte:
                expiration = record.get("expiration")
                if not expiration:
                    continue
                try:
                    expiration_date = date.fromisoformat(expiration[:10])
                except ValueError:
                    continue
                if expiration_gte and expiration_date < expiration_gte:
                    continue
                if expiration_lte and expiration_date > expiration_lte:
                    continue

            filtered.append(record)

        return filtered

    async def _fetch_option_chain(
        self,
        underlying: str,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
    ) -> list[Any]:
        """Fetch option chain for a single underlying."""
        if self._fetch_options_func:
            return await self._fetch_options_func(
                underlying=underlying,
                expiration_gte=expiration_gte,
                expiration_lte=expiration_lte,
            )

        settings = get_settings()
        if settings.allow_stub_data:
            return []
        raise RuntimeError("Bulk options fetcher not configured")

    async def _process_adjustments_job(self, job: BulkJob) -> None:
        """Process a bulk adjustment factors job."""
        job.status = BulkJobStatus.RUNNING
        job.started_at = datetime.now(UTC)

        request: BulkAdjustmentFactorsRequest = job.request

        try:
            start_date = date.fromisoformat(request.start)
        except ValueError as e:
            job.status = BulkJobStatus.FAILED
            job.error = f"Invalid start date: {request.start}"
            job.completed_at = datetime.now(UTC)
            logger.error("bulk_adjustments_invalid_start", job_id=job.job_id, error=str(e))
            return

        try:
            end_date = date.fromisoformat(request.end) if request.end else date.today()
        except ValueError as e:
            job.status = BulkJobStatus.FAILED
            job.error = f"Invalid end date: {request.end}"
            job.completed_at = datetime.now(UTC)
            logger.error("bulk_adjustments_invalid_end", job_id=job.job_id, error=str(e))
            return

        if end_date < start_date:
            job.status = BulkJobStatus.FAILED
            job.error = "end date must be after start date"
            job.completed_at = datetime.now(UTC)
            return

        try:
            sem = asyncio.Semaphore(self._max_concurrency)

            async def _fetch_symbol(symbol: str) -> list[dict[str, Any]] | None:
                if job.status == BulkJobStatus.FAILED:
                    return None
                async with sem:
                    try:
                        return await self._fetch_adjustment_factors(symbol, start_date, end_date)
                    except Exception as e:
                        logger.warning(
                            "bulk_adjustments_fetch_failed",
                            job_id=job.job_id,
                            symbol=symbol,
                            error=str(e),
                        )
                        return None

            async def _fetch_symbol_with_key(
                symbol: str,
            ) -> tuple[str, list[dict[str, Any]] | None]:
                return symbol, await _fetch_symbol(symbol)

            tasks = [
                asyncio.create_task(_fetch_symbol_with_key(symbol)) for symbol in request.symbols
            ]
            for task in asyncio.as_completed(tasks):
                try:
                    symbol, factors = await task
                except Exception as e:
                    logger.warning(
                        "bulk_adjustments_task_failed",
                        job_id=job.job_id,
                        symbol="unknown",
                        error=str(e),
                    )
                    factors = None

                if factors:
                    job.results.extend(factors)
                    job.records_fetched += len(factors)

                job.symbols_complete += 1

            if job.status == BulkJobStatus.RUNNING:
                job.status = BulkJobStatus.COMPLETE
                job.completed_at = datetime.now(UTC)
                logger.info(
                    "bulk_adjustments_job_complete",
                    job_id=job.job_id,
                    records=job.records_fetched,
                    duration_seconds=(job.completed_at - job.started_at).total_seconds(),
                )

        except Exception as e:
            job.status = BulkJobStatus.FAILED
            job.error = str(e)
            job.completed_at = datetime.now(UTC)
            logger.error("bulk_adjustments_job_failed", job_id=job.job_id, error=str(e))

    async def _fetch_adjustment_factors(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> list[dict[str, Any]]:
        """Fetch adjustment factors for a single symbol."""
        if self._fetch_adjustments_func:
            return await self._fetch_adjustments_func(symbol=symbol, start=start, end=end)

        settings = get_settings()
        if settings.allow_stub_data:
            from gateway.core.adjustments import get_adjustment_service

            service = get_adjustment_service()
            factors = await service.get_factors(symbol, start, end)
            return [
                {
                    "symbol": symbol,
                    **factor.to_dict(),
                }
                for factor in factors
            ]

        raise RuntimeError("Bulk adjustment factors fetcher not configured")

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
