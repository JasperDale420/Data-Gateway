"""Bulk data API endpoints.

Implements bulk data retrieval endpoints as specified in PRD.
"""

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from gateway.api.deps import get_registry, require_api_key, require_provider_rate_limit
from gateway.config import get_settings
from gateway.core.bulk import (
    BulkAdjustmentFactorsRequest,
    BulkBarsRequest,
    BulkJobStatus,
    BulkOptionsRequest,
    get_bulk_manager,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/bulk", tags=["Bulk Data"])


# ─────────────────────────────────────────────────────────────────────────────
# Request Models
# ─────────────────────────────────────────────────────────────────────────────


class BulkBarsRequestModel(BaseModel):
    """Request body for bulk bars endpoint."""

    symbols: list[str] = Field(
        ...,
        description="List of symbols (max 500)",
        min_length=1,
        max_length=500,
    )
    start: str = Field(
        ...,
        description="Start date (YYYY-MM-DD or ISO8601)",
        examples=["2023-01-01"],
    )
    end: str = Field(
        ...,
        description="End date (YYYY-MM-DD or ISO8601)",
        examples=["2024-01-01"],
    )
    timeframe: str = Field(
        default="1Day",
        description="Bar timeframe",
        examples=["1Min", "5Min", "1Hour", "1Day"],
    )
    adjusted: bool = Field(
        default=False,
        description="Whether to return split/dividend adjusted prices",
    )
    format: str = Field(
        default="jsonl",
        description="Output format: jsonl, json, or parquet",
    )


class BulkOptionsRequestModel(BaseModel):
    """Request body for bulk options chains endpoint."""

    underlyings: list[str] = Field(
        ...,
        description="List of underlying symbols",
        min_length=1,
        max_length=50,
    )
    date: str = Field(
        ...,
        description="Date for options chain",
        examples=["2024-01-15"],
    )
    expiration_range: dict[str, int] | None = Field(
        default=None,
        description="DTE range filter (min_dte, max_dte)",
    )
    moneyness_range: dict[str, float] | None = Field(
        default=None,
        description="Moneyness filter (min_delta, max_delta)",
    )
    format: str = Field(
        default="jsonl",
        description="Output format",
    )


class BulkAdjustmentFactorsRequestModel(BaseModel):
    """Request body for bulk adjustment factors endpoint."""

    symbols: list[str] = Field(
        ...,
        description="List of symbols (max 500)",
        min_length=1,
        max_length=500,
    )
    start: str = Field(
        ...,
        description="Start date (YYYY-MM-DD)",
        examples=["2020-01-01"],
    )
    end: str | None = Field(
        default=None,
        description="End date (YYYY-MM-DD). Defaults to today.",
        examples=["2024-01-01"],
    )
    format: str = Field(
        default="jsonl",
        description="Output format: jsonl or json",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Response Models
# ─────────────────────────────────────────────────────────────────────────────


class BulkJobCreatedResponse(BaseModel):
    """Response when a bulk job is created."""

    job_id: str
    status: str = "accepted"
    estimated_records: int | None = None
    estimated_size_mb: float | None = None


class BulkJobStatusResponse(BaseModel):
    """Response for job status check."""

    job_id: str
    status: str
    progress: float
    symbols_total: int
    symbols_complete: int
    records_fetched: int
    started_at: str | None = None
    completed_at: str | None = None
    eta_seconds: int | None = None
    error: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/bars",
    response_model=BulkJobCreatedResponse,
    summary="Create bulk bars job",
    description="Submit a bulk request for historical bars across multiple symbols. "
    "Returns a job ID to track progress and download results.",
)
async def create_bulk_bars_job(
    request: BulkBarsRequestModel,
    client: Any = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
) -> BulkJobCreatedResponse:
    """Create a bulk bars fetch job."""
    manager = get_bulk_manager()
    settings = get_settings()

    provider = registry.get("alpaca")
    if provider:

        async def _fetch_bars(
            *,
            symbol: str,
            start: str,
            end: str,
            timeframe: str,
            adjusted: bool,
        ):
            await require_provider_rate_limit("alpaca")
            start_dt = _parse_datetime(start, end_of_day=False)
            end_dt = _parse_datetime(end, end_of_day=True)
            bars = await provider.get_bars(
                symbols=[symbol],
                timeframe=timeframe,
                start=start_dt,
                end=end_dt,
                adjustment="all" if adjusted else "raw",
            )
            return [bar.model_dump(mode="json") for bar in bars]

        manager.set_bars_fetcher(_fetch_bars)
    elif not settings.allow_stub_data:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    # Convert to internal request
    bulk_request = BulkBarsRequest(
        symbols=[s.upper() for s in request.symbols],
        start=request.start,
        end=request.end,
        timeframe=request.timeframe,
        adjusted=request.adjusted,
        format=request.format,
    )

    # Validate
    errors = bulk_request.validate()
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    try:
        job = await manager.create_bars_job(bulk_request, client.id)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Estimate records (rough: symbols * trading days)
    # Assume ~252 trading days per year
    estimated_records = len(request.symbols) * 252

    return BulkJobCreatedResponse(
        job_id=job.job_id,
        status="accepted",
        estimated_records=estimated_records,
        estimated_size_mb=round(estimated_records * 0.0005, 2),  # ~500 bytes per record
    )


def _parse_datetime(value: str, end_of_day: bool) -> datetime:
    """Parse ISO8601/date strings into timezone-aware datetime."""
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        dt = datetime.fromisoformat(f"{value}T00:00:00")
    if "T" not in value and end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


@router.get(
    "/jobs/{job_id}",
    response_model=BulkJobStatusResponse,
    summary="Get job status",
    description="Check the status and progress of a bulk data job.",
)
async def get_job_status(
    job_id: str,
    client: Any = Depends(require_api_key),
) -> BulkJobStatusResponse:
    """Get status of a bulk job."""
    manager = get_bulk_manager()
    job = await manager.get_job(job_id)

    if not job or job.client_id != client.id:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    return BulkJobStatusResponse(
        job_id=job.job_id,
        status=job.status.value,
        progress=round(job.progress, 3),
        symbols_total=job.symbols_total,
        symbols_complete=job.symbols_complete,
        records_fetched=job.records_fetched,
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        eta_seconds=job.eta_seconds,
        error=job.error,
    )


@router.get(
    "/jobs/{job_id}/download",
    summary="Download job results",
    description="Download results of a completed bulk job. "
    "Supports JSONL (default) and JSON formats.",
)
async def download_job_results(
    job_id: str,
    format: str = Query(default="jsonl", description="Output format: jsonl or json"),
    client: Any = Depends(require_api_key),
) -> Response:
    """Download results from a completed job."""
    manager = get_bulk_manager()
    job = await manager.get_job(job_id)

    if not job or job.client_id != client.id:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    if job.status != BulkJobStatus.COMPLETE:
        raise HTTPException(
            status_code=400,
            detail=f"Job not complete, status: {job.status.value}",
        )

    if format == "jsonl":
        content = manager.get_results_jsonl(job_id)
        return Response(
            content=content,
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": f"attachment; filename={job_id}.jsonl",
            },
        )
    elif format == "json":
        import json

        content = json.dumps({"data": job.results})
        return Response(
            content=content,
            media_type="application/json",
            headers={
                "Content-Disposition": f"attachment; filename={job_id}.json",
            },
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported format: {format}. Use 'jsonl' or 'json'.",
        )


@router.delete(
    "/jobs/{job_id}",
    response_model=SuccessResponse,
    summary="Cancel job",
    description="Cancel a pending or running bulk job.",
)
async def cancel_job(
    job_id: str,
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Cancel a bulk job."""
    manager = get_bulk_manager()

    job = await manager.get_job(job_id)
    if not job or job.client_id != client.id:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    cancelled = await manager.cancel_job(job_id)

    if not cancelled:
        return {
            "job_id": job_id,
            "cancelled": False,
            "message": f"Cannot cancel job with status: {job.status.value}",
        }

    return {
        "job_id": job_id,
        "cancelled": True,
        "message": "Job cancelled",
    }


@router.get(
    "/jobs",
    response_model=SuccessResponse,
    summary="List jobs",
    description="List all bulk jobs for the authenticated client.",
)
async def list_jobs(
    status: str | None = Query(default=None, description="Filter by status"),
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """List all bulk jobs."""
    manager = get_bulk_manager()
    jobs = await manager.list_jobs(client.id)

    # Filter by status if provided
    if status:
        jobs = [j for j in jobs if j.status.value == status]

    return {
        "jobs": [j.to_dict() for j in jobs],
        "count": len(jobs),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Options
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/options/chains",
    response_model=BulkJobCreatedResponse,
    summary="Create bulk options chains job",
    description="Submit a bulk request for options chains across multiple underlyings.",
)
async def create_bulk_options_job(
    request: BulkOptionsRequestModel,
    client: Any = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
) -> BulkJobCreatedResponse:
    """Create a bulk options chain fetch job.

    Fetches option chains for multiple underlyings using the configured provider.
    """
    manager = get_bulk_manager()
    settings = get_settings()

    provider = registry.get("alpaca")
    if not provider:
        if settings.allow_stub_data:
            # Allow stub behavior if explicitly enabled
            job_id = f"bulk-opt-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            return BulkJobCreatedResponse(
                job_id=job_id,
                status="accepted",
                estimated_records=len(request.underlyings) * 1000,
                estimated_size_mb=len(request.underlyings) * 2.0,
            )
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    async def _fetch_chain(
        *,
        underlying: str,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
    ):
        await require_provider_rate_limit("alpaca")
        return await provider.get_option_chain(
            underlying=underlying,
            expiration_gte=expiration_gte,
            expiration_lte=expiration_lte,
        )

    manager.set_options_fetcher(_fetch_chain)

    bulk_request = BulkOptionsRequest(
        underlyings=[u.upper() for u in request.underlyings],
        date=request.date,
        expiration_range=request.expiration_range,
        moneyness_range=request.moneyness_range,
        format=request.format,
    )

    errors = bulk_request.validate()
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    try:
        job = await manager.create_options_job(bulk_request, client.id)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return BulkJobCreatedResponse(
        job_id=job.job_id,
        status="accepted",
        estimated_records=len(request.underlyings) * 1000,
        estimated_size_mb=len(request.underlyings) * 2.0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Adjustment Factors (Stub)
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/adjustment-factors",
    response_model=BulkJobCreatedResponse,
    summary="Create bulk adjustment factors job",
    description="Submit a bulk request for adjustment factors across multiple symbols.",
)
async def create_bulk_adjustment_factors_job(
    request: BulkAdjustmentFactorsRequestModel,
    client: Any = Depends(require_api_key),
) -> BulkJobCreatedResponse:
    """Create a bulk adjustment factors job."""
    settings = get_settings()
    if not settings.allow_stub_data:
        raise HTTPException(
            status_code=501,
            detail="Bulk adjustment factors are not configured (stub disabled)",
        )

    manager = get_bulk_manager()

    bulk_request = BulkAdjustmentFactorsRequest(
        symbols=[s.upper() for s in request.symbols],
        start=request.start,
        end=request.end,
        format=request.format,
    )

    errors = bulk_request.validate()
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    try:
        job = await manager.create_adjustments_job(bulk_request, client.id)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return BulkJobCreatedResponse(
        job_id=job.job_id,
        status="accepted",
        estimated_records=len(request.symbols) * 10,
        estimated_size_mb=round(len(request.symbols) * 0.1, 2),
    )
