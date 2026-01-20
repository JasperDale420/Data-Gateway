"""Bulk data API endpoints.

Implements bulk data retrieval endpoints as specified in PRD.
"""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from gateway.api.deps import require_api_key
from gateway.core.bulk import (
    BulkBarsRequest,
    BulkJobStatus,
    get_bulk_manager,
)
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
) -> BulkJobCreatedResponse:
    """Create a bulk bars fetch job."""
    manager = get_bulk_manager()

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
        job = await manager.create_bars_job(bulk_request)
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

    if not job:
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

    if not job:
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
    if not job:
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
    jobs = await manager.list_jobs()

    # Filter by status if provided
    if status:
        jobs = [j for j in jobs if j.status.value == status]

    return {
        "jobs": [j.to_dict() for j in jobs],
        "count": len(jobs),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Options (Stub)
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
) -> BulkJobCreatedResponse:
    """Create a bulk options chain fetch job.

    Note: This is a stub endpoint. Full implementation pending.
    """
    # Stub: return a placeholder job ID
    job_id = f"bulk-opt-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    return BulkJobCreatedResponse(
        job_id=job_id,
        status="accepted",
        estimated_records=len(request.underlyings) * 1000,
        estimated_size_mb=len(request.underlyings) * 2.0,
    )
