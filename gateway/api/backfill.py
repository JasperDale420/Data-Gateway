"""Backfill API endpoints for historical data fetching."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from gateway.api.deps import require_api_key
from gateway.core.backfill import (
    BackfillRequest,
    BackfillStatus,
    get_backfill_engine,
)

router = APIRouter(prefix="/api/v1/backfill", tags=["backfill"])


def _job_response(job: Any) -> dict[str, Any]:
    """Format a BackfillJob for API response."""
    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "provider": job.request.provider,
        "feed": job.request.feed,
        "symbols_total": job.symbols_total,
        "symbols_complete": job.symbols_complete,
        "progress": job.progress,
        "records_published": job.records_published,
        "errors": job.errors[:20],  # Limit error list in response
        "created_at": job.created_at.isoformat(),
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "eta_seconds": job.eta_seconds,
    }


@router.post("", status_code=202)
async def submit_backfill(
    request: BackfillRequest,
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Submit a new backfill job.

    Queues a historical data fetch for the specified provider, feed, symbols,
    and date range. Data is published through the Redis Streams -> Heber pipeline.
    """
    engine = get_backfill_engine()

    try:
        job = engine.submit(request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return {
        "success": True,
        "data": _job_response(job),
    }


@router.post("/cancel-all", status_code=200)
async def cancel_all_backfills(
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Cancel all running or queued backfill jobs."""
    engine = get_backfill_engine()
    cancelled_count = engine.cancel_all()
    return {
        "success": True,
        "meta": {"cancelled": cancelled_count},
    }


@router.delete("", status_code=200)
async def flush_backfills(
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Flush all completed jobs from history."""
    engine = get_backfill_engine()
    purged_count = engine.flush()
    return {
        "success": True,
        "meta": {"purged": purged_count},
    }


@router.get("", status_code=200)
async def list_backfill_jobs(
    status: str | None = None,
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """List all backfill jobs, optionally filtered by status."""
    engine = get_backfill_engine()
    jobs = engine.list_jobs()

    if status:
        try:
            filter_status = BackfillStatus(status)
            jobs = [j for j in jobs if j.status == filter_status]
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status filter: {status}. Valid: {[s.value for s in BackfillStatus]}",
            )

    return {
        "success": True,
        "data": [_job_response(j) for j in jobs],
        "meta": {"total": len(jobs)},
    }


@router.get("/feeds", status_code=200)
async def list_supported_feeds(
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """List supported (provider, feed) pairs for backfill."""
    engine = get_backfill_engine()
    return {
        "success": True,
        "data": engine.supported_feeds,
    }


@router.get("/{job_id}", status_code=200)
async def get_backfill_status(
    job_id: str,
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Get status and progress of a backfill job."""
    engine = get_backfill_engine()
    job = engine.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    response = _job_response(job)
    # Include per-symbol breakdown in detail view
    response["symbols"] = {
        sym: {
            "status": sp.status,
            "chunks_total": sp.chunks_total,
            "chunks_complete": sp.chunks_complete,
            "records_published": sp.records_published,
            "errors": sp.errors[:5],
        }
        for sym, sp in job.symbols_progress.items()
    }

    return {
        "success": True,
        "data": response,
    }


@router.delete("/{job_id}", status_code=200)
async def cancel_backfill(
    job_id: str,
    _client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Cancel a running or queued backfill job."""
    engine = get_backfill_engine()
    job = engine.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    if not engine.cancel(job_id):
        raise HTTPException(
            status_code=409,
            detail=f"Job cannot be cancelled (status: {job.status.value})",
        )

    return {
        "success": True,
        "data": _job_response(job),
        "meta": {"message": "Job cancelled"},
    }
