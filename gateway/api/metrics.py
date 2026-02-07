"""Prometheus metrics endpoint."""

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from gateway.api.deps import require_api_key
from gateway.core.metrics import update_memory_metrics_if_due

router = APIRouter(
    tags=["metrics"],
    dependencies=[Depends(require_api_key)],
)


@router.get("/metrics", response_class=PlainTextResponse)
async def get_metrics():
    """Prometheus metrics endpoint.

    Returns metrics in Prometheus text format for scraping.
    """
    # Update memory metrics on an interval; still serve scrape output each request.
    update_memory_metrics_if_due()

    return PlainTextResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
