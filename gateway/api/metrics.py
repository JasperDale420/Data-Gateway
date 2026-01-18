"""Prometheus metrics endpoint."""

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from gateway.core.metrics import update_memory_metrics

router = APIRouter(tags=["metrics"])


@router.get("/metrics", response_class=PlainTextResponse)
async def get_metrics():
    """Prometheus metrics endpoint.

    Returns metrics in Prometheus text format for scraping.
    """
    # Update dynamic metrics before generating output
    update_memory_metrics()

    return PlainTextResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
