"""Alpaca news endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_END_TIME,
    DESC_START_TIME,
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/news", response_model=SuccessResponse)
async def get_news(
    symbols: str | None = Query(default=None, description="Comma-separated symbols: AAPL,MSFT"),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    limit: int = Query(default=10, le=50, description="Max articles to return"),
    include_content: bool = Query(default=False, description="Include full article content"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get news articles with optional symbol filtering."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = None
        if symbols:
            symbols_list = [s.strip().upper() for s in symbols.split(",")]

        articles = await provider.get_news(
            symbols=symbols_list,
            start=start,
            end=end,
            limit=limit,
            include_content=include_content,
        )

        return {
            "success": True,
            "data": {
                "articles": [a.model_dump(mode="json") for a in articles],
            },
            "meta": {
                "count": len(articles),
                "provider": "alpaca",
                "symbols_filter": symbols_list,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
