"""Alpaca news endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from gateway.api.alpaca.common import (
    DESC_END_TIME,
    DESC_START_TIME,
    Client,
    execute_alpaca_provider_call,
    get_registry,
    parse_comma_values,
    require_api_key,
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
    symbols_list = parse_comma_values(symbols, uppercase=True) if symbols else None
    articles = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_news(
            symbols=symbols_list,
            start=start,
            end=end,
            limit=limit,
            include_content=include_content,
        ),
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
