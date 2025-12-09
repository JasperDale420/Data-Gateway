from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import date
from dataingestion.features.news_data.schemas import NewsQuery
from dataingestion.features.news_data.ingestor import news_ingestor
from dataingestion.shared.cache import cached
import asyncio

router = APIRouter(prefix="/news", tags=["NewsAPI"])


@router.get("/headlines")
@cached(ttl=300, prefix="news_headlines")  # Cache for 5 minutes
async def get_headlines(
    q: str,
    from_date: Optional[date] = Query(None, alias="from"),
    to_date: Optional[date] = Query(None, alias="to"),
    language: str = "en",
    sort_by: str = "relevancy",
    page_size: int = 100,
):
    """Get headlines from NewsAPI (non-blocking)."""
    try:
        query = NewsQuery(
            q=q,
            from_param=from_date,
            to=to_date,
            language=language,
            sort_by=sort_by,
            page_size=page_size,
        )
        # Run synchronous ingestor in a thread pool
        return await asyncio.to_thread(news_ingestor.ingest, query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
