from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date
from dataingestion.features.fred_data.schemas import FredQuery
from dataingestion.features.fred_data.ingestor import fred_ingestor
from dataingestion.shared.cache import cached
import asyncio

router = APIRouter(prefix="/fred", tags=["FRED"])


@router.get("/series/{series_id}")
@cached(ttl=86400, prefix="fred_series")  # Cache for 24 hours
async def get_series(
    series_id: str, start_date: Optional[date] = None, end_date: Optional[date] = None
):
    """Get economic series from FRED (non-blocking)."""
    try:
        query = FredQuery(series_id=series_id, start_date=start_date, end_date=end_date)
        # Run synchronous ingestor in a thread pool
        df = await asyncio.to_thread(fred_ingestor.ingest, query)

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="Series not found or no data")

        # Return as JSON records
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
