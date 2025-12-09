from fastapi import APIRouter, HTTPException
from dataingestion.features.yfinance_data.schemas import YFinanceResponse, YFinanceQuery
from dataingestion.features.yfinance_data.ingestor import yfinance_ingestor
from dataingestion.shared.cache import cached
import asyncio

router = APIRouter(prefix="/yfinance", tags=["yfinance"])


@router.get("/history/{ticker}", response_model=YFinanceResponse)
@cached(ttl=3600, prefix="yf_history")
async def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    """Get historical data from yfinance (non-blocking)."""
    try:
        query = YFinanceQuery(ticker=ticker, period=period, interval=interval)
        # Run synchronous ingestor in a thread pool
        return await asyncio.to_thread(yfinance_ingestor.get_history, query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
