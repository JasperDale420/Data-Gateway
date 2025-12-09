from fastapi import APIRouter, HTTPException
from datetime import datetime
from dataingestion.features.coinbase_data.schemas import CoinbaseCandleQuery
from dataingestion.features.coinbase_data.ingestor import coinbase_ingestor
from dataingestion.shared.cache import cached
import asyncio

router = APIRouter(prefix="/coinbase", tags=["Coinbase"])


@router.get("/candles/{product_id}")
@cached(ttl=60, prefix="cb_candles")  # Short cache for market data
async def get_candles(
    product_id: str, start: datetime, end: datetime, granularity: str = "ONE_HOUR"
):
    """Get candles from Coinbase Advanced Trade (non-blocking)."""
    try:
        query = CoinbaseCandleQuery(
            product_id=product_id, start=start, end=end, granularity=granularity
        )
        # Run synchronous ingestor in a thread pool
        return await asyncio.to_thread(coinbase_ingestor.get_candles, query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
