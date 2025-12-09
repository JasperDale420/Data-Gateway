from pydantic import BaseModel
from typing import List
from datetime import datetime


class YFinanceQuery(BaseModel):
    ticker: str
    period: str = "1mo"  # 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    interval: str = "1d"  # 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo


class YFinanceBar(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    dividends: float
    stock_splits: float


class YFinanceResponse(BaseModel):
    ticker: str
    period: str
    interval: str
    bars: List[YFinanceBar]
