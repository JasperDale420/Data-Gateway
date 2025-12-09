from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class Bar(BaseModel):
    t: datetime
    o: float
    h: float
    l: float  # noqa: E741
    c: float
    v: float
    n: Optional[int] = None
    vw: Optional[float] = None


class BarResponse(BaseModel):
    symbol: str
    bars: List[Bar]


class Snapshot(BaseModel):
    symbol: str
    price: float
    size: int
    timestamp: datetime


class OptionBar(BaseModel):
    t: datetime
    o: float
    h: float
    l: float  # noqa: E741
    c: float
    v: int
    n: int
    vw: float


class OptionTrade(BaseModel):
    t: datetime
    x: str
    p: float
    s: int
    c: str


class OptionBarResponse(BaseModel):
    symbol: str
    bars: List[OptionBar]
    next_page_token: Optional[str] = None


class OptionTradeResponse(BaseModel):
    symbol: str
    trades: List[OptionTrade]
    next_page_token: Optional[str] = None


class OrderRequest(BaseModel):
    symbol: str
    qty: float
    side: str
    type: str = "market"
    time_in_force: str = "day"
    limit_price: Optional[float] = None


class OrderResponse(BaseModel):
    id: str
    symbol: str
    status: str
    qty: Optional[float] = None
    filled_qty: Optional[float] = None
    side: Optional[str] = None
    type: Optional[str] = None


class AccountResponse(BaseModel):
    id: str
    status: str
    cash: str
    portfolio_value: str
    buying_power: str


class OptionContract(BaseModel):
    symbol: str
    type: str
    strike_price: float
    expiration_date: str
    open_interest: Optional[float] = None
