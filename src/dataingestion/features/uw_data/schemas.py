from pydantic import BaseModel
from typing import List, Optional, Any
from datetime import datetime


class Flow(BaseModel):
    id: Optional[str] = None
    ticker: str
    expiry: Optional[str] = None
    strike: Optional[float] = None
    put_call: Optional[str] = None
    premium: Optional[float] = None
    size: Optional[int] = None
    price: Optional[float] = None
    timestamp: Optional[datetime] = None
    # Add other fields as needed based on actual API response


class FlowResponse(BaseModel):
    ticker: str
    flow: List[Flow]


class DarkPoolTrade(BaseModel):
    ticker: str
    price: float
    size: int
    timestamp: datetime
    exchange: Optional[str] = None


class DarkPoolResponse(BaseModel):
    ticker: str
    ticker: str
    trades: List[DarkPoolTrade]


class MarketTide(BaseModel):
    timestamp: datetime
    call_premium: float
    put_premium: float
    net_premium: float
    call_volume: int
    put_volume: int
    # Add other fields as needed


class TideResponse(BaseModel):
    tide: List[MarketTide]


class InsiderTransaction(BaseModel):
    ticker: str
    filing_date: datetime
    transaction_date: Optional[datetime] = None
    insider_name: str
    insider_title: Optional[str] = None
    transaction_type: str
    shares: Optional[float] = None
    price: Optional[float] = None
    value: Optional[float] = None


class InsiderResponse(BaseModel):
    transactions: List[InsiderTransaction]


class CongressTrade(BaseModel):
    ticker: str
    politician: str
    party: Optional[str] = None
    chamber: Optional[str] = None
    transaction_date: Optional[datetime] = None
    filing_date: datetime
    transaction_type: str
    amount_min: Optional[float] = None
    amount_max: Optional[float] = None


class CongressResponse(BaseModel):
    trades: List[CongressTrade]


class NewsHeadline(BaseModel):
    id: str
    headline: str
    url: Optional[str] = None
    source: str
    timestamp: datetime
    tickers: List[str] = []
    sentiment: Optional[Any] = None


class NewsResponse(BaseModel):
    headlines: List[NewsHeadline]


class ShortInterest(BaseModel):
    ticker: str
    date: datetime
    short_interest: float
    float_shares: Optional[float] = None
    days_to_cover: Optional[float] = None


class ShortInterestResponse(BaseModel):
    data: List[ShortInterest]


class GreekExposure(BaseModel):
    ticker: str
    timestamp: datetime
    price: float
    gamma_per_one_percent_move_oi: Optional[float] = None
    delta_per_one_percent_move_oi: Optional[float] = None
    charm_per_one_percent_move_oi: Optional[float] = None
    vanna_per_one_percent_move_oi: Optional[float] = None
    gamma_per_one_percent_move_vol: Optional[float] = None
    delta_per_one_percent_move_vol: Optional[float] = None
    charm_per_one_percent_move_vol: Optional[float] = None
    vanna_per_one_percent_move_vol: Optional[float] = None


class GreekExposureResponse(BaseModel):
    data: List[GreekExposure]


class OptionContract(BaseModel):
    option_symbol: str
    underlying_symbol: str
    expiry: str
    strike: float
    option_type: str


class OptionContractResponse(BaseModel):
    contracts: List[OptionContract]


class MaxPain(BaseModel):
    ticker: str
    date: datetime
    strike: float


class MaxPainResponse(BaseModel):
    data: List[MaxPain]


class InstitutionHolding(BaseModel):
    ticker: str
    shares: float
    value: float
    portfolio_percent: Optional[float] = None


class InstitutionResponse(BaseModel):
    institution: str
    holdings: List[InstitutionHolding]


class ETFHolding(BaseModel):
    ticker: str
    name: str  # Company name
    sector: Optional[str] = None
    shares: float
    weight: float


class ETFHoldingsResponse(BaseModel):
    etf: str
    holdings: List[ETFHolding]


class ScreenerResult(BaseModel):
    ticker: str
    price: float
    volume: float
    market_cap: Optional[float] = None
    industry: Optional[str] = None
    sector: Optional[str] = None


class ScreenerResponse(BaseModel):
    results: List[ScreenerResult]


class Seasonality(BaseModel):
    month: int
    avg_return: float
    win_rate: float


class SeasonalityResponse(BaseModel):
    ticker: str
    monthly_data: List[Seasonality]


class NetFlow(BaseModel):
    date: datetime
    price: float
    total_market_net_premium: float
    total_market_net_volume: int


class NetFlowResponse(BaseModel):
    ticker: str
    data: List[NetFlow]


class GroupFlow(BaseModel):
    date: datetime
    sector: Optional[str] = None
    industry: Optional[str] = None
    net_premium: float
    net_volume: int


class GroupFlowResponse(BaseModel):
    ticker: str
    data: List[GroupFlow]


class DarkPoolRecentResponse(BaseModel):
    trades: List[DarkPoolTrade]


class ShortVolume(BaseModel):
    date: datetime
    volume: int
    short_volume: int
    short_exempt_volume: Optional[int] = None
    market_share: Optional[float] = None


class ShortVolumeResponse(BaseModel):
    ticker: str
    data: List[ShortVolume]


class FailToDeliver(BaseModel):
    date: datetime
    quantity: int
    price: Optional[float] = None
    amount: Optional[float] = None


class FailToDeliverResponse(BaseModel):
    ticker: str
    data: List[FailToDeliver]


class OptionContractHistoryResponse(BaseModel):
    contract_id: str
    data: List[Flow]  # Reusing Flow model for history as it shares structure


class OptionScreenerResult(BaseModel):
    ticker: str
    expiry: str
    strike: float
    put_call: str
    avg_premium: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    implied_volatility: Optional[float] = None


class OptionScreenerResponse(BaseModel):
    contracts: List[OptionScreenerResult]


class EarningsResult(BaseModel):
    ticker: str
    date: datetime
    eps_est: Optional[float] = None
    eps_act: Optional[float] = None
    rev_est: Optional[float] = None
    rev_act: Optional[float] = None
    time: Optional[str] = None  # 'bmo' or 'amc'


class EarningsResponse(BaseModel):
    ticker: str
    earnings: List[EarningsResult]


class CongressLateReport(BaseModel):
    politician: str
    party: str
    state: str
    chamber: str
    filed_date: datetime
    published_date: datetime
    days_late: int


class CongressLateReportResponse(BaseModel):
    reports: List[CongressLateReport]


class PoliticianPortfolio(BaseModel):
    politician: str
    portfolio: List[dict]  # Simplified for now, can be specific strict model if known


class PoliticianPortfolioResponse(BaseModel):
    data: PoliticianPortfolio


class StockInfo(BaseModel):
    ticker: str
    company_name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


class StockInfoResponse(BaseModel):
    data: StockInfo


class StockCandle(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class StockOHLCResponse(BaseModel):
    ticker: str
    candles: List[StockCandle]


class UserAlert(BaseModel):
    id: str
    name: str
    conditions: dict
    created_at: datetime
    active: bool


class AlertResponse(BaseModel):
    alerts: List[UserAlert]
