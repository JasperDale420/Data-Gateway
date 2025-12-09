from fastapi import APIRouter, HTTPException
from dataingestion.features.uw_data.schemas import (
    FlowResponse,
    DarkPoolResponse,
    TideResponse,
    InsiderResponse,
    CongressResponse,
    NewsResponse,
    ShortInterestResponse,
    GreekExposureResponse,
    OptionContractResponse,
    MaxPainResponse,
    InstitutionResponse,
    ETFHoldingsResponse,
    ScreenerResponse,
    SeasonalityResponse,
    NetFlowResponse,
    GroupFlowResponse,
    DarkPoolRecentResponse,
    ShortVolumeResponse,
    FailToDeliverResponse,
    OptionContractHistoryResponse,
    OptionScreenerResponse,
    EarningsResponse,
    CongressLateReportResponse,
    PoliticianPortfolioResponse,
    StockInfoResponse,
    StockOHLCResponse,
    AlertResponse,
)
from dataingestion.features.uw_data.ingestor import uw_ingestor
from dataingestion.shared.cache import cached

router = APIRouter(prefix="/uw", tags=["Unusual Whales"])


@router.get("/flow/{ticker}", response_model=FlowResponse)
@cached(ttl=60, prefix="uw_flow")  # Short cache for flow
async def get_flow(ticker: str):
    """Get option flow for a ticker."""
    try:
        return await uw_ingestor.get_flow(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/darkpool/{ticker}", response_model=DarkPoolResponse)
@cached(ttl=60, prefix="uw_darkpool")
async def get_darkpool(ticker: str):
    """Get dark pool trades for a ticker."""
    try:
        return await uw_ingestor.get_darkpool(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market/tide", response_model=TideResponse)
@cached(ttl=60, prefix="uw_market_tide")
async def get_market_tide():
    """Get market tide data."""
    try:
        return await uw_ingestor.get_market_tide()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market/sector-tide/{sector}", response_model=TideResponse)
@cached(ttl=60, prefix="uw_sector_tide")
async def get_sector_tide(sector: str):
    """Get sector tide data."""
    try:
        return await uw_ingestor.get_sector_tide(sector)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market/etf-tide/{ticker}", response_model=TideResponse)
@cached(ttl=60, prefix="uw_etf_tide")
async def get_etf_tide(ticker: str):
    """Get ETF tide data."""
    try:
        return await uw_ingestor.get_etf_tide(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insider/transactions", response_model=InsiderResponse)
@cached(ttl=300, prefix="uw_insider")
async def get_insider_transactions():
    """Get insider transactions."""
    try:
        return await uw_ingestor.get_insider_transactions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/congress/trades", response_model=CongressResponse)
@cached(ttl=300, prefix="uw_congress")
async def get_congress_trades():
    """Get congress trades."""
    try:
        return await uw_ingestor.get_congress_trades()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news", response_model=NewsResponse)
@cached(ttl=60, prefix="uw_news")
async def get_news_headlines():
    """Get news headlines."""
    try:
        return await uw_ingestor.get_news_headlines()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shorts/{ticker}", response_model=ShortInterestResponse)
@cached(ttl=3600, prefix="uw_shorts")
async def get_short_interest(ticker: str):
    """Get short interest data."""
    try:
        return await uw_ingestor.get_short_interest(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock/{ticker}/greek-exposure", response_model=GreekExposureResponse)
@cached(ttl=60, prefix="uw_greeks")
async def get_greek_exposure(ticker: str):
    """Get greek exposure."""
    try:
        return await uw_ingestor.get_greek_exposure(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock/{ticker}/option-contracts", response_model=OptionContractResponse)
@cached(ttl=300, prefix="uw_contracts")  # Longer cache for contract lists
async def get_option_contracts(ticker: str):
    """Get option contracts."""
    try:
        return await uw_ingestor.get_option_contracts(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock/{ticker}/max-pain", response_model=MaxPainResponse)
@cached(ttl=3600, prefix="uw_max_pain")
async def get_max_pain(ticker: str):
    """Get max pain."""
    try:
        return await uw_ingestor.get_max_pain(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/institution/{name}/holdings", response_model=InstitutionResponse)
@cached(ttl=3600, prefix="uw_inst_holdings")
async def get_institution_holdings(name: str):
    """Get institution holdings."""
    try:
        return await uw_ingestor.get_institution_holdings(name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/etf/{ticker}/holdings", response_model=ETFHoldingsResponse)
@cached(ttl=3600, prefix="uw_etf_holdings")
async def get_etf_holdings(ticker: str):
    """Get ETF holdings."""
    try:
        return await uw_ingestor.get_etf_holdings(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/screener/stocks", response_model=ScreenerResponse)
@cached(ttl=300, prefix="uw_screener")
async def get_screener_stocks():
    """Get stock screener results."""
    try:
        return await uw_ingestor.get_screener_stocks()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/seasonality/{ticker}/monthly", response_model=SeasonalityResponse)
@cached(ttl=86400, prefix="uw_seasonality")  # Long cache for seasonality
async def get_seasonality(ticker: str):
    """Get monthly seasonality."""
    try:
        return await uw_ingestor.get_seasonality(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/net-flow/{ticker}", response_model=NetFlowResponse)
@cached(ttl=60, prefix="uw_net_flow")
async def get_net_flow(ticker: str):
    """Get net flow data."""
    try:
        return await uw_ingestor.get_net_flow(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/group-flow/{ticker}", response_model=GroupFlowResponse)
@cached(ttl=60, prefix="uw_group_flow")
async def get_group_flow(ticker: str):
    """Get group flow data."""
    try:
        return await uw_ingestor.get_group_flow(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/darkpool/recent", response_model=DarkPoolRecentResponse)
@cached(ttl=60, prefix="uw_recent_darkpool")
async def get_recent_darkpool():
    """Get recent dark pool orders."""
    try:
        return await uw_ingestor.get_recent_darkpool_orders()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shorts/{ticker}/volume", response_model=ShortVolumeResponse)
@cached(ttl=3600, prefix="uw_short_vol")
async def get_short_volume(ticker: str):
    """Get short volume data."""
    try:
        return await uw_ingestor.get_short_volume(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shorts/{ticker}/ftds", response_model=FailToDeliverResponse)
@cached(ttl=3600, prefix="uw_ftds")
async def get_ftds(ticker: str):
    """Get FTD data."""
    try:
        return await uw_ingestor.get_ftds(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/option-contract/{contract_id}/history",
    response_model=OptionContractHistoryResponse,
)
@cached(ttl=3600, prefix="uw_contract_history")
async def get_contract_history(contract_id: str):
    """Get option contract history."""
    try:
        return await uw_ingestor.get_option_contract_history(contract_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/screener/options", response_model=OptionScreenerResponse)
@cached(ttl=60, prefix="uw_screener_opts")
async def get_option_screener():
    """Get option screener results."""
    try:
        return await uw_ingestor.get_option_screener()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/earnings/{ticker}", response_model=EarningsResponse)
@cached(ttl=3600, prefix="uw_earnings")
async def get_earnings(ticker: str):
    """Get earnings data."""
    try:
        return await uw_ingestor.get_earnings(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/congress/late-reports", response_model=CongressLateReportResponse)
@cached(ttl=3600, prefix="uw_late_reports")
async def get_congress_late_reports():
    """Get congress late reports."""
    try:
        return await uw_ingestor.get_congress_late_reports()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/politician-portfolios/{politician}", response_model=PoliticianPortfolioResponse
)
@cached(ttl=3600, prefix="uw_poly_port")
async def get_politician_portfolio(politician: str):
    """Get politician portfolio (Enterprise)."""
    try:
        return await uw_ingestor.get_politician_portfolio(politician)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock/{ticker}/info", response_model=StockInfoResponse)
@cached(ttl=3600, prefix="uw_stock_info")
async def get_stock_info(ticker: str):
    """Get stock info."""
    try:
        return await uw_ingestor.get_stock_info(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock/{ticker}/ohlc", response_model=StockOHLCResponse)
@cached(ttl=3600, prefix="uw_stock_ohlc")
async def get_stock_ohlc(ticker: str):
    """Get stock OHLC."""
    try:
        return await uw_ingestor.get_stock_ohlc(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts", response_model=AlertResponse)
@cached(ttl=60, prefix="uw_alerts")
async def get_alerts():
    """Get user alerts."""
    try:
        return await uw_ingestor.get_user_alerts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
