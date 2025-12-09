from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
from typing import Optional, List
from dataingestion.features.alpaca_data.schemas import (
    BarResponse,
    OptionBarResponse,
    OptionTradeResponse,
    OrderRequest,
    OrderResponse,
    AccountResponse,
    OptionContract,
)
from dataingestion.features.alpaca_data.ingestor import alpaca_ingestor
from dataingestion.shared.cache import cached

router = APIRouter(prefix="/alpaca", tags=["Alpaca"])


@router.get("/account", response_model=AccountResponse)
async def get_account():
    """Get Alpaca Account Info."""
    try:
        acct = alpaca_ingestor.get_account()
        return AccountResponse(
            id=str(acct.id),
            status=str(acct.status),
            cash=str(acct.cash),
            portfolio_value=str(acct.portfolio_value),
            buying_power=str(acct.buying_power),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/orders", response_model=OrderResponse)
async def place_order(order: OrderRequest):
    """Submit an order."""
    try:
        res = alpaca_ingestor.submit_order(
            symbol=order.symbol,
            qty=order.qty,
            side=order.side,
            type=order.type,
            time_in_force=order.time_in_force,
            limit_price=order.limit_price,
        )
        return OrderResponse(
            id=str(res.id),
            symbol=res.symbol,
            status=str(res.status),
            qty=float(res.qty) if res.qty else None,
            filled_qty=float(res.filled_qty) if res.filled_qty else None,
            side=str(res.side),
            type=str(res.type),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quotes/latest/{symbol}")
async def get_latest_quote(symbol: str):
    """Get latest stock quote."""
    try:
        quote = alpaca_ingestor.get_latest_quote(symbol)
        if not quote:
            return {}
        
        return {
            "symbol": symbol,
            "ask_price": quote.ask_price,
            "bid_price": quote.bid_price,
            "timestamp": str(quote.timestamp)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/options/contracts/{symbol}", response_model=List[OptionContract])
async def get_contracts(symbol: str, limit: int = 100):
    """Get active option contracts for an underlying symbol."""
    try:
        res = alpaca_ingestor.get_option_contracts(symbol, limit)
        contracts = []
        if hasattr(res, "option_contracts"):
            for c in res.option_contracts:
                contracts.append(
                    OptionContract(
                        symbol=c.symbol,
                        type=c.type,
                        strike_price=c.strike_price,
                        expiration_date=str(c.expiration_date),
                        open_interest=(
                            float(c.open_interest) if c.open_interest else None
                        ),
                    )
                )
        return contracts
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bars/{symbol}", response_model=BarResponse)
@cached(ttl=3600, prefix="alpaca_bars")
async def get_bars(
    symbol: str,
    timeframe: str = "1Day",
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
):
    """
    Get historical bars for a symbol.
    Default start is 30 days ago.
    """
    if not start:
        start = datetime.now() - timedelta(days=30)

    try:
        # Note: Alpaca SDK is synchronous, so we should ideally run this in a threadpool
        # For now, we'll call it directly, but in high load, use run_in_executor
        import asyncio

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, alpaca_ingestor.get_bars, symbol, timeframe, start, end
        )
        # The lambda wrapper is needed because get_bars is currently synchronous in my implementation above?
        # Wait, I defined get_bars as async but the SDK calls are sync.
        # Let me fix the ingestor to be sync and wrap it here, OR make ingestor use run_in_executor internally.
        # I'll check the ingestor implementation again.
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/options/bars/{symbol}", response_model=OptionBarResponse)
@cached(ttl=3600, prefix="alpaca_options_bars")
async def get_option_bars(
    symbol: str,
    timeframe: str = "1Day",
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
):
    """
    Get historical option bars for a symbol.
    Default start is 30 days ago.
    """
    if not start:
        start = datetime.now() - timedelta(days=30)

    try:
        import asyncio

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, alpaca_ingestor.get_option_bars, symbol, timeframe, start, end
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/options/trades/{symbol}", response_model=OptionTradeResponse)
@cached(ttl=3600, prefix="alpaca_options_trades")
async def get_option_trades(
    symbol: str, start: Optional[datetime] = None, end: Optional[datetime] = None
):
    """
    Get historical option trades for a symbol.
    Default start is 1 day ago.
    """
    if not start:
        start = datetime.now() - timedelta(days=1)

    try:
        import asyncio

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, alpaca_ingestor.get_option_trades, symbol, start, end
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
