import asyncio
from datetime import datetime, timedelta
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOptionContractsRequest
from dataingestion.features.alpaca_data.ingestor import alpaca_ingestor
from dataingestion.shared.config import get_settings

settings = get_settings()

async def verify():
    print("Verifying Alpaca Options Data Ingestion...")
    
    # 1. Find a valid option symbol using TradingClient
    trading_client = TradingClient(settings.APCA_API_KEY_ID, settings.APCA_API_SECRET_KEY, paper=True)
    
    # Get a near-term expiration date
    # This is a bit tricky without complex logic, so let's try to list contracts for AAPL
    # and pick the first one.
    req = GetOptionContractsRequest(underlying_symbols=["AAPL"], limit=1, status="active")
    contracts = trading_client.get_option_contracts(req)
    
    if not contracts.option_contracts:
        print("No active AAPL option contracts found. Cannot verify.")
        return

    symbol = contracts.option_contracts[0].symbol
    print(f"Testing with Option Symbol: {symbol}")
    
    # 2. Test get_option_bars
    print(f"\nFetching bars for {symbol}...")
    end = datetime.now()
    start = end - timedelta(days=5)
    
    try:
        bars_resp = alpaca_ingestor.get_option_bars(symbol, "1Hour", start, end)
        print(f"Success! Retrieved {len(bars_resp.bars)} bars.")
        if bars_resp.bars:
            print(f"Sample Bar: {bars_resp.bars[0]}")
    except Exception as e:
        print(f"Error fetching bars: {e}")

    # 3. Test get_option_trades
    print(f"\nFetching trades for {symbol}...")
    # Trades are high volume, keep window short
    start_trades = end - timedelta(minutes=15)
    
    try:
        trades_resp = alpaca_ingestor.get_option_trades(symbol, start_trades, end)
        print(f"Success! Retrieved {len(trades_resp.trades)} trades.")
        if trades_resp.trades:
            print(f"Sample Trade: {trades_resp.trades[0]}")
    except Exception as e:
        print(f"Error fetching trades: {e}")

if __name__ == "__main__":
    asyncio.run(verify())
