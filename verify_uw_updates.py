import asyncio
import sys
# Hack to avoid importing local 'unusualwhales' stub from Kairos if present in sys.path
paths_to_remove = [p for p in sys.path if "Kairos" in p]
for p in paths_to_remove:
    sys.path.remove(p)

from dataingestion.features.uw_data.ingestor import uw_ingestor
from dataingestion.shared.config import get_settings

settings = get_settings()

async def verify():
    print("Verifying Unusual Whales Updates...")
    
    # 1. Market Tide
    print("\nFetching Market Tide...")
    tide = await uw_ingestor.get_market_tide()
    print(f"Retrieved {len(tide.tide)} tide records.")
    if tide.tide:
        print(f"Sample: {tide.tide[0]}")

    # 2. Sector Tide (e.g., Technology)
    print("\nFetching Sector Tide (Technology)...")
    sector_tide = await uw_ingestor.get_sector_tide("Technology")
    print(f"Retrieved {len(sector_tide.tide)} sector tide records.")

    # 3. ETF Tide (e.g., SPY)
    print("\nFetching ETF Tide (SPY)...")
    etf_tide = await uw_ingestor.get_etf_tide("SPY")
    print(f"Retrieved {len(etf_tide.tide)} ETF tide records.")

    # 4. Insider Transactions
    print("\nFetching Insider Transactions...")
    insiders = await uw_ingestor.get_insider_transactions()
    print(f"Retrieved {len(insiders.transactions)} transactions.")
    if insiders.transactions:
        print(f"Sample: {insiders.transactions[0]}")

    # 5. Congress Trades
    print("\nFetching Congress Trades...")
    congress = await uw_ingestor.get_congress_trades()
    print(f"Retrieved {len(congress.trades)} trades.")
    if congress.trades:
        print(f"Sample: {congress.trades[0]}")

    # 6. News Headlines
    print("\nFetching News Headlines...")
    news = await uw_ingestor.get_news_headlines()
    print(f"Retrieved {len(news.headlines)} headlines.")
    if news.headlines:
        print(f"Sample: {news.headlines[0]}")

    # 7. Short Interest (e.g., AAPL)
    print("\nFetching Short Interest (AAPL)...")
    shorts = await uw_ingestor.get_short_interest("AAPL")
    print(f"Retrieved {len(shorts.data)} short interest records.")
    if shorts.data:
        print(f"Sample: {shorts.data[0]}")

if __name__ == "__main__":
    asyncio.run(verify())
