import asyncio
from datetime import datetime

from gateway.providers.alpaca import AlpacaProvider


async def main():
    provider = AlpacaProvider()
    await provider.initialize({})
    # Use naive datetimes
    start = datetime(2026, 1, 13)
    end = datetime(2026, 3, 4)
    try:
        bars = await provider.get_bars(["AAPL"], "1Day", start, end)
        print(f"Success! Fetched {len(bars)} bars.")
    except Exception as e:
        print(f"Failed: {e}")
    finally:
        await provider.shutdown()


asyncio.run(main())
