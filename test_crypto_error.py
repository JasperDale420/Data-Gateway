import asyncio
from datetime import UTC, datetime

from gateway.providers.alpaca import AlpacaProvider


async def main():
    provider = AlpacaProvider()
    await provider.initialize({})
    try:
        await provider.get_crypto_bars(
            "BTC/USD", "1Day", datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)
        )
    except Exception as e:
        print(f"Exception type: {type(e)}")
        print(f"Exception: {e}")


if __name__ == "__main__":
    asyncio.run(main())
