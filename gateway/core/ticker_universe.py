"""Ticker universe management for UW EOD polling.

Maintains a set of tickers to poll: a static core set of high-liquidity
names plus a dynamic set refreshed daily from UW's stock screener.
"""

import asyncio
from datetime import UTC, datetime

import structlog

logger = structlog.get_logger()

# Default core tickers: mega-caps, major ETFs, sector ETFs
DEFAULT_CORE_TICKERS: list[str] = [
    # Broad market ETFs
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    # Mega-cap tech
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    # Semis / high-beta tech
    "AMD",
    "NFLX",
    # Financials
    "JPM",
    "GS",
    "BAC",
    # SPDR sector ETFs (11 GICS sectors)
    "XLF",
    "XLE",
    "XLK",
    "XLV",
    "XLI",
    "XLP",
    "XLU",
    "XLB",
    "XLRE",
    "XLC",
    # Commodities / bonds / volatility
    "GLD",
    "TLT",
    "HYG",
]


class TickerUniverse:
    """Manages the ticker universe for per-symbol UW polling.

    Combines a static core set with a dynamic set pulled daily
    from UW's stock screener (sorted by options activity).
    """

    def __init__(
        self,
        core_tickers: list[str] | None = None,
        dynamic_count: int = 20,
    ):
        self._core = [t.upper() for t in (core_tickers or DEFAULT_CORE_TICKERS)]
        self._dynamic_count = max(0, dynamic_count)
        self._dynamic: list[str] = []
        self._last_refresh: datetime | None = None
        self._last_refresh_date: str | None = None
        self._lock = asyncio.Lock()

    @property
    def core_tickers(self) -> list[str]:
        """Static core tickers (always polled)."""
        return list(self._core)

    @property
    def dynamic_tickers(self) -> list[str]:
        """Dynamic tickers from screener (rotated daily)."""
        return list(self._dynamic)

    @property
    def all_tickers(self) -> list[str]:
        """Combined deduplicated universe (core + dynamic)."""
        seen: set[str] = set()
        result: list[str] = []
        for t in self._core + self._dynamic:
            if t not in seen:
                seen.add(t)
                result.append(t)
        return result

    @property
    def last_refresh(self) -> datetime | None:
        """When dynamic tickers were last refreshed."""
        return self._last_refresh

    async def refresh_dynamic(self, provider) -> int:
        """Refresh dynamic tickers from UW stock screener.

        Only refreshes once per calendar day. Returns the number
        of dynamic tickers after refresh.

        Args:
            provider: UnusualWhalesProvider instance with get_stock_screener()

        Returns:
            Number of dynamic tickers in the universe
        """
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        async with self._lock:
            if self._dynamic_count == 0:
                return 0

            if self._last_refresh_date == today:
                logger.debug("ticker_universe_skip_refresh", reason="already_refreshed_today")
                return len(self._dynamic)

            try:
                screener_results = await provider.get_stock_screener(
                    limit=self._dynamic_count * 2,
                )

                new_dynamic: list[str] = []
                core_set = set(self._core)
                for result in screener_results:
                    symbol = result.symbol if hasattr(result, "symbol") else result.get("symbol", "")
                    if symbol and symbol.upper() not in core_set:
                        new_dynamic.append(symbol.upper())
                    if len(new_dynamic) >= self._dynamic_count:
                        break

                self._dynamic = new_dynamic
                self._last_refresh = datetime.now(UTC)
                self._last_refresh_date = today

                logger.info(
                    "ticker_universe_refreshed",
                    core_count=len(self._core),
                    dynamic_count=len(self._dynamic),
                    total=len(self.all_tickers),
                    dynamic_tickers=self._dynamic[:10],
                )
                return len(self._dynamic)

            except Exception as e:
                logger.error(
                    "ticker_universe_refresh_failed",
                    error=str(e),
                    exc_info=True,
                )
                return len(self._dynamic)
