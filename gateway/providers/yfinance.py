"""yfinance-based data provider for fundamentals and financials."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from gateway.core.logger import logger
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar


class YFinanceProvider(DataProvider):
    """yfinance-based provider for company fundamentals and financials.

    yfinance is a scraper-based library. All blocking SDK calls are
    offloaded via ``asyncio.to_thread``. Response caching (300s TTL)
    is handled at the API route layer (``gateway/api/yf.py``).
    """

    def __init__(self):
        self._initialized = False

    @property
    def name(self) -> str:
        return "yfinance"

    @property
    def supported_feeds(self) -> list[str]:
        return ["fundamentals", "financials", "history"]

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_quotes=False,
            supports_trades=False,
            supports_options=True,
            supports_streaming=False,
            supports_historical=True,
            max_symbols_per_request=10,
            max_historical_range_days=3650,
            rate_limit_requests_per_minute=30,
            supports_adjusted_prices=True,
            supports_extended_hours=False,
            supported_timeframes=["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"],
        )

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize yfinance provider."""
        try:
            import yfinance  # noqa: F401

            self._initialized = True
            logger.info("yfinance_provider_initialized")
        except ImportError:
            logger.error("yfinance_not_installed", hint="pip install yfinance")
            raise RuntimeError("yfinance package not installed")

    async def shutdown(self) -> None:
        """Shutdown provider."""
        self._initialized = False
        logger.info("yfinance_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Check provider health."""
        if not self._initialized:
            return HealthStatus(healthy=False, error="Not initialized")

        try:
            import yfinance as yf

            def _probe():
                ticker = yf.Ticker("AAPL")
                return ticker.info.get("symbol")

            start = datetime.now(UTC)
            await asyncio.to_thread(_probe)
            latency = (datetime.now(UTC) - start).total_seconds() * 1000

            return HealthStatus(
                healthy=True,
                latency_ms=latency,
                last_check=datetime.now(UTC),
            )
        except Exception as e:
            return HealthStatus(
                healthy=False,
                error=str(e),
                last_check=datetime.now(UTC),
            )

    # ─────────────────────────────────────────────────────────────────
    # Ticker Info
    # ─────────────────────────────────────────────────────────────────

    async def get_ticker_info(self, symbol: str) -> dict[str, Any]:
        """Get full ticker information."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            return ticker.info

        return await asyncio.to_thread(_fetch)

    async def get_company_info(self, symbol: str) -> dict[str, Any]:
        """Get company info (sector, industry, description)."""
        info = await self.get_ticker_info(symbol)
        return {
            "symbol": symbol.upper(),
            "name": info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "description": info.get("longBusinessSummary"),
            "website": info.get("website"),
            "employees": info.get("fullTimeEmployees"),
            "country": info.get("country"),
            "city": info.get("city"),
        }

    # ─────────────────────────────────────────────────────────────────
    # Financials
    # ─────────────────────────────────────────────────────────────────

    async def get_financials(self, symbol: str) -> dict[str, Any]:
        """Get income statement, balance sheet, cash flow."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            return {
                "income_statement": (ticker.income_stmt.to_dict() if ticker.income_stmt is not None else {}),
                "balance_sheet": (ticker.balance_sheet.to_dict() if ticker.balance_sheet is not None else {}),
                "cash_flow": ticker.cashflow.to_dict() if ticker.cashflow is not None else {},
            }

        return await asyncio.to_thread(_fetch)

    async def get_earnings(self, symbol: str) -> dict[str, Any]:
        """Get quarterly and annual earnings."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            earnings = ticker.earnings_history
            return {
                "earnings_history": earnings.to_dict() if earnings is not None else {},
                "quarterly_earnings": (
                    ticker.quarterly_earnings.to_dict() if ticker.quarterly_earnings is not None else {}
                ),
            }

        return await asyncio.to_thread(_fetch)

    # ─────────────────────────────────────────────────────────────────
    # Historical Data
    # ─────────────────────────────────────────────────────────────────

    # Canonical NormalizedBar timeframe → yfinance `interval` string.
    # yfinance accepts e.g. `1m`, `5m`, `1h`, `1d`, `1wk`, `1mo`.
    _CANONICAL_TIMEFRAME_MAP: dict[str, str] = {
        "1Min": "1m",
        "2Min": "2m",
        "5Min": "5m",
        "15Min": "15m",
        "30Min": "30m",
        "1Hour": "1h",
        "1Day": "1d",
        "1Week": "1wk",
        "1Month": "1mo",
    }

    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> list[NormalizedBar]:
        """Canonical `DataProvider.get_bars` dispatcher.

        Translates `timeframe` to the yfinance `interval` argument and
        delegates per-symbol to `get_history`, passing `start`/`end` as
        ISO date strings (yfinance reads inclusive start, exclusive end).
        Accepts either canonical NormalizedBar timeframes (`1Min`,
        `5Min`, `1Day`, ...) or raw yfinance intervals (`1m`, `1d`).
        """
        # Accept either the canonical form or the raw yfinance interval.
        yf_interval = self._CANONICAL_TIMEFRAME_MAP.get(timeframe, timeframe)

        start_str = start.strftime("%Y-%m-%d") if isinstance(start, datetime) else start
        end_str = end.strftime("%Y-%m-%d") if isinstance(end, datetime) else end

        all_bars: list[NormalizedBar] = []
        for symbol in symbols:
            bars = await self.get_history(
                symbol=symbol,
                interval=yf_interval,
                start=start_str,
                end=end_str,
                **kwargs,
            )
            all_bars.extend(bars)
        return all_bars

    async def get_history(
        self,
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
        start: str | None = None,
        end: str | None = None,
    ) -> list[NormalizedBar]:
        """Get historical OHLCV data."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            # auto_adjust=False keeps Close split-adjusted but NOT
            # dividend-adjusted, matching Alpaca's adjustment="split" basis. With
            # the yfinance default (auto_adjust=True) Close is split+dividend
            # adjusted, so an Alpaca→yfinance failover silently switched the
            # price basis and introduced a discontinuity at the boundary.
            if start and end:
                df = ticker.history(start=start, end=end, interval=interval, auto_adjust=False)
            else:
                df = ticker.history(period=period, interval=interval, auto_adjust=False)
            return df

        df = await asyncio.to_thread(_fetch)
        return self._bars_from_history_df(df=df, symbol=symbol, interval=interval)

    @staticmethod
    def _bars_from_history_df(df: Any, symbol: str, interval: str) -> list[NormalizedBar]:
        """Convert historical DataFrame rows into normalized bars.

        Uses `itertuples` for lower row-iteration overhead than `iterrows`.
        """
        if df is None or getattr(df, "empty", False):
            return []

        symbol_upper = symbol.upper()
        results: list[NormalizedBar] = []
        append = results.append
        for row in df.itertuples(index=True):
            timestamp_value = row.Index
            timestamp = (
                timestamp_value.to_pydatetime() if hasattr(timestamp_value, "to_pydatetime") else timestamp_value
            )
            append(
                NormalizedBar(
                    symbol=symbol_upper,
                    timestamp=timestamp,
                    open=Decimal(str(row.Open)),
                    high=Decimal(str(row.High)),
                    low=Decimal(str(row.Low)),
                    close=Decimal(str(row.Close)),
                    volume=int(row.Volume),
                    provider="yfinance",
                    timeframe=interval,
                )
            )
        return results

    # ─────────────────────────────────────────────────────────────────
    # Options
    # ─────────────────────────────────────────────────────────────────

    async def get_options_expirations(self, symbol: str) -> list[str]:
        """Get available option expirations."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            return list(ticker.options)

        return await asyncio.to_thread(_fetch)

    async def get_options_chain(self, symbol: str, expiration: str) -> dict[str, Any]:
        """Get option chain for a specific expiration."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            chain = ticker.option_chain(expiration)
            return {
                "calls": chain.calls.to_dict(orient="records"),
                "puts": chain.puts.to_dict(orient="records"),
            }

        return await asyncio.to_thread(_fetch)

    # ─────────────────────────────────────────────────────────────────
    # Analyst & Institutional
    # ─────────────────────────────────────────────────────────────────

    async def get_recommendations(self, symbol: str) -> list[dict]:
        """Get analyst recommendations."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            recs = ticker.recommendations
            if recs is None or recs.empty:
                return []
            return recs.reset_index().to_dict(orient="records")

        return await asyncio.to_thread(_fetch)

    async def get_holders(self, symbol: str) -> dict[str, Any]:
        """Get institutional and insider holders."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            return {
                "major_holders": (ticker.major_holders.to_dict() if ticker.major_holders is not None else {}),
                "institutional_holders": (
                    ticker.institutional_holders.to_dict(orient="records")
                    if ticker.institutional_holders is not None
                    else []
                ),
                "insider_holders": (
                    ticker.insider_roster_holders.to_dict(orient="records")
                    if ticker.insider_roster_holders is not None
                    else []
                ),
            }

        return await asyncio.to_thread(_fetch)

    async def get_calendar(self, symbol: str) -> dict[str, Any]:
        """Get earnings and dividend calendar."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            cal = ticker.calendar
            if cal is None:
                return {}
            if hasattr(cal, "to_dict"):
                return cal.to_dict()
            return dict(cal) if cal else {}

        return await asyncio.to_thread(_fetch)

    # ─────────────────────────────────────────────────────────────────
    # Phase 4: Additional Data
    # ─────────────────────────────────────────────────────────────────

    async def get_dividends(self, symbol: str) -> list[dict]:
        """Get historical dividend payments."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            divs = ticker.dividends
            if divs is None or divs.empty:
                return []
            return [{"date": str(date.date()), "dividend": float(val)} for date, val in divs.items()]

        return await asyncio.to_thread(_fetch)

    async def get_splits(self, symbol: str) -> list[dict]:
        """Get historical stock splits."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            splits = ticker.splits
            if splits is None or splits.empty:
                return []
            return [{"date": str(date.date()), "ratio": str(val)} for date, val in splits.items()]

        return await asyncio.to_thread(_fetch)

    async def get_actions(self, symbol: str) -> dict[str, Any]:
        """Get combined dividends and splits."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            actions = ticker.actions
            if actions is None or actions.empty:
                return {"dividends": [], "splits": []}
            return actions.reset_index().to_dict(orient="records")

        return await asyncio.to_thread(_fetch)

    async def get_news(self, symbol: str) -> list[dict]:
        """Get recent news articles."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            news = ticker.news
            if not news:
                return []
            return [
                {
                    "title": n.get("title"),
                    "publisher": n.get("publisher"),
                    "link": n.get("link"),
                    "published": n.get("providerPublishTime"),
                    "type": n.get("type"),
                }
                for n in news
            ]

        return await asyncio.to_thread(_fetch)

    async def get_sustainability(self, symbol: str) -> dict[str, Any]:
        """Get ESG sustainability scores."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            sus = ticker.sustainability
            if sus is None or sus.empty:
                return {}
            return sus.to_dict()

        return await asyncio.to_thread(_fetch)

    async def get_major_holders(self, symbol: str) -> dict[str, Any]:
        """Get major holders breakdown (% held by insiders, institutions)."""
        import yfinance as yf

        def _fetch():
            ticker = yf.Ticker(symbol.upper())
            mh = ticker.major_holders
            return self._major_holders_from_df(mh)

        return await asyncio.to_thread(_fetch)

    @staticmethod
    def _major_holders_from_df(df: Any) -> dict[str, Any]:
        """Convert major holders DataFrame into key/value mapping.

        Uses `itertuples` to avoid `iterrows` row-Series allocation overhead.
        """
        if df is None or getattr(df, "empty", False):
            return {}

        result: dict[str, Any] = {}
        for row in df.itertuples(index=False):
            values = tuple(row)
            if len(values) < 2:
                continue
            label = values[1]
            value = values[0]
            result[str(label)] = value
        return result
