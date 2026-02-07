"""Alpha Vantage data provider for quotes, time series, and fundamentals."""

import asyncio
import csv
import io
import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog

from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar, NormalizedQuote

logger = structlog.get_logger()

PROVIDER_NOT_INIT = "Provider not initialized"
API_KEY_NOT_SET = "Alpha Vantage API key not configured"
DEFAULT_QUOTES_MAX_CONCURRENCY = 2


class AlphaVantageProvider(DataProvider):
    """Alpha Vantage data provider for market data and fundamentals."""

    def __init__(self) -> None:
        self._api_key: str = ""
        self._client: httpx.AsyncClient | None = None
        self._base_url: str = "https://www.alphavantage.co/query"
        self._quotes_max_concurrency: int = DEFAULT_QUOTES_MAX_CONCURRENCY

    @property
    def name(self) -> str:
        return "alphavantage"

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_quotes=True,
            supports_trades=False,
            supports_streaming=False,
            supports_historical=True,
            rate_limit_requests_per_minute=5,  # Free tier: 5 calls/min
        )

    @property
    def supported_feeds(self) -> list[str]:
        return ["quotes", "bars", "fundamentals"]

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize Alpha Vantage client."""
        api_key_env = config.get("api_key_env", "ALPHAVANTAGE_API_KEY")
        self._api_key = os.environ.get(api_key_env, "")
        raw_quotes_concurrency = config.get(
            "quotes_max_concurrency", DEFAULT_QUOTES_MAX_CONCURRENCY
        )
        try:
            self._quotes_max_concurrency = min(5, max(1, int(raw_quotes_concurrency)))
        except (TypeError, ValueError):
            logger.warning(
                "alphavantage_invalid_quotes_max_concurrency",
                value=raw_quotes_concurrency,
                default=DEFAULT_QUOTES_MAX_CONCURRENCY,
            )
            self._quotes_max_concurrency = DEFAULT_QUOTES_MAX_CONCURRENCY

        if not self._api_key:
            logger.warning("alphavantage_api_key_not_set", env_var=api_key_env)
            return

        self._client = httpx.AsyncClient(
            timeout=30.0,
            event_hooks=httpx_event_hooks("alphavantage"),
        )
        logger.info("alphavantage_provider_initialized")

    async def shutdown(self) -> None:
        """Shutdown provider."""
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("alphavantage_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Health check - test API connectivity."""
        if not self._api_key:
            return HealthStatus(healthy=False, error=API_KEY_NOT_SET)

        if not self._client:
            return HealthStatus(healthy=False, error=PROVIDER_NOT_INIT)

        try:
            start = datetime.now(UTC)
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "GLOBAL_QUOTE",
                    "symbol": "IBM",
                    "apikey": self._api_key,
                },
            )
            latency = (datetime.now(UTC) - start).total_seconds() * 1000

            if response.status_code == 200:
                data = response.json()
                if "Global Quote" in data or "Note" not in data:
                    return HealthStatus(
                        healthy=True,
                        latency_ms=latency,
                        last_check=datetime.now(UTC),
                    )
                else:
                    return HealthStatus(
                        healthy=False,
                        error="Rate limit exceeded",
                        last_check=datetime.now(UTC),
                    )
            else:
                return HealthStatus(
                    healthy=False,
                    error=f"HTTP {response.status_code}",
                    last_check=datetime.now(UTC),
                )
        except Exception as e:
            return HealthStatus(
                healthy=False,
                error=str(e),
                last_check=datetime.now(UTC),
            )

    # ─────────────────────────────────────────────────────────────────
    # Quote Methods
    # ─────────────────────────────────────────────────────────────────

    async def get_quote(self, symbol: str) -> NormalizedQuote | None:
        """Get real-time quote using GLOBAL_QUOTE endpoint."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "GLOBAL_QUOTE",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            # Check for rate limit
            if "Note" in data:
                logger.warning("alphavantage_rate_limit", message=data["Note"])
                raise RuntimeError("Rate limit exceeded")

            quote_data = data.get("Global Quote", {})
            if not quote_data:
                logger.warning("alphavantage_no_quote", symbol=symbol)
                return None

            price = Decimal(quote_data.get("05. price", "0"))

            return NormalizedQuote(
                symbol=symbol.upper(),
                timestamp=datetime.now(UTC),
                bid_price=price,
                ask_price=price,
                bid_size=0,
                ask_size=0,
                provider="alphavantage",
            )
        except Exception as e:
            logger.error("alphavantage_quote_failed", symbol=symbol, error=str(e))
            raise

    async def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Get quotes for multiple symbols."""
        semaphore = asyncio.Semaphore(self._quotes_max_concurrency)

        async def _fetch_quote(symbol: str) -> NormalizedQuote | None:
            async with semaphore:
                try:
                    return await self.get_quote(symbol)
                except Exception as e:
                    logger.warning("alphavantage_quote_skipped", symbol=symbol, error=str(e))
                    return None

        results = await asyncio.gather(*(_fetch_quote(symbol) for symbol in symbols))
        return [quote for quote in results if quote]

    # ─────────────────────────────────────────────────────────────────
    # Time Series / Historical Bars
    # ─────────────────────────────────────────────────────────────────

    async def get_intraday(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: str = "compact",
    ) -> list[NormalizedBar]:
        """Get intraday time series data.

        Intervals: 1min, 5min, 15min, 30min, 60min
        Outputsize: compact (100 points) or full (30 days)
        """
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "TIME_SERIES_INTRADAY",
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "outputsize": outputsize,
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            time_series_key = f"Time Series ({interval})"
            time_series = data.get(time_series_key, {})

            # Map interval to timeframe
            interval_map = {
                "1min": "1Min",
                "5min": "5Min",
                "15min": "15Min",
                "30min": "30Min",
                "60min": "1Hour",
            }
            timeframe = interval_map.get(interval, "5Min")

            bars = []
            for timestamp_str, ohlcv in time_series.items():
                bars.append(
                    NormalizedBar(
                        symbol=symbol.upper(),
                        timestamp=datetime.fromisoformat(timestamp_str.replace(" ", "T")),
                        open=Decimal(ohlcv.get("1. open", "0")),
                        high=Decimal(ohlcv.get("2. high", "0")),
                        low=Decimal(ohlcv.get("3. low", "0")),
                        close=Decimal(ohlcv.get("4. close", "0")),
                        volume=int(ohlcv.get("5. volume", 0)),
                        provider="alphavantage",
                        timeframe=timeframe,
                    )
                )

            # Sort by timestamp descending (most recent first)
            bars.sort(key=lambda x: x.timestamp, reverse=True)
            logger.info("alphavantage_intraday_fetched", symbol=symbol, count=len(bars))
            return bars

        except Exception as e:
            logger.error("alphavantage_intraday_failed", symbol=symbol, error=str(e))
            raise

    async def get_daily(
        self,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = True,
    ) -> list[NormalizedBar]:
        """Get daily time series data.

        Outputsize: compact (100 days) or full (20+ years)
        """
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        function = "TIME_SERIES_DAILY_ADJUSTED" if adjusted else "TIME_SERIES_DAILY"

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": function,
                    "symbol": symbol.upper(),
                    "outputsize": outputsize,
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            time_series_key = "Time Series (Daily)"
            time_series = data.get(time_series_key, {})

            bars = []
            for date_str, ohlcv in time_series.items():
                bars.append(
                    NormalizedBar(
                        symbol=symbol.upper(),
                        timestamp=datetime.fromisoformat(date_str),
                        open=Decimal(ohlcv.get("1. open", "0")),
                        high=Decimal(ohlcv.get("2. high", "0")),
                        low=Decimal(ohlcv.get("3. low", "0")),
                        close=Decimal(ohlcv.get("4. close", ohlcv.get("5. adjusted close", "0"))),
                        volume=int(ohlcv.get("6. volume", ohlcv.get("5. volume", 0))),
                        provider="alphavantage",
                        timeframe="1Day",
                    )
                )

            bars.sort(key=lambda x: x.timestamp, reverse=True)
            logger.info("alphavantage_daily_fetched", symbol=symbol, count=len(bars))
            return bars

        except Exception as e:
            logger.error("alphavantage_daily_failed", symbol=symbol, error=str(e))
            raise

    async def get_weekly(self, symbol: str, adjusted: bool = True) -> list[NormalizedBar]:
        """Get weekly time series data."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        function = "TIME_SERIES_WEEKLY_ADJUSTED" if adjusted else "TIME_SERIES_WEEKLY"

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": function,
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            time_series_key = "Weekly Adjusted Time Series" if adjusted else "Weekly Time Series"
            time_series = data.get(time_series_key, {})

            bars = []
            for date_str, ohlcv in time_series.items():
                bars.append(
                    NormalizedBar(
                        symbol=symbol.upper(),
                        timestamp=datetime.fromisoformat(date_str),
                        open=Decimal(ohlcv.get("1. open", "0")),
                        high=Decimal(ohlcv.get("2. high", "0")),
                        low=Decimal(ohlcv.get("3. low", "0")),
                        close=Decimal(ohlcv.get("4. close", ohlcv.get("5. adjusted close", "0"))),
                        volume=int(ohlcv.get("6. volume", ohlcv.get("5. volume", 0))),
                        provider="alphavantage",
                        timeframe="1Week",
                    )
                )

            bars.sort(key=lambda x: x.timestamp, reverse=True)
            logger.info("alphavantage_weekly_fetched", symbol=symbol, count=len(bars))
            return bars

        except Exception as e:
            logger.error("alphavantage_weekly_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Fundamentals
    # ─────────────────────────────────────────────────────────────────

    async def get_company_overview(self, symbol: str) -> dict[str, Any]:
        """Get company overview (fundamentals, ratios, etc)."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "OVERVIEW",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            if not data or "Symbol" not in data:
                return {}

            return {
                "symbol": data.get("Symbol"),
                "name": data.get("Name"),
                "description": data.get("Description"),
                "exchange": data.get("Exchange"),
                "currency": data.get("Currency"),
                "country": data.get("Country"),
                "sector": data.get("Sector"),
                "industry": data.get("Industry"),
                "market_cap": data.get("MarketCapitalization"),
                "pe_ratio": data.get("PERatio"),
                "peg_ratio": data.get("PEGRatio"),
                "book_value": data.get("BookValue"),
                "dividend_per_share": data.get("DividendPerShare"),
                "dividend_yield": data.get("DividendYield"),
                "eps": data.get("EPS"),
                "revenue_per_share": data.get("RevenuePerShareTTM"),
                "profit_margin": data.get("ProfitMargin"),
                "operating_margin": data.get("OperatingMarginTTM"),
                "return_on_assets": data.get("ReturnOnAssetsTTM"),
                "return_on_equity": data.get("ReturnOnEquityTTM"),
                "revenue": data.get("RevenueTTM"),
                "gross_profit": data.get("GrossProfitTTM"),
                "ebitda": data.get("EBITDA"),
                "52_week_high": data.get("52WeekHigh"),
                "52_week_low": data.get("52WeekLow"),
                "50_day_ma": data.get("50DayMovingAverage"),
                "200_day_ma": data.get("200DayMovingAverage"),
                "beta": data.get("Beta"),
                "shares_outstanding": data.get("SharesOutstanding"),
                "analyst_target_price": data.get("AnalystTargetPrice"),
                "provider": "alphavantage",
            }

        except Exception as e:
            logger.error("alphavantage_overview_failed", symbol=symbol, error=str(e))
            raise

    async def get_earnings(self, symbol: str) -> dict[str, Any]:
        """Get earnings data (annual and quarterly)."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "EARNINGS",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            return {
                "symbol": data.get("symbol", symbol.upper()),
                "annual_earnings": data.get("annualEarnings", []),
                "quarterly_earnings": data.get("quarterlyEarnings", []),
                "provider": "alphavantage",
            }

        except Exception as e:
            logger.error("alphavantage_earnings_failed", symbol=symbol, error=str(e))
            raise

    async def get_income_statement(self, symbol: str) -> dict[str, Any]:
        """Get income statement data."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "INCOME_STATEMENT",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            return {
                "symbol": data.get("symbol", symbol.upper()),
                "annual_reports": data.get("annualReports", []),
                "quarterly_reports": data.get("quarterlyReports", []),
                "provider": "alphavantage",
            }

        except Exception as e:
            logger.error("alphavantage_income_failed", symbol=symbol, error=str(e))
            raise

    async def get_balance_sheet(self, symbol: str) -> dict[str, Any]:
        """Get balance sheet data."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "BALANCE_SHEET",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            return {
                "symbol": data.get("symbol", symbol.upper()),
                "annual_reports": data.get("annualReports", []),
                "quarterly_reports": data.get("quarterlyReports", []),
                "provider": "alphavantage",
            }

        except Exception as e:
            logger.error("alphavantage_balance_failed", symbol=symbol, error=str(e))
            raise

    async def get_cash_flow(self, symbol: str) -> dict[str, Any]:
        """Get cash flow statement data."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "CASH_FLOW",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            return {
                "symbol": data.get("symbol", symbol.upper()),
                "annual_reports": data.get("annualReports", []),
                "quarterly_reports": data.get("quarterlyReports", []),
                "provider": "alphavantage",
            }

        except Exception as e:
            logger.error("alphavantage_cashflow_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Time Series Extended
    # ─────────────────────────────────────────────────────────────────

    async def get_monthly(self, symbol: str, adjusted: bool = True) -> list[NormalizedBar]:
        """Get monthly time series data."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        function = "TIME_SERIES_MONTHLY_ADJUSTED" if adjusted else "TIME_SERIES_MONTHLY"
        ts_key = "Monthly Adjusted Time Series" if adjusted else "Monthly Time Series"

        try:
            response = await self._client.get(
                self._base_url,
                params={"function": function, "symbol": symbol.upper(), "apikey": self._api_key},
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            time_series = data.get(ts_key, {})
            bars = []
            for date_str, values in time_series.items():
                bars.append(
                    NormalizedBar(
                        symbol=symbol.upper(),
                        timestamp=datetime.fromisoformat(date_str),
                        open=Decimal(values.get("1. open", "0")),
                        high=Decimal(values.get("2. high", "0")),
                        low=Decimal(values.get("3. low", "0")),
                        close=Decimal(
                            values.get("4. close", "0")
                            if not adjusted
                            else values.get("5. adjusted close", "0")
                        ),
                        volume=int(values.get("6. volume" if adjusted else "5. volume", 0)),
                        timeframe="1Month",
                        provider="alphavantage",
                    )
                )
            return sorted(bars, key=lambda x: x.timestamp)

        except Exception as e:
            logger.error("alphavantage_monthly_failed", symbol=symbol, error=str(e))
            raise

    async def search_symbols(self, keywords: str) -> list[dict[str, Any]]:
        """Search for symbols by keywords."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={"function": "SYMBOL_SEARCH", "keywords": keywords, "apikey": self._api_key},
            )
            response.raise_for_status()
            data = response.json()

            return [
                {
                    "symbol": m.get("1. symbol"),
                    "name": m.get("2. name"),
                    "type": m.get("3. type"),
                    "region": m.get("4. region"),
                    "currency": m.get("8. currency"),
                }
                for m in data.get("bestMatches", [])
            ]

        except Exception as e:
            logger.error("alphavantage_search_failed", keywords=keywords, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Technical Indicators (Generic method for all indicators)
    # ─────────────────────────────────────────────────────────────────

    async def get_technical_indicator(
        self,
        symbol: str,
        indicator: str,
        interval: str = "daily",
        time_period: int = 14,
        series_type: str = "close",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Generic method for all technical indicators.

        Supports: SMA, EMA, WMA, DEMA, TEMA, TRIMA, KAMA, RSI, MACD, STOCH,
                  BBANDS, ADX, CCI, MFI, ATR, AD, OBV, and more.
        """
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        params: dict[str, Any] = {
            "function": indicator.upper(),
            "symbol": symbol.upper(),
            "interval": interval,
            "apikey": self._api_key,
        }

        # Add time_period for indicators that need it
        if indicator.upper() not in ["MACD", "STOCH", "AD", "OBV"]:
            params["time_period"] = time_period

        # Add series_type for indicators that need it
        if indicator.upper() not in ["AD", "OBV", "ATR", "STOCH", "MFI", "ADX", "CCI"]:
            params["series_type"] = series_type

        # Add any extra params (e.g., fastperiod, slowperiod for MACD)
        params.update(kwargs)

        try:
            response = await self._client.get(self._base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            # Find the technical analysis key
            ta_key = None
            for key in data.keys():
                if "Technical Analysis" in key:
                    ta_key = key
                    break

            values = data.get(ta_key, {}) if ta_key else {}

            return {
                "symbol": symbol.upper(),
                "indicator": indicator.upper(),
                "interval": interval,
                "data": [
                    {"date": date, **vals}
                    for date, vals in sorted(values.items(), reverse=True)[:100]  # Last 100 points
                ],
                "meta": data.get("Meta Data", {}),
            }

        except Exception as e:
            logger.error(
                "alphavantage_indicator_failed", symbol=symbol, indicator=indicator, error=str(e)
            )
            raise

    # Convenience methods for common indicators
    async def get_sma(
        self,
        symbol: str,
        interval: str = "daily",
        time_period: int = 20,
        series_type: str = "close",
    ) -> dict[str, Any]:
        """Simple Moving Average."""
        return await self.get_technical_indicator(symbol, "SMA", interval, time_period, series_type)

    async def get_ema(
        self,
        symbol: str,
        interval: str = "daily",
        time_period: int = 20,
        series_type: str = "close",
    ) -> dict[str, Any]:
        """Exponential Moving Average."""
        return await self.get_technical_indicator(symbol, "EMA", interval, time_period, series_type)

    async def get_rsi(
        self,
        symbol: str,
        interval: str = "daily",
        time_period: int = 14,
        series_type: str = "close",
    ) -> dict[str, Any]:
        """Relative Strength Index."""
        return await self.get_technical_indicator(symbol, "RSI", interval, time_period, series_type)

    async def get_macd(
        self, symbol: str, interval: str = "daily", series_type: str = "close"
    ) -> dict[str, Any]:
        """Moving Average Convergence Divergence."""
        return await self.get_technical_indicator(symbol, "MACD", interval, series_type=series_type)

    async def get_bbands(
        self,
        symbol: str,
        interval: str = "daily",
        time_period: int = 20,
        series_type: str = "close",
    ) -> dict[str, Any]:
        """Bollinger Bands."""
        return await self.get_technical_indicator(
            symbol, "BBANDS", interval, time_period, series_type
        )

    async def get_stoch(self, symbol: str, interval: str = "daily") -> dict[str, Any]:
        """Stochastic Oscillator."""
        return await self.get_technical_indicator(symbol, "STOCH", interval)

    async def get_adx(
        self, symbol: str, interval: str = "daily", time_period: int = 14
    ) -> dict[str, Any]:
        """Average Directional Index."""
        return await self.get_technical_indicator(symbol, "ADX", interval, time_period)

    async def get_cci(
        self, symbol: str, interval: str = "daily", time_period: int = 20
    ) -> dict[str, Any]:
        """Commodity Channel Index."""
        return await self.get_technical_indicator(symbol, "CCI", interval, time_period)

    async def get_atr(
        self, symbol: str, interval: str = "daily", time_period: int = 14
    ) -> dict[str, Any]:
        """Average True Range."""
        return await self.get_technical_indicator(symbol, "ATR", interval, time_period)

    async def get_obv(self, symbol: str, interval: str = "daily") -> dict[str, Any]:
        """On Balance Volume."""
        return await self.get_technical_indicator(symbol, "OBV", interval)

    # ─────────────────────────────────────────────────────────────────
    # Forex
    # ─────────────────────────────────────────────────────────────────

    async def get_forex_rate(self, from_currency: str, to_currency: str) -> dict[str, Any]:
        """Get real-time exchange rate."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": from_currency.upper(),
                    "to_currency": to_currency.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            rate_data = data.get("Realtime Currency Exchange Rate", {})
            return {
                "from_currency": rate_data.get("1. From_Currency Code"),
                "to_currency": rate_data.get("3. To_Currency Code"),
                "exchange_rate": rate_data.get("5. Exchange Rate"),
                "last_refreshed": rate_data.get("6. Last Refreshed"),
                "time_zone": rate_data.get("7. Time Zone"),
            }

        except Exception as e:
            logger.error("alphavantage_forex_rate_failed", error=str(e))
            raise

    async def get_forex_daily(self, from_symbol: str, to_symbol: str) -> list[dict[str, Any]]:
        """Get daily forex time series."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "FX_DAILY",
                    "from_symbol": from_symbol.upper(),
                    "to_symbol": to_symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            ts = data.get("Time Series FX (Daily)", {})
            return [
                {
                    "date": date,
                    "open": v.get("1. open"),
                    "high": v.get("2. high"),
                    "low": v.get("3. low"),
                    "close": v.get("4. close"),
                }
                for date, v in sorted(ts.items(), reverse=True)[:100]
            ]

        except Exception as e:
            logger.error("alphavantage_forex_daily_failed", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Crypto
    # ─────────────────────────────────────────────────────────────────

    async def get_crypto_rating(self, symbol: str) -> dict[str, Any]:
        """Get crypto rating/health index."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "CRYPTO_RATING",
                    "symbol": symbol.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            return data.get("Crypto Rating (FCAS)", {})

        except Exception as e:
            logger.error("alphavantage_crypto_rating_failed", symbol=symbol, error=str(e))
            raise

    async def get_crypto_daily(self, symbol: str, market: str = "USD") -> list[dict[str, Any]]:
        """Get daily crypto time series."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={
                    "function": "DIGITAL_CURRENCY_DAILY",
                    "symbol": symbol.upper(),
                    "market": market.upper(),
                    "apikey": self._api_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            ts = data.get("Time Series (Digital Currency Daily)", {})
            return [
                {
                    "date": date,
                    "open": v.get(f"1a. open ({market.upper()})"),
                    "high": v.get(f"2a. high ({market.upper()})"),
                    "low": v.get(f"3a. low ({market.upper()})"),
                    "close": v.get(f"4a. close ({market.upper()})"),
                    "volume": v.get("5. volume"),
                }
                for date, v in sorted(ts.items(), reverse=True)[:100]
            ]

        except Exception as e:
            logger.error("alphavantage_crypto_daily_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Economic Indicators
    # ─────────────────────────────────────────────────────────────────

    async def get_economic_indicator(
        self, indicator: str, interval: str = "annual"
    ) -> dict[str, Any]:
        """Get economic indicator data.

        Supports: REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD, FEDERAL_FUNDS_RATE,
                  CPI, INFLATION, RETAIL_SALES, DURABLES, UNEMPLOYMENT, NONFARM_PAYROLL
        """
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        params: dict[str, str] = {"function": indicator.upper(), "apikey": self._api_key}
        if indicator.upper() in ["REAL_GDP", "TREASURY_YIELD", "FEDERAL_FUNDS_RATE", "CPI"]:
            params["interval"] = interval

        try:
            response = await self._client.get(self._base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if "Note" in data:
                raise RuntimeError("Rate limit exceeded")

            return {
                "name": data.get("name"),
                "interval": data.get("interval"),
                "unit": data.get("unit"),
                "data": data.get("data", [])[:50],  # Last 50 data points
            }

        except Exception as e:
            logger.error("alphavantage_economic_failed", indicator=indicator, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Calendars & Listings
    # ─────────────────────────────────────────────────────────────────

    def _parse_csv_response(self, payload: str) -> list[dict[str, Any]]:
        """Parse Alpha Vantage CSV payload to list-of-dicts."""
        text = payload.strip()
        if not text:
            return []

        rows = list(csv.DictReader(io.StringIO(text)))
        if not rows:
            return []
        return [dict(row) for row in rows]

    async def get_earnings_calendar(
        self, symbol: str | None = None, horizon: str = "3month"
    ) -> list[dict[str, Any]]:
        """Get earnings calendar.

        Args:
            symbol: Optional symbol to filter
            horizon: 3month, 6month, or 12month
        """
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        params: dict[str, str] = {
            "function": "EARNINGS_CALENDAR",
            "horizon": horizon,
            "apikey": self._api_key,
        }
        if symbol:
            params["symbol"] = symbol.upper()

        try:
            response = await self._client.get(self._base_url, params=params)
            response.raise_for_status()
            return self._parse_csv_response(response.text)

        except Exception as e:
            logger.error("alphavantage_earnings_calendar_failed", error=str(e))
            raise

    async def get_ipo_calendar(self) -> list[dict[str, Any]]:
        """Get upcoming IPO calendar."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        try:
            response = await self._client.get(
                self._base_url,
                params={"function": "IPO_CALENDAR", "apikey": self._api_key},
            )
            response.raise_for_status()
            return self._parse_csv_response(response.text)

        except Exception as e:
            logger.error("alphavantage_ipo_calendar_failed", error=str(e))
            raise

    async def get_listing_status(
        self, state: str = "active", date: str | None = None
    ) -> list[dict[str, Any]]:
        """Get listing status (active or delisted).

        Args:
            state: 'active' or 'delisted'
            date: Optional date for historical delisted lookup (YYYY-MM-DD)
        """
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)

        params: dict[str, str] = {
            "function": "LISTING_STATUS",
            "state": state,
            "apikey": self._api_key,
        }
        if date:
            params["date"] = date

        try:
            response = await self._client.get(self._base_url, params=params)
            response.raise_for_status()
            return self._parse_csv_response(response.text)

        except Exception as e:
            logger.error("alphavantage_listing_status_failed", state=state, error=str(e))
            raise
