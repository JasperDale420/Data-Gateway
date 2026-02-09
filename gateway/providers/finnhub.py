"""Finnhub data provider for quotes, company profiles, and financials."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import structlog

from gateway.core.metrics import httpx_event_hooks, record_provider_quote_batch_size
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar, NormalizedQuote

logger = structlog.get_logger()


class FinnhubProvider(DataProvider):
    """Finnhub data provider for market data, company profiles, and news."""

    def __init__(self) -> None:
        self._api_key: str = ""
        self._client: httpx.AsyncClient | None = None
        self._base_url: str = "https://finnhub.io/api/v1"
        self._quotes_max_concurrency: int = 3

    @property
    def name(self) -> str:
        return "finnhub"

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_quotes=True,
            supports_trades=False,
            supports_streaming=False,
            supports_historical=True,
            supports_news=True,
            rate_limit_requests_per_minute=60,  # Free tier limit
        )

    @property
    def supported_feeds(self) -> list[str]:
        return ["quotes", "bars", "news", "fundamentals"]

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize Finnhub client."""
        api_key_env = config.get("api_key_env", "FINNHUB_API_KEY")
        self._api_key = os.environ.get(api_key_env, "")

        if not self._api_key:
            logger.warning("finnhub_api_key_not_set", env_var=api_key_env)
            return

        quotes_max_concurrency = config.get("quotes_max_concurrency")
        if quotes_max_concurrency is not None:
            try:
                self._quotes_max_concurrency = max(1, int(quotes_max_concurrency))
            except (TypeError, ValueError):
                logger.warning(
                    "finnhub_invalid_quotes_max_concurrency",
                    value=quotes_max_concurrency,
                    fallback=self._quotes_max_concurrency,
                )

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=30.0,
            params={"token": self._api_key},
            event_hooks=httpx_event_hooks("finnhub"),
        )
        logger.info("finnhub_provider_initialized")

    async def shutdown(self) -> None:
        """Shutdown provider."""
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("finnhub_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Health check - test API connectivity."""
        if not self._api_key:
            return HealthStatus(
                healthy=False,
                error="Finnhub API key not configured",
            )

        if not self._client:
            return HealthStatus(healthy=False, error="Client not initialized")

        try:
            start = datetime.now(UTC)
            response = await self._client.get("/quote", params={"symbol": "AAPL"})
            latency = (datetime.now(UTC) - start).total_seconds() * 1000

            if response.status_code == 200:
                return HealthStatus(
                    healthy=True,
                    latency_ms=latency,
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
        """Get real-time quote for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get("/quote", params={"symbol": symbol.upper()})
            response.raise_for_status()
            data = response.json()

            # Finnhub returns 0 values for invalid symbols
            if data.get("c", 0) == 0 and data.get("t", 0) == 0:
                logger.warning("finnhub_invalid_symbol", symbol=symbol)
                return None

            return NormalizedQuote(
                symbol=symbol.upper(),
                timestamp=datetime.fromtimestamp(data.get("t", 0), tz=UTC),
                bid_price=Decimal(str(data.get("c", 0))),  # Using current as approx
                ask_price=Decimal(str(data.get("c", 0))),
                bid_size=0,
                ask_size=0,
                provider="finnhub",
            )
        except Exception as e:
            logger.error("finnhub_quote_failed", symbol=symbol, error=str(e))
            raise

    async def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Get quotes for multiple symbols."""
        record_provider_quote_batch_size(self.name, len(symbols))
        if not symbols:
            return []

        sem = asyncio.Semaphore(max(1, self._quotes_max_concurrency))

        async def _fetch(symbol: str) -> NormalizedQuote | None:
            try:
                async with sem:
                    return await self.get_quote(symbol)
            except Exception as e:
                logger.warning("finnhub_quote_skipped", symbol=symbol, error=str(e))
                return None

        quotes = await asyncio.gather(*(_fetch(symbol) for symbol in symbols))
        return [quote for quote in quotes if quote is not None]

    # ─────────────────────────────────────────────────────────────────
    # Historical Bars
    # ─────────────────────────────────────────────────────────────────

    async def get_bars(
        self,
        symbol: str,
        resolution: str = "D",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[NormalizedBar]:
        """Get historical OHLCV bars.

        Resolution: 1, 5, 15, 30, 60, D, W, M
        """
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not end:
            end = datetime.now(UTC)
        if not start:
            start = end - timedelta(days=30)

        try:
            response = await self._client.get(
                "/stock/candle",
                params={
                    "symbol": symbol.upper(),
                    "resolution": resolution,
                    "from": int(start.timestamp()),
                    "to": int(end.timestamp()),
                },
            )
            response.raise_for_status()
            data = response.json()

            if data.get("s") != "ok":
                logger.warning("finnhub_no_data", symbol=symbol, status=data.get("s"))
                return []

            bars = []
            timestamps = data.get("t", [])
            opens = data.get("o", [])
            highs = data.get("h", [])
            lows = data.get("l", [])
            closes = data.get("c", [])
            volumes = data.get("v", [])

            # Map resolution to timeframe
            timeframe_map = {
                "1": "1Min",
                "5": "5Min",
                "15": "15Min",
                "30": "30Min",
                "60": "1Hour",
                "D": "1Day",
                "W": "1Week",
                "M": "1Month",
            }
            timeframe = timeframe_map.get(resolution, "1Day")

            for ts, open_px, high_px, low_px, close_px, volume in zip(
                timestamps,
                opens,
                highs,
                lows,
                closes,
                volumes,
                strict=False,
            ):
                bars.append(
                    NormalizedBar(
                        symbol=symbol.upper(),
                        timestamp=datetime.fromtimestamp(ts, tz=UTC),
                        open=Decimal(str(open_px)),
                        high=Decimal(str(high_px)),
                        low=Decimal(str(low_px)),
                        close=Decimal(str(close_px)),
                        volume=int(volume),
                        provider="finnhub",
                        timeframe=timeframe,
                    )
                )

            logger.info("finnhub_bars_fetched", symbol=symbol, count=len(bars))
            return bars

        except Exception as e:
            logger.error("finnhub_bars_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Company Profile & Fundamentals
    # ─────────────────────────────────────────────────────────────────

    async def get_company_profile(self, symbol: str) -> dict[str, Any]:
        """Get company profile information."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/profile2",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            data = response.json()

            if not data:
                return {}

            return {
                "symbol": data.get("ticker", symbol.upper()),
                "name": data.get("name"),
                "country": data.get("country"),
                "currency": data.get("currency"),
                "exchange": data.get("exchange"),
                "ipo_date": data.get("ipo"),
                "market_cap": data.get("marketCapitalization"),
                "shares_outstanding": data.get("shareOutstanding"),
                "industry": data.get("finnhubIndustry"),
                "logo": data.get("logo"),
                "website": data.get("weburl"),
                "phone": data.get("phone"),
            }
        except Exception as e:
            logger.error("finnhub_profile_failed", symbol=symbol, error=str(e))
            raise

    async def get_financials(self, symbol: str) -> dict[str, Any]:
        """Get basic financials (52-week high/low, beta, etc)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/metric",
                params={"symbol": symbol.upper(), "metric": "all"},
            )
            response.raise_for_status()
            data = response.json()

            metrics = data.get("metric", {})
            return {
                "symbol": data.get("symbol", symbol.upper()),
                "52_week_high": metrics.get("52WeekHigh"),
                "52_week_low": metrics.get("52WeekLow"),
                "beta": metrics.get("beta"),
                "pe_ratio": metrics.get("peBasicExclExtraTTM"),
                "eps": metrics.get("epsBasicExclExtraItemsTTM"),
                "dividend_yield": metrics.get("dividendYieldIndicatedAnnual"),
                "market_cap": metrics.get("marketCapitalization"),
                "revenue_per_share": metrics.get("revenuePerShareTTM"),
                "price_to_book": metrics.get("pbQuarterly"),
                "price_to_sales": metrics.get("psTTM"),
            }
        except Exception as e:
            logger.error("finnhub_financials_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # News
    # ─────────────────────────────────────────────────────────────────

    async def get_news(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get company news articles."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not end:
            end = datetime.now(UTC)
        if not start:
            start = end - timedelta(days=7)

        try:
            response = await self._client.get(
                "/company-news",
                params={
                    "symbol": symbol.upper(),
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                },
            )
            response.raise_for_status()
            data = response.json()

            articles = []
            for article in data[:50]:  # Limit to 50
                articles.append(
                    {
                        "article_id": str(article.get("id", "")),
                        "headline": article.get("headline", ""),
                        "summary": article.get("summary"),
                        "url": article.get("url", ""),
                        "source": article.get("source", ""),
                        "image_url": article.get("image"),
                        "published_at": datetime.fromtimestamp(
                            article.get("datetime", 0), tz=UTC
                        ).isoformat(),
                        "symbols": [symbol.upper()],
                        "category": article.get("category"),
                        "provider": "finnhub",
                    }
                )

            logger.info("finnhub_news_fetched", symbol=symbol, count=len(articles))
            return articles

        except Exception as e:
            logger.error("finnhub_news_failed", symbol=symbol, error=str(e))
            raise

    async def get_market_news(self, category: str = "general") -> list[dict[str, Any]]:
        """Get general market news.

        Categories: general, forex, crypto, merger
        """
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/news",
                params={"category": category},
            )
            response.raise_for_status()
            data = response.json()

            articles = []
            for article in data[:50]:
                articles.append(
                    {
                        "article_id": str(article.get("id", "")),
                        "headline": article.get("headline", ""),
                        "summary": article.get("summary"),
                        "url": article.get("url", ""),
                        "source": article.get("source", ""),
                        "image_url": article.get("image"),
                        "published_at": datetime.fromtimestamp(
                            article.get("datetime", 0), tz=UTC
                        ).isoformat(),
                        "category": article.get("category"),
                        "provider": "finnhub",
                    }
                )

            logger.info("finnhub_market_news_fetched", category=category, count=len(articles))
            return articles

        except Exception as e:
            logger.error("finnhub_market_news_failed", category=category, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Earnings & Calendar
    # ─────────────────────────────────────────────────────────────────

    async def get_earnings_calendar(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get earnings calendar."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not end:
            end = datetime.now(UTC) + timedelta(days=7)
        if not start:
            start = datetime.now(UTC) - timedelta(days=1)

        try:
            response = await self._client.get(
                "/calendar/earnings",
                params={
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                },
            )
            response.raise_for_status()
            data = response.json()

            earnings = []
            for item in data.get("earningsCalendar", []):
                earnings.append(
                    {
                        "symbol": item.get("symbol"),
                        "date": item.get("date"),
                        "hour": item.get("hour"),  # bmo, amc, dmh
                        "eps_estimate": item.get("epsEstimate"),
                        "eps_actual": item.get("epsActual"),
                        "revenue_estimate": item.get("revenueEstimate"),
                        "revenue_actual": item.get("revenueActual"),
                        "year": item.get("year"),
                        "quarter": item.get("quarter"),
                    }
                )

            logger.info("finnhub_earnings_calendar_fetched", count=len(earnings))
            return earnings

        except Exception as e:
            logger.error("finnhub_earnings_calendar_failed", error=str(e))
            raise

    async def get_recommendation_trends(self, symbol: str) -> list[dict[str, Any]]:
        """Get analyst recommendation trends."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/recommendation",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            data = response.json()

            return [
                {
                    "symbol": symbol.upper(),
                    "period": item.get("period"),
                    "strong_buy": item.get("strongBuy"),
                    "buy": item.get("buy"),
                    "hold": item.get("hold"),
                    "sell": item.get("sell"),
                    "strong_sell": item.get("strongSell"),
                }
                for item in data
            ]

        except Exception as e:
            logger.error("finnhub_recommendations_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Stock Estimates
    # ─────────────────────────────────────────────────────────────────

    async def get_eps_estimates(self, symbol: str, freq: str = "quarterly") -> dict[str, Any]:
        """Get EPS estimates for a symbol.

        Args:
            symbol: Stock symbol
            freq: quarterly or annual
        """
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/eps-estimate",
                params={"symbol": symbol.upper(), "freq": freq},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "frequency": freq,
                "estimates": data.get("data", []),
            }

        except Exception as e:
            logger.error("finnhub_eps_estimates_failed", symbol=symbol, error=str(e))
            raise

    async def get_revenue_estimates(self, symbol: str, freq: str = "quarterly") -> dict[str, Any]:
        """Get revenue estimates for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/revenue-estimate",
                params={"symbol": symbol.upper(), "freq": freq},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "frequency": freq,
                "estimates": data.get("data", []),
            }

        except Exception as e:
            logger.error("finnhub_revenue_estimates_failed", symbol=symbol, error=str(e))
            raise

    async def get_ebit_estimates(self, symbol: str, freq: str = "quarterly") -> dict[str, Any]:
        """Get EBIT estimates for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/ebit-estimate",
                params={"symbol": symbol.upper(), "freq": freq},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "frequency": freq,
                "estimates": data.get("data", []),
            }

        except Exception as e:
            logger.error("finnhub_ebit_estimates_failed", symbol=symbol, error=str(e))
            raise

    async def get_ebitda_estimates(self, symbol: str, freq: str = "quarterly") -> dict[str, Any]:
        """Get EBITDA estimates for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/ebitda-estimate",
                params={"symbol": symbol.upper(), "freq": freq},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "frequency": freq,
                "estimates": data.get("data", []),
            }

        except Exception as e:
            logger.error("finnhub_ebitda_estimates_failed", symbol=symbol, error=str(e))
            raise

    async def get_price_target(self, symbol: str) -> dict[str, Any]:
        """Get analyst price target for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/price-target",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": data.get("symbol", symbol.upper()),
                "target_high": data.get("targetHigh"),
                "target_low": data.get("targetLow"),
                "target_mean": data.get("targetMean"),
                "target_median": data.get("targetMedian"),
                "last_updated": data.get("lastUpdated"),
            }

        except Exception as e:
            logger.error("finnhub_price_target_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Stock Fundamentals Extended
    # ─────────────────────────────────────────────────────────────────

    async def get_peers(self, symbol: str) -> list[str]:
        """Get peer companies for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/peers",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()  # Returns list of peer symbols

        except Exception as e:
            logger.error("finnhub_peers_failed", symbol=symbol, error=str(e))
            raise

    async def get_metrics(self, symbol: str, metric: str = "all") -> dict[str, Any]:
        """Get key financial metrics for a symbol."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/metric",
                params={"symbol": symbol.upper(), "metric": metric},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "metric": data.get("metric", {}),
                "series": data.get("series", {}),
            }

        except Exception as e:
            logger.error("finnhub_metrics_failed", symbol=symbol, error=str(e))
            raise

    async def get_executives(self, symbol: str) -> list[dict[str, Any]]:
        """Get company executives and compensation."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/executive",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            data = response.json()

            return [
                {
                    "name": exec.get("name"),
                    "title": exec.get("position"),
                    "age": exec.get("age"),
                    "year_born": exec.get("yearBorn"),
                    "compensation": exec.get("compensation"),
                    "since": exec.get("since"),
                }
                for exec in data.get("executive", [])
            ]

        except Exception as e:
            logger.error("finnhub_executives_failed", symbol=symbol, error=str(e))
            raise

    async def get_ownership(self, symbol: str, limit: int = 20) -> dict[str, Any]:
        """Get institutional ownership data."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/ownership",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "owners": data.get("ownership", []),
            }

        except Exception as e:
            logger.error("finnhub_ownership_failed", symbol=symbol, error=str(e))
            raise

    async def get_fund_ownership(self, symbol: str, limit: int = 20) -> dict[str, Any]:
        """Get mutual fund and ETF ownership data."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/fund-ownership",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": symbol.upper(),
                "owners": data.get("ownership", []),
            }

        except Exception as e:
            logger.error("finnhub_fund_ownership_failed", symbol=symbol, error=str(e))
            raise

    async def get_insider_transactions(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        """Get insider transactions (SEC Form 4)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        params: dict[str, Any] = {"symbol": symbol.upper()}
        if start:
            params["from"] = start.strftime("%Y-%m-%d")
        if end:
            params["to"] = end.strftime("%Y-%m-%d")

        try:
            response = await self._client.get(
                "/stock/insider-transactions",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            return [
                {
                    "name": tx.get("name"),
                    "share": tx.get("share"),
                    "change": tx.get("change"),
                    "filing_date": tx.get("filingDate"),
                    "transaction_date": tx.get("transactionDate"),
                    "transaction_code": tx.get("transactionCode"),
                    "transaction_price": tx.get("transactionPrice"),
                }
                for tx in data.get("data", [])
            ]

        except Exception as e:
            logger.error("finnhub_insider_transactions_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Stock Analysis
    # ─────────────────────────────────────────────────────────────────

    async def get_insider_sentiment(self, symbol: str) -> dict[str, Any]:
        """Get aggregate insider sentiment."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/insider-sentiment",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "symbol": data.get("symbol", symbol.upper()),
                "data": data.get("data", []),
            }

        except Exception as e:
            logger.error("finnhub_insider_sentiment_failed", symbol=symbol, error=str(e))
            raise

    async def get_upgrade_downgrade(self, symbol: str) -> list[dict[str, Any]]:
        """Get analyst upgrade/downgrade history."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/upgrade-downgrade",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_upgrade_downgrade_failed", symbol=symbol, error=str(e))
            raise

    async def get_social_sentiment(self, symbol: str) -> dict[str, Any]:
        """Get social media sentiment."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/stock/social-sentiment",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_social_sentiment_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # ETFs
    # ─────────────────────────────────────────────────────────────────

    async def get_etf_profile(self, symbol: str) -> dict[str, Any]:
        """Get ETF profile."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/etf/profile",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_etf_profile_failed", symbol=symbol, error=str(e))
            raise

    async def get_etf_holdings(self, symbol: str) -> dict[str, Any]:
        """Get ETF holdings."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/etf/holdings",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_etf_holdings_failed", symbol=symbol, error=str(e))
            raise

    async def get_etf_sector_exposure(self, symbol: str) -> dict[str, Any]:
        """Get ETF sector exposure."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/etf/sector",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_etf_sector_failed", symbol=symbol, error=str(e))
            raise

    async def get_etf_country_exposure(self, symbol: str) -> dict[str, Any]:
        """Get ETF country exposure."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/etf/country",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_etf_country_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Indices
    # ─────────────────────────────────────────────────────────────────

    async def get_index_constituents(self, symbol: str) -> dict[str, Any]:
        """Get index constituents."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/index/constituents",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_index_constituents_failed", symbol=symbol, error=str(e))
            raise

    async def get_index_historical_constituents(self, symbol: str) -> dict[str, Any]:
        """Get historical index constituent changes."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/index/historical-constituents",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_index_historical_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Technical Analysis
    # ─────────────────────────────────────────────────────────────────

    async def get_support_resistance(self, symbol: str, resolution: str = "D") -> dict[str, Any]:
        """Get support/resistance levels."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/scan/support-resistance",
                params={"symbol": symbol.upper(), "resolution": resolution},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_support_resistance_failed", symbol=symbol, error=str(e))
            raise

    async def get_pattern_recognition(self, symbol: str, resolution: str = "D") -> dict[str, Any]:
        """Get recognized chart patterns."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/scan/pattern",
                params={"symbol": symbol.upper(), "resolution": resolution},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_pattern_recognition_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Forex
    # ─────────────────────────────────────────────────────────────────

    async def get_forex_rates(self, base: str = "USD") -> dict[str, Any]:
        """Get all forex rates for a base currency."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/forex/rates",
                params={"base": base.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_forex_rates_failed", base=base, error=str(e))
            raise

    async def get_forex_exchanges(self) -> list[str]:
        """List supported forex exchanges."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get("/forex/exchange")
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_forex_exchanges_failed", error=str(e))
            raise

    async def get_forex_symbols(self, exchange: str) -> list[dict[str, Any]]:
        """Get forex symbols for an exchange."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/forex/symbol",
                params={"exchange": exchange},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_forex_symbols_failed", exchange=exchange, error=str(e))
            raise

    async def get_forex_candles(
        self,
        symbol: str,
        resolution: str = "D",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """Get forex OHLC candles."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not end:
            end = datetime.now(UTC)
        if not start:
            start = end - timedelta(days=30)

        try:
            response = await self._client.get(
                "/forex/candle",
                params={
                    "symbol": symbol,
                    "resolution": resolution,
                    "from": int(start.timestamp()),
                    "to": int(end.timestamp()),
                },
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_forex_candles_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Crypto
    # ─────────────────────────────────────────────────────────────────

    async def get_crypto_exchanges(self) -> list[str]:
        """List supported crypto exchanges."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get("/crypto/exchange")
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_crypto_exchanges_failed", error=str(e))
            raise

    async def get_crypto_symbols(self, exchange: str) -> list[dict[str, Any]]:
        """Get crypto symbols for an exchange."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/crypto/symbol",
                params={"exchange": exchange},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_crypto_symbols_failed", exchange=exchange, error=str(e))
            raise

    async def get_crypto_candles(
        self,
        symbol: str,
        resolution: str = "D",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """Get crypto OHLC candles."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not end:
            end = datetime.now(UTC)
        if not start:
            start = end - timedelta(days=30)

        try:
            response = await self._client.get(
                "/crypto/candle",
                params={
                    "symbol": symbol,
                    "resolution": resolution,
                    "from": int(start.timestamp()),
                    "to": int(end.timestamp()),
                },
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_crypto_candles_failed", symbol=symbol, error=str(e))
            raise

    async def get_crypto_profile(self, symbol: str) -> dict[str, Any]:
        """Get crypto profile/metadata."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/crypto/profile",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_crypto_profile_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Mutual Funds
    # ─────────────────────────────────────────────────────────────────

    async def get_mutual_fund_profile(self, symbol: str) -> dict[str, Any]:
        """Get mutual fund profile."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/mutual-fund/profile",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_mf_profile_failed", symbol=symbol, error=str(e))
            raise

    async def get_mutual_fund_holdings(self, symbol: str) -> dict[str, Any]:
        """Get mutual fund holdings."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/mutual-fund/holdings",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_mf_holdings_failed", symbol=symbol, error=str(e))
            raise

    async def get_mutual_fund_sector(self, symbol: str) -> dict[str, Any]:
        """Get mutual fund sector exposure."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get(
                "/mutual-fund/sector",
                params={"symbol": symbol.upper()},
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_mf_sector_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Alternative Data
    # ─────────────────────────────────────────────────────────────────

    async def get_fda_calendar(self) -> list[dict[str, Any]]:
        """Get FDA drug approval calendar."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        try:
            response = await self._client.get("/fda/calendar")
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error("finnhub_fda_calendar_failed", error=str(e))
            raise

    async def get_congress_trading(
        self, symbol: str | None = None, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        """Get congressional trading data."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        if start:
            params["from"] = start.strftime("%Y-%m-%d")
        if end:
            params["to"] = end.strftime("%Y-%m-%d")

        try:
            response = await self._client.get("/stock/congressional-trading", params=params)
            response.raise_for_status()
            return response.json().get("data", [])

        except Exception as e:
            logger.error("finnhub_congress_trading_failed", symbol=symbol, error=str(e))
            raise

    async def get_lobbying(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        """Get lobbying data."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        params: dict[str, Any] = {"symbol": symbol.upper()}
        if start:
            params["from"] = start.strftime("%Y-%m-%d")
        if end:
            params["to"] = end.strftime("%Y-%m-%d")

        try:
            response = await self._client.get("/stock/lobbying", params=params)
            response.raise_for_status()
            return response.json().get("data", [])

        except Exception as e:
            logger.error("finnhub_lobbying_failed", symbol=symbol, error=str(e))
            raise

    async def get_usa_spending(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        """Get USA government spending data."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        params: dict[str, Any] = {"symbol": symbol.upper()}
        if start:
            params["from"] = start.strftime("%Y-%m-%d")
        if end:
            params["to"] = end.strftime("%Y-%m-%d")

        try:
            response = await self._client.get("/stock/usa-spending", params=params)
            response.raise_for_status()
            return response.json().get("data", [])

        except Exception as e:
            logger.error("finnhub_usa_spending_failed", symbol=symbol, error=str(e))
            raise
