"""NewsAPI.org news data provider."""

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from gateway.core.http_client import create_async_http_client
from gateway.core.logger import logger
from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities


class NewsProvider(DataProvider):
    """NewsAPI.org news data provider for articles and sentiment."""

    def __init__(self) -> None:
        self._api_key: str = ""
        self._client: httpx.AsyncClient | None = None
        self._base_url: str = "https://newsapi.org/v2"

    @property
    def name(self) -> str:
        return "news"

    @property
    def supported_feeds(self) -> list[str]:
        return ["news"]

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_news=True,
            supports_bars=False,
            supports_quotes=False,
            supports_trades=False,
            supports_streaming=False,
            supports_historical=True,
            rate_limit_requests_per_minute=100,
        )

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize with credentials from environment."""
        api_key_env = config.get("api_key_env", "NEWS_API_KEY")
        self._api_key = os.environ.get(api_key_env, "")

        if not self._api_key:
            logger.warning(
                "news_credentials_missing",
                api_key_env=api_key_env,
            )

        self._client = create_async_http_client(
            base_url=self._base_url,
            timeout=30.0,
            headers={"X-Api-Key": self._api_key},
            event_hooks=httpx_event_hooks("news"),
        )

        logger.info("news_provider_initialized", api="newsapi.org")

    async def shutdown(self) -> None:
        """Close connections."""
        if self._client:
            await self._client.aclose()
            self._client = None

        logger.info("news_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Check API connectivity."""
        if not self._api_key:
            return HealthStatus(
                healthy=False,
                error="News API key not configured",
            )

        if not self._client:
            return HealthStatus(healthy=False, error="Client not initialized")

        try:
            start = datetime.now(UTC)
            # Simple API test - get top headlines
            response = await self._client.get(
                "/top-headlines",
                params={"country": "us", "pageSize": 1},
            )
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
    # News API Methods
    # ─────────────────────────────────────────────────────────────────

    async def get_articles(
        self,
        symbols: list[str] | None = None,
        keywords: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 50,
        cursor: str | None = None,
        sort: str = "desc",
    ) -> dict[str, Any]:
        """Search news articles with optional filters."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not self._api_key:
            raise RuntimeError("News API key not configured")

        # Build query string from symbols and keywords
        query_parts = []
        if symbols:
            query_parts.extend(symbols)
        if keywords:
            query_parts.append(keywords)

        query = " OR ".join(query_parts) if query_parts else "stock market"

        # Default time range: last 7 days (NewsAPI free tier limit)
        if not end:
            end = datetime.now(UTC)
        if not start:
            start = end - timedelta(days=7)

        # Build params
        params: dict[str, Any] = {
            "q": query,
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "pageSize": min(limit, 100),  # NewsAPI max is 100
            "sortBy": "publishedAt" if sort == "desc" else "relevancy",
            "language": "en",
        }

        # Add pagination
        if cursor:
            params["page"] = int(cursor)

        try:
            response = await self._client.get("/everything", params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                raise RuntimeError(data.get("message", "NewsAPI error"))

            articles = []
            for article in data.get("articles", []):
                articles.append(self._normalize_article(article))

            # Build pagination info
            total_results = data.get("totalResults", 0)
            current_page = int(cursor) if cursor else 1
            has_more = len(articles) == limit and current_page * limit < total_results

            logger.info("news_articles_fetched", count=len(articles))

            return {
                "articles": articles,
                "pagination": {
                    "cursor": str(current_page + 1) if has_more else None,
                    "total_results": total_results,
                    "has_more": has_more,
                },
            }

        except httpx.HTTPStatusError as e:
            logger.error("news_articles_error", status=e.response.status_code, error=str(e))
            raise
        except Exception as e:
            logger.error("news_articles_failed", error=str(e))
            raise

    async def get_article(self, article_id: str) -> dict[str, Any]:
        """Get a specific article by ID (URL in NewsAPI)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        # NewsAPI doesn't have a get-by-ID endpoint
        raise NotImplementedError("NewsAPI does not support article lookup by ID")

    async def get_sentiment(self, symbol: str) -> dict[str, Any]:
        """Get aggregated sentiment for a symbol (basic implementation)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        if not self._api_key:
            raise RuntimeError("News API key not configured")

        # Get recent articles for the symbol
        end = datetime.now(UTC)
        start = end - timedelta(days=7)

        try:
            params: dict[str, str | int] = {
                "q": symbol,
                "from": start.strftime("%Y-%m-%d"),
                "to": end.strftime("%Y-%m-%d"),
                "pageSize": 50,
                "sortBy": "publishedAt",
                "language": "en",
            }

            response = await self._client.get("/everything", params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                raise RuntimeError(data.get("message", "NewsAPI error"))

            articles = data.get("articles", [])

            # NewsAPI doesn't provide sentiment, so we do basic keyword analysis
            positive_keywords = [
                "surge",
                "gain",
                "rise",
                "jump",
                "beat",
                "profit",
                "growth",
                "bullish",
                "upgrade",
            ]
            negative_keywords = [
                "drop",
                "fall",
                "loss",
                "decline",
                "miss",
                "crash",
                "bearish",
                "downgrade",
                "concern",
            ]

            positive_count = 0
            negative_count = 0
            neutral_count = 0

            for article in articles:
                title = (article.get("title") or "").lower()
                description = (article.get("description") or "").lower()
                text = title + " " + description

                pos_hits = sum(1 for kw in positive_keywords if kw in text)
                neg_hits = sum(1 for kw in negative_keywords if kw in text)

                if pos_hits > neg_hits:
                    positive_count += 1
                elif neg_hits > pos_hits:
                    negative_count += 1
                else:
                    neutral_count += 1

            total = positive_count + negative_count + neutral_count
            if total > 0:
                sentiment_score = (positive_count - negative_count) / total
            else:
                sentiment_score = 0.0

            # Determine label
            if sentiment_score > 0.2:
                label = "bullish"
            elif sentiment_score < -0.2:
                label = "bearish"
            else:
                label = "neutral"

            result = {
                "symbol": symbol,
                "score": round(sentiment_score, 4),
                "label": label,
                "article_count": len(articles),
                "positive_count": positive_count,
                "negative_count": negative_count,
                "neutral_count": neutral_count,
                "period_days": 7,
                "timestamp": datetime.now(UTC).isoformat(),
            }

            logger.info("news_sentiment_fetched", symbol=symbol, score=sentiment_score, label=label)
            return result

        except httpx.HTTPStatusError as e:
            logger.error("news_sentiment_error", symbol=symbol, status=e.response.status_code)
            raise
        except Exception as e:
            logger.error("news_sentiment_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Normalization Helpers
    # ─────────────────────────────────────────────────────────────────

    def _normalize_article(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Normalize NewsAPI article to canonical schema."""
        return {
            "article_id": raw.get("url", ""),  # Use URL as ID
            "headline": raw.get("title", ""),
            "summary": raw.get("description"),
            "content": raw.get("content"),
            "url": raw.get("url", ""),
            "source": raw.get("source", {}).get("name", ""),
            "author": raw.get("author"),
            "published_at": raw.get("publishedAt", ""),
            "image_url": raw.get("urlToImage"),
            "symbols": [],  # NewsAPI doesn't provide ticker tags
            "sentiment": None,  # NewsAPI doesn't provide sentiment
            "provider": "newsapi",
        }
