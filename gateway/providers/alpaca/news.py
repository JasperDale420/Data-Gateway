"""Alpaca news mixin — news articles."""

from datetime import datetime
from typing import Any

import httpx

from gateway.core.http_client import http_retry
from gateway.core.logger import logger
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED, _AlpacaMixinBase


class AlpacaNewsMixin(_AlpacaMixinBase):
    """News methods."""

    @http_retry
    async def get_news(
        self,
        symbols: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10,
        include_content: bool = False,
    ) -> list:
        """Fetch news articles from Alpaca with automatic pagination."""
        from gateway.schemas import NormalizedNewsArticle, NormalizedNewsImage

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedNewsArticle] = []

        params: dict[str, Any] = {"limit": min(limit, 50)}  # Max 50 per page per Alpaca API
        if symbols:
            params["symbols"] = ",".join(symbols)
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()
        if include_content:
            params["include_content"] = "true"

        try:
            while True:
                response = await self._client.get("/v1beta1/news", params=params)
                response.raise_for_status()
                data = response.json()

                for article in data.get("news", []):
                    # Parse images array from Alpaca response
                    raw_images = article.get("images", [])
                    images = [
                        NormalizedNewsImage(
                            url=img.get("url", ""),
                            size=img.get("size"),
                        )
                        for img in raw_images
                        if isinstance(img, dict) and img.get("url")
                    ]

                    # Parse updated_at if present
                    updated_at = None
                    if article.get("updated_at"):
                        try:
                            updated_at = self._parse_timestamp(article["updated_at"])
                        except (ValueError, TypeError):
                            pass

                    results.append(
                        NormalizedNewsArticle(
                            article_id=str(article.get("id", "")),
                            headline=article.get("headline", ""),
                            summary=article.get("summary"),
                            content=article.get("content"),
                            url=article.get("url"),
                            source=article.get("source", "unknown"),
                            author=article.get("author"),
                            published_at=self._parse_timestamp(article.get("created_at", "")),
                            updated_at=updated_at,
                            symbols=article.get("symbols", []),
                            images=images,
                            provider="alpaca",
                        )
                    )

                next_token = data.get("next_page_token")
                if not next_token or len(results) >= limit:
                    break
                params["page_token"] = next_token

            # Trim to requested limit in case last page overshot
            results = results[:limit]

            logger.info("alpaca_news_fetched", articles=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_news_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results
