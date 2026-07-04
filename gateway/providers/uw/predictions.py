"""UW prediction-market mixin.

Raw-HTTP endpoints for UnusualWhales prediction-market data not covered by the
vendored SDK (v5.1): market insiders / whales / smart-money, per-asset market
details / liquidity / positions, unusual markets, and user lookup / search.
"""

from typing import Any
from urllib.parse import quote


class UWPredictionsMixin:
    """Mixin providing UW prediction-market REST endpoints."""

    async def get_prediction_insiders(self) -> Any:
        """Potential insider activity on prediction markets."""
        return await self._raw_get("/api/predictions/insiders", {})

    async def get_prediction_market(self, asset_id: str) -> Any:
        """Prediction market details for a given asset ID."""
        return await self._raw_get(f"/api/predictions/market/{quote(asset_id)}", {})

    async def get_prediction_market_liquidity(self, asset_id: str) -> Any:
        """Liquidity data for a given prediction market asset."""
        return await self._raw_get(f"/api/predictions/market/{quote(asset_id)}/liquidity", {})

    async def get_prediction_market_positions(self, asset_id: str) -> Any:
        """Positions for a given prediction market asset."""
        return await self._raw_get(f"/api/predictions/market/{quote(asset_id)}/positions", {})

    async def get_prediction_search_users(self, q: str) -> Any:
        """Search for prediction market users by query."""
        return await self._raw_get("/api/predictions/search-users", {"q": q})

    async def get_prediction_smart_money(
        self,
        categories: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
    ) -> Any:
        """Profitable prediction market traders."""
        return await self._raw_get(
            "/api/predictions/smart-money",
            {"categories": categories, "min_price": min_price, "max_price": max_price},
        )

    async def get_prediction_unusual_markets(
        self,
        categories: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Prediction markets with unusual activity."""
        return await self._raw_get(
            "/api/predictions/unusual",
            {"categories": categories, "limit": limit, "offset": offset},
        )

    async def get_prediction_user(self, user_id: str) -> Any:
        """Prediction market user profile by user/wallet ID."""
        return await self._raw_get(f"/api/predictions/user/{quote(user_id)}", {})

    async def get_prediction_whales(self) -> Any:
        """Large prediction market traders."""
        return await self._raw_get("/api/predictions/whales", {})
