"""Alpaca forex mixin — forex rates and historical bars."""

from datetime import datetime
from typing import Any

import httpx

from gateway.core.http_client import http_retry
from gateway.core.logger import logger
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED
from gateway.schemas import NormalizedBar


class AlpacaForexMixin:
    """Foreign exchange methods."""

    @http_retry
    async def get_forex_rates(self, pairs: list[str]) -> dict[str, Any]:
        """Fetch latest forex rates from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            pairs_param = ",".join(pairs)
            response = await self._client.get("/v1beta1/forex/rates/latest", params={"currency_pairs": pairs_param})
            response.raise_for_status()
            data = response.json()

            rates = {}
            for pair, rate_data in data.get("rates", {}).items():
                rates[pair] = {
                    "bid": rate_data.get("bp"),
                    "ask": rate_data.get("ap"),
                    "mid": (rate_data.get("bp", 0) + rate_data.get("ap", 0)) / 2,
                    "timestamp": rate_data.get("t"),
                }

            logger.info("alpaca_forex_rates_fetched", pairs=len(pairs), rates=len(rates))
            return {"rates": rates, "provider": "alpaca"}

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_forex_rates_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    @http_retry
    async def get_forex_rates_historical(
        self,
        pairs: list[str],
        timeframe: str = "1Day",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> dict[str, list[NormalizedBar]]:
        """Fetch historical forex rates from Alpaca with automatic pagination."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        alpaca_timeframe = self._convert_timeframe(timeframe)
        params: dict[str, Any] = {
            "currency_pairs": ",".join(pairs),
            "timeframe": alpaca_timeframe,
            "limit": max(1, min(limit, 10000)),
        }
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            pages = await self._paginate("/v1beta1/forex/bars", params, "bars", limit=limit)
            results: dict[str, list[NormalizedBar]] = {
                pair: [self._normalize_bar(pair, bar, timeframe=alpaca_timeframe) for bar in bars]
                for pair, bars in pages.items()
            }

            logger.info(
                "alpaca_forex_historical_fetched",
                pairs=len(pairs),
                total_bars=sum(len(b) for b in results.values()),
            )

            return results

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_forex_historical_error", status=e.response.status_code, error=str(e))
            raise
