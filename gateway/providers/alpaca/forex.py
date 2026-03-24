"""Alpaca forex mixin — forex rates and historical bars."""

from datetime import datetime
from typing import Any

import httpx
import structlog

from gateway.core.http_client import http_retry
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED
from gateway.schemas import NormalizedBar

logger = structlog.get_logger()


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

        results: dict[str, list[NormalizedBar]] = {}

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
            while True:
                response = await self._client.get("/v1beta1/forex/bars", params=params)
                response.raise_for_status()
                data = response.json()

                for pair, bars in data.get("bars", {}).items():
                    if pair not in results:
                        results[pair] = []
                    results[pair].extend(self._normalize_bar(pair, bar, timeframe=alpaca_timeframe) for bar in bars)

                next_token = data.get("next_page_token")
                total_bars = sum(len(b) for b in results.values())
                if not next_token or total_bars >= limit:
                    break
                params["page_token"] = next_token

            logger.info(
                "alpaca_forex_historical_fetched",
                pairs=len(pairs),
                total_bars=sum(len(b) for b in results.values()),
            )

            return results

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_forex_historical_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise
