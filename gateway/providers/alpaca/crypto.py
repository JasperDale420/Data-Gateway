"""Alpaca crypto mixin — crypto bars, trades, quotes, snapshots, orderbook."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog

from gateway.core.http_client import http_retry
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade

logger = structlog.get_logger()


class AlpacaCryptoMixin:
    """Cryptocurrency data methods."""

    @http_retry
    async def get_crypto_bars(
        self,
        pair: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[NormalizedBar]:
        """Fetch historical crypto bars from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedBar] = []

        alpaca_timeframe = self._convert_timeframe(timeframe)
        params: dict[str, Any] = {
            "timeframe": alpaca_timeframe,
            "limit": limit,
        }
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            # Crypto endpoint uses v1beta3
            response = await self._client.get("/v1beta3/crypto/us/bars", params={"symbols": pair, **params})
            response.raise_for_status()
            data = response.json()

            for symbol, bars in data.get("bars", {}).items():
                for bar in bars:
                    results.append(self._normalize_bar(symbol, bar, timeframe=alpaca_timeframe))

            logger.info("alpaca_crypto_bars_fetched", pair=pair, bars=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_bars_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_crypto_trades(
        self,
        pair: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[NormalizedTrade]:
        """Fetch historical crypto trades from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []

        params: dict[str, Any] = {"symbols": pair, "limit": limit}
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            response = await self._client.get("/v1beta3/crypto/us/trades", params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, trades in data.get("trades", {}).items():
                for trade in trades:
                    results.append(self._normalize_trade(symbol, trade))

            logger.info("alpaca_crypto_trades_fetched", pair=pair, trades=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_trades_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_crypto_quotes(self, pairs: list[str]) -> dict[str, NormalizedQuote]:
        """Fetch latest crypto quotes from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            symbols = ",".join(pairs)
            response = await self._client.get("/v1beta3/crypto/us/latest/quotes", params={"symbols": symbols})
            response.raise_for_status()
            data = response.json()

            results = {}
            for pair, quote in data.get("quotes", {}).items():
                results[pair] = self._normalize_quote(pair, quote)

            return results

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    async def get_historical_crypto_quotes(
        self,
        pair: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10000,
    ) -> list[NormalizedQuote]:
        """Fetch historical crypto quotes from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedQuote] = []
        request_limit = max(1, min(limit, 10000))

        params: dict[str, Any] = {
            "symbols": pair,
            "limit": request_limit,
        }
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            while True:
                response = await self._client.get("/v1beta3/crypto/us/quotes", params=params)
                response.raise_for_status()
                data = response.json()

                for symbol, quotes in data.get("quotes", {}).items():
                    for quote in quotes:
                        results.append(self._normalize_quote(symbol, quote))

                next_token = data.get("next_page_token")
                if not next_token:
                    break
                params["page_token"] = next_token

            logger.info("alpaca_historical_crypto_quotes_fetched", pair=pair, quotes=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_historical_crypto_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_crypto_snapshots(self, pairs: list[str]) -> dict[str, Any]:
        """Fetch current crypto snapshots from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            symbols = ",".join(pairs)
            response = await self._client.get("/v1beta3/crypto/us/snapshots", params={"symbols": symbols})
            response.raise_for_status()
            data = response.json()

            results = {}
            for pair, snap in data.get("snapshots", {}).items():
                results[pair] = {
                    "symbol": pair,
                    "latest_bar": snap.get("dailyBar"),
                    "latest_quote": snap.get("latestQuote"),
                    "latest_trade": snap.get("latestTrade"),
                    "minute_bar": snap.get("minuteBar"),
                }

            return results

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_snapshots_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    @http_retry
    async def get_crypto_latest_bars(self, pairs: list[str]) -> dict[str, Any]:
        """Get latest bars for crypto pairs."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        symbols_param = ",".join(pairs)

        try:
            response = await self._client.get(
                "/v1beta3/crypto/us/latest/bars",
                params={"symbols": symbols_param},
            )
            response.raise_for_status()
            data = response.json()

            logger.info("alpaca_crypto_latest_bars_fetched", count=len(data.get("bars", {})))
            return data.get("bars", {})

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_crypto_latest_bars_error", status=e.response.status_code)
            raise

    @http_retry
    async def get_crypto_latest_trades(self, pairs: list[str]) -> dict[str, Any]:
        """Get latest trades for crypto pairs."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        symbols_param = ",".join(pairs)

        try:
            response = await self._client.get(
                "/v1beta3/crypto/us/latest/trades",
                params={"symbols": symbols_param},
            )
            response.raise_for_status()
            data = response.json()

            logger.info("alpaca_crypto_latest_trades_fetched", count=len(data.get("trades", {})))
            return data.get("trades", {})

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_crypto_latest_trades_error", status=e.response.status_code)
            raise

    @http_retry
    async def get_crypto_orderbook(self, pair: str) -> dict[str, Any]:
        """Get crypto orderbook for a trading pair."""
        from gateway.schemas import NormalizedOrderbook, NormalizedOrderbookLevel

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                "/v1beta3/crypto/us/orderbooks/latest",
                params={"symbols": pair.upper()},
            )
            response.raise_for_status()
            data = response.json()

            orderbooks = data.get("orderbooks", {})
            if pair.upper() not in orderbooks:
                return {"symbol": pair.upper(), "bids": [], "asks": []}

            ob_data = orderbooks[pair.upper()]

            bids = [
                NormalizedOrderbookLevel(
                    price=Decimal(str(b.get("p", 0))),
                    size=Decimal(str(b.get("s", 0))),
                    side="bid",
                )
                for b in ob_data.get("b", [])
            ]

            asks = [
                NormalizedOrderbookLevel(
                    price=Decimal(str(a.get("p", 0))),
                    size=Decimal(str(a.get("s", 0))),
                    side="ask",
                )
                for a in ob_data.get("a", [])
            ]

            timestamp_str = ob_data.get("t")
            timestamp = self._parse_timestamp(str(timestamp_str)) if timestamp_str else datetime.now(UTC)

            result = NormalizedOrderbook(
                symbol=pair.upper(),
                timestamp=timestamp,
                bids=bids,
                asks=asks,
                provider="alpaca",
            )

            logger.info("alpaca_crypto_orderbook_fetched", pair=pair)
            return result.model_dump()

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_orderbook_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise
