"""Alpaca market data mixin — bars, quotes, trades, snapshots, screeners, meta, logos, fixed income."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from gateway.core.http_client import http_retry
from gateway.core.logger import logger
from gateway.core.metrics import record_provider_quote_batch_size
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED, _AlpacaMixinBase
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade


class AlpacaMarketMixin(_AlpacaMixinBase):
    """Stock/equity market data methods."""

    @http_retry
    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> list[NormalizedBar]:
        """Fetch historical bars from Alpaca with automatic pagination."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        total_limit = kwargs.get("limit", 10000)
        alpaca_timeframe = self._convert_timeframe(timeframe)
        params: dict[str, Any] = {
            "symbols": ",".join(symbols),
            "timeframe": alpaca_timeframe,
            "start": start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat(),
            "end": end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat(),
            "feed": kwargs.get("feed") or self._feed,
            "adjustment": kwargs.get("adjustment", "split"),
            "limit": max(1, min(total_limit, 10000)),
        }

        try:
            pages = await self._paginate("/v2/stocks/bars", params, "bars", limit=total_limit)
            results = [
                self._normalize_bar(symbol, bar, timeframe=alpaca_timeframe)
                for symbol, bars in pages.items()
                for bar in bars
            ]

            logger.info("alpaca_bars_fetched", symbols=len(symbols), bars=len(results))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused (e.g. requesting an index symbol like SPX
            # from /v2/stocks/bars → 400) and must not flood the ERROR log; only
            # 5xx are genuine upstream failures. Mirrors api/alpaca/common.py.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_bars_error", status=status, error=str(e))
            raise

        return results

    @http_retry
    async def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Fetch latest quotes from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        record_provider_quote_batch_size(self.name, len(symbols))
        results: list[NormalizedQuote] = []
        symbols_param = ",".join(symbols)

        try:
            response = await self._client.get(
                "/v2/stocks/quotes/latest",
                params={"symbols": symbols_param, "feed": self._feed},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, quote in data.get("quotes", {}).items():
                results.append(self._normalize_quote(symbol, quote))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log(
                "alpaca_quotes_error",
                status=status,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_trades(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        limit: int = 10000,
    ) -> list[NormalizedTrade]:
        """Fetch historical trades from Alpaca with automatic pagination."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        params: dict[str, str | int] = {
            "symbols": ",".join(symbols),
            "start": start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat(),
            "end": end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat(),
            "feed": self._feed,
            "limit": max(1, min(limit, 10000)),
        }

        try:
            pages = await self._paginate("/v2/stocks/trades", params, "trades", limit=limit)
            results = [self._normalize_trade(symbol, trade) for symbol, trades in pages.items() for trade in trades]

            logger.info("alpaca_trades_fetched", symbols=len(symbols), trades=len(results))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_trades_error", status=status, error=str(e))
            raise

        return results

    @http_retry
    async def get_latest_bars(self, symbols: list[str]) -> list[NormalizedBar]:
        """Fetch latest bars from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedBar] = []
        symbols_param = ",".join(symbols)

        try:
            response = await self._client.get(
                "/v2/stocks/bars/latest",
                params={"symbols": symbols_param, "feed": self._feed},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, bar in data.get("bars", {}).items():
                results.append(self._normalize_bar(symbol, bar))

            logger.info("alpaca_latest_bars_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_latest_bars_error", status=status, error=str(e))
            raise

        return results

    @http_retry
    async def get_latest_trades(self, symbols: list[str]) -> list[NormalizedTrade]:
        """Fetch latest trades from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []
        symbols_param = ",".join(symbols)

        try:
            response = await self._client.get(
                "/v2/stocks/trades/latest",
                params={"symbols": symbols_param, "feed": self._feed},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, trade in data.get("trades", {}).items():
                results.append(self._normalize_trade(symbol, trade))

            logger.info("alpaca_latest_trades_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_latest_trades_error", status=status, error=str(e))
            raise

        return results

    @http_retry
    async def get_historical_quotes(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        limit: int = 10000,
    ) -> list[NormalizedQuote]:
        """Fetch historical quotes from Alpaca with automatic pagination."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        params: dict[str, str | int] = {
            "symbols": ",".join(symbols),
            "start": start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat(),
            "end": end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat(),
            "feed": self._feed,
            "limit": max(1, min(limit, 10000)),
        }

        try:
            pages = await self._paginate("/v2/stocks/quotes", params, "quotes", limit=limit)
            results = [self._normalize_quote(symbol, quote) for symbol, quotes in pages.items() for quote in quotes]

            logger.info("alpaca_historical_quotes_fetched", symbols=len(symbols), quotes=len(results))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_historical_quotes_error", status=status, error=str(e))
            raise

        return results

    @http_retry
    async def get_snapshots(self, symbols: list[str]) -> dict[str, Any]:
        """Get current snapshots for symbols."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        symbols_param = ",".join(symbols)

        try:
            response = await self._client.get(
                "/v2/stocks/snapshots",
                params={"symbols": symbols_param, "feed": self._feed},
            )
            response.raise_for_status()
            data = response.json()

            # Alpaca returns snapshots at the top level keyed by symbol,
            # not under a "snapshots" sub-key
            snapshots = data.get("snapshots", data)
            # Filter out non-snapshot keys like 'next_page_token'
            snapshots = {k: v for k, v in snapshots.items() if isinstance(v, dict)}

            logger.info("alpaca_snapshots_fetched", count=len(snapshots))
            return snapshots

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_snapshots_error", status=status, error=str(e))
            raise

    @http_retry
    async def get_auctions(
        self,
        symbols: list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Get auction data for symbols."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        symbols_param = ",".join(symbols)
        params: dict[str, Any] = {"symbols": symbols_param, "feed": self._feed, "limit": limit}
        if start:
            params["start"] = start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat()
        if end:
            params["end"] = end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat()

        try:
            response = await self._client.get("/v2/stocks/auctions", params=params)
            response.raise_for_status()
            data = response.json()

            logger.info("alpaca_auctions_fetched", count=len(data.get("auctions", {})))
            return data.get("auctions", {})

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # 4xx are client-caused; only 5xx are genuine upstream failures.
            log = logger.warning if status < 500 else logger.error
            log("alpaca_auctions_error", status=status, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Screener APIs
    # ─────────────────────────────────────────────────────────────────

    @http_retry
    async def get_most_actives(
        self,
        by: str = "volume",
        top: int = 10,
    ) -> list:
        """Get most active stocks by volume or trade count."""
        from gateway.schemas import NormalizedMostActive

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedMostActive] = []

        params: dict[str, str | int] = {
            "by": by,  # "volume" or "trades"
            "top": min(top, 100),  # Max 100 per Alpaca API
        }

        try:
            response = await self._client.get("/v1beta1/screener/stocks/most-actives", params=params)
            response.raise_for_status()
            data = response.json()

            for stock in data.get("most_actives", []):
                results.append(
                    NormalizedMostActive(
                        symbol=stock.get("symbol", ""),
                        volume=int(stock.get("volume", 0)),
                        trade_count=int(stock.get("trade_count", 0)),
                        provider="alpaca",
                    )
                )

            logger.info("alpaca_most_actives_fetched", count=len(results), by=by)

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_most_actives_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_movers(
        self,
        market_type: str = "stocks",
        top: int = 10,
    ) -> dict[str, list]:
        """Get top market movers (gainers and losers)."""
        from gateway.schemas import NormalizedMover

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        params = {
            "top": min(top, 50),  # Max 50 per Alpaca API
        }

        try:
            response = await self._client.get(f"/v1beta1/screener/{market_type}/movers", params=params)
            response.raise_for_status()
            data = response.json()

            gainers: list[NormalizedMover] = []
            losers: list[NormalizedMover] = []

            for gainer in data.get("gainers", []):
                gainers.append(
                    NormalizedMover(
                        symbol=gainer.get("symbol", ""),
                        price=Decimal(str(gainer.get("price", 0))),
                        change=Decimal(str(gainer.get("change", 0))),
                        percent_change=Decimal(str(gainer.get("percent_change", 0))),
                        provider="alpaca",
                    )
                )

            for loser in data.get("losers", []):
                losers.append(
                    NormalizedMover(
                        symbol=loser.get("symbol", ""),
                        price=Decimal(str(loser.get("price", 0))),
                        change=Decimal(str(loser.get("change", 0))),
                        percent_change=Decimal(str(loser.get("percent_change", 0))),
                        provider="alpaca",
                    )
                )

            logger.info(
                "alpaca_movers_fetched",
                gainers=len(gainers),
                losers=len(losers),
                market_type=market_type,
            )

            return {"gainers": gainers, "losers": losers}

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_movers_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    # ─────────────────────────────────────────────────────────────────
    # Metadata APIs
    # ─────────────────────────────────────────────────────────────────

    @http_retry
    async def get_condition_codes(self, asset_class: str = "stocks") -> dict:
        """Get condition codes for trades/quotes."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                f"/v2/{asset_class}/meta/conditions",
                params={"tick_type": "trades"},
            )
            response.raise_for_status()
            data = response.json()

            logger.info("alpaca_condition_codes_fetched", asset_class=asset_class)
            return data

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_condition_codes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    @http_retry
    async def get_exchange_codes(self, asset_class: str = "stocks") -> dict:
        """Get exchange codes."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(f"/v2/{asset_class}/meta/exchanges")
            response.raise_for_status()
            data = response.json()

            logger.info("alpaca_exchange_codes_fetched", asset_class=asset_class)
            return data

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_exchange_codes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    # ─────────────────────────────────────────────────────────────────
    # Logos API
    # ─────────────────────────────────────────────────────────────────

    @http_retry
    async def get_logo(self, symbol: str, placeholder: bool = True) -> bytes | None:
        """Get company logo image for a symbol.

        Args:
            symbol: Stock symbol
            placeholder: Whether to return placeholder if logo not found

        Returns:
            PNG image bytes or None if not found and placeholder=False
        """
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            params: dict[str, Any] = {}
            if not placeholder:
                params["placeholder"] = "false"

            response = await self._client.get(
                f"/v1beta1/logos/{symbol.upper()}",
                params=params,
            )
            response.raise_for_status()

            logger.info("alpaca_logo_fetched", symbol=symbol)
            return response.content

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404 and not placeholder:
                return None
            logger.error(
                "alpaca_logo_error",
                symbol=symbol,
                status=e.response.status_code,
                error=str(e),
            )
            raise

    # ─────────────────────────────────────────────────────────────────
    # Fixed Income API
    # ─────────────────────────────────────────────────────────────────

    @http_retry
    async def get_fixed_income_prices(self, isins: list[str]) -> dict[str, dict]:
        """Get latest prices for fixed income securities (US Treasuries).

        Args:
            isins: List of ISIN identifiers (max 1000)

        Returns:
            Dict mapping ISIN to price data with keys: t, p, ytm, ytw
        """
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                "/v1beta1/fixed_income/latest/prices",
                params={"isins": ",".join(isins[:1000])},
            )
            response.raise_for_status()
            data = response.json()

            prices = data.get("prices", {})
            logger.info("alpaca_fixed_income_prices_fetched", count=len(prices))
            return prices

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_fixed_income_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise
