"""Alpaca Markets data provider."""

import os
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog

# Alpaca SDK imports for Trading API
from alpaca.common.enums import Sort
from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    ActivityType,
    AssetClass,
    AssetExchange,
    AssetStatus,
    OrderSide,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.requests import (
    ClosePositionRequest,
    CreateWatchlistRequest,
    GetAssetsRequest,
    GetCalendarRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLimitOrderRequest,
    StopOrderRequest,
    UpdateWatchlistRequest,
)

from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade

logger = structlog.get_logger()

# Alpaca API endpoints
DATA_BASE_URL = "https://data.alpaca.markets"
STREAM_URL = "wss://stream.data.alpaca.markets/v2"
TRADING_BASE_URL = "https://api.alpaca.markets"  # Use paper-api.alpaca.markets for paper trading

# Error message constants
ERR_PROVIDER_NOT_INITIALIZED = "Provider not initialized"
ERR_TRADING_CLIENT_NOT_INITIALIZED = "Trading client not initialized"
UTC_OFFSET = "+00:00"


class AlpacaProvider(DataProvider):
    """Alpaca Markets data provider with REST and WebSocket support."""

    def __init__(self):
        self._api_key: str = ""
        self._secret_key: str = ""
        self._base_url: str = DATA_BASE_URL
        self._feed: str = "sip"
        self._client: httpx.AsyncClient | None = None  # Market Data (HTTP)
        self._trading_client: TradingClient | None = None  # Trading API (SDK)
        self._paper: bool = False
        self._ws: Any | None = None
        self._subscriptions: set[str] = set()

    @property
    def name(self) -> str:
        return "alpaca"

    @property
    def supported_feeds(self) -> list[str]:
        return ["bars", "quotes", "trades", "options"]

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_quotes=True,
            supports_trades=True,
            supports_options=True,
            supports_streaming=True,
            supports_historical=True,
            max_symbols_per_request=1000,
            max_historical_range_days=365,
            rate_limit_requests_per_minute=200,
            supports_adjusted_prices=True,
            supports_extended_hours=True,
            supported_timeframes=["1Min", "5Min", "15Min", "30Min", "1Hour", "1Day"],
        )

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize with credentials from environment."""
        api_key_env = config.get("api_key_env", "APCA_API_KEY_ID")
        secret_key_env = config.get("secret_key_env", "APCA_API_SECRET_KEY")

        self._api_key = os.environ.get(api_key_env, "")
        self._secret_key = os.environ.get(secret_key_env, "")
        self._feed = config.get("feed", "sip")

        if not self._api_key or not self._secret_key:
            logger.warning(
                "alpaca_credentials_missing",
                api_key_env=api_key_env,
                secret_key_env=secret_key_env,
            )

        # Create HTTP client for Market Data API
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "APCA-API-KEY-ID": self._api_key,
                "APCA-API-SECRET-KEY": self._secret_key,
            },
            timeout=30.0,
            event_hooks=httpx_event_hooks("alpaca"),
        )

        # Create SDK TradingClient for Trading API
        # Detect paper trading from env
        trading_url = os.environ.get("APCA_API_BASE_URL", TRADING_BASE_URL)
        self._paper = "paper" in trading_url.lower()

        if self._api_key and self._secret_key:
            self._trading_client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
        else:
            self._trading_client = None

        logger.info("alpaca_provider_initialized", feed=self._feed, paper=self._paper)

    async def shutdown(self) -> None:
        """Close connections."""
        if self._client:
            await self._client.aclose()
            self._client = None

        # SDK TradingClient doesn't need explicit cleanup
        self._trading_client = None

        if self._ws:
            await self._ws.close()
            self._ws = None

        logger.info("alpaca_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Check API connectivity."""
        if not self._client:
            return HealthStatus(healthy=False, error="Client not initialized")

        try:
            start = datetime.now(UTC)
            response = await self._client.get("/v2/stocks/AAPL/bars/latest")
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
    # REST API
    # ─────────────────────────────────────────────────────────────────

    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> list[NormalizedBar]:
        """Fetch historical bars from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedBar] = []

        # Alpaca accepts comma-separated symbols
        symbols_param = ",".join(symbols)

        params = {
            "symbols": symbols_param,
            "timeframe": self._convert_timeframe(timeframe),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "feed": kwargs.get("feed", self._feed),
            "adjustment": kwargs.get("adjustment", "raw"),
            "limit": kwargs.get("limit", 10000),
        }

        try:
            response = await self._client.get("/v2/stocks/bars", params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, bars in data.get("bars", {}).items():
                for bar in bars:
                    results.append(self._normalize_bar(symbol, bar))

            logger.info(
                "alpaca_bars_fetched",
                symbols=len(symbols),
                bars=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_bars_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    async def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Fetch latest quotes from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

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
            logger.error(
                "alpaca_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    async def get_trades(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> list[NormalizedTrade]:
        """Fetch historical trades from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []
        symbols_param = ",".join(symbols)

        params: dict[str, str | int] = {
            "symbols": symbols_param,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "feed": self._feed,
            "limit": 10000,
        }

        try:
            response = await self._client.get("/v2/stocks/trades", params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, trades in data.get("trades", {}).items():
                for trade in trades:
                    results.append(self._normalize_trade(symbol, trade))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_trades_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

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
            logger.error("alpaca_latest_bars_error", status=e.response.status_code)
            raise

        return results

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
            logger.error("alpaca_latest_trades_error", status=e.response.status_code)
            raise

        return results

    async def get_historical_quotes(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        limit: int = 10000,
    ) -> list[NormalizedQuote]:
        """Fetch historical quotes from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedQuote] = []
        symbols_param = ",".join(symbols)

        params: dict[str, str | int] = {
            "symbols": symbols_param,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "feed": self._feed,
            "limit": limit,
        }

        try:
            response = await self._client.get("/v2/stocks/quotes", params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, quotes in data.get("quotes", {}).items():
                for quote in quotes:
                    results.append(self._normalize_quote(symbol, quote))

            logger.info("alpaca_historical_quotes_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_historical_quotes_error", status=e.response.status_code)
            raise

        return results

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
            logger.error("alpaca_snapshots_error", status=e.response.status_code)
            raise

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
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            response = await self._client.get("/v2/stocks/auctions", params=params)
            response.raise_for_status()
            data = response.json()

            logger.info("alpaca_auctions_fetched", count=len(data.get("auctions", {})))
            return data.get("auctions", {})

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_auctions_error", status=e.response.status_code)
            raise

    # ─────────────────────────────────────────────────────────────────
    # Options REST API
    # ─────────────────────────────────────────────────────────────────

    async def get_option_chain(
        self,
        underlying: str,
        expiration_date: str | None = None,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        option_type: str | None = None,
    ) -> list:
        """Get option chain with greeks for an underlying."""
        from gateway.schemas import NormalizedOptionContract

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        params: dict[str, Any] = {
            "underlying_symbols": underlying.upper(),
            "feed": "indicative",
            "limit": 1000,
        }

        if expiration_date:
            params["expiration_date"] = expiration_date
        if expiration_gte:
            params["expiration_date_gte"] = expiration_gte
        if expiration_lte:
            params["expiration_date_lte"] = expiration_lte
        if strike_gte:
            params["strike_price_gte"] = strike_gte
        if strike_lte:
            params["strike_price_lte"] = strike_lte
        if option_type:
            params["type"] = option_type

        results: list[NormalizedOptionContract] = []

        try:
            response = await self._client.get(
                "/v1beta1/options/snapshots",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            for contract_symbol, snapshot in data.get("snapshots", {}).items():
                contract = self._normalize_option_contract(contract_symbol, snapshot)
                if contract:
                    results.append(contract)

            logger.info(
                "alpaca_option_chain_fetched",
                underlying=underlying,
                contracts=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_chain_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    async def get_option_bars(
        self,
        contracts: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[NormalizedBar]:
        """Get historical bars for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedBar] = []
        symbols_param = ",".join(contracts)

        params: dict[str, str | int] = {
            "symbols": symbols_param,
            "timeframe": self._convert_timeframe(timeframe),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit,
        }

        try:
            response = await self._client.get(
                "/v1beta1/options/bars",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            for symbol, bars in data.get("bars", {}).items():
                for bar in bars:
                    results.append(self._normalize_bar(symbol, bar))

            logger.info(
                "alpaca_option_bars_fetched",
                contracts=len(contracts),
                bars=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_bars_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    async def get_option_quotes(self, contracts: list[str]) -> list[NormalizedQuote]:
        """Get latest quotes for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedQuote] = []
        symbols_param = ",".join(contracts)

        try:
            response = await self._client.get(
                "/v1beta1/options/quotes/latest",
                params={"symbols": symbols_param, "feed": "indicative"},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, quote in data.get("quotes", {}).items():
                results.append(self._normalize_quote(symbol, quote))

            logger.info(
                "alpaca_option_quotes_fetched",
                contracts=len(contracts),
                quotes=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    async def get_option_trades(
        self,
        contracts: list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[NormalizedTrade]:
        """Get historical trades for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []
        symbols_param = ",".join(contracts)
        params: dict[str, Any] = {"symbols": symbols_param, "feed": "indicative", "limit": limit}
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            response = await self._client.get("/v1beta1/options/trades", params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, trades in data.get("trades", {}).items():
                for trade in trades:
                    results.append(self._normalize_trade(symbol, trade))

            logger.info("alpaca_option_trades_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_option_trades_error", status=e.response.status_code)
            raise

        return results

    async def get_option_latest_trades(self, contracts: list[str]) -> list[NormalizedTrade]:
        """Get latest trades for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []
        symbols_param = ",".join(contracts)

        try:
            response = await self._client.get(
                "/v1beta1/options/trades/latest",
                params={"symbols": symbols_param, "feed": "indicative"},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, trade in data.get("trades", {}).items():
                results.append(self._normalize_trade(symbol, trade))

            logger.info("alpaca_option_latest_trades_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_option_latest_trades_error", status=e.response.status_code)
            raise

        return results

    async def get_option_snapshots(self, underlying: str) -> dict[str, Any]:
        """Get snapshots for all options on an underlying."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                f"/v1beta1/options/snapshots/{underlying.upper()}",
                params={"feed": "indicative"},
            )
            response.raise_for_status()
            data = response.json()

            logger.info(
                "alpaca_option_snapshots_fetched",
                underlying=underlying,
                count=len(data.get("snapshots", {})),
            )
            return data.get("snapshots", {})

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_option_snapshots_error", status=e.response.status_code)
            raise

    def _normalize_option_contract(self, contract_symbol: str, snapshot: dict[str, Any]):
        """Normalize option snapshot to NormalizedOptionContract."""
        from gateway.schemas import NormalizedOptionContract

        try:
            quote = snapshot.get("latestQuote", {})
            trade = snapshot.get("latestTrade", {})
            greeks = snapshot.get("greeks", {})

            # Parse contract symbol for underlying/expiry/strike/type
            # OCC format: AAPL250117C00200000
            underlying = contract_symbol[:4].rstrip("0123456789")
            if not underlying:
                underlying = contract_symbol[:3]

            return NormalizedOptionContract(
                contract_symbol=contract_symbol,
                underlying=underlying,
                expiration=snapshot.get("expiration_date", ""),
                strike=Decimal(str(snapshot.get("strike_price", 0))),
                option_type=snapshot.get("type", "call"),
                bid=Decimal(str(quote.get("bp", 0))),
                ask=Decimal(str(quote.get("ap", 0))),
                last=Decimal(str(trade.get("p", 0))),
                volume=int(snapshot.get("open_interest", 0)),
                open_interest=int(snapshot.get("open_interest", 0)),
                delta=Decimal(str(greeks.get("delta", 0))) if greeks.get("delta") else None,
                gamma=Decimal(str(greeks.get("gamma", 0))) if greeks.get("gamma") else None,
                theta=Decimal(str(greeks.get("theta", 0))) if greeks.get("theta") else None,
                vega=Decimal(str(greeks.get("vega", 0))) if greeks.get("vega") else None,
                iv=(
                    Decimal(str(snapshot.get("impliedVolatility", 0)))
                    if snapshot.get("impliedVolatility")
                    else None
                ),
                provider="alpaca",
                timestamp=datetime.now(UTC),
            )
        except Exception as e:
            logger.warning(
                "alpaca_option_contract_normalize_failed",
                contract=contract_symbol,
                error=str(e),
            )
            return None

    # ─────────────────────────────────────────────────────────────────
    # Crypto REST API
    # ─────────────────────────────────────────────────────────────────

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

        params: dict[str, Any] = {
            "timeframe": self._convert_timeframe(timeframe),
            "limit": limit,
        }
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            # Crypto endpoint uses v1beta3
            response = await self._client.get(
                "/v1beta3/crypto/us/bars", params={"symbols": pair, **params}
            )
            response.raise_for_status()
            data = response.json()

            for symbol, bars in data.get("bars", {}).items():
                for bar in bars:
                    results.append(self._normalize_bar(symbol, bar))

            logger.info("alpaca_crypto_bars_fetched", pair=pair, bars=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_bars_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

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

    async def get_crypto_quotes(self, pair: str) -> NormalizedQuote | None:
        """Fetch latest crypto quote from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                "/v1beta3/crypto/us/latest/quotes", params={"symbols": pair}
            )
            response.raise_for_status()
            data = response.json()

            quotes = data.get("quotes", {})
            if pair in quotes:
                return self._normalize_quote(pair, quotes[pair])

            return None

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

    async def get_crypto_snapshot(self, pair: str) -> dict[str, Any]:
        """Fetch current crypto snapshot from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                "/v1beta3/crypto/us/snapshots", params={"symbols": pair}
            )
            response.raise_for_status()
            data = response.json()

            snapshots = data.get("snapshots", {})
            if pair in snapshots:
                snap = snapshots[pair]
                return {
                    "symbol": pair,
                    "latest_bar": snap.get("dailyBar"),
                    "latest_quote": snap.get("latestQuote"),
                    "latest_trade": snap.get("latestTrade"),
                    "minute_bar": snap.get("minuteBar"),
                }

            return {"symbol": pair, "error": "No snapshot available"}

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_crypto_snapshot_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

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

    # ─────────────────────────────────────────────────────────────────
    # Forex REST API (REST only - no WebSocket per PRD)
    # ─────────────────────────────────────────────────────────────────

    async def get_forex_rates(self, pairs: list[str]) -> dict[str, Any]:
        """Fetch latest forex rates from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            pairs_param = ",".join(pairs)
            response = await self._client.get(
                "/v1beta1/forex/rates/latest", params={"currency_pairs": pairs_param}
            )
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

    async def get_forex_rates_historical(
        self,
        pairs: list[str],
        timeframe: str = "1Day",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> dict[str, list[NormalizedBar]]:
        """Fetch historical forex rates from Alpaca."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: dict[str, list[NormalizedBar]] = {}

        params: dict[str, Any] = {
            "currency_pairs": ",".join(pairs),
            "timeframe": self._convert_timeframe(timeframe),
            "limit": limit,
        }
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            response = await self._client.get("/v1beta1/forex/bars", params=params)
            response.raise_for_status()
            data = response.json()

            for pair, bars in data.get("bars", {}).items():
                results[pair] = [self._normalize_bar(pair, bar) for bar in bars]

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

    # ─────────────────────────────────────────────────────────────────
    # News API (Phase 1)
    # ─────────────────────────────────────────────────────────────────

    async def get_news(
        self,
        symbols: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10,
        include_content: bool = False,
    ) -> list:
        """Fetch news articles from Alpaca."""
        from gateway.schemas import NormalizedNewsArticle

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedNewsArticle] = []

        params: dict[str, Any] = {"limit": min(limit, 50)}  # Max 50 per Alpaca API
        if symbols:
            params["symbols"] = ",".join(symbols)
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()
        if include_content:
            params["include_content"] = "true"

        try:
            response = await self._client.get("/v1beta1/news", params=params)
            response.raise_for_status()
            data = response.json()

            for article in data.get("news", []):
                results.append(
                    NormalizedNewsArticle(
                        article_id=str(article.get("id", "")),
                        headline=article.get("headline", ""),
                        summary=article.get("summary"),
                        content=article.get("content"),
                        url=article.get("url"),
                        source=article.get("source", "unknown"),
                        author=article.get("author"),
                        published_at=datetime.fromisoformat(
                            article.get("created_at", "").replace("Z", UTC_OFFSET)
                        ),
                        symbols=article.get("symbols", []),
                        provider="alpaca",
                    )
                )

            logger.info("alpaca_news_fetched", articles=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_news_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    # ─────────────────────────────────────────────────────────────────
    # Screener APIs (Phase 1)
    # ─────────────────────────────────────────────────────────────────

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
            response = await self._client.get(
                "/v1beta1/screener/stocks/most-actives", params=params
            )
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
            response = await self._client.get(
                f"/v1beta1/screener/{market_type}/movers", params=params
            )
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
    # Phase 2: Corporate Actions
    # ─────────────────────────────────────────────────────────────────

    async def get_corporate_actions(
        self,
        symbols: list[str],
        types: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list:
        """Get corporate actions (splits, dividends, etc.) for symbols."""
        from gateway.schemas import NormalizedCorporateAction

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedCorporateAction] = []

        params: dict[str, Any] = {"symbols": ",".join(symbols)}
        if types:
            params["types"] = ",".join(types)
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()

        try:
            response = await self._client.get("/v1beta1/corporate-actions", params=params)
            response.raise_for_status()
            data = response.json()

            # Process different action types
            for action_type in ["dividends", "splits", "mergers", "spinoffs"]:
                for action in data.get(action_type, []):
                    symbol = action.get("symbol", "")
                    results.append(
                        NormalizedCorporateAction(
                            symbol=symbol,
                            action_type=action_type.rstrip("s"),  # "dividends" -> "dividend"
                            ex_date=action.get("ex_date", ""),
                            record_date=action.get("record_date"),
                            payable_date=action.get("payable_date"),
                            amount=(
                                Decimal(str(action.get("cash_amount", 0)))
                                if action.get("cash_amount")
                                else None
                            ),
                            ratio=action.get("new_rate") or action.get("ratio"),
                            provider="alpaca",
                        )
                    )

            logger.info("alpaca_corporate_actions_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_corporate_actions_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Metadata & Orderbook
    # ─────────────────────────────────────────────────────────────────

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
            timestamp = (
                datetime.fromisoformat(str(timestamp_str).replace("Z", UTC_OFFSET))
                if timestamp_str
                else datetime.now(UTC)
            )

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

    # ─────────────────────────────────────────────────────────────────
    # Logos API
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # WebSocket Streaming
    # ─────────────────────────────────────────────────────────────────

    async def subscribe(self, symbols: list[str], feeds: list[str]) -> None:
        """Subscribe to real-time data via WebSocket."""
        # Build subscription message
        subscribe_msg: dict[str, str | list[str]] = {"action": "subscribe"}

        for feed in feeds:
            if feed == "bars":
                subscribe_msg["bars"] = symbols
            elif feed == "quotes":
                subscribe_msg["quotes"] = symbols
            elif feed == "trades":
                subscribe_msg["trades"] = symbols

        self._subscriptions.update(symbols)
        logger.info(
            "alpaca_subscribe",
            symbols=len(symbols),
            feeds=feeds,
            total_subscriptions=len(self._subscriptions),
        )

        # WebSocket connection will be handled by multiplexer

    async def unsubscribe(self, symbols: list[str], feeds: list[str]) -> None:
        """Unsubscribe from real-time data."""
        self._subscriptions -= set(symbols)
        logger.info(
            "alpaca_unsubscribe",
            symbols=len(symbols),
            remaining=len(self._subscriptions),
        )

    # ─────────────────────────────────────────────────────────────────
    # Trading API (using alpaca-py SDK)
    # ─────────────────────────────────────────────────────────────────

    def _model_to_dict(self, obj: Any) -> dict[str, Any]:
        """Convert SDK model to dict, handling nested models."""
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")
        elif hasattr(obj, "__dict__"):
            return {
                k: self._model_to_dict(v) if hasattr(v, "__dict__") else v
                for k, v in obj.__dict__.items()
                if not k.startswith("_")
            }
        return obj

    def get_account(self) -> dict[str, Any]:
        """Get account information."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            account = self._trading_client.get_account()
            data = self._model_to_dict(account)
            logger.info("alpaca_account_fetched", status=data.get("status"))
            return data
        except APIError as e:
            logger.error("alpaca_account_error", error=str(e))
            raise

    def create_order(
        self,
        symbol: str,
        qty: float | None = None,
        notional: float | None = None,
        side: str = "buy",
        order_type: str = "market",
        time_in_force: str = "day",
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
        extended_hours: bool = False,
    ) -> dict[str, Any]:
        """Create a new order using SDK."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        # Map string enums to SDK enums
        order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
        tif_map = {
            "day": TimeInForce.DAY,
            "gtc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK,
            "opg": TimeInForce.OPG,
            "cls": TimeInForce.CLS,
        }
        tif = tif_map.get(time_in_force.lower(), TimeInForce.DAY)

        try:
            request: (
                MarketOrderRequest | LimitOrderRequest | StopOrderRequest | StopLimitOrderRequest
            )
            if order_type.lower() == "market":
                request = MarketOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    notional=notional,
                    side=order_side,
                    time_in_force=tif,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                )
            elif order_type.lower() == "limit":
                request = LimitOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    notional=notional,
                    side=order_side,
                    time_in_force=tif,
                    limit_price=limit_price,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                )
            elif order_type.lower() == "stop":
                request = StopOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    notional=notional,
                    side=order_side,
                    time_in_force=tif,
                    stop_price=stop_price,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                )
            elif order_type.lower() == "stop_limit":
                request = StopLimitOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    side=order_side,
                    time_in_force=tif,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                )
            else:
                raise ValueError(f"Unsupported order type: {order_type}")

            order = self._trading_client.submit_order(request)
            data = self._model_to_dict(order)
            logger.info("alpaca_order_created", order_id=data.get("id"), symbol=symbol, side=side)
            return data

        except APIError as e:
            logger.error("alpaca_order_create_error", error=str(e))
            raise

    def get_orders(
        self,
        status: str = "open",
        limit: int = 100,
        direction: str = "desc",
        symbols: list[str] | None = None,
        nested: bool = True,
        side: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get all orders with optional filters."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = GetOrdersRequest(
                status=QueryOrderStatus(status),
                limit=min(limit, 500),
                direction=Sort(direction),
                symbols=symbols,
                nested=nested,
                side=OrderSide(side) if side else None,
            )
            orders = self._trading_client.get_orders(request)
            result = [self._model_to_dict(o) for o in orders]
            logger.info("alpaca_orders_fetched", count=len(result), status=status)
            return result

        except APIError as e:
            logger.error("alpaca_orders_error", error=str(e))
            raise

    def get_order(self, order_id: str) -> dict[str, Any]:
        """Get a specific order by ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            order = self._trading_client.get_order_by_id(order_id)
            return self._model_to_dict(order)
        except APIError as e:
            logger.error("alpaca_order_get_error", order_id=order_id, error=str(e))
            raise

    def get_order_by_client_id(self, client_order_id: str) -> dict[str, Any]:
        """Get order by client order ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            order = self._trading_client.get_order_by_client_id(client_order_id)
            return self._model_to_dict(order)
        except APIError:
            logger.error("alpaca_order_get_by_client_error", client_order_id=client_order_id)
            raise

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            self._trading_client.cancel_order_by_id(order_id)
            logger.info("alpaca_order_cancelled", order_id=order_id)
            return True
        except APIError as e:
            logger.error("alpaca_order_cancel_error", order_id=order_id, error=str(e))
            raise

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        """Cancel all open orders."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            cancelled = self._trading_client.cancel_orders()
            result = [self._model_to_dict(c) for c in cancelled]
            logger.info("alpaca_orders_cancelled_all", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_orders_cancel_all_error", error=str(e))
            raise

    def get_positions(self) -> list[dict[str, Any]]:
        """Get all open positions."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            positions = self._trading_client.get_all_positions()
            result = [self._model_to_dict(p) for p in positions]
            logger.info("alpaca_positions_fetched", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_positions_error", error=str(e))
            raise

    def get_position(self, symbol: str) -> dict[str, Any]:
        """Get position for a specific symbol."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            position = self._trading_client.get_open_position(symbol.upper())
            return self._model_to_dict(position)
        except APIError as e:
            logger.error("alpaca_position_error", symbol=symbol, error=str(e))
            raise

    def close_position(
        self,
        symbol: str,
        qty: float | None = None,
        percentage: float | None = None,
    ) -> dict[str, Any]:
        """Close a position (fully or partially)."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = ClosePositionRequest(
                qty=str(qty) if qty else None, percentage=str(percentage) if percentage else None
            )
            order = self._trading_client.close_position(symbol.upper(), close_options=request)
            data = self._model_to_dict(order)
            logger.info("alpaca_position_closed", symbol=symbol, qty=qty, percentage=percentage)
            return data
        except APIError as e:
            logger.error("alpaca_position_close_error", symbol=symbol, error=str(e))
            raise

    def close_all_positions(self, cancel_orders: bool = True) -> list[dict[str, Any]]:
        """Close all open positions."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            closed = self._trading_client.close_all_positions(cancel_orders=cancel_orders)
            result = [self._model_to_dict(c) for c in closed]
            logger.info("alpaca_positions_closed_all", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_positions_close_all_error", error=str(e))
            raise

    def exercise_option_position(self, symbol_or_contract_id: str) -> dict[str, Any]:
        """Exercise an options position.

        Args:
            symbol_or_contract_id: The OCC symbol or contract ID to exercise

        Returns:
            Response from the exercise request
        """
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            # The Alpaca SDK exercise_options_position returns None (void)
            self._trading_client.exercise_options_position(symbol_or_contract_id)
            logger.info("alpaca_option_exercised", symbol=symbol_or_contract_id)
            return {"status": "exercised", "symbol": symbol_or_contract_id}
        except APIError as e:
            logger.error(
                "alpaca_option_exercise_error",
                symbol=symbol_or_contract_id,
                error=str(e),
            )
            raise

    def do_not_exercise_option(self, symbol_or_contract_id: str) -> dict[str, Any]:
        """Mark an options position as do-not-exercise.

        Args:
            symbol_or_contract_id: The OCC symbol or contract ID to mark as DNE

        Returns:
            Response from the DNE request
        """
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            # This endpoint is not in alpaca-py SDK, use REST directly
            response = httpx.post(
                f"{self._base_url}/v2/positions/{symbol_or_contract_id}/do-not-exercise",
                headers={
                    "APCA-API-KEY-ID": self._api_key,
                    "APCA-API-SECRET-KEY": self._secret_key,
                },
            )
            response.raise_for_status()
            data = response.json() if response.content else {"status": "do_not_exercise"}
            logger.info("alpaca_option_dne", symbol=symbol_or_contract_id)
            return data
        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_dne_error",
                symbol=symbol_or_contract_id,
                status=e.response.status_code,
                error=str(e),
            )
            raise

    def get_portfolio_history(
        self,
        period: str | None = None,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        extended_hours: bool = False,
    ) -> dict[str, Any]:
        """Get account portfolio history."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            history = self._trading_client.get_portfolio_history(
                period=period,
                timeframe=timeframe,
                date_start=start,
                date_end=end,
                extended_hours=extended_hours,
            )
            data = self._model_to_dict(history)
            logger.info("alpaca_portfolio_history_fetched", period=period)
            return data
        except APIError as e:
            logger.error("alpaca_portfolio_history_error", error=str(e))
            raise

    def get_assets(
        self,
        status: str | None = None,
        asset_class: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get available assets."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = GetAssetsRequest(
                status=AssetStatus(status) if status else None,
                asset_class=AssetClass(asset_class) if asset_class else None,
                exchange=AssetExchange(exchange) if exchange else None,
            )
            assets = self._trading_client.get_all_assets(request)
            result = [self._model_to_dict(a) for a in assets]
            logger.info("alpaca_assets_fetched", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_assets_error", error=str(e))
            raise

    def get_asset(self, symbol: str) -> dict[str, Any]:
        """Get a specific asset by symbol."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            asset = self._trading_client.get_asset(symbol.upper())
            return self._model_to_dict(asset)
        except APIError as e:
            logger.error("alpaca_asset_error", symbol=symbol, error=str(e))
            raise

    def get_clock(self) -> dict[str, Any]:
        """Get market clock."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            clock = self._trading_client.get_clock()
            return self._model_to_dict(clock)
        except APIError as e:
            logger.error("alpaca_clock_error", error=str(e))
            raise

    def get_calendar(
        self,
        start: date | None = None,
        end: date | None = None,
    ) -> list[dict[str, Any]]:
        """Get trading calendar."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = GetCalendarRequest(start=start, end=end)
            calendar = self._trading_client.get_calendar(request)
            return [self._model_to_dict(c) for c in calendar]
        except APIError as e:
            logger.error("alpaca_calendar_error", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Account Configuration & Activities
    # ─────────────────────────────────────────────────────────────────

    def get_account_configurations(self) -> dict[str, Any]:
        """Get account configuration settings."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            config = self._trading_client.get_account_configurations()
            return self._model_to_dict(config)
        except APIError as e:
            logger.error("alpaca_account_config_error", error=str(e))
            raise

    def set_account_configurations(
        self,
        dtbp_check: str | None = None,
        trade_confirm_email: str | None = None,
        suspend_trade: bool | None = None,
        no_shorting: bool | None = None,
        fractional_trading: bool | None = None,
        max_margin_multiplier: str | None = None,
        pdt_check: str | None = None,
    ) -> dict[str, Any]:
        """Update account configuration settings."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            config = self._trading_client.set_account_configurations(
                dtbp_check=dtbp_check,
                trade_confirm_email=trade_confirm_email,
                suspend_trade=suspend_trade,
                no_shorting=no_shorting,
                fractional_trading=fractional_trading,
                max_margin_multiplier=max_margin_multiplier,
                pdt_check=pdt_check,
            )
            return self._model_to_dict(config)
        except APIError as e:
            logger.error("alpaca_account_config_set_error", error=str(e))
            raise

    def get_account_activities(
        self,
        activity_types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Get account activities."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            activities = self._trading_client.get_account_activities(
                activity_types=(
                    [ActivityType(t) for t in activity_types] if activity_types else None
                ),
            )
            return [self._model_to_dict(a) for a in activities]
        except APIError as e:
            logger.error("alpaca_activities_error", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Watchlists
    # ─────────────────────────────────────────────────────────────────

    def get_watchlists(self) -> list[dict[str, Any]]:
        """Get all watchlists."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlists = self._trading_client.get_watchlists()
            return [self._model_to_dict(w) for w in watchlists]
        except APIError as e:
            logger.error("alpaca_watchlists_error", error=str(e))
            raise

    def create_watchlist(self, name: str, symbols: list[str] | None = None) -> dict[str, Any]:
        """Create a new watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = CreateWatchlistRequest(name=name, symbols=symbols or [])
            watchlist = self._trading_client.create_watchlist(request)
            return self._model_to_dict(watchlist)
        except APIError as e:
            logger.error("alpaca_watchlist_create_error", error=str(e))
            raise

    def get_watchlist(self, watchlist_id: str) -> dict[str, Any]:
        """Get a specific watchlist by ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlist = self._trading_client.get_watchlist_by_id(watchlist_id)
            return self._model_to_dict(watchlist)
        except APIError as e:
            logger.error("alpaca_watchlist_error", watchlist_id=watchlist_id, error=str(e))
            raise

    def update_watchlist(
        self,
        watchlist_id: str,
        name: str | None = None,
        symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        """Update a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = UpdateWatchlistRequest(name=name, symbols=symbols)
            watchlist = self._trading_client.update_watchlist_by_id(watchlist_id, request)
            return self._model_to_dict(watchlist)
        except APIError as e:
            logger.error("alpaca_watchlist_update_error", watchlist_id=watchlist_id, error=str(e))
            raise

    def delete_watchlist(self, watchlist_id: str) -> bool:
        """Delete a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            self._trading_client.delete_watchlist_by_id(watchlist_id)
            logger.info("alpaca_watchlist_deleted", watchlist_id=watchlist_id)
            return True
        except APIError as e:
            logger.error("alpaca_watchlist_delete_error", watchlist_id=watchlist_id, error=str(e))
            raise

    def add_asset_to_watchlist(self, watchlist_id: str, symbol: str) -> dict[str, Any]:
        """Add an asset to a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlist = self._trading_client.add_asset_to_watchlist_by_id(watchlist_id, symbol)
            return self._model_to_dict(watchlist)
        except APIError:
            logger.error(
                "alpaca_watchlist_add_asset_error", watchlist_id=watchlist_id, symbol=symbol
            )
            raise

    def remove_asset_from_watchlist(self, watchlist_id: str, symbol: str) -> dict[str, Any]:
        """Remove an asset from a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlist = self._trading_client.remove_asset_from_watchlist_by_id(watchlist_id, symbol)
            return self._model_to_dict(watchlist)
        except APIError:
            logger.error(
                "alpaca_watchlist_remove_asset_error", watchlist_id=watchlist_id, symbol=symbol
            )
            raise

    # ─────────────────────────────────────────────────────────────────
    # Order Modifications
    # ─────────────────────────────────────────────────────────────────

    def replace_order(
        self,
        order_id: str,
        qty: float | None = None,
        limit_price: float | None = None,
        stop_price: float | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Replace/modify an existing order."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            tif = TimeInForce(time_in_force) if time_in_force else None
            request = ReplaceOrderRequest(
                qty=int(qty) if qty else None,
                limit_price=limit_price,
                stop_price=stop_price,
                time_in_force=tif,
                client_order_id=client_order_id,
            )
            order = self._trading_client.replace_order_by_id(order_id, request)
            return self._model_to_dict(order)
        except APIError as e:
            logger.error("alpaca_order_replace_error", order_id=order_id, error=str(e))
            raise

    # Normalization
    # ─────────────────────────────────────────────────────────────────

    def _normalize_bar(self, symbol: str, raw: dict[str, Any]) -> NormalizedBar:
        """Convert Alpaca bar to normalized format."""
        return NormalizedBar(
            symbol=symbol,
            timestamp=datetime.fromisoformat(raw["t"].replace("Z", UTC_OFFSET)),
            open=Decimal(str(raw["o"])),
            high=Decimal(str(raw["h"])),
            low=Decimal(str(raw["l"])),
            close=Decimal(str(raw["c"])),
            volume=int(raw["v"]),
            vwap=Decimal(str(raw["vw"])) if raw.get("vw") else None,
            trade_count=raw.get("n"),
            provider="alpaca",
        )

    def _normalize_quote(self, symbol: str, raw: dict[str, Any]) -> NormalizedQuote:
        """Convert Alpaca quote to normalized format."""
        return NormalizedQuote(
            symbol=symbol,
            timestamp=datetime.fromisoformat(raw["t"].replace("Z", UTC_OFFSET)),
            bid_price=Decimal(str(raw["bp"])),
            bid_size=int(raw["bs"]),
            ask_price=Decimal(str(raw["ap"])),
            ask_size=int(raw["as"]),
            bid_exchange=raw.get("bx"),
            ask_exchange=raw.get("ax"),
            conditions=raw.get("c", []),
            tape=raw.get("z"),
            provider="alpaca",
        )

    def _normalize_trade(self, symbol: str, raw: dict[str, Any]) -> NormalizedTrade:
        """Convert Alpaca trade to normalized format."""
        return NormalizedTrade(
            symbol=symbol,
            timestamp=datetime.fromisoformat(raw["t"].replace("Z", UTC_OFFSET)),
            price=Decimal(str(raw["p"])),
            size=int(raw["s"]),
            trade_id=str(raw["i"]) if raw.get("i") else None,
            exchange=raw.get("x"),  # Stocks only
            conditions=raw.get("c", []),  # Stocks only
            tape=raw.get("z"),  # Stocks only
            taker_side=raw.get("tks"),  # Crypto only (B=buy, S=sell)
            provider="alpaca",
        )

    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert gateway timeframe to Alpaca format."""
        # Gateway uses "1Min", Alpaca uses "1Min" - already compatible
        return timeframe
