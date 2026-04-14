"""Alpaca base mixin — constants, init, lifecycle, normalizers."""

import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from alpaca.trading.client import TradingClient

from gateway.core.http_client import create_async_http_client, http_retry
from gateway.core.logger import logger
from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade

# Alpaca API endpoints
DATA_BASE_URL = "https://data.alpaca.markets"
STREAM_URL = "wss://stream.data.alpaca.markets/v2"
TRADING_BASE_URL = "https://paper-api.alpaca.markets"  # Paper by default; set APCA_API_BASE_URL for live

# Error message constants
ERR_PROVIDER_NOT_INITIALIZED = "Provider not initialized"
ERR_TRADING_CLIENT_NOT_INITIALIZED = "Trading client not initialized"
UTC_OFFSET = "+00:00"


class AlpacaBaseMixin(DataProvider):
    """Core Alpaca mixin: init, lifecycle, normalizers, subscribe/unsubscribe."""

    def __init__(self):
        self._api_key: str = ""
        self._secret_key: str = ""
        self._base_url: str = DATA_BASE_URL
        self._feed: str = "sip"
        self._options_feed: str = "opra"
        self._client: httpx.AsyncClient | None = None  # Market Data (HTTP)
        self._trading_client: TradingClient | None = None  # Trading API (SDK)
        self._trading_base_url: str = TRADING_BASE_URL
        self._paper: bool = False
        self._ws: Any | None = None
        self._subscriptions: set[str] = set()

    @property
    def name(self) -> str:
        return "alpaca"

    @property
    def supported_feeds(self) -> list[str]:
        return ["bars", "quotes", "trades", "options", "option_bars", "option_quotes", "option_trades"]

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_quotes=True,
            supports_trades=True,
            supports_options=True,
            supports_news=True,
            supports_streaming=True,
            supports_historical=True,
            max_symbols_per_request=1000,
            max_historical_range_days=365,
            rate_limit_requests_per_minute=10000,
            supports_adjusted_prices=True,
            supports_extended_hours=True,
            supported_timeframes=["1Min", "5Min", "15Min", "30Min", "1Hour", "1Day"],
        )

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize with credentials from environment."""
        from gateway.config import get_settings

        api_key_env = config.get("api_key_env", "APCA_API_KEY_ID")
        secret_key_env = config.get("secret_key_env", "APCA_API_SECRET_KEY")

        self._api_key = os.environ.get(api_key_env, "")
        self._secret_key = os.environ.get(secret_key_env, "")
        self._feed = config.get("feed", "sip")
        self._options_feed = self._normalize_options_feed(
            config.get("options_feed", get_settings().stream_options_feed)
        )

        if not self._api_key or not self._secret_key:
            logger.warning(
                "alpaca_credentials_missing",
                api_key_env=api_key_env,
                secret_key_env=secret_key_env,
            )

        # Create HTTP client for Market Data API
        self._client = create_async_http_client(
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
        self._trading_base_url = trading_url
        self._paper = "paper" in trading_url.lower()

        if self._api_key and self._secret_key:
            self._trading_client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
            # The Alpaca SDK's TradingClient uses requests.Session internally,
            # which defaults to no timeout (hangs indefinitely on stale connections).
            # Set a read timeout to prevent trading calls from blocking forever.
            trading_timeout = get_settings().alpaca_trading_timeout_seconds
            self._trading_client._session.timeout = trading_timeout  # type: ignore[attr-defined]
        else:
            self._trading_client = None

        logger.info(
            "alpaca_provider_initialized",
            feed=self._feed,
            options_feed=self._options_feed,
            paper=self._paper,
        )

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

    @http_retry
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
    # Normalization helpers
    # ─────────────────────────────────────────────────────────────────

    def _parse_timestamp(self, value: str | datetime) -> datetime:
        """Parse Alpaca timestamps with fast-path support for datetime values."""
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value.replace("Z", UTC_OFFSET))

    def _normalize_bar(self, symbol: str, raw: dict[str, Any], timeframe: str = "1Min") -> NormalizedBar:
        """Convert Alpaca bar to normalized format."""
        return NormalizedBar(
            symbol=symbol,
            timestamp=self._parse_timestamp(raw["t"]),
            open=Decimal(str(raw["o"])),
            high=Decimal(str(raw["h"])),
            low=Decimal(str(raw["l"])),
            close=Decimal(str(raw["c"])),
            volume=Decimal(str(raw["v"])),
            vwap=Decimal(str(raw["vw"])) if raw.get("vw") else None,
            trade_count=raw.get("n"),
            provider="alpaca",
            timeframe=timeframe,
        )

    def _normalize_quote(self, symbol: str, raw: dict[str, Any]) -> NormalizedQuote:
        """Convert Alpaca quote to normalized format."""
        conditions = self._normalize_conditions(raw.get("c"))

        return NormalizedQuote(
            symbol=symbol,
            timestamp=self._parse_timestamp(raw["t"]),
            bid_price=Decimal(str(raw["bp"])),
            bid_size=Decimal(str(raw["bs"])),
            ask_price=Decimal(str(raw["ap"])),
            ask_size=Decimal(str(raw["as"])),
            bid_exchange=raw.get("bx", raw.get("x")),
            ask_exchange=raw.get("ax", raw.get("x")),
            conditions=conditions,
            tape=raw.get("z"),
            provider="alpaca",
        )

    def _normalize_trade(self, symbol: str, raw: dict[str, Any]) -> NormalizedTrade:
        """Convert Alpaca trade to normalized format."""
        return NormalizedTrade(
            symbol=symbol,
            timestamp=self._parse_timestamp(raw["t"]),
            price=Decimal(str(raw["p"])),
            size=Decimal(str(raw["s"])),
            trade_id=str(raw["i"]) if raw.get("i") else None,
            exchange=raw.get("x"),  # Stocks only
            conditions=self._normalize_conditions(raw.get("c")),
            tape=raw.get("z"),  # Stocks only
            taker_side=raw.get("tks"),  # Crypto only (B=buy, S=sell)
            update=raw.get("u"),  # Trade correction: canceled, incorrect, corrected
            provider="alpaca",
        )

    @staticmethod
    def _normalize_conditions(raw_conditions: Any) -> list[str]:
        if isinstance(raw_conditions, list):
            return raw_conditions
        if isinstance(raw_conditions, str):
            stripped = raw_conditions.strip()
            return [item for item in stripped.split(",") if item] if stripped else []
        if raw_conditions is None:
            return []
        return [str(raw_conditions)]

    @staticmethod
    def _normalize_options_feed(value: Any) -> str:
        normalized = str(value or "opra").strip().lower()
        if normalized not in {"opra", "indicative"}:
            raise ValueError("options_feed must be 'opra' or 'indicative'")
        return normalized

    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert gateway timeframe to Alpaca format."""
        mapping = {
            "1m": "1Min",
            "5m": "5Min",
            "15m": "15Min",
            "30m": "30Min",
            "1h": "1Hour",
            "1d": "1Day",
            "1w": "1Week",
            "1M": "1Month",
        }
        return mapping.get(timeframe, timeframe)

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
