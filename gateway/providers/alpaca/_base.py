"""Alpaca base mixin — constants, init, lifecycle, normalizers."""

import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import httpx
from alpaca.trading.client import TradingClient
from requests import Session

from gateway.core.http_client import create_async_http_client, http_retry
from gateway.core.logger import logger
from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade

# Alpaca API endpoints
DATA_BASE_URL = "https://data.alpaca.markets"
STREAM_URL = "wss://stream.data.alpaca.markets/v2"
TRADING_BASE_URL = "https://paper-api.alpaca.markets"  # Paper by default; set APCA_API_BASE_URL for live
LIVE_TRADING_BASE_URL = "https://api.alpaca.markets"  # Real capital — requires explicit opt-in

# Error message constants
ERR_PROVIDER_NOT_INITIALIZED = "Provider not initialized"
ERR_TRADING_CLIENT_NOT_INITIALIZED = "Trading client not initialized"
UTC_OFFSET = "+00:00"


def _install_session_default_timeout(session: Session, timeout_seconds: float) -> None:
    """Inject a default HTTP timeout into a requests Session.

    Why: alpaca-py creates Session() with no timeout, so asyncio's wait_for
    cannot cancel an in-flight thread. Wrapping ``session.request`` ensures
    every call without an explicit timeout still gets one, freeing the
    trading thread pool when upstream hangs.
    """
    original_request = session.request

    def request_with_default_timeout(method: str, url: str, **kwargs: Any) -> Any:
        kwargs.setdefault("timeout", timeout_seconds)
        return original_request(method, url, **kwargs)

    # Any-cast keeps both typed (types-requests in CI) and untyped (local)
    # requests stubs happy with the instance-level monkey-patch.
    cast(Any, session).request = request_with_default_timeout


class AlpacaBaseMixin(DataProvider):
    """Core Alpaca mixin: init, lifecycle, normalizers, subscribe/unsubscribe."""

    @staticmethod
    def _resolve_paper_mode(configured_url: str, *, allow_live: bool) -> bool:
        """Return True (paper) unless live is BOTH opted-in and explicitly targeted.

        Fail-safe: a missing, empty, or typo'd ``APCA_API_BASE_URL`` returns
        paper. Live requires ``allow_live`` (GATEWAY_ALLOW_LIVE_TRADING) AND the
        configured URL to exactly match the recognized live endpoint.
        """
        return not (allow_live and configured_url.strip() == LIVE_TRADING_BASE_URL)

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
        # Pool limits prevent connection storms during option capture bursts.
        # keepalive_expiry=30 bridges the 60s capture interval so connections
        # are reused instead of re-established each cycle.
        self._client = create_async_http_client(
            base_url=self._base_url,
            headers={
                "APCA-API-KEY-ID": self._api_key,
                "APCA-API-SECRET-KEY": self._secret_key,
            },
            timeout=httpx.Timeout(30.0, connect=5.0),
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
                keepalive_expiry=30.0,
            ),
            event_hooks=httpx_event_hooks("alpaca"),
        )

        # Create SDK TradingClient for Trading API
        # Detect paper trading from env
        # Paper trading is the FAIL-SAFE default. Going live requires BOTH an
        # explicit opt-in (GATEWAY_ALLOW_LIVE_TRADING=true) AND the recognized
        # live URL, so a missing/empty/typo'd APCA_API_BASE_URL can never
        # silently route the shared account to real capital (CLAUDE.md: paper
        # must be the default). The substring `"paper" in url` test it replaces
        # failed OPEN — an empty value yielded paper=False → live.
        self._paper = self._resolve_paper_mode(
            os.environ.get("APCA_API_BASE_URL", ""),
            allow_live=get_settings().allow_live_trading,
        )
        self._trading_base_url = TRADING_BASE_URL if self._paper else LIVE_TRADING_BASE_URL
        if not self._paper:
            logger.warning(
                "alpaca_live_trading_enabled",
                url=self._trading_base_url,
                msg="LIVE trading active — real capital at risk on the shared account",
            )

        if self._api_key and self._secret_key:
            self._trading_client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
            _install_session_default_timeout(
                self._trading_client._session,
                timeout_seconds=get_settings().alpaca_trading_http_timeout_seconds,
            )
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

        # alpaca-py's TradingClient holds a `requests.Session` whose
        # connection pool is not released by GC promptly. Close it explicitly
        # so pooled HTTPS sockets to the trading endpoint are reaped on shutdown.
        if self._trading_client is not None:
            session = getattr(self._trading_client, "_session", None)
            if session is not None:
                try:
                    session.close()
                except Exception as e:
                    logger.debug("alpaca_trading_session_close_failed", error=str(e))
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

    async def _paginate(
        self,
        url: str,
        params: dict[str, Any],
        data_key: str,
        limit: int | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Shared pagination loop for Alpaca REST endpoints.

        Handles the ``next_page_token`` loop common to bars, trades,
        quotes, options, news, and corporate-actions endpoints.

        Args:
            url: Alpaca API path (e.g. ``/v2/stocks/bars``).
            params: Initial query-parameter dict (mutated with ``page_token``).
            data_key: Top-level key in the response that holds the keyed data
                (e.g. ``"bars"``, ``"trades"``, ``"quotes"``).  The value is
                expected to be a ``dict[str, list]`` (symbol -> items).
            limit: Maximum total items to collect across all pages.
                ``None`` means paginate until exhausted.

        Returns:
            Merged ``dict[str, list]`` mapping symbols to item lists,
            truncated so total items <= *limit* when specified.
        """
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        collected: dict[str, list[dict[str, Any]]] = {}
        total = 0

        while True:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, items in data.get(data_key, {}).items():
                collected.setdefault(symbol, []).extend(items)
                total += len(items)

            next_token = data.get("next_page_token")
            if not next_token or (limit is not None and total >= limit):
                break
            params["page_token"] = next_token

        # Truncate to limit
        if limit is not None and total > limit:
            budget = limit
            trimmed: dict[str, list[dict[str, Any]]] = {}
            for sym, items in collected.items():
                take = min(len(items), budget)
                if take > 0:
                    trimmed[sym] = items[:take]
                    budget -= take
                if budget <= 0:
                    break
            return trimmed

        return collected

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


if TYPE_CHECKING:
    # Feature mixins inherit this alias so type checkers can see the shared
    # AlpacaBaseMixin surface (_client, _trading_client, _paginate, ...). It
    # must have no runtime footprint: AlpacaProvider lists AlpacaBaseMixin LAST
    # in its bases, so any real base with members here (including a Protocol
    # with method stubs) would sit before AlpacaBaseMixin in the MRO and shadow
    # the real implementations.
    _AlpacaMixinBase = AlpacaBaseMixin
else:
    _AlpacaMixinBase = object
