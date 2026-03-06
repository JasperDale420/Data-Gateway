"""Data provider base class and capabilities."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade


@dataclass
class ProviderCapabilities:
    """Declares what the provider can do."""

    # Data types
    supports_bars: bool = False
    supports_quotes: bool = False
    supports_trades: bool = False
    supports_options: bool = False
    supports_news: bool = False
    supports_flow: bool = False
    supports_darkpool: bool = False

    # Modes
    supports_streaming: bool = False
    supports_historical: bool = False

    # Limits
    max_symbols_per_request: int = 100
    max_historical_range_days: int = 365
    rate_limit_requests_per_minute: int = 600

    # Features
    supports_adjusted_prices: bool = False
    supports_extended_hours: bool = False

    # Timeframes (for bars)
    supported_timeframes: list[str] = field(default_factory=lambda: ["1Min", "5Min", "15Min", "1Hour", "1Day"])


@dataclass
class HealthStatus:
    """Provider health status."""

    healthy: bool
    error: str | None = None
    latency_ms: float | None = None
    last_check: datetime | None = None


class DataProvider(ABC):
    """Base interface for all data providers."""

    # ─────────────────────────────────────────────────────────────────
    # Required: Provider Identity
    # ─────────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier (e.g., 'alpaca', 'polygon')."""
        pass

    @property
    @abstractmethod
    def supported_feeds(self) -> list[str]:
        """List of supported feed types: ['bars', 'quotes', 'trades', 'options']."""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """Provider capabilities and limitations."""
        pass

    # ─────────────────────────────────────────────────────────────────
    # Required: Lifecycle
    # ─────────────────────────────────────────────────────────────────

    @abstractmethod
    async def initialize(self, config: dict[str, Any]) -> None:
        """Called once at gateway startup. Load credentials, warm up connections."""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Clean shutdown. Close connections, flush buffers."""
        pass

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """Return current health status for monitoring."""
        pass

    # ─────────────────────────────────────────────────────────────────
    # Optional: WebSocket Streaming (implement if supports real-time)
    # ─────────────────────────────────────────────────────────────────

    async def subscribe(self, symbols: list[str], feeds: list[str]) -> None:
        """Subscribe to real-time data. Optional for REST-only providers."""
        raise NotImplementedError("Provider does not support streaming")

    async def unsubscribe(self, symbols: list[str], feeds: list[str]) -> None:
        """Unsubscribe from real-time data."""
        raise NotImplementedError("Provider does not support streaming")

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield normalized messages from upstream. Must be async generator."""
        raise NotImplementedError("Provider does not support streaming")
        yield  # Make it a generator

    # ─────────────────────────────────────────────────────────────────
    # Optional: REST API (implement if supports historical data)
    # ─────────────────────────────────────────────────────────────────

    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> list[NormalizedBar]:
        """Fetch historical bars. Return normalized data."""
        raise NotImplementedError("Provider does not support historical bars")

    async def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Fetch current quotes."""
        raise NotImplementedError("Provider does not support quotes")

    async def get_trades(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> list[NormalizedTrade]:
        """Fetch historical trades."""
        raise NotImplementedError("Provider does not support trades")

    # ─────────────────────────────────────────────────────────────────
    # Optional Lifecycle Hooks
    # ─────────────────────────────────────────────────────────────────

    async def on_client_connect(self, client_id: str) -> None:  # noqa: B027
        """Called when a new client connects. Use for per-client setup."""
        pass

    async def on_client_disconnect(self, client_id: str) -> None:  # noqa: B027
        """Called when a client disconnects. Use for cleanup."""
        pass

    async def on_subscription_change(  # noqa: B027
        self,
        added: list[str],
        removed: list[str],
    ) -> None:
        """Called when aggregate subscriptions change."""
        pass
