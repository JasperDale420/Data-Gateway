"""Massive (formerly Polygon.io) data provider for historical aggregate bars."""

import asyncio
import os
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from gateway.core.http_client import create_async_http_client, http_retry
from gateway.core.logger import logger
from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import NormalizedBar

PROVIDER_NOT_INIT = "Provider not initialized"
API_KEY_NOT_SET = "Massive API key not configured"  # pragma: allowlist secret
DEFAULT_BASE_URL = "https://api.massive.com"
DEFAULT_BARS_MAX_CONCURRENCY = 2  # Free tier: 5 calls/min — keep concurrency low
DEFAULT_MAX_PAGES = 50  # Safety bound on next_url pagination per symbol
DEFAULT_MIN_REQUEST_INTERVAL_S = 0.0  # Set to ~12.0 for the 5 req/min free tier

# Tickers Massive serves are uppercase alphanumerics with optional '.'/'-' separators
# (e.g. AAPL, BRK.A, BRK.PRA). Reject anything else so it can never alter the URL path.
_TICKER_RE = re.compile(r"^[A-Z0-9]+(?:[.\-][A-Z0-9]+)*$")

# NormalizedBar timeframe -> Massive (multiplier, timespan)
TIMEFRAME_MAP: dict[str, tuple[int, str]] = {
    "1Min": (1, "minute"),
    "5Min": (5, "minute"),
    "15Min": (15, "minute"),
    "30Min": (30, "minute"),
    "1Hour": (1, "hour"),
    "1Day": (1, "day"),
    "1Week": (1, "week"),
    "1Month": (1, "month"),
}


class MassiveError(RuntimeError):
    """Generic Massive provider error (malformed payload, bad provider status)."""


class MassivePaginationLimitError(MassiveError):
    """Raised when next_url pagination exceeds max_pages or loops.

    This is deliberately fatal: returning a partially-paginated history would
    silently truncate backtests and rolling feature windows.
    """


class _RatePacer:
    """Enforce a minimum interval between outbound requests across all callers."""

    def __init__(self, min_interval_s: float) -> None:
        self._min = max(0.0, min_interval_s)
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        if self._min <= 0:
            return
        async with self._lock:
            loop = asyncio.get_event_loop()
            delta = loop.time() - self._last
            if delta < self._min:
                await asyncio.sleep(self._min - delta)
            self._last = asyncio.get_event_loop().time()


class MassiveProvider(DataProvider):
    """Massive (ex-Polygon.io) provider — historical aggregate (OHLCV) bars."""

    def __init__(self) -> None:
        self._api_key: str = ""
        self._client: httpx.AsyncClient | None = None
        self._base_url: str = DEFAULT_BASE_URL
        self._bars_max_concurrency: int = DEFAULT_BARS_MAX_CONCURRENCY
        self._max_pages: int = DEFAULT_MAX_PAGES
        self._pacer: _RatePacer = _RatePacer(DEFAULT_MIN_REQUEST_INTERVAL_S)

    @property
    def name(self) -> str:
        return "massive"

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_historical=True,
            supports_adjusted_prices=True,
            rate_limit_requests_per_minute=5,  # Free tier; paid tiers are unlimited
            supported_timeframes=list(TIMEFRAME_MAP.keys()),
        )

    @property
    def supported_feeds(self) -> list[str]:
        return ["bars"]

    @property
    def api_key_configured(self) -> bool:
        """Return True if the Massive API key is set."""
        return bool(self._api_key)

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize Massive client. Reads the API key from the configured env var."""
        api_key_env = config.get("api_key_env", "MASSIVE_API_KEY")
        self._api_key = os.environ.get(api_key_env, "")
        self._base_url = config.get("base_url", DEFAULT_BASE_URL).rstrip("/")

        self._bars_max_concurrency = _coerce_positive_int(
            config.get("bars_max_concurrency"), DEFAULT_BARS_MAX_CONCURRENCY, maximum=16
        )
        self._max_pages = _coerce_positive_int(config.get("max_pages"), DEFAULT_MAX_PAGES, maximum=1000)
        self._pacer = _RatePacer(
            _coerce_float(config.get("min_request_interval_seconds"), DEFAULT_MIN_REQUEST_INTERVAL_S)
        )

        if not self._api_key:
            logger.warning("massive_api_key_not_set", env_var=api_key_env)
            return

        self._client = create_async_http_client(
            base_url=self._base_url,
            timeout=30.0,
            headers={"Authorization": f"Bearer {self._api_key}"},
            event_hooks=httpx_event_hooks("massive"),
        )
        logger.info("massive_provider_initialized", base_url=self._base_url)

    async def shutdown(self) -> None:
        """Shutdown provider."""
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("massive_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Health check — fetch the previous-day bar for a liquid symbol (1 call)."""
        if not self._api_key:
            return HealthStatus(healthy=False, error=API_KEY_NOT_SET)
        if not self._client:
            return HealthStatus(healthy=False, error=PROVIDER_NOT_INIT)

        try:
            start = datetime.now(UTC)
            response = await self._client.get("/v2/aggs/ticker/AAPL/prev")
            latency = (datetime.now(UTC) - start).total_seconds() * 1000

            if response.status_code != 200:
                return HealthStatus(
                    healthy=False,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                    last_check=datetime.now(UTC),
                )
            data = response.json()
            status = data.get("status") if isinstance(data, dict) else None
            if status not in ("OK", "DELAYED"):
                return HealthStatus(
                    healthy=False,
                    error=f"provider status={status}: {str(data)[:200]}",
                    last_check=datetime.now(UTC),
                )
            return HealthStatus(healthy=True, latency_ms=latency, last_check=datetime.now(UTC))
        except Exception as e:
            return HealthStatus(healthy=False, error=str(e), last_check=datetime.now(UTC))

    def _ensure_ready(self) -> httpx.AsyncClient:
        """Validate provider readiness and return initialized HTTP client."""
        if not self._client:
            raise RuntimeError(PROVIDER_NOT_INIT)
        if not self._api_key:
            raise RuntimeError(API_KEY_NOT_SET)
        return self._client

    def _require_same_host(self, url: str) -> str:
        """Reject a next_url whose host differs from base_url (prevents Bearer-token leak)."""
        base = urlsplit(self._base_url)
        nxt = urlsplit(url)
        if (nxt.scheme, nxt.netloc) != (base.scheme, base.netloc):
            raise MassiveError(f"next_url host mismatch: {nxt.scheme}://{nxt.netloc} != {base.scheme}://{base.netloc}")
        return url

    @http_retry
    async def _fetch_page(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetch a single aggregates page. `url` may be a path or an absolute (same-host) next_url."""
        client = self._ensure_ready()
        await self._pacer.wait()
        response = await client.get(url, params=params)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                await asyncio.sleep(int(retry_after))
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise MassiveError(f"unexpected non-object response from {url}")
        status = data.get("status")
        # Require an explicit success status and reject any error-bearing 200 body,
        # so a malformed/error response can never be silently read as "no bars".
        if status not in ("OK", "DELAYED") or data.get("error"):
            raise MassiveError(
                f"Massive error from {url}: status={status!r} message={data.get('error') or data.get('message')!r}"
            )
        results = data.get("results")
        if results is not None and not isinstance(results, list):
            raise MassiveError(f"Massive returned non-list results from {url}: {type(results).__name__}")
        return data

    async def _fetch_symbol_bars(
        self,
        symbol: str,
        multiplier: int,
        timespan: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        adjusted: bool,
    ) -> list[NormalizedBar]:
        """Fetch all aggregate bars for one symbol, following same-host next_url pagination."""
        ticker = symbol.strip().upper()
        if not _TICKER_RE.match(ticker):
            raise ValueError(f"Unsupported ticker for Massive request: {symbol!r}")

        from_ms = int(start.timestamp() * 1000)
        to_ms = int(end.timestamp() * 1000)
        path = f"/v2/aggs/ticker/{quote(ticker, safe='')}/range/{multiplier}/{timespan}/{from_ms}/{to_ms}"
        params: dict[str, Any] | None = {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000}

        bars: list[NormalizedBar] = []
        next_url: str | None = path
        seen: set[str] = set()
        pages = 0
        while next_url:
            if pages >= self._max_pages:
                raise MassivePaginationLimitError(
                    f"{ticker}: exceeded max_pages={self._max_pages} (collected {len(bars)} bars)"
                )
            if next_url in seen:
                raise MassivePaginationLimitError(f"{ticker}: next_url loop detected at {next_url}")
            seen.add(next_url)

            data = await self._fetch_page(next_url, params)
            for row in data.get("results") or []:
                if not isinstance(row, dict):
                    raise MassiveError(f"{ticker}: non-dict aggregate row: {type(row).__name__}")
                bars.append(_row_to_bar(ticker, row, timeframe))

            raw_next = data.get("next_url")
            next_url = self._require_same_host(raw_next) if raw_next else None
            params = None  # next_url already carries the query string
            pages += 1

        logger.info("massive_bars_fetched", symbol=ticker, timeframe=timeframe, count=len(bars))
        return bars

    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> list[NormalizedBar]:
        """Fetch historical aggregate bars for one or more symbols.

        Pass ``adjusted=False`` to retrieve raw (unadjusted) OHLCV — useful for
        applying split/dividend factors yourself rather than trusting the
        provider's adjustment.

        Fails loud: auth, rate-limit, pagination, and malformed-payload errors
        propagate rather than being silently returned as empty bars.
        """
        self._ensure_ready()
        if timeframe not in TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported: {sorted(TIMEFRAME_MAP)}")
        for label, value in (("start", start), ("end", end)):
            if value.tzinfo is None:
                raise ValueError(f"{label} must be timezone-aware (got naive datetime)")
        multiplier, timespan = TIMEFRAME_MAP[timeframe]
        adjusted = bool(kwargs.get("adjusted", True))

        # Validate all tickers up front so a bad symbol fails before any request is scheduled.
        normalized = [s.strip().upper() for s in symbols]
        for original, sym in zip(symbols, normalized, strict=True):
            if not _TICKER_RE.match(sym):
                raise ValueError(f"Unsupported ticker for Massive request: {original!r}")

        semaphore = asyncio.Semaphore(self._bars_max_concurrency)

        async def _fetch(symbol: str) -> list[NormalizedBar]:
            async with semaphore:
                return await self._fetch_symbol_bars(symbol, multiplier, timespan, timeframe, start, end, adjusted)

        results = await asyncio.gather(*(_fetch(symbol) for symbol in normalized))
        return [bar for symbol_bars in results for bar in symbol_bars]


def _coerce_positive_int(value: Any, default: int, *, maximum: int) -> int:
    """Clamp a config value to [1, maximum], falling back to default on bad input."""
    try:
        return min(maximum, max(1, int(value)))
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float) -> float:
    """Parse a non-negative float config value, falling back to default on bad input."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _row_to_bar(ticker: str, row: dict[str, Any], timeframe: str) -> NormalizedBar:
    """Convert a Massive aggregate result row into a NormalizedBar.

    Raises MassiveError on a malformed row rather than fabricating values
    (e.g. a missing volume must not silently become zero).
    """
    try:
        timestamp = datetime.fromtimestamp(int(row["t"]) / 1000, tz=UTC)
        return NormalizedBar(
            symbol=ticker,
            timestamp=timestamp,
            open=Decimal(str(row["o"])),
            high=Decimal(str(row["h"])),
            low=Decimal(str(row["l"])),
            close=Decimal(str(row["c"])),
            volume=Decimal(str(row["v"])),
            vwap=Decimal(str(row["vw"])) if row.get("vw") is not None else None,
            trade_count=int(row["n"]) if row.get("n") is not None else None,
            provider="massive",
            timeframe=timeframe,
        )
    except (KeyError, TypeError, ValueError, InvalidOperation) as e:
        keys = sorted(row) if isinstance(row, dict) else type(row).__name__
        raise MassiveError(f"malformed aggregate row for {ticker}: {e!r} (row={keys})") from e
