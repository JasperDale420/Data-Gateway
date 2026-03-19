"""Autonomous Alpaca quotes poller for continuous quote data ingestion.

The StreamMultiplexer only delivers quotes when a WebSocket client actively
subscribes.  This poller provides a REST-based fallback that runs on a
schedule during market hours, ensuring quote data flows to Heber regardless
of client subscription state.

Pattern follows the UW poller (``gateway.core.uw_poller``):
- Runs as an asyncio background task during application lifespan
- Uses ``TradingCalendar`` for market-hours gating
- Wraps each quote in an ``EventEnvelope`` via ``wrap_event``
- Publishes to the data sink for Heber integration
- Deduplicates via in-memory OrderedDict + optional Redis cache
"""

import asyncio
import contextlib
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import structlog

from gateway.config import get_settings
from gateway.core.cache import RedisCache
from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event

logger = structlog.get_logger()

# Stream name for Heber integration (same as UW poller)
HEBER_STREAM = "heber:events"

# Eastern Time
ET = ZoneInfo("America/New_York")

# Default symbols to poll when no explicit list is configured.
# Covers major indices, mega-caps, sector ETFs — same universe as ticker_universe.py.
DEFAULT_QUOTES_SYMBOLS: list[str] = [
    # Broad market ETFs
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    # Mega-cap tech
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    # Semis / high-beta
    "AMD",
    "NFLX",
    # Financials
    "JPM",
    "GS",
    "BAC",
    # SPDR sector ETFs
    "XLF",
    "XLE",
    "XLK",
    "XLV",
    "XLI",
    "XLP",
    "XLU",
    "XLB",
    "XLRE",
    "XLC",
    # Commodities / bonds / volatility
    "GLD",
    "TLT",
    "HYG",
    "VIX",
]

# Max symbols per Alpaca REST request (API supports comma-separated list)
MAX_SYMBOLS_PER_REQUEST = 100


class AlpacaQuotesPoller:
    """Background poller that fetches latest quotes via Alpaca REST API.

    Publishes ``EventEnvelope`` dicts to the data-sink for Heber consumption.
    Only active during regular market hours (9:30-16:00 ET, weekdays,
    excluding holidays).
    """

    def __init__(
        self,
        symbols: list[str] | None = None,
        poll_interval_seconds: int = 30,
    ):
        settings = get_settings()
        self._symbols = [s.upper() for s in (symbols or DEFAULT_QUOTES_SYMBOLS)]
        self._poll_interval = max(5, poll_interval_seconds)
        self._running = False
        self._task: asyncio.Task | None = None
        self._calendar = TradingCalendar()

        # Provider instance (lazy-initialised in start())
        self._provider: Any = None

        # Dedup cache — OrderedDict for O(1) FIFO eviction (mirrors UW poller pattern)
        self._seen_ids: OrderedDict[str, datetime] = OrderedDict()
        self._cache_ttl_seconds = 300  # 5 minutes (quotes change fast)

        # Cached market-hours check (30s TTL to avoid repeated calendar lookups)
        self._market_hours_cache: tuple[bool, float] = (False, 0.0)
        self._MARKET_HOURS_CACHE_TTL = 30.0

        # Optional Redis dedup layer
        self._redis_dedupe: RedisCache | None = None
        if settings.cache_redis_enabled and settings.cache_redis_url:
            self._redis_dedupe = RedisCache(
                redis_url=settings.cache_redis_url,
                default_ttl=self._cache_ttl_seconds,
            )

    # ─────────────────────────────────────────────────────────────────────
    # Market hours helpers
    # ─────────────────────────────────────────────────────────────────────

    def _is_market_hours(self) -> bool:
        """Check if market is currently open (cached for 30s)."""
        import time as _time_mod

        now = _time_mod.monotonic()
        cached_val, cached_at = self._market_hours_cache
        if now - cached_at < self._MARKET_HOURS_CACHE_TTL:
            return cached_val
        result = self._calendar.is_market_open()
        self._market_hours_cache = (result, now)
        return result

    # ─────────────────────────────────────────────────────────────────────
    # Dedup helpers
    # ─────────────────────────────────────────────────────────────────────

    def _is_duplicate(self, event_id: str) -> bool:
        return event_id in self._seen_ids

    def _mark_seen(self, event_id: str) -> None:
        self._seen_ids[event_id] = datetime.now(UTC)
        self._seen_ids.move_to_end(event_id)

    def _cleanup_cache(self) -> None:
        """Remove expired entries via O(1) FIFO pops."""
        now = datetime.now(UTC)
        cutoff = self._cache_ttl_seconds
        removed = 0
        while self._seen_ids:
            eid, ts = next(iter(self._seen_ids.items()))
            if (now - ts).total_seconds() > cutoff:
                del self._seen_ids[eid]
                removed += 1
            else:
                break
        if removed:
            logger.debug("quotes_poller_cache_cleanup", removed=removed, remaining=len(self._seen_ids))

    async def _check_redis_duplicates(self, items: list[tuple[str, str]]) -> set[str]:
        """Batch-check Redis for duplicate event IDs via MGET."""
        if self._redis_dedupe is None or not items:
            return set()
        cache_keys = [ck for _, ck in items]
        try:
            found = await self._redis_dedupe.mget(cache_keys)
        except Exception as e:
            logger.warning("quotes_poller_redis_dedupe_mget_failed", count=len(cache_keys), error=str(e))
            return set()
        return {eid for eid, ck in items if ck in found}

    # ─────────────────────────────────────────────────────────────────────
    # Publish
    # ─────────────────────────────────────────────────────────────────────

    async def _publish_envelopes(
        self,
        sink_registry: Any,
        envelopes: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Publish envelopes to data sink with deduplication.

        Returns (published_count, duplicate_count).
        """
        if not envelopes:
            return 0, 0

        duplicates = 0
        to_publish: list[tuple[dict[str, Any], str, str | None]] = []
        dedupe_items: list[tuple[str, str]] = []

        for envelope in envelopes:
            event_id = str(envelope.get("event_id", ""))
            if not event_id:
                to_publish.append((envelope, "", None))
                continue
            if self._is_duplicate(event_id):
                duplicates += 1
                continue
            cache_key = f"alpaca:quotes:{event_id}"
            dedupe_items.append((event_id, cache_key))
            to_publish.append((envelope, event_id, cache_key))

        redis_dups = await self._check_redis_duplicates(dedupe_items)
        if redis_dups:
            duplicates += len(redis_dups)
            to_publish = [item for item in to_publish if not item[1] or item[1] not in redis_dups]

        if not to_publish:
            return 0, duplicates

        messages: list[tuple[str, dict[str, Any]]] = [(HEBER_STREAM, env) for env, _, _ in to_publish]

        try:
            if type(sink_registry).__name__ == "DataSinkRegistry":
                published = await sink_registry.publish_all_batch(messages)
            else:
                for topic, env in messages:
                    await sink_registry.publish_all(topic, env)
                published = len(messages)
        except Exception as exc:
            logger.warning("quotes_poller_batch_publish_failed", count=len(messages), error=str(exc))
            published = 0

        if published > 0:
            redis_items: list[tuple[str, Any]] = []
            for _env, event_id, cache_key in to_publish[:published]:
                if event_id:
                    self._mark_seen(event_id)
                    if self._redis_dedupe is not None and cache_key:
                        redis_items.append((cache_key, True))
            if redis_items and self._redis_dedupe is not None:
                await self._redis_dedupe.set_many(redis_items, ttl=self._cache_ttl_seconds)

        return published, duplicates

    # ─────────────────────────────────────────────────────────────────────
    # Core polling
    # ─────────────────────────────────────────────────────────────────────

    async def _poll_quotes(self, sink_registry: Any) -> None:
        """Fetch latest quotes and publish to data sink."""
        if self._provider is None:
            return

        try:
            # Alpaca supports batching symbols in a single request
            all_quotes = []
            for i in range(0, len(self._symbols), MAX_SYMBOLS_PER_REQUEST):
                batch = self._symbols[i : i + MAX_SYMBOLS_PER_REQUEST]
                quotes = await self._provider.get_quotes(batch)
                all_quotes.extend(quotes)

            if not all_quotes:
                logger.debug("quotes_poller_no_quotes")
                return

            envelopes: list[dict[str, Any]] = []
            for quote in all_quotes:
                envelope = wrap_event(
                    event=quote.model_dump(mode="json"),
                    provider="alpaca",
                    feed="quotes",
                    source="rest",
                )
                envelopes.append(envelope)

            published, duplicates = await self._publish_envelopes(sink_registry, envelopes)

            logger.info(
                "quotes_poller_published",
                fetched=len(all_quotes),
                published=published,
                duplicates=duplicates,
                symbols=len(self._symbols),
            )

        except Exception as e:
            logger.error("quotes_poller_poll_error", error=str(e), exc_info=True)

    # ─────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the background polling task."""
        if self._running:
            logger.warning("quotes_poller_already_running")
            return

        # Initialize Alpaca provider
        from gateway.core.globals import get_registry

        registry = get_registry()
        self._provider = registry.get("alpaca")
        if self._provider is None:
            logger.error("quotes_poller_no_alpaca_provider")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(
            "quotes_poller_started",
            interval_seconds=self._poll_interval,
            symbols=len(self._symbols),
        )

    async def stop(self) -> None:
        """Stop the background polling task."""
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("quotes_poller_stopped", cached_ids=len(self._seen_ids))

    async def _poll_loop(self) -> None:
        """Main polling loop — runs every interval during market hours."""
        from gateway.core.globals import get_sink_registry

        while self._running:
            try:
                sink_registry = get_sink_registry()
                if not sink_registry:
                    logger.debug("quotes_poller_no_sink")
                    await asyncio.sleep(self._poll_interval)
                    continue

                if self._is_market_hours():
                    await self._poll_quotes(sink_registry)

                # Periodic cache cleanup
                self._cleanup_cache()

            except Exception as e:
                logger.error("quotes_poller_loop_error", error=str(e), exc_info=True)

            await asyncio.sleep(self._poll_interval)

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return lightweight runtime telemetry for admin surfaces."""
        return {
            "running": self._running,
            "poll_interval_seconds": self._poll_interval,
            "symbols_count": len(self._symbols),
            "symbols": self._symbols[:10],
            "dedupe_cache_entries": len(self._seen_ids),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton (mirrors UW poller pattern)
# ─────────────────────────────────────────────────────────────────────────────

_quotes_poller: AlpacaQuotesPoller | None = None


def get_quotes_poller() -> AlpacaQuotesPoller | None:
    """Get the global quotes poller instance."""
    return _quotes_poller


async def start_quotes_poller(
    symbols: list[str] | None = None,
    poll_interval_seconds: int = 30,
) -> AlpacaQuotesPoller:
    """Start the global Alpaca quotes poller."""
    global _quotes_poller

    if _quotes_poller is not None:
        return _quotes_poller

    _quotes_poller = AlpacaQuotesPoller(
        symbols=symbols,
        poll_interval_seconds=poll_interval_seconds,
    )
    await _quotes_poller.start()
    return _quotes_poller


async def stop_quotes_poller() -> None:
    """Stop the global Alpaca quotes poller."""
    global _quotes_poller

    if _quotes_poller is not None:
        await _quotes_poller.stop()
        _quotes_poller = None
