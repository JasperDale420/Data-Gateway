"""Autonomous Alpaca trades poller for continuous trade-tape ingestion.

Mirrors the gateway.core.quotes_poller pattern: the StreamMultiplexer only
delivers trades when a WebSocket client actively subscribes. This poller
provides a REST-based fallback that runs on a schedule during market hours,
ensuring trade data flows to Heber regardless of client subscription state.

Pattern follows ``gateway.core.base_poller.BasePoller``:
- Runs as an asyncio background task during application lifespan
- Uses ``TradingCalendar`` for market-hours gating
- Wraps each trade in an ``EventEnvelope`` via ``wrap_event``
- Publishes to the data sink for Heber integration
- Deduplicates via in-memory OrderedDict + optional Redis cache
"""

from typing import Any
from zoneinfo import ZoneInfo

from gateway.config import get_settings
from gateway.core.base_poller import BasePoller, DedupMixin
from gateway.core.cache import RedisCache
from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event
from gateway.core.logger import logger

# Stream name for Heber integration (same as quotes/UW pollers)
HEBER_STREAM = "heber:events"

# Eastern Time
ET = ZoneInfo("America/New_York")

# Default symbols — same universe as quotes_poller (mega-caps + sector ETFs).
DEFAULT_TRADES_SYMBOLS: list[str] = [
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

# Max symbols per Alpaca REST request
MAX_SYMBOLS_PER_REQUEST = 100


class AlpacaTradesPoller(DedupMixin, BasePoller):
    """Background poller that fetches latest trades via Alpaca REST API.

    Publishes ``EventEnvelope`` dicts to the data sink for Heber consumption.
    Only active during regular market hours (9:30-16:00 ET, weekdays,
    excluding holidays).
    """

    def __init__(
        self,
        symbols: list[str] | None = None,
        poll_interval_seconds: int = 30,
    ):
        settings = get_settings()
        self._symbols = [s.upper() for s in (symbols or DEFAULT_TRADES_SYMBOLS)]

        super().__init__(
            poll_interval_seconds=max(5, poll_interval_seconds),
            poller_name="trades_poller",
        )
        self._calendar = TradingCalendar()

        self._provider: Any = None

        # Dedup cache — trades have stable trade_id, so 5 min TTL is plenty.
        self._init_dedup(cache_ttl_seconds=300)

        self._market_hours_cache: tuple[bool, float] = (False, 0.0)
        self._MARKET_HOURS_CACHE_TTL = 30.0

        self._redis_dedupe: RedisCache | None = None
        if settings.cache_redis_enabled and settings.cache_redis_url:
            self._redis_dedupe = RedisCache(
                redis_url=settings.cache_redis_url,
                default_ttl=self._cache_ttl_seconds,
            )

    # ─────────────────────────────────────────────────────────────────────
    # BasePoller hooks
    # ─────────────────────────────────────────────────────────────────────

    async def _on_start(self) -> bool:
        from gateway.core.globals import get_registry

        registry = get_registry()
        self._provider = registry.get("alpaca")
        if self._provider is None:
            logger.error("trades_poller_no_alpaca_provider")
            return False

        logger.info(
            "trades_poller_started",
            interval_seconds=self._poll_interval,
            symbols=len(self._symbols),
        )
        return True

    async def stop(self) -> None:
        await super().stop()
        logger.info("trades_poller_stopped", cached_ids=len(self._seen_ids))

    async def _poll_once(self) -> None:
        from gateway.core.globals import get_sink_registry

        sink_registry = get_sink_registry()
        if not sink_registry:
            logger.debug("trades_poller_no_sink")
            return

        if self._is_market_hours():
            await self._poll_trades(sink_registry)

        self._cleanup_cache()

    # ─────────────────────────────────────────────────────────────────────
    # Market hours
    # ─────────────────────────────────────────────────────────────────────

    def _is_market_hours(self) -> bool:
        import time as _time_mod

        now = _time_mod.monotonic()
        cached_val, cached_at = self._market_hours_cache
        if now - cached_at < self._MARKET_HOURS_CACHE_TTL:
            return cached_val
        result = self._calendar.is_market_open()
        self._market_hours_cache = (result, now)
        return result

    # ─────────────────────────────────────────────────────────────────────
    # Redis dedup
    # ─────────────────────────────────────────────────────────────────────

    async def _check_redis_duplicates(self, items: list[tuple[str, str]]) -> set[str]:
        if self._redis_dedupe is None or not items:
            return set()
        cache_keys = [ck for _, ck in items]
        try:
            found = await self._redis_dedupe.mget(cache_keys)
        except Exception as e:
            logger.warning("trades_poller_redis_dedupe_mget_failed", count=len(cache_keys), error=str(e))
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
            cache_key = f"alpaca:trades:{event_id}"
            dedupe_items.append((event_id, cache_key))
            to_publish.append((envelope, event_id, cache_key))

        redis_dups = await self._check_redis_duplicates(dedupe_items)
        if redis_dups:
            duplicates += len(redis_dups)
            to_publish = [item for item in to_publish if not item[1] or item[1] not in redis_dups]

        if not to_publish:
            return 0, duplicates

        messages: list[tuple[str, dict[str, Any]]] = [(HEBER_STREAM, env) for env, _, _ in to_publish]

        # Use per-message results so partial-batch failures don't mis-mark
        # events. See quotes_poller for the full bug write-up.
        try:
            if hasattr(sink_registry, "publish_all_batch_results"):
                results = await sink_registry.publish_all_batch_results(messages)
            elif hasattr(sink_registry, "publish_all_batch"):
                count = await sink_registry.publish_all_batch(messages)
                results = [count == len(messages)] * len(messages)
            else:
                for topic, env in messages:
                    await sink_registry.publish_all(topic, env)
                results = [True] * len(messages)
        except Exception as exc:
            logger.warning("trades_poller_batch_publish_failed", count=len(messages), error=str(exc))
            results = [False] * len(messages)

        published = sum(1 for ok in results if ok)

        if published > 0:
            redis_items: list[tuple[str, Any]] = []
            for (_env, event_id, cache_key), ok in zip(to_publish, results, strict=True):
                if not ok:
                    continue
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

    async def _poll_trades(self, sink_registry: Any) -> None:
        """Fetch latest trades and publish to data sink."""
        if self._provider is None:
            return

        try:
            all_trades: list[Any] = []
            for i in range(0, len(self._symbols), MAX_SYMBOLS_PER_REQUEST):
                batch = self._symbols[i : i + MAX_SYMBOLS_PER_REQUEST]
                trades = await self._provider.get_latest_trades(batch)
                all_trades.extend(trades)

            if not all_trades:
                logger.debug("trades_poller_no_trades")
                return

            envelopes: list[dict[str, Any]] = []
            for trade in all_trades:
                envelope = wrap_event(
                    event=trade.model_dump(mode="json"),
                    provider="alpaca",
                    feed="trades",
                    source="rest",
                )
                envelopes.append(envelope)

            published, duplicates = await self._publish_envelopes(sink_registry, envelopes)

            logger.info(
                "trades_poller_published",
                fetched=len(all_trades),
                published=published,
                duplicates=duplicates,
                symbols=len(self._symbols),
            )

        except Exception as e:
            logger.error("trades_poller_poll_error", error=str(e), exc_info=True)

    # ─────────────────────────────────────────────────────────────────────
    # Telemetry
    # ─────────────────────────────────────────────────────────────────────

    def get_runtime_snapshot(self) -> dict[str, Any]:
        return super().get_runtime_snapshot() | {
            "symbols_count": len(self._symbols),
            "symbols": self._symbols[:10],
            "dedupe_cache_entries": len(self._seen_ids),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton (mirrors quotes_poller / uw_poller pattern)
# ─────────────────────────────────────────────────────────────────────────────

_trades_poller: AlpacaTradesPoller | None = None


def get_trades_poller() -> AlpacaTradesPoller | None:
    """Get the global trades poller instance."""
    return _trades_poller


async def start_trades_poller(
    symbols: list[str] | None = None,
    poll_interval_seconds: int = 30,
) -> AlpacaTradesPoller:
    """Start the global Alpaca trades poller."""
    global _trades_poller

    if _trades_poller is not None:
        return _trades_poller

    _trades_poller = AlpacaTradesPoller(
        symbols=symbols,
        poll_interval_seconds=poll_interval_seconds,
    )
    await _trades_poller.start()
    return _trades_poller


async def stop_trades_poller() -> None:
    """Stop the global Alpaca trades poller."""
    global _trades_poller

    if _trades_poller is not None:
        await _trades_poller.stop()
        _trades_poller = None
