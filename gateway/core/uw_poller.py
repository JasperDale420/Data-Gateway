"""Background polling tasks for REST-only data providers.

Unusual Whales doesn't push data via WebSocket, so we need to poll periodically.
This module provides background tasks that run while Gateway is up.

Features:
- Polls every 5 minutes during market hours (uses TradingCalendar for holidays)
- Deduplicates events using event_id cache
- Uses larger limits in the morning to handle high volume
"""

import asyncio
from datetime import UTC, datetime, time
from time import monotonic as _monotonic
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from gateway.config import get_settings
from gateway.core.base_poller import BasePoller, DedupMixin
from gateway.core.cache import RedisCache
from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event
from gateway.core.ticker_universe import TickerUniverse

if TYPE_CHECKING:
    from gateway.providers.uw import UnusualWhalesProvider

from gateway.core.logger import logger
from gateway.core.timeutils import parse_timestamp

# Stream name for Heber integration
HEBER_STREAM = "heber:events"

# Eastern Time for morning rush detection
ET = ZoneInfo("America/New_York")
MORNING_RUSH_START = time(9, 30)
MORNING_RUSH_END = time(10, 30)

# Poll settings
DEFAULT_POLL_INTERVAL = 300  # 5 minutes for flow
DARKPOOL_RUSH_INTERVAL = 15  # 15s during morning rush (9:30-10:30 ET)
DARKPOOL_MARKET_INTERVAL = 30  # 30s during normal market hours
DARKPOOL_EXTENDED_INTERVAL = 60  # 60s during extended hours
BASE_LOOP_INTERVAL = 15  # Base loop tick (must be <= smallest poll interval)
MARKET_TIDE_POLL_INTERVAL = 3600  # 1 hour (API returns full day's data)

# Extended hours (darkpool runs here too)
PREMARKET_START = time(4, 0)  # 4:00 AM ET
AFTERHOURS_END = time(20, 0)  # 8:00 PM ET

# Sector names for sector tide polling — must match UW SDK Sector enum values exactly.
# See: vendor/unusualwhales_sdk/unusualwhales/models/sector.py
GICS_SECTORS = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
]


class UWPoller(DedupMixin, BasePoller):
    """Background poller for Unusual Whales data.

    Polls UW endpoints at configured intervals during market hours
    and publishes to data sink with deduplication.
    """

    def __init__(
        self,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL,
        flow_enabled: bool = True,
        darkpool_enabled: bool = True,
        market_tide_enabled: bool = True,
        sector_tide_enabled: bool = True,
        eod_enabled: bool = False,
        eod_hour: int = 16,
        eod_minute: int = 30,
        eod_concurrency: int = 5,
    ):
        settings = get_settings()
        super().__init__(
            poll_interval_seconds=poll_interval_seconds,
            poller_name="uw_poller",
        )
        # Keep legacy alias for code that reads self.poll_interval
        self.poll_interval = poll_interval_seconds
        self.flow_enabled = flow_enabled
        self.darkpool_enabled = darkpool_enabled
        self.market_tide_enabled = market_tide_enabled
        self.sector_tide_enabled = sector_tide_enabled
        self.eod_enabled = eod_enabled
        self._eod_hour = eod_hour
        self._eod_minute = eod_minute
        self._eod_concurrency = max(1, eod_concurrency)
        self._provider: UnusualWhalesProvider | None = None
        self._calendar = TradingCalendar()
        self._ticker_universe: TickerUniverse | None = None

        # Track EOD polling — once per trading day
        self._last_eod_date: str | None = None

        # Deduplication cache — OrderedDict for O(1) FIFO eviction.
        self._init_dedup(cache_ttl_seconds=7200)  # 2 hours
        self._publish_max_inflight = max(1, int(settings.uw_poller_publish_max_inflight))

        # Cache market hours lookups (30-second TTL)
        self._market_hours_cache: tuple[bool, float] = (False, 0.0)
        self._extended_hours_cache: tuple[bool, float] = (False, 0.0)
        self._MARKET_HOURS_CACHE_TTL = 30.0
        self._EXTENDED_HOURS_CACHE_TTL = 30.0
        self._redis_dedupe: RedisCache | None = None
        if settings.cache_redis_enabled and settings.cache_redis_url:
            self._redis_dedupe = RedisCache(
                redis_url=settings.cache_redis_url,
                default_ttl=self._cache_ttl_seconds,
            )

        # Flow tracks its own interval (every 5 minutes)
        self._last_flow_poll: datetime | None = None
        self._flow_interval = DEFAULT_POLL_INTERVAL

        # Darkpool tracks its own polling time (adaptive interval)
        self._last_darkpool_poll: datetime | None = None

        # Market tide polls hourly since API returns full day's data
        self._last_tide_poll: datetime | None = None
        self._tide_interval = MARKET_TIDE_POLL_INTERVAL

        # Sector tide has its own independent timer
        self._last_sector_tide_poll: datetime | None = None

    def _is_market_hours(self) -> bool:
        """Check if market is currently open (cached for 30s)."""
        now = _monotonic()
        cached_val, cached_at = self._market_hours_cache
        if now - cached_at < self._MARKET_HOURS_CACHE_TTL:
            return cached_val
        result = self._calendar.is_market_open()
        self._market_hours_cache = (result, now)
        return result

    def _is_extended_hours(self) -> bool:
        """Check if we're in extended trading hours (cached for 30s)."""
        now = _monotonic()
        cached_val, cached_at = self._extended_hours_cache
        if now - cached_at < self._EXTENDED_HOURS_CACHE_TTL:
            return cached_val
        now_et = datetime.now(ET)
        current_time = now_et.time()
        result = PREMARKET_START <= current_time <= AFTERHOURS_END and self._calendar.is_trading_day(now_et.date())
        self._extended_hours_cache = (result, now)
        return result

    def _is_morning_rush(self) -> bool:
        """Check if we're in high-volume morning period (first hour of trading)."""
        now_et = datetime.now(ET)
        current_time = now_et.time()
        return MORNING_RUSH_START <= current_time <= MORNING_RUSH_END

    @staticmethod
    def _parse_ts(ts_value: str | None) -> datetime | None:
        return parse_timestamp(ts_value)

    def _get_poll_limit(self) -> int:
        """Get poll limit based on time of day.

        NOTE: UW API limits darkpool endpoint to max 200 per call.
        Actual trade volume is 500-800 per 5-minute window, so we only
        capture a subset of trades with current polling approach.
        Consider more frequent polling or a different strategy for complete coverage.
        """
        return 200  # API max limit

    async def _load_redis_duplicate_ids(
        self,
        dedupe_items: list[tuple[str, str]],
    ) -> set[str]:
        """Fetch duplicate flags via a single MGET round trip."""
        if self._redis_dedupe is None or not dedupe_items:
            return set()

        cache_keys = [cache_key for _, cache_key in dedupe_items]
        try:
            found = await self._redis_dedupe.mget(cache_keys)
        except Exception as e:
            logger.warning("uw_poller_redis_dedupe_mget_failed", count=len(cache_keys), error=str(e))
            return set()

        return {event_id for (event_id, cache_key) in dedupe_items if cache_key in found}

    async def _publish_envelopes(
        self,
        sink_registry: Any,
        envelopes: list[dict[str, Any]],
        dedupe_prefix: str,
        missing_event_log: str,
    ) -> tuple[int, int]:
        """Publish envelopes via batch pipeline with deduplication.

        Uses ``publish_all_batch`` when the sink registry supports it
        (single Redis pipeline per call) and falls back to individual
        ``publish_all`` calls otherwise.
        """
        if not envelopes:
            return 0, 0

        duplicates = 0
        to_publish: list[tuple[dict[str, Any], str, str | None]] = []
        dedupe_items: list[tuple[str, str]] = []

        for envelope in envelopes:
            event_id = str(envelope.get("event_id", ""))
            if not event_id:
                logger.warning(missing_event_log)
                to_publish.append((envelope, "", None))
                continue
            if self._is_duplicate(event_id):
                duplicates += 1
                continue
            dedupe_cache_key = f"{dedupe_prefix}:{event_id}"
            dedupe_items.append((event_id, dedupe_cache_key))
            to_publish.append((envelope, event_id, dedupe_cache_key))

        redis_duplicates = await self._load_redis_duplicate_ids(dedupe_items)
        if redis_duplicates:
            duplicates += len(redis_duplicates)
            to_publish = [item for item in to_publish if not item[1] or item[1] not in redis_duplicates]

        if not to_publish:
            return 0, duplicates

        # Batch publish: build message list and send in a single pipeline call
        messages: list[tuple[str, dict[str, Any]]] = [(HEBER_STREAM, envelope) for envelope, _, _ in to_publish]

        try:
            if hasattr(sink_registry, "publish_all_batch"):
                published = await sink_registry.publish_all_batch(messages)
            else:
                # Fallback for mocks / non-registry objects
                for topic, envelope in messages:
                    await sink_registry.publish_all(topic, envelope)
                published = len(messages)
        except Exception as exc:
            logger.warning(
                "uw_poller_batch_publish_failed",
                count=len(messages),
                error=str(exc),
            )
            published = 0

        # Mark all successfully-published items as seen and batch Redis dedup SETs
        if published > 0:
            redis_items: list[tuple[str, Any]] = []
            for _envelope, event_id, cache_key in to_publish[:published]:
                if event_id:
                    self._mark_seen(event_id)
                    if self._redis_dedupe is not None and cache_key:
                        redis_items.append((cache_key, True))

            if redis_items:
                await self._redis_dedupe.set_many(redis_items, ttl=self._cache_ttl_seconds)

        return published, duplicates

    def _should_poll_tide(self) -> bool:
        """Check if enough time has passed to poll market tide again."""
        if self._last_tide_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_tide_poll).total_seconds()
        return elapsed >= self._tide_interval

    def _should_poll_sector_tide(self) -> bool:
        """Check if enough time has passed to poll sector tide again."""
        if self._last_sector_tide_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_sector_tide_poll).total_seconds()
        return elapsed >= self._tide_interval

    def _should_poll_flow(self) -> bool:
        """Check if enough time has passed to poll flow alerts again (every 5 minutes)."""
        if self._last_flow_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_flow_poll).total_seconds()
        return elapsed >= self._flow_interval

    def _get_darkpool_interval(self) -> int:
        """Return adaptive darkpool poll interval based on time of day.

        - 15s during morning rush (9:30-10:30 ET) for peak volume capture
        - 30s during normal market hours
        - 60s during extended hours (pre-market, after-hours)
        """
        if self._is_morning_rush():
            return DARKPOOL_RUSH_INTERVAL
        if self._is_market_hours():
            return DARKPOOL_MARKET_INTERVAL
        return DARKPOOL_EXTENDED_INTERVAL

    def _should_poll_darkpool(self) -> bool:
        """Check if enough time has passed to poll darkpool again."""
        if self._last_darkpool_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_darkpool_poll).total_seconds()
        return elapsed >= self._get_darkpool_interval()

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return lightweight runtime/tuning telemetry for admin surfaces."""
        from gateway.core.globals import get_sink_registry

        sink_registry = get_sink_registry()
        return {
            "running": self._running,
            "sink_available": sink_registry is not None,
            "enabled": True,
            "publish_max_inflight": self._publish_max_inflight,
            "dedupe_cache_entries": len(self._seen_ids),
            "dedupe_cache_ttl_seconds": self._cache_ttl_seconds,
            "poll_intervals_seconds": {
                "flow": self._flow_interval,
                "darkpool": self._get_darkpool_interval(),
                "tide": self._tide_interval,
                "base_loop": BASE_LOOP_INTERVAL,
            },
            "feeds": {
                "flow": self.flow_enabled,
                "darkpool": self.darkpool_enabled,
                "market_tide": self.market_tide_enabled,
                "sector_tide": self.sector_tide_enabled,
                "eod": self.eod_enabled,
            },
            "eod": {
                "hour": self._eod_hour,
                "minute": self._eod_minute,
                "concurrency": self._eod_concurrency,
                "last_run_date": self._last_eod_date,
            },
        }

    async def _on_start(self) -> bool:
        """Initialize UW provider and ticker universe."""
        from gateway.providers.uw import UnusualWhalesProvider

        self._provider = UnusualWhalesProvider()
        await self._provider.initialize(
            {
                "api_key_env": "UNUSUAL_WHALES_API_KEY",  # pragma: allowlist secret
            }
        )

        if not self._provider._initialized:
            logger.error("uw_poller_provider_not_initialized")
            return False

        # Initialize ticker universe for EOD polling
        if self.eod_enabled:
            settings = get_settings()
            core = (
                [t.strip().upper() for t in settings.uw_core_tickers.split(",") if t.strip()]
                if settings.uw_core_tickers
                else None
            )
            self._ticker_universe = TickerUniverse(
                core_tickers=core,
                dynamic_count=settings.uw_dynamic_ticker_count,
            )

        logger.info(
            "uw_poller_started",
            interval_seconds=self.poll_interval,
            flow=self.flow_enabled,
            darkpool=self.darkpool_enabled,
            market_tide=self.market_tide_enabled,
            eod=self.eod_enabled,
        )
        return True

    async def stop(self) -> None:
        """Stop the background polling task and shut down provider."""
        await super().stop()
        if self._provider:
            await self._provider.shutdown()
        logger.info("uw_poller_stopped", cached_ids=len(self._seen_ids))

    async def _poll_once(self) -> None:
        """Not used — UW poller overrides _poll_loop for custom timing."""
        raise NotImplementedError("UWPoller uses custom _poll_loop")

    async def _poll_loop(self) -> None:
        """Main polling loop."""
        from gateway.core.globals import get_sink_registry

        # Base loop tick — must be <= smallest poll interval
        base_interval = BASE_LOOP_INTERVAL

        while self._running:
            try:
                sink_registry = get_sink_registry()
                if not sink_registry:
                    logger.warning("uw_poller_no_sink")
                    await asyncio.sleep(base_interval)
                    continue

                poll_limit = self._get_poll_limit()

                # Poll flow alerts (every 5 minutes during market hours)
                if self.flow_enabled and self._should_poll_flow():
                    if self._is_market_hours():
                        logger.info("uw_poller_polling_flow", limit=poll_limit)
                        await self._poll_flow_alerts(sink_registry, poll_limit)
                        self._last_flow_poll = datetime.now(UTC)

                # Poll darkpool (every minute during market AND extended hours)
                if self.darkpool_enabled and self._should_poll_darkpool():
                    if self._is_market_hours() or self._is_extended_hours():
                        logger.info("uw_poller_polling_darkpool", limit=poll_limit)
                        await self._poll_darkpool(sink_registry, poll_limit)
                        self._last_darkpool_poll = datetime.now(UTC)

                # Poll market tide (hourly since API returns full day's data)
                if self.market_tide_enabled and self._should_poll_tide():
                    if self._is_market_hours():
                        await self._poll_market_tide(sink_registry)
                        self._last_tide_poll = datetime.now(UTC)

                # Poll sector tides (hourly, independent timer from market tide)
                if self.sector_tide_enabled and self._should_poll_sector_tide():
                    if self._is_market_hours():
                        await self._poll_sector_tides(sink_registry)
                        self._last_sector_tide_poll = datetime.now(UTC)

                # EOD snapshot polling (once per trading day after market close)
                if self.eod_enabled and self._should_poll_eod():
                    logger.info("uw_poller_starting_eod_snapshots")
                    await self._poll_eod_snapshots(sink_registry)

                # EOD snapshot polling (once per trading day after market close)
                if self.eod_enabled and self._should_poll_eod():
                    logger.info("uw_poller_starting_eod_snapshots")
                    await self._poll_eod_snapshots(sink_registry)

                # Periodic cache cleanup
                self._cleanup_cache()

            except Exception as e:
                logger.error("uw_poller_error", error=str(e), exc_info=True)

            await asyncio.sleep(base_interval)

    async def _poll_flow_alerts(self, sink_registry, limit: int) -> None:
        """Poll and publish flow alerts with deduplication."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        try:
            alerts = await self._provider.get_flow_alerts(limit=limit)
            out_of_order = 0
            prev_ts: datetime | None = None
            envelopes: list[dict[str, Any]] = []

            for alert in alerts:
                envelope = wrap_event(
                    event=alert.model_dump(),
                    provider="unusual_whales",
                    feed="flow_alerts",
                    source="rest",
                )

                ts_event = self._parse_ts(envelope.get("ts_event"))
                if ts_event and prev_ts and ts_event < prev_ts:
                    out_of_order += 1
                    logger.debug(
                        "uw_flow_out_of_order_ts",
                        prev_ts=prev_ts.isoformat(),
                        curr_ts=ts_event.isoformat(),
                    )
                if ts_event:
                    prev_ts = ts_event
                envelopes.append(envelope)

            published, duplicates = await self._publish_envelopes(
                sink_registry=sink_registry,
                envelopes=envelopes,
                dedupe_prefix="uw:flow",
                missing_event_log="uw_flow_missing_event_id",
            )

            logger.info(
                "uw_poller_flow_published",
                fetched=len(alerts),
                published=published,
                duplicates=duplicates,
                out_of_order=out_of_order,
            )
        except Exception as e:
            logger.error("uw_poller_flow_error", error=str(e))

    async def _poll_darkpool(self, sink_registry, limit: int) -> None:
        """Poll and publish darkpool trades with deduplication."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        try:
            trades = await self._provider.get_darkpool_recent(limit=limit)
            out_of_order = 0
            prev_ts: datetime | None = None
            envelopes: list[dict[str, Any]] = []

            for trade in trades:
                envelope = wrap_event(
                    event=trade.model_dump(),
                    provider="unusual_whales",
                    feed="darkpool",
                    source="rest",
                )

                ts_event = self._parse_ts(envelope.get("ts_event"))
                if ts_event and prev_ts and ts_event < prev_ts:
                    out_of_order += 1
                    logger.debug(
                        "uw_darkpool_out_of_order_ts",
                        prev_ts=prev_ts.isoformat(),
                        curr_ts=ts_event.isoformat(),
                    )
                if ts_event:
                    prev_ts = ts_event
                envelopes.append(envelope)

            published, duplicates = await self._publish_envelopes(
                sink_registry=sink_registry,
                envelopes=envelopes,
                dedupe_prefix="uw:darkpool",
                missing_event_log="uw_darkpool_missing_event_id",
            )

            logger.info(
                "uw_poller_darkpool_published",
                fetched=len(trades),
                published=published,
                duplicates=duplicates,
                out_of_order=out_of_order,
            )
        except Exception as e:
            logger.error("uw_poller_darkpool_error", error=str(e))

    async def _poll_market_tide(self, sink_registry) -> None:
        """Poll and publish market tide data.

        Since we poll every 5 minutes but UW publishes every minute,
        we fetch all tides and take the last 5 to avoid missing data.
        Deduplication ensures we don't republish overlapping records.
        """
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        try:
            tides = await self._provider.get_market_tide()

            # Take last 5 to cover the 5-minute polling gap
            recent_tides = tides[-5:] if len(tides) > 5 else tides

            out_of_order = 0
            prev_ts: datetime | None = None
            envelopes: list[dict[str, Any]] = []

            for tide in recent_tides:
                envelope = wrap_event(
                    event=tide.model_dump(),
                    provider="unusual_whales",
                    feed="market_tide",
                    source="rest",
                )

                ts_event = self._parse_ts(envelope.get("ts_event"))
                if ts_event and prev_ts and ts_event < prev_ts:
                    out_of_order += 1
                    logger.debug(
                        "uw_market_tide_out_of_order_ts",
                        prev_ts=prev_ts.isoformat(),
                        curr_ts=ts_event.isoformat(),
                    )
                if ts_event:
                    prev_ts = ts_event
                envelopes.append(envelope)

            published, duplicates = await self._publish_envelopes(
                sink_registry=sink_registry,
                envelopes=envelopes,
                dedupe_prefix="uw:market_tide",
                missing_event_log="uw_market_tide_missing_event_id",
            )

            if published or duplicates:
                logger.info(
                    "uw_poller_market_tide_published",
                    fetched=len(tides),
                    recent=len(recent_tides),
                    published=published,
                    duplicates=duplicates,
                    out_of_order=out_of_order,
                )
        except Exception as e:
            logger.error("uw_poller_market_tide_error", error=str(e))

    async def _poll_sector_tides(self, sink_registry) -> None:
        """Poll and publish sector tide data for all GICS sectors.

        Runs hourly on same schedule as market tide.
        """
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        total_published = 0
        total_duplicates = 0
        total_out_of_order = 0
        sectors_polled = 0

        for sector in GICS_SECTORS:
            try:
                tides = await self._provider.get_sector_tide(sector)

                # Take last 5 records for each sector
                recent_tides = tides[-5:] if len(tides) > 5 else tides
                prev_ts: datetime | None = None
                sector_envelopes: list[dict[str, Any]] = []

                for tide in recent_tides:
                    envelope = wrap_event(
                        event=tide,
                        provider="unusual_whales",
                        feed="sector_tide",
                        source="rest",
                    )

                    ts_event = self._parse_ts(envelope.get("ts_event"))
                    if ts_event and prev_ts and ts_event < prev_ts:
                        total_out_of_order += 1
                        logger.debug(
                            "uw_sector_tide_out_of_order_ts",
                            sector=sector,
                            prev_ts=prev_ts.isoformat(),
                            curr_ts=ts_event.isoformat(),
                        )
                    if ts_event:
                        prev_ts = ts_event
                    sector_envelopes.append(envelope)

                published, duplicates = await self._publish_envelopes(
                    sink_registry=sink_registry,
                    envelopes=sector_envelopes,
                    dedupe_prefix="uw:sector_tide",
                    missing_event_log="uw_sector_tide_missing_event_id",
                )
                total_published += published
                total_duplicates += duplicates

                sectors_polled += 1

            except Exception as e:
                logger.error("uw_poller_sector_tide_error", sector=sector, error=str(e))

        if total_published or total_duplicates:
            logger.info(
                "uw_poller_sector_tides_published",
                sectors=sectors_polled,
                published=total_published,
                duplicates=total_duplicates,
                out_of_order=total_out_of_order,
            )

    # ─────────────────────────────────────────────────────────────────────
    # EOD Snapshot Polling
    # ─────────────────────────────────────────────────────────────────────

    def _should_poll_eod(self) -> bool:
        """Check if EOD snapshots should be polled.

        Fires once per trading day at the configured hour/minute (ET).
        """
        now_et = datetime.now(ET)
        today_str = now_et.strftime("%Y-%m-%d")

        # Already polled today
        if self._last_eod_date == today_str:
            return False

        # Only fire after the configured time
        if now_et.hour < self._eod_hour:
            return False
        if now_et.hour == self._eod_hour and now_et.minute < self._eod_minute:
            return False

        # Must be a trading day
        return self._calendar.is_trading_day(now_et.date())

    async def _poll_eod_snapshots(self, sink_registry) -> None:
        """Orchestrate all EOD per-ticker polls with bounded concurrency."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        if self._ticker_universe is None:
            raise RuntimeError("Ticker universe not initialized for EOD polling")

        # Refresh dynamic tickers from screener
        await self._ticker_universe.refresh_dynamic(self._provider)
        tickers = self._ticker_universe.all_tickers

        logger.info("uw_eod_starting", ticker_count=len(tickers))

        sem = asyncio.Semaphore(self._eod_concurrency)

        # Per-ticker endpoints
        per_ticker_polls = [
            ("greek_exposure", self._poll_eod_greek_exposure),
            ("iv_rank", self._poll_eod_iv_rank),
            ("oi_change", self._poll_eod_oi_change),
            ("historic_option_volume", self._poll_eod_option_volume),
            ("short_interest", self._poll_eod_short_interest),
            ("short_volume", self._poll_eod_short_volume),
            ("ftds", self._poll_eod_ftds),
        ]

        totals: dict[str, dict[str, int]] = {}

        for feed_name, poll_fn in per_ticker_polls:
            published_total = 0
            errors = 0

            async def _poll_ticker(
                ticker: str,
                _fn=poll_fn,
                _feed=feed_name,
            ) -> tuple[int, int]:
                async with sem:
                    try:
                        count = await _fn(sink_registry, ticker)
                        return (count, 0)
                    except Exception as e:
                        logger.error(
                            f"uw_eod_{_feed}_ticker_error",
                            ticker=ticker,
                            error=str(e),
                        )
                        return (0, 1)

            results = await asyncio.gather(
                *[_poll_ticker(t) for t in tickers],
                return_exceptions=True,
            )

            for r in results:
                if isinstance(r, tuple):
                    published_total += r[0]
                    errors += r[1]
                else:
                    errors += 1

            totals[feed_name] = {"published": published_total, "errors": errors}

        # Market-wide endpoints (no ticker required)
        try:
            congress_count = await self._poll_eod_congress_trades(sink_registry)
            totals["congress_trades"] = {"published": congress_count, "errors": 0}
        except Exception as e:
            logger.error("uw_eod_congress_error", error=str(e))
            totals["congress_trades"] = {"published": 0, "errors": 1}

        try:
            insider_count = await self._poll_eod_insiders(sink_registry)
            totals["insider_trades"] = {"published": insider_count, "errors": 0}
        except Exception as e:
            logger.error("uw_eod_insiders_error", error=str(e))
            totals["insider_trades"] = {"published": 0, "errors": 1}

        # Mark today as polled
        self._last_eod_date = datetime.now(ET).strftime("%Y-%m-%d")

        logger.info("uw_eod_completed", totals=totals)

    async def _poll_eod_greek_exposure(self, sink_registry, ticker: str) -> int:
        """Poll Greek exposure for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_greek_exposure(ticker)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item.model_dump(),
                provider="unusual_whales",
                feed="greek_exposure",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:gex",
            missing_event_log="uw_gex_missing_event_id",
        )
        return published

    async def _poll_eod_iv_rank(self, sink_registry, ticker: str) -> int:
        """Poll IV rank for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        result = await self._provider.get_iv_rank(ticker)
        if not result:
            return 0

        envelope = wrap_event(
            event=result.model_dump(),
            provider="unusual_whales",
            feed="iv_rank",
            source="rest",
        )

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=[envelope],
            dedupe_prefix="uw:ivr",
            missing_event_log="uw_ivr_missing_event_id",
        )
        return published

    async def _poll_eod_oi_change(self, sink_registry, ticker: str) -> int:
        """Poll OI change for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_oi_change(ticker)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item.model_dump(),
                provider="unusual_whales",
                feed="oi_change",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:oi",
            missing_event_log="uw_oi_missing_event_id",
        )
        return published

    async def _poll_eod_option_volume(self, sink_registry, ticker: str) -> int:
        """Poll historic option volume for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_historic_option_volume(ticker)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item,
                provider="unusual_whales",
                feed="historic_option_volume",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:optvol",
            missing_event_log="uw_optvol_missing_event_id",
        )
        return published

    async def _poll_eod_short_interest(self, sink_registry, ticker: str) -> int:
        """Poll short interest for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_short_interest(ticker)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item.model_dump(),
                provider="unusual_whales",
                feed="short_interest",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:si",
            missing_event_log="uw_si_missing_event_id",
        )
        return published

    async def _poll_eod_short_volume(self, sink_registry, ticker: str) -> int:
        """Poll short volume for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_short_volume(ticker)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item.model_dump(),
                provider="unusual_whales",
                feed="short_volume",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:sv",
            missing_event_log="uw_sv_missing_event_id",
        )
        return published

    async def _poll_eod_ftds(self, sink_registry, ticker: str) -> int:
        """Poll FTDs for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_ftds(ticker)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item.model_dump(),
                provider="unusual_whales",
                feed="ftds",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:ftd",
            missing_event_log="uw_ftd_missing_event_id",
        )
        return published

    async def _poll_eod_congress_trades(self, sink_registry) -> int:
        """Poll congress trades (market-wide, no ticker needed)."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_congress_trades(limit=200)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item,
                provider="unusual_whales",
                feed="congress_trades",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:congress",
            missing_event_log="uw_congress_missing_event_id",
        )
        return published

    async def _poll_eod_insiders(self, sink_registry) -> int:
        """Poll insider trades (market-wide, no ticker needed)."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._provider.get_insiders(limit=200)
        if not results:
            return 0

        envelopes = [
            wrap_event(
                event=item,
                provider="unusual_whales",
                feed="insider_trades",
                source="rest",
            )
            for item in results
        ]

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:insider",
            missing_event_log="uw_insider_missing_event_id",
        )
        return published


# Global poller instance
_uw_poller: UWPoller | None = None


def get_uw_poller() -> UWPoller | None:
    """Get the global UW poller instance."""
    return _uw_poller


def get_uw_poller_snapshot() -> dict[str, Any]:
    """Get UW poller runtime snapshot for status surfaces."""
    poller = get_uw_poller()
    if poller is None:
        return {
            "running": False,
            "enabled": False,
            "publish_max_inflight": None,
            "dedupe_cache_entries": 0,
            "dedupe_cache_ttl_seconds": None,
            "poll_intervals_seconds": {},
            "feeds": {},
            "eod": {},
        }
    return poller.get_runtime_snapshot()


async def start_uw_poller(
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL,
    flow_enabled: bool = True,
    darkpool_enabled: bool = True,
    market_tide_enabled: bool = True,
    sector_tide_enabled: bool = True,
    eod_enabled: bool = False,
    eod_hour: int = 16,
    eod_minute: int = 30,
    eod_concurrency: int = 5,
) -> UWPoller:
    """Start the global UW poller."""
    global _uw_poller

    if _uw_poller is not None:
        return _uw_poller

    _uw_poller = UWPoller(
        poll_interval_seconds=poll_interval_seconds,
        flow_enabled=flow_enabled,
        darkpool_enabled=darkpool_enabled,
        market_tide_enabled=market_tide_enabled,
        sector_tide_enabled=sector_tide_enabled,
        eod_enabled=eod_enabled,
        eod_hour=eod_hour,
        eod_minute=eod_minute,
        eod_concurrency=eod_concurrency,
    )
    await _uw_poller.start()
    return _uw_poller


async def stop_uw_poller() -> None:
    """Stop the global UW poller."""
    global _uw_poller

    if _uw_poller is not None:
        await _uw_poller.stop()
        _uw_poller = None
