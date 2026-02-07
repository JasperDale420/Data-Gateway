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
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import structlog

from gateway.config import get_settings
from gateway.core.cache import RedisCache
from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event

if TYPE_CHECKING:
    from gateway.providers.uw import UnusualWhalesProvider

logger = structlog.get_logger()

# Stream name for Heber integration
HEBER_STREAM = "heber:events"

# Eastern Time for morning rush detection
ET = ZoneInfo("America/New_York")
MORNING_RUSH_START = time(9, 30)
MORNING_RUSH_END = time(10, 30)

# Poll settings
DEFAULT_POLL_INTERVAL = 300  # 5 minutes for flow
DARKPOOL_POLL_INTERVAL = 60  # 1 minute for darkpool (API max 200/call)
MARKET_TIDE_POLL_INTERVAL = 3600  # 1 hour (API returns full day's data)

# Extended hours (darkpool runs here too)
PREMARKET_START = time(4, 0)  # 4:00 AM ET
AFTERHOURS_END = time(20, 0)  # 8:00 PM ET

# GICS Sectors for sector tide polling
GICS_SECTORS = [
    "Technology",
    "Healthcare",
    "Financial",
    "Consumer Cyclical",
    "Communication Services",
    "Industrials",
    "Consumer Defensive",
    "Energy",
    "Basic Materials",
    "Real Estate",
    "Utilities",
]


class UWPoller:
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
    ):
        settings = get_settings()
        self.poll_interval = poll_interval_seconds
        self.flow_enabled = flow_enabled
        self.darkpool_enabled = darkpool_enabled
        self.market_tide_enabled = market_tide_enabled
        self.sector_tide_enabled = sector_tide_enabled
        self._running = False
        self._task: asyncio.Task | None = None
        self._provider: UnusualWhalesProvider | None = None
        self._calendar = TradingCalendar()

        # Deduplication cache (event_id -> timestamp)
        # Keep IDs for last 2 hours to handle polling overlap
        self._seen_ids: dict[str, datetime] = {}
        self._cache_ttl_seconds = 7200  # 2 hours
        self._publish_max_inflight = max(1, int(settings.uw_poller_publish_max_inflight))
        self._redis_dedupe: RedisCache | None = None
        if settings.cache_redis_enabled and settings.cache_redis_url:
            self._redis_dedupe = RedisCache(
                redis_url=settings.cache_redis_url,
                default_ttl=self._cache_ttl_seconds,
            )

        # Flow tracks its own interval (every 5 minutes)
        self._last_flow_poll: datetime | None = None
        self._flow_interval = DEFAULT_POLL_INTERVAL

        # Darkpool tracks its own polling time (runs every minute)
        self._last_darkpool_poll: datetime | None = None
        self._darkpool_interval = DARKPOOL_POLL_INTERVAL

        # Market/sector tide polls hourly since API returns full day's data
        self._last_tide_poll: datetime | None = None
        self._tide_interval = MARKET_TIDE_POLL_INTERVAL

    def _is_market_hours(self) -> bool:
        """Check if market is currently open using TradingCalendar."""
        return self._calendar.is_market_open()

    def _is_extended_hours(self) -> bool:
        """Check if we're in extended trading hours (pre-market or after-hours).

        Darkpool trades occur during extended hours as well.
        """
        now_et = datetime.now(ET)
        current_time = now_et.time()
        # Extended hours: 4:00 AM - 8:00 PM ET on trading days
        return PREMARKET_START <= current_time <= AFTERHOURS_END and self._calendar.is_trading_day(
            now_et.date()
        )

    def _is_morning_rush(self) -> bool:
        """Check if we're in high-volume morning period (first hour of trading)."""
        now_et = datetime.now(ET)
        current_time = now_et.time()
        return MORNING_RUSH_START <= current_time <= MORNING_RUSH_END

    @staticmethod
    def _parse_ts(ts_value: str | None) -> datetime | None:
        if not ts_value:
            return None
        try:
            return datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    def _get_poll_limit(self) -> int:
        """Get poll limit based on time of day.

        NOTE: UW API limits darkpool endpoint to max 200 per call.
        Actual trade volume is 500-800 per 5-minute window, so we only
        capture a subset of trades with current polling approach.
        Consider more frequent polling or a different strategy for complete coverage.
        """
        return 200  # API max limit

    def _is_duplicate(self, event_id: str) -> bool:
        """Check if event has already been seen."""
        if event_id in self._seen_ids:
            return True
        return False

    def _mark_seen(self, event_id: str) -> None:
        """Mark event as seen."""
        self._seen_ids[event_id] = datetime.now(UTC)

    def _cleanup_cache(self) -> None:
        """Remove expired entries from dedup cache."""
        now = datetime.now(UTC)
        expired = [
            eid
            for eid, ts in self._seen_ids.items()
            if (now - ts).total_seconds() > self._cache_ttl_seconds
        ]
        for eid in expired:
            del self._seen_ids[eid]

        if expired:
            logger.debug(
                "uw_poller_cache_cleanup", removed=len(expired), remaining=len(self._seen_ids)
            )

    async def _load_redis_duplicate_ids(
        self,
        dedupe_items: list[tuple[str, str]],
    ) -> set[str]:
        """Fetch duplicate flags from Redis cache in parallel."""
        if self._redis_dedupe is None or not dedupe_items:
            return set()

        checks = await asyncio.gather(
            *(self._redis_dedupe.get(cache_key) for _, cache_key in dedupe_items),
            return_exceptions=True,
        )
        duplicates: set[str] = set()
        for (event_id, cache_key), result in zip(dedupe_items, checks, strict=False):
            if isinstance(result, Exception):
                logger.warning(
                    "uw_poller_redis_dedupe_get_failed",
                    cache_key=cache_key,
                    error=str(result),
                )
                continue
            if result is not None:
                duplicates.add(event_id)
        return duplicates

    async def _publish_envelopes(
        self,
        sink_registry: Any,
        envelopes: list[dict[str, Any]],
        dedupe_prefix: str,
        missing_event_log: str,
    ) -> tuple[int, int]:
        """Publish envelopes with bounded concurrency and batched dedupe checks."""
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
            to_publish = [
                item for item in to_publish if not item[1] or item[1] not in redis_duplicates
            ]

        publish_sem = asyncio.Semaphore(max(1, self._publish_max_inflight))

        async def _publish_one(
            item: tuple[dict[str, Any], str, str | None],
        ) -> tuple[tuple[dict[str, Any], str, str | None], Exception | None]:
            envelope, _, _ = item
            try:
                async with publish_sem:
                    await sink_registry.publish_all(HEBER_STREAM, envelope)
                return item, None
            except Exception as exc:
                return item, exc

        publish_results = await asyncio.gather(*(_publish_one(item) for item in to_publish))

        published = 0
        redis_sets = []
        for (envelope, event_id, cache_key), publish_error in publish_results:
            if publish_error is not None:
                logger.warning(
                    "uw_poller_publish_failed",
                    event_id=event_id or "missing",
                    feed=envelope.get("feed"),
                    error=str(publish_error),
                )
                continue

            published += 1
            if event_id:
                self._mark_seen(event_id)
                if self._redis_dedupe is not None and cache_key:
                    redis_sets.append(
                        self._redis_dedupe.set(cache_key, True, ttl=self._cache_ttl_seconds)
                    )

        if redis_sets:
            set_results = await asyncio.gather(*redis_sets, return_exceptions=True)
            for result in set_results:
                if isinstance(result, Exception):
                    logger.warning("uw_poller_redis_dedupe_set_failed", error=str(result))

        return published, duplicates

    def _should_poll_tide(self) -> bool:
        """Check if enough time has passed to poll market tide again."""
        if self._last_tide_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_tide_poll).total_seconds()
        return elapsed >= self._tide_interval

    def _should_poll_flow(self) -> bool:
        """Check if enough time has passed to poll flow alerts again (every 5 minutes)."""
        if self._last_flow_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_flow_poll).total_seconds()
        return elapsed >= self._flow_interval

    def _should_poll_darkpool(self) -> bool:
        """Check if enough time has passed to poll darkpool again (every minute)."""
        if self._last_darkpool_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_darkpool_poll).total_seconds()
        return elapsed >= self._darkpool_interval

    async def start(self) -> None:
        """Start the background polling task."""
        if self._running:
            logger.warning("uw_poller_already_running")
            return

        # Initialize UW provider
        from gateway.providers.uw import UnusualWhalesProvider

        self._provider = UnusualWhalesProvider()
        await self._provider.initialize(
            {
                "api_key_env": "UNUSUAL_WHALES_API_KEY",  # pragma: allowlist secret
            }
        )

        if not self._provider._initialized:
            logger.error("uw_poller_provider_not_initialized")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(
            "uw_poller_started",
            interval_seconds=self.poll_interval,
            flow=self.flow_enabled,
            darkpool=self.darkpool_enabled,
            market_tide=self.market_tide_enabled,
        )

    async def stop(self) -> None:
        """Stop the background polling task."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._provider:
            await self._provider.shutdown()
        logger.info("uw_poller_stopped", cached_ids=len(self._seen_ids))

    async def _poll_loop(self) -> None:
        """Main polling loop."""
        from gateway.api.deps import get_sink_registry

        # Base interval is 1 minute (darkpool frequency)
        base_interval = DARKPOOL_POLL_INTERVAL

        while self._running:
            try:
                sink_registry = get_sink_registry()
                if not sink_registry:
                    logger.debug("uw_poller_no_sink")
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

                # Poll sector tides (hourly, same schedule as market tide)
                if self.sector_tide_enabled and self._should_poll_tide():
                    if self._is_market_hours():
                        await self._poll_sector_tides(sink_registry)

                # Periodic cache cleanup
                self._cleanup_cache()

            except Exception as e:
                logger.error("uw_poller_error", error=str(e), exc_info=True)

            await asyncio.sleep(base_interval)

    async def _poll_flow_alerts(self, sink_registry, limit: int) -> None:
        """Poll and publish flow alerts with deduplication."""
        assert self._provider is not None
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
                    logger.warning(
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
        assert self._provider is not None
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
                    logger.warning(
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
        assert self._provider is not None
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
                    logger.warning(
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
        assert self._provider is not None
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
                        logger.warning(
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


# Global poller instance
_uw_poller: UWPoller | None = None


def get_uw_poller() -> UWPoller | None:
    """Get the global UW poller instance."""
    return _uw_poller


async def start_uw_poller(
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL,
    flow_enabled: bool = True,
    darkpool_enabled: bool = True,
    market_tide_enabled: bool = True,
    sector_tide_enabled: bool = True,
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
    )
    await _uw_poller.start()
    return _uw_poller


async def stop_uw_poller() -> None:
    """Stop the global UW poller."""
    global _uw_poller

    if _uw_poller is not None:
        await _uw_poller.stop()
        _uw_poller = None
