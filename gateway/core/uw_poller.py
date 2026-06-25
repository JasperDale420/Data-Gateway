"""Background polling tasks for REST-only data providers.

Unusual Whales doesn't push data via WebSocket, so we need to poll periodically.
This module provides background tasks that run while Gateway is up.

Features:
- Polls every 5 minutes during market hours (uses TradingCalendar for holidays)
- Deduplicates events using event_id cache
- Uses larger limits in the morning to handle high volume
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, time
from time import monotonic as _monotonic
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from gateway.config import get_settings
from gateway.core.base_poller import BasePoller, DedupMixin
from gateway.core.cache import RedisCache
from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event
from gateway.core.metrics import record_feed_published, record_message_dropped, record_poller_duplicates
from gateway.core.ticker_universe import TickerUniverse
from gateway.core.uw_eod_state import UwEodStateStore

if TYPE_CHECKING:
    from gateway.providers.uw import UnusualWhalesProvider

from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from gateway.core.logger import logger
from gateway.core.timeutils import parse_timestamp
from gateway.providers.uw.transient import is_transient_upstream_error

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
        self._eod_state = UwEodStateStore(
            settings.uw_eod_state_path,
            stale_after_seconds=settings.uw_eod_claim_stale_after_seconds,
        )

        # EOD work is fanned out across hundreds of tickers × 8 feeds and can
        # take many minutes. Awaiting it inline in ``_poll_loop`` starves the
        # darkpool / extended-hours pollers that are explicitly meant to keep
        # running during EOD. Spawning it as a background task lets the main
        # loop continue to service all other intervals.
        self._eod_task: asyncio.Task[None] | None = None

        # Optional flow fan-out tap. When set (see gateway.main), each flow
        # envelope that was successfully published to heber:events is also
        # handed to this coroutine for WS delivery. Default None ⇒ zero behavior
        # change. Flow only — darkpool/tide/EOD never invoke it.
        self.on_flow_envelope: Callable[[dict[str, Any]], Awaitable[None]] | None = None

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
        on_published: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> tuple[int, int]:
        """Publish envelopes via batch pipeline with deduplication.

        Uses ``publish_all_batch`` when the sink registry supports it
        (single Redis pipeline per call) and falls back to individual
        ``publish_all`` calls otherwise.

        ``on_published`` (flow only) is awaited once per envelope that was
        actually written to Redis, AFTER the publish, so a WS fan-out tap sees
        exactly the deduplicated set the Redis stream saw. A tap failure is
        swallowed and never affects the publish result.
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

        # Publish first (batch when supported, otherwise per-envelope), then mark
        # dedup state ONLY for events that were successfully published. Marking
        # before publishing would suppress re-publish on a failed batch, so a
        # later cycle could never re-send a lost event. Content-derived event_ids
        # keep Heber-side dedup effective if a re-publish produces a duplicate.
        #
        # ``published_indices`` holds the EXACT positions in ``to_publish`` that
        # landed in Redis — not a "first N" count. On a partial Redis failure
        # (e.g. item 0 fails, item 1 succeeds) the count path would mark/fan-out
        # item 0 (which Heber never got) and drop item 1 (which Heber DID get),
        # breaking the push/poll event-id parity Orion's deduper relies on.
        messages: list[tuple[str, dict[str, Any]]] = [(HEBER_STREAM, envelope) for envelope, _, _ in to_publish]
        # ``published_indices`` is the EXACT set of positions in ``to_publish``
        # that landed in Redis when the sink can report them; ``published`` is
        # the count returned for logging/back-compat.
        published_indices: set[int] | None
        published: int
        try:
            if hasattr(sink_registry, "publish_all_batch_indexed"):
                published_indices = await sink_registry.publish_all_batch_indexed(messages)
                published = len(published_indices)
            elif hasattr(sink_registry, "publish_all_batch"):
                published = await sink_registry.publish_all_batch(messages)
                if published >= len(messages):
                    # Full success — every event landed, so mark/tap them all.
                    published_indices = set(range(len(messages)))
                else:
                    # Count-only partial: the API does not say WHICH events
                    # landed. Marking the first N could permanently suppress an
                    # event that never reached Redis, so mark/tap none and let a
                    # later cycle re-publish (Heber-side dedup absorbs any dup).
                    published_indices = None
            else:
                # Fallback for mocks / non-registry objects
                for topic, envelope in messages:
                    await sink_registry.publish_all(topic, envelope)
                published_indices = set(range(len(messages)))
                published = len(messages)
        except Exception as exc:
            logger.warning(
                "uw_poller_batch_publish_failed",
                count=len(messages),
                error=str(exc),
            )
            published_indices = set()
            published = 0

        if published_indices:
            redis_items: list[tuple[str, Any]] = []
            for idx in sorted(published_indices):
                envelope, event_id, cache_key = to_publish[idx]
                if event_id:
                    self._mark_seen(event_id)
                    if self._redis_dedupe is not None and cache_key:
                        redis_items.append((cache_key, True))
                # Fan published flow envelopes out to WS subscribers. Best-effort:
                # a tap failure must never break the publish accounting. Only the
                # exact succeeded envelopes are tapped, so push mirrors the Redis
                # stream set exactly (event-id parity holds on partial failures).
                if on_published is not None:
                    try:
                        await on_published(envelope)
                    except Exception as tap_exc:
                        logger.warning("uw_poller_flow_tap_failed", error=str(tap_exc))
            if redis_items and self._redis_dedupe is not None:
                await self._redis_dedupe.set_many(redis_items, ttl=self._cache_ttl_seconds)

        feed = envelopes[0].get("feed", "unknown")
        record_feed_published(feed, published)
        record_poller_duplicates(feed, duplicates)
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
        """Stop the background polling task and shut down provider.

        Also cancels any in-flight EOD background task so shutdown doesn't
        leave a long-running fan-out wedging the event loop. We give the
        task a brief window to settle before forcing cancellation.
        """
        # Cancel the EOD task first so producers exit before the poll loop
        # tears down its sink_registry reference.
        eod_task = self._eod_task
        self._eod_task = None
        if eod_task is not None and not eod_task.done():
            eod_task.cancel()
            try:
                await asyncio.wait_for(eod_task, timeout=2.0)
            except (asyncio.CancelledError, TimeoutError):
                pass
            except Exception as e:
                logger.warning("uw_eod_task_stop_error", error=str(e))

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

                # EOD snapshot polling (once per trading day after market close).
                # Spawn as a background task so the main loop can continue
                # servicing darkpool / extended-hours / market-tide polls while
                # the EOD fan-out (tickers × 8 feeds) is in flight. Without
                # this, awaiting EOD inline starves all other pollers for the
                # duration of the EOD run.
                if self.eod_enabled and self._should_poll_eod() and (self._eod_task is None or self._eod_task.done()):
                    logger.info("uw_poller_starting_eod_snapshots")
                    # Mark the date *before* spawning so a second tick within
                    # the same minute doesn't double-schedule the EOD work
                    # before the task gets a chance to run. The prior value is
                    # handed to the runner so it can roll the guard back on
                    # failure and allow a same-day retry.
                    previous_eod_date = self._last_eod_date
                    self._last_eod_date = datetime.now(ET).strftime("%Y-%m-%d")
                    self._eod_task = asyncio.create_task(
                        self._run_eod_with_logging(sink_registry, previous_eod_date=previous_eod_date),
                        name="uw_poller_eod",
                    )

                # Surface any exception from a finished EOD task so it lands
                # in the error log instead of being silently swallowed.
                if self._eod_task is not None and self._eod_task.done():
                    try:
                        exc = self._eod_task.exception()
                    except (asyncio.CancelledError, asyncio.InvalidStateError):
                        exc = None
                    if exc is not None:
                        logger.error(
                            "uw_eod_task_error",
                            error=str(exc),
                            exc_info=(type(exc), exc, exc.__traceback__),
                        )
                    self._eod_task = None

                # Periodic cache cleanup
                self._cleanup_cache()

            except Exception as e:
                logger.error("uw_poller_error", error=str(e), exc_info=True)

            await asyncio.sleep(base_interval)

    def _build_feed_envelopes(
        self,
        records: list[Any],
        *,
        feed: str,
        out_of_order_log: str,
        use_model_dump: bool = True,
        sector: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Build envelopes from a record list, returning them plus the count of
        records whose ``ts_event`` regressed relative to the previous record.

        ``use_model_dump`` serializes each record via ``model_dump()``; when
        false the raw record (already a dict) is wrapped directly (sector tide).
        """
        out_of_order = 0
        prev_ts: datetime | None = None
        envelopes: list[dict[str, Any]] = []

        for idx, record in enumerate(records):
            try:
                envelope = wrap_event(
                    event=record.model_dump() if use_model_dump else record,
                    provider="unusual_whales",
                    feed=feed,
                    source="rest",
                )
            except Exception as exc:
                logger.warning(
                    "uw_poller_envelope_build_skipped",
                    feed=feed,
                    record_index=idx,
                    error=str(exc),
                )
                # Meter the skip so steady partial loss from UW schema drift
                # surfaces on the dropped-message alert, not just in logs.
                record_message_dropped(reason="envelope_build_skipped", feed=feed)
                continue

            ts_event = self._parse_ts(envelope.get("ts_event"))
            if ts_event and prev_ts and ts_event < prev_ts:
                out_of_order += 1
                extra = {"sector": sector} if sector is not None else {}
                logger.debug(
                    out_of_order_log,
                    prev_ts=prev_ts.isoformat(),
                    curr_ts=ts_event.isoformat(),
                    **extra,
                )
            if ts_event:
                prev_ts = ts_event
            envelopes.append(envelope)

        return envelopes, out_of_order

    async def _poll_single_feed(
        self,
        sink_registry,
        *,
        fetch,
        feed: str,
        dedupe_prefix: str,
        missing_event_log: str,
        log_event: str,
        out_of_order_log: str,
        error_log: str,
        slice_recent: bool = False,
        log_only_when_active: bool = False,
        on_published: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        """Fetch one UW feed, build envelopes, publish, and log.

        Shared body for the single-fetch pollers (flow, darkpool, market_tide).
        ``slice_recent`` keeps only the last 5 records and adds ``recent`` to the
        published log; ``log_only_when_active`` suppresses the summary log when
        nothing was published or deduplicated (both market tide behavior).
        ``on_published`` (flow only) is forwarded to ``_publish_envelopes`` so
        published envelopes can be tapped for WS fan-out.
        """
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        try:
            records = await fetch()
            # Take last 5 to cover the 5-minute polling gap (market tide).
            effective = records[-5:] if slice_recent and len(records) > 5 else records

            # Build envelopes off the event loop: model_dump + wrap_event +
            # BLAKE2b over up to 200 records per cycle blocked the loop ~5s
            # (runtime-observed loop-stall on darkpool), starving WS heartbeats
            # and the sink drain. Mirrors option_capture/bulk offload. The fn is
            # pure (no shared-state mutation), so a thread is safe.
            envelopes, out_of_order = await asyncio.to_thread(
                self._build_feed_envelopes, effective, feed=feed, out_of_order_log=out_of_order_log
            )
            published, duplicates = await self._publish_envelopes(
                sink_registry=sink_registry,
                envelopes=envelopes,
                dedupe_prefix=dedupe_prefix,
                missing_event_log=missing_event_log,
                on_published=on_published,
            )

            if log_only_when_active and not (published or duplicates):
                return
            log_fields: dict[str, Any] = {
                "fetched": len(records),
                "published": published,
                "duplicates": duplicates,
                "out_of_order": out_of_order,
            }
            if slice_recent:
                log_fields["recent"] = len(effective)
            logger.info(log_event, **log_fields)
        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning(error_log, error=str(e))
            else:
                logger.error(error_log, error=str(e), exc_info=True)

    async def _poll_flow_alerts(self, sink_registry, limit: int) -> None:
        """Poll and publish flow alerts with deduplication."""
        await self._poll_single_feed(
            sink_registry,
            fetch=lambda: self._provider.get_flow_alerts(limit=limit),
            feed="flow_alerts",
            dedupe_prefix="uw:flow",
            missing_event_log="uw_flow_missing_event_id",
            log_event="uw_poller_flow_published",
            out_of_order_log="uw_flow_out_of_order_ts",
            error_log="uw_poller_flow_error",
            on_published=self.on_flow_envelope,
        )

    async def _poll_darkpool(self, sink_registry, limit: int) -> None:
        """Poll and publish darkpool trades with deduplication."""
        await self._poll_single_feed(
            sink_registry,
            fetch=lambda: self._provider.get_darkpool_recent(limit=limit),
            feed="darkpool",
            dedupe_prefix="uw:darkpool",
            missing_event_log="uw_darkpool_missing_event_id",
            log_event="uw_poller_darkpool_published",
            out_of_order_log="uw_darkpool_out_of_order_ts",
            error_log="uw_poller_darkpool_error",
        )

    async def _poll_market_tide(self, sink_registry) -> None:
        """Poll and publish market tide data (last 5 records cover the poll gap)."""
        await self._poll_single_feed(
            sink_registry,
            fetch=lambda: self._provider.get_market_tide(),
            feed="market_tide",
            dedupe_prefix="uw:market_tide",
            missing_event_log="uw_market_tide_missing_event_id",
            log_event="uw_poller_market_tide_published",
            out_of_order_log="uw_market_tide_out_of_order_ts",
            error_log="uw_poller_market_tide_error",
            slice_recent=True,
            log_only_when_active=True,
        )

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
                sector_envelopes, sector_out_of_order = await asyncio.to_thread(
                    self._build_feed_envelopes,
                    recent_tides,
                    feed="sector_tide",
                    out_of_order_log="uw_sector_tide_out_of_order_ts",
                    use_model_dump=False,
                    sector=sector,
                )
                total_out_of_order += sector_out_of_order

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
        if self._eod_state.should_defer(today_str):
            return False

        # Only fire after the configured time
        if now_et.hour < self._eod_hour:
            return False
        if now_et.hour == self._eod_hour and now_et.minute < self._eod_minute:
            return False

        # Must be a trading day
        return self._calendar.is_trading_day(now_et.date())

    async def _run_eod_with_logging(self, sink_registry, *, previous_eod_date: str | None = None) -> None:
        """Wrapper around ``_poll_eod_snapshots`` for the EOD background task.

        Logs success/error and clears the task reference so the next-day
        scheduler can re-spawn cleanly. Cancellation (e.g. on shutdown)
        propagates through. On failure it rolls back the in-process date guard
        and releases the persistent run claim so today's EOD can be retried on
        the next poll tick (or after a restart) instead of being lost until the
        stale window expires.
        """
        try:
            await self._poll_eod_snapshots(sink_registry)
        except asyncio.CancelledError:
            logger.info("uw_eod_task_cancelled", last_eod_date=self._last_eod_date)
            raise
        except Exception as e:
            logger.error("uw_eod_task_error", error=str(e), exc_info=True)
            today_str = datetime.now(ET).strftime("%Y-%m-%d")
            self._last_eod_date = previous_eod_date
            self._eod_state.release(today_str)

    async def _poll_eod_snapshots(self, sink_registry) -> None:
        """Orchestrate all EOD per-ticker polls with bounded concurrency."""
        today_str = datetime.now(ET).strftime("%Y-%m-%d")
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        if self._ticker_universe is None:
            raise RuntimeError("Ticker universe not initialized for EOD polling")
        if not self._eod_state.claim(today_str):
            logger.info("uw_eod_skipped_persistent_state", trading_date=today_str)
            return

        # Refresh dynamic tickers from screener
        await self._ticker_universe.refresh_dynamic(self._provider)
        tickers = self._ticker_universe.all_tickers

        logger.info("uw_eod_starting", ticker_count=len(tickers))

        sem = asyncio.Semaphore(self._eod_concurrency)

        # Per-ticker endpoints
        per_ticker_polls = [
            ("greek_exposure", self._poll_eod_greek_exposure),
            ("iv_rank", self._poll_eod_iv_rank),
            ("iv_term_structure", self._poll_eod_iv_term_structure),
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

        # Persist completion for crash recovery / multi-instance skip, and set
        # the in-process guard. ``_last_eod_date`` is also set in ``_poll_loop``
        # before this task is spawned (prevents a second tick double-scheduling);
        # setting it again here is idempotent.
        self._eod_state.mark_completed(today_str, totals=totals)
        self._last_eod_date = today_str

        logger.info("uw_eod_completed", totals=totals)

    @staticmethod
    def _wrap_eod_results(
        results: list[Any],
        *,
        feed: str,
        use_model_dump: bool = True,
        **overrides: Any,
    ) -> list[dict[str, Any]]:
        """Wrap a list of EOD records into envelopes off the event loop.

        Pure/synchronous so the per-ticker EOD fan-out can offload it via
        ``asyncio.to_thread`` — building envelopes (model_dump + BLAKE2b) for the
        higher-row feeds (greek_exposure especially) across the whole ticker
        universe otherwise runs on the loop, observed as a multi-second stall
        during the 16:30 EOD run.
        """
        return [
            wrap_event(
                event=item.model_dump() if use_model_dump else item,
                provider="unusual_whales",
                feed=feed,
                source="rest",
                **overrides,
            )
            for item in results
        ]

    async def _poll_eod_greek_exposure(self, sink_registry, ticker: str) -> int:
        """Poll Greek exposure for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._fetch_with_retry("greek_exposure", lambda: self._provider.get_greek_exposure(ticker))
        if not results:
            return 0

        envelopes = await asyncio.to_thread(self._wrap_eod_results, results, feed="greek_exposure")

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
        result = await self._fetch_with_retry("iv_rank", lambda: self._provider.get_iv_rank(ticker))
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

    async def _poll_eod_iv_term_structure(self, sink_registry, ticker: str) -> int:
        """Poll IV term structure for a single ticker.

        Emits one envelope per expiry returned by UW. Despite each row carrying
        an ``expiry`` field, this is per-underlying analytics (IV across the
        term structure of the *same* ticker) — not per-option-contract data —
        so we override the envelope's ``instrument_type``/``instrument_key`` to
        the equity form. ``_infer_instrument_type`` would otherwise see
        ``expiry`` and tag every row as ``instrument_type=option`` /
        ``instrument_key=option:{ticker}``, which is malformed (no OCC suffix)
        and makes Heber's writer-side ``is_valid_instrument_key()`` reject 100%
        of rows on Bronze→Silver normalization.
        """
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._fetch_with_retry(
            "iv_term_structure", lambda: self._provider.get_iv_term_structure(ticker)
        )
        if not results:
            return 0

        symbol = ticker.upper()
        instrument_key = f"equity:{symbol}"
        envelopes = await asyncio.to_thread(
            self._wrap_eod_results,
            results,
            feed="iv_term_structure",
            instrument_type_override="equity",
            instrument_key_override=instrument_key,
            symbol_override=symbol,
        )

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:ivts",
            missing_event_log="uw_ivts_missing_event_id",
        )
        return published

    async def _poll_eod_oi_change(self, sink_registry, ticker: str) -> int:
        """Poll OI change for a single ticker."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._fetch_with_retry("oi_change", lambda: self._provider.get_oi_change(ticker))
        if not results:
            return 0

        envelopes = await asyncio.to_thread(self._wrap_eod_results, results, feed="oi_change")

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:oi",
            missing_event_log="uw_oi_missing_event_id",
        )
        return published

    async def _poll_eod_option_volume(self, sink_registry, ticker: str) -> int:
        """Poll historic option volume for a single ticker.

        Each row carries an ``expiry`` field (volume bucketed per expiration),
        but this is per-underlying analytics -- NOT per-option-contract data.
        Without the equity overrides below, ``_infer_instrument_type`` would
        see ``expiry`` in the payload and tag every row as
        ``instrument_type=option`` with ``instrument_key=option:{ticker}``
        (no OCC suffix), which Heber's writer-side
        ``is_valid_instrument_key()`` rejects -- dropping 100% of rows on
        Bronze→Silver normalization. See ``_poll_eod_iv_term_structure`` for
        the canonical pattern.
        """
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._fetch_with_retry(
            "historic_option_volume", lambda: self._provider.get_historic_option_volume(ticker)
        )
        if not results:
            return 0

        symbol = ticker.upper()
        instrument_key = f"equity:{symbol}"
        envelopes = await asyncio.to_thread(
            self._wrap_eod_results,
            results,
            feed="historic_option_volume",
            use_model_dump=False,
            instrument_type_override="equity",
            instrument_key_override=instrument_key,
            symbol_override=symbol,
        )

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
        results = await self._fetch_with_retry("short_interest", lambda: self._provider.get_short_interest(ticker))
        if not results:
            return 0

        envelopes = await asyncio.to_thread(self._wrap_eod_results, results, feed="short_interest")

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
        results = await self._fetch_with_retry("short_volume", lambda: self._provider.get_short_volume(ticker))
        if not results:
            return 0

        envelopes = await asyncio.to_thread(self._wrap_eod_results, results, feed="short_volume")

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
        results = await self._fetch_with_retry("ftds", lambda: self._provider.get_ftds(ticker))
        if not results:
            return 0

        envelopes = await asyncio.to_thread(self._wrap_eod_results, results, feed="ftds")

        published, _ = await self._publish_envelopes(
            sink_registry=sink_registry,
            envelopes=envelopes,
            dedupe_prefix="uw:ftd",
            missing_event_log="uw_ftd_missing_event_id",
        )
        return published

    async def _fetch_with_retry(self, fetch_label: str, fetch) -> Any:
        """Run an EOD fetch with bounded retry on transient upstream errors.

        UW occasionally answers EOD market-wide endpoints with a single 5xx
        (e.g. congress_trades returned one 503 and dropped to zero rows). Retry
        transient upstream failures with exponential backoff so a momentary
        brownout doesn't silently zero out a feed for the whole day.
        """
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception(is_transient_upstream_error),
            reraise=True,
        ):
            with attempt:
                if attempt.retry_state.attempt_number > 1:
                    logger.warning(
                        "uw_eod_fetch_retry",
                        feed=fetch_label,
                        attempt=attempt.retry_state.attempt_number,
                    )
                return await fetch()
        return []

    @staticmethod
    def _stamp_filing_ts_event(items: list[dict], *date_keys: str) -> None:
        """Stamp a stable ``timestamp`` onto each item from its filing date.

        Congress/insider payloads carry no event-time field, so ``wrap_event``
        falls back to ``now()`` and the SAME trade re-fetched on a later run gets
        a fresh ``ts_event`` (and therefore a fresh ``event_id``), producing
        duplicate Bronze rows. Deriving ``timestamp`` from the filing date
        (falling back to the transaction date) makes the event_id content-stable
        across runs so Heber-side dedup absorbs any re-publish.
        """
        for item in items:
            if not isinstance(item, dict) or item.get("timestamp"):
                continue
            for key in date_keys:
                value = item.get(key)
                if value:
                    item["timestamp"] = value
                    break

    async def _poll_eod_congress_trades(self, sink_registry) -> int:
        """Poll congress trades (market-wide, no ticker needed)."""
        if self._provider is None:
            raise RuntimeError("UW provider not initialized")
        results = await self._fetch_with_retry(
            "congress_trades",
            lambda: self._provider.get_congress_trades(limit=200),
        )
        if not results:
            return 0

        self._stamp_filing_ts_event(results, "filed_at_date", "transaction_date")

        envelopes = await asyncio.to_thread(
            self._wrap_eod_results, results, feed="congress_trades", use_model_dump=False
        )

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
        results = await self._fetch_with_retry(
            "insider_trades",
            lambda: self._provider.get_insiders(limit=200),
        )
        if not results:
            return 0

        self._stamp_filing_ts_event(results, "filing_date", "transaction_date")

        envelopes = await asyncio.to_thread(
            self._wrap_eod_results, results, feed="insider_trades", use_model_dump=False
        )

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
