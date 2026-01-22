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
from zoneinfo import ZoneInfo

import structlog

from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event

logger = structlog.get_logger()

# Stream name for Heber integration
HEBER_STREAM = "heber:events"

# Eastern Time for morning rush detection
ET = ZoneInfo("America/New_York")
MORNING_RUSH_START = time(9, 30)
MORNING_RUSH_END = time(10, 30)

# Poll settings
DEFAULT_POLL_INTERVAL = 300  # 5 minutes for flow/darkpool
MARKET_TIDE_POLL_INTERVAL = 3600  # 1 hour (API returns full day's data)


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
    ):
        self.poll_interval = poll_interval_seconds
        self.flow_enabled = flow_enabled
        self.darkpool_enabled = darkpool_enabled
        self.market_tide_enabled = market_tide_enabled
        self._running = False
        self._task: asyncio.Task | None = None
        self._provider = None  # type: ignore[assignment]
        self._calendar = TradingCalendar()

        # Deduplication cache (event_id -> timestamp)
        # Keep IDs for last 2 hours to handle polling overlap
        self._seen_ids: dict[str, datetime] = {}
        self._cache_ttl_seconds = 7200  # 2 hours

        # Market tide polls hourly since API returns full day's data
        self._last_tide_poll: datetime | None = None
        self._tide_interval = MARKET_TIDE_POLL_INTERVAL

    def _is_market_hours(self) -> bool:
        """Check if market is currently open using TradingCalendar."""
        return self._calendar.is_market_open()

    def _is_morning_rush(self) -> bool:
        """Check if we're in high-volume morning period (first hour of trading)."""
        now_et = datetime.now(ET)
        current_time = now_et.time()
        return MORNING_RUSH_START <= current_time <= MORNING_RUSH_END

    def _get_poll_limit(self) -> int:
        """Get poll limit based on time of day."""
        if self._is_morning_rush():
            return 200  # Higher limit during morning rush
        return 100  # Standard limit

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

    def _should_poll_tide(self) -> bool:
        """Check if enough time has passed to poll market tide again."""
        if self._last_tide_poll is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_tide_poll).total_seconds()
        return elapsed >= self._tide_interval

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

        while self._running:
            try:
                # Only poll during market hours
                if not self._is_market_hours():
                    logger.debug("uw_poller_outside_market_hours")
                    await asyncio.sleep(60)  # Check every minute when market is closed
                    continue

                sink_registry = get_sink_registry()
                if not sink_registry:
                    logger.debug("uw_poller_no_sink")
                    await asyncio.sleep(self.poll_interval)
                    continue

                poll_limit = self._get_poll_limit()
                logger.info(
                    "uw_poller_polling", limit=poll_limit, morning_rush=self._is_morning_rush()
                )

                # Poll flow alerts
                if self.flow_enabled:
                    await self._poll_flow_alerts(sink_registry, poll_limit)

                # Poll darkpool
                if self.darkpool_enabled:
                    await self._poll_darkpool(sink_registry, poll_limit)

                # Poll market tide (hourly since API returns full day's data)
                if self.market_tide_enabled and self._should_poll_tide():
                    await self._poll_market_tide(sink_registry)
                    self._last_tide_poll = datetime.now(UTC)

                # Periodic cache cleanup
                self._cleanup_cache()

            except Exception as e:
                logger.error("uw_poller_error", error=str(e), exc_info=True)

            await asyncio.sleep(self.poll_interval)

    async def _poll_flow_alerts(self, sink_registry, limit: int) -> None:
        """Poll and publish flow alerts with deduplication."""
        try:
            alerts = await self._provider.get_flow_alerts(limit=limit)
            published = 0
            duplicates = 0

            for alert in alerts:
                envelope = wrap_event(
                    event=alert.model_dump(),
                    provider="unusual_whales",
                    feed="flow_alerts",
                    source="rest",
                )

                event_id = envelope.get("event_id", "")
                if self._is_duplicate(event_id):
                    duplicates += 1
                    continue

                await sink_registry.publish_all(HEBER_STREAM, envelope)
                self._mark_seen(event_id)
                published += 1

            logger.info(
                "uw_poller_flow_published",
                fetched=len(alerts),
                published=published,
                duplicates=duplicates,
            )
        except Exception as e:
            logger.error("uw_poller_flow_error", error=str(e))

    async def _poll_darkpool(self, sink_registry, limit: int) -> None:
        """Poll and publish darkpool trades with deduplication."""
        try:
            trades = await self._provider.get_darkpool_recent(limit=limit)
            published = 0
            duplicates = 0

            for trade in trades:
                envelope = wrap_event(
                    event=trade.model_dump(),
                    provider="unusual_whales",
                    feed="darkpool",
                    source="rest",
                )

                event_id = envelope.get("event_id", "")
                if self._is_duplicate(event_id):
                    duplicates += 1
                    continue

                await sink_registry.publish_all(HEBER_STREAM, envelope)
                self._mark_seen(event_id)
                published += 1

            logger.info(
                "uw_poller_darkpool_published",
                fetched=len(trades),
                published=published,
                duplicates=duplicates,
            )
        except Exception as e:
            logger.error("uw_poller_darkpool_error", error=str(e))

    async def _poll_market_tide(self, sink_registry) -> None:
        """Poll and publish market tide data.

        Since we poll every 5 minutes but UW publishes every minute,
        we fetch all tides and take the last 5 to avoid missing data.
        Deduplication ensures we don't republish overlapping records.
        """
        try:
            tides = await self._provider.get_market_tide()

            # Take last 5 to cover the 5-minute polling gap
            recent_tides = tides[-5:] if len(tides) > 5 else tides

            published = 0
            duplicates = 0

            for tide in recent_tides:
                envelope = wrap_event(
                    event=tide.model_dump(),
                    provider="unusual_whales",
                    feed="market_tide",
                    source="rest",
                )

                event_id = envelope.get("event_id", "")
                if self._is_duplicate(event_id):
                    duplicates += 1
                    continue

                await sink_registry.publish_all(HEBER_STREAM, envelope)
                self._mark_seen(event_id)
                published += 1

            if published or duplicates:
                logger.info(
                    "uw_poller_market_tide_published",
                    fetched=len(tides),
                    recent=len(recent_tides),
                    published=published,
                    duplicates=duplicates,
                )
        except Exception as e:
            logger.error("uw_poller_market_tide_error", error=str(e))


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
    )
    await _uw_poller.start()
    return _uw_poller


async def stop_uw_poller() -> None:
    """Stop the global UW poller."""
    global _uw_poller

    if _uw_poller is not None:
        await _uw_poller.stop()
        _uw_poller = None
