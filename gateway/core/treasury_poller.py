"""Treasury yield poller for daily macro data ingestion via Alpha Vantage.

Fetches daily Treasury yield data (configurable maturities, default 2-year
and 10-year) from Alpha Vantage and emits ``EventEnvelope`` dicts to the
data sink for Heber consumption.

Treasury yields update once per business day, so the default poll interval
is 24 hours.  The poller only emits the most recent data point per maturity
to avoid re-publishing historical data on every poll.

Pattern follows ``gateway.core.quotes_poller``:
- Runs as an asyncio background task during application lifespan
- Wraps each data point in an ``EventEnvelope`` via ``wrap_event``
- Publishes to the data sink for Heber integration
- Module-level singleton with ``start_`` / ``stop_`` / ``get_`` accessors
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any

import structlog

from gateway.core.envelope import wrap_event

logger = structlog.get_logger()

# Stream name for Heber integration (same as other pollers)
HEBER_STREAM = "heber:events"

# Valid Alpha Vantage Treasury yield maturities
VALID_MATURITIES = frozenset({"3month", "2year", "5year", "7year", "10year", "30year"})


class TreasuryYieldPoller:
    """Background poller that fetches Treasury yield data via Alpha Vantage.

    Publishes ``EventEnvelope`` dicts to the data-sink for Heber consumption.
    Runs on a configurable schedule (default: once every 24 hours).
    """

    def __init__(
        self,
        maturities: list[str] | None = None,
        poll_interval_seconds: int = 86400,
    ) -> None:
        self._maturities = [m.lower() for m in (maturities or ["2year", "10year"]) if m.lower() in VALID_MATURITIES]
        if not self._maturities:
            self._maturities = ["2year", "10year"]

        self._poll_interval = max(3600, poll_interval_seconds)
        self._running = False
        self._task: asyncio.Task | None = None
        self._provider: Any = None
        self._last_poll_time: datetime | None = None
        self._last_poll_count: int = 0

    # ─────────────────────────────────────────────────────────────────────
    # Core polling
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_yields(self) -> list[dict[str, Any]]:
        """Fetch the most recent Treasury yield for each configured maturity.

        Returns a list of payload dicts, one per maturity.
        """
        if self._provider is None:
            return []

        results: list[dict[str, Any]] = []

        for maturity in self._maturities:
            try:
                data = await self._provider.get_economic_indicator(
                    indicator="TREASURY_YIELD",
                    interval="daily",
                    maturity=maturity,
                )
                data_points = data.get("data", [])
                if not data_points:
                    logger.warning("treasury_poller_no_data", maturity=maturity)
                    continue

                # Alpha Vantage returns data sorted newest-first.
                # Take only the most recent data point.
                latest = data_points[0]
                date_str = latest.get("date", "")
                value_str = latest.get("value", "")

                # Skip placeholder values (Alpha Vantage uses "." for missing)
                if not value_str or value_str == ".":
                    logger.warning("treasury_poller_missing_value", maturity=maturity, date=date_str)
                    continue

                try:
                    yield_pct = float(value_str)
                except (ValueError, TypeError):
                    logger.warning(
                        "treasury_poller_invalid_value",
                        maturity=maturity,
                        date=date_str,
                        value=value_str,
                    )
                    continue

                results.append(
                    {
                        "date": date_str,
                        "maturity": maturity,
                        "yield_pct": yield_pct,
                    }
                )

            except Exception as e:
                logger.error(
                    "treasury_poller_fetch_error",
                    maturity=maturity,
                    error=str(e),
                    exc_info=True,
                )

        return results

    async def _poll_and_publish(self, sink_registry: Any) -> None:
        """Fetch Treasury yields and publish to data sink."""
        yields = await self._fetch_yields()
        if not yields:
            logger.debug("treasury_poller_no_yields")
            return

        published = 0
        for yield_data in yields:
            try:
                # Build a timestamp from the date string for ts_event
                date_str = yield_data["date"]
                try:
                    ts_event = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
                except (ValueError, TypeError):
                    ts_event = datetime.now(UTC)

                envelope = wrap_event(
                    event=yield_data,
                    provider="alphavantage",
                    feed="treasury_yields",
                    source="rest",
                    instrument_type_override="macro",
                    instrument_key_override=f"macro:treasury_yield:{yield_data['maturity']}",
                    symbol_override=f"TREASURY_{yield_data['maturity'].upper()}",
                    ts_ingest=datetime.now(UTC),
                )
                # Override ts_event after wrap_event (wrap_event may pick it up
                # from the payload's "date" field, but ISO format is cleaner)
                envelope["ts_event"] = ts_event.isoformat()

                await sink_registry.publish_all(HEBER_STREAM, envelope)
                published += 1

            except Exception as e:
                logger.error(
                    "treasury_poller_publish_error",
                    maturity=yield_data.get("maturity"),
                    error=str(e),
                    exc_info=True,
                )

        self._last_poll_time = datetime.now(UTC)
        self._last_poll_count = published

        logger.info(
            "treasury_poller_published",
            fetched=len(yields),
            published=published,
            maturities=self._maturities,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the background polling task."""
        if self._running:
            logger.warning("treasury_poller_already_running")
            return

        from gateway.core.globals import get_registry

        registry = get_registry()
        self._provider = registry.get("alphavantage")
        if self._provider is None:
            logger.error("treasury_poller_no_alphavantage_provider")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(
            "treasury_poller_started",
            interval_seconds=self._poll_interval,
            maturities=self._maturities,
        )

    async def stop(self) -> None:
        """Stop the background polling task."""
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("treasury_poller_stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop — runs at the configured interval."""
        from gateway.core.globals import get_sink_registry

        # Run an initial poll shortly after startup (30s delay for provider init)
        await asyncio.sleep(30)

        while self._running:
            try:
                sink_registry = get_sink_registry()
                if not sink_registry:
                    logger.debug("treasury_poller_no_sink")
                    await asyncio.sleep(60)
                    continue

                await self._poll_and_publish(sink_registry)

            except Exception as e:
                logger.error("treasury_poller_loop_error", error=str(e), exc_info=True)

            await asyncio.sleep(self._poll_interval)

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return lightweight runtime telemetry for admin surfaces."""
        return {
            "running": self._running,
            "poll_interval_seconds": self._poll_interval,
            "maturities": self._maturities,
            "last_poll_time": self._last_poll_time.isoformat() if self._last_poll_time else None,
            "last_poll_count": self._last_poll_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton (mirrors quotes_poller / uw_poller pattern)
# ─────────────────────────────────────────────────────────────────────────────

_treasury_poller: TreasuryYieldPoller | None = None


def get_treasury_poller() -> TreasuryYieldPoller | None:
    """Get the global treasury yield poller instance."""
    return _treasury_poller


async def start_treasury_poller(
    maturities: list[str] | None = None,
    poll_interval_seconds: int = 86400,
) -> TreasuryYieldPoller:
    """Start the global Treasury yield poller."""
    global _treasury_poller

    if _treasury_poller is not None:
        return _treasury_poller

    _treasury_poller = TreasuryYieldPoller(
        maturities=maturities,
        poll_interval_seconds=poll_interval_seconds,
    )
    await _treasury_poller.start()
    return _treasury_poller


async def stop_treasury_poller() -> None:
    """Stop the global Treasury yield poller."""
    global _treasury_poller

    if _treasury_poller is not None:
        await _treasury_poller.stop()
        _treasury_poller = None
