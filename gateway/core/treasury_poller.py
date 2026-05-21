"""Treasury yield poller for daily macro data ingestion via Alpha Vantage.

Fetches daily Treasury yield data (configurable maturities, default 2-year
and 10-year) from Alpha Vantage and emits ``EventEnvelope`` dicts to the
data sink for Heber consumption.

Treasury yields update once per business day, so the default poll interval
is 24 hours.  The poller only emits the most recent data point per maturity
to avoid re-publishing historical data on every poll.

Rate-limit aware: inserts a 15-second delay between maturity fetches to
stay within Alpha Vantage's free-tier limit of 5 calls/min.  On rate-limit
errors, applies exponential backoff before retrying the affected maturity.

Pattern follows ``gateway.core.base_poller.BasePoller``:
- Runs as an asyncio background task during application lifespan
- Wraps each data point in an ``EventEnvelope`` via ``wrap_event``
- Publishes to the data sink for Heber integration
- Module-level singleton with ``start_`` / ``stop_`` / ``get_`` accessors
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from gateway.core.base_poller import BasePoller
from gateway.core.envelope import wrap_event
from gateway.core.logger import logger

# Stream name for Heber integration (same as other pollers)
HEBER_STREAM = "heber:events"

# Valid Alpha Vantage Treasury yield maturities
VALID_MATURITIES = frozenset({"3month", "2year", "5year", "7year", "10year", "30year"})

# Rate-limit constants for Alpha Vantage free tier (5 calls/min, 500/day)
_INTER_REQUEST_DELAY_SECONDS = 15  # delay between maturity fetches (~4 calls/min max)
_RATE_LIMIT_BACKOFF_BASE = 60  # initial backoff on rate-limit error (seconds)
_RATE_LIMIT_BACKOFF_MAX = 300  # maximum backoff (5 minutes)
_RATE_LIMIT_MAX_RETRIES = 3  # max retries per maturity on rate-limit errors


class TreasuryYieldPoller(BasePoller):
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

        super().__init__(
            poll_interval_seconds=max(3600, poll_interval_seconds),
            poller_name="treasury_poller",
        )
        self._provider: Any = None
        self._last_poll_time: datetime | None = None
        self._last_poll_count: int = 0
        self._premium_blocked_maturities: set[str] = set()

    # ─────────────────────────────────────────────────────────────────────
    # BasePoller hooks
    # ─────────────────────────────────────────────────────────────────────

    async def _on_start(self) -> bool:
        from gateway.core.globals import get_registry

        registry = get_registry()
        self._provider = registry.get("alphavantage")
        if self._provider is None:
            logger.error("treasury_poller_no_alphavantage_provider")
            return False

        # Check that the API key is actually configured before starting the
        # poll loop.  Without it every request will fail with API_KEY_NOT_SET.
        if not getattr(self._provider, "api_key_configured", True):
            logger.warning(
                "treasury_poller_skipped_no_api_key",
                reason="Alpha Vantage API key not configured — treasury poller will not start",
            )
            return False

        logger.info(
            "treasury_poller_started",
            interval_seconds=self._poll_interval,
            maturities=self._maturities,
        )
        return True

    async def stop(self) -> None:
        await super().stop()
        logger.info("treasury_poller_stopped")

    async def _poll_loop(self) -> None:
        """Override to add an initial 30s startup delay."""
        await asyncio.sleep(30)
        await super()._poll_loop()

    async def _poll_once(self) -> None:
        from gateway.core.globals import get_sink_registry

        sink_registry = get_sink_registry()
        if not sink_registry:
            logger.debug("treasury_poller_no_sink")
            return

        await self._poll_and_publish(sink_registry)

    # ─────────────────────────────────────────────────────────────────────
    # Core polling
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_yields(self) -> list[dict[str, Any]]:
        """Fetch the most recent Treasury yield for each configured maturity.

        Inserts a delay between maturity fetches to respect Alpha Vantage's
        free-tier rate limit (5 calls/min).  On rate-limit errors, applies
        exponential backoff before retrying the same maturity.  Premium
        endpoint errors are recorded permanently and the maturity is skipped
        on all future poll cycles.

        Returns a list of payload dicts, one per maturity.
        """
        if self._provider is None:
            return []

        results: list[dict[str, Any]] = []

        for idx, maturity in enumerate(self._maturities):
            # Skip maturities that have been permanently flagged as premium-only
            if maturity in self._premium_blocked_maturities:
                logger.debug("treasury_poller_premium_skip", maturity=maturity)
                continue

            # Rate-limit pacing: wait between requests (skip delay before the first)
            if idx > 0:
                await asyncio.sleep(_INTER_REQUEST_DELAY_SECONDS)

            result = await self._fetch_single_maturity(maturity)
            if result is not None:
                results.append(result)

        return results

    async def _fetch_single_maturity(self, maturity: str) -> dict[str, Any] | None:
        """Fetch a single maturity with exponential backoff on rate-limit errors.

        Returns the yield payload dict, or None if the fetch failed.
        """
        from gateway.providers.alphavantage import AlphaVantagePremiumError, AlphaVantageRateLimitError

        backoff = _RATE_LIMIT_BACKOFF_BASE

        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                data = await self._provider.get_economic_indicator(
                    indicator="TREASURY_YIELD",
                    interval="daily",
                    maturity=maturity,
                )
                data_points = data.get("data", [])
                if not data_points:
                    logger.warning("treasury_poller_no_data", maturity=maturity)
                    return None

                # Alpha Vantage returns data sorted newest-first.
                # Take only the most recent data point.
                latest = data_points[0]
                date_str = latest.get("date", "")
                value_str = latest.get("value", "")

                # Skip placeholder values (Alpha Vantage uses "." for missing)
                if not value_str or value_str == ".":
                    logger.warning("treasury_poller_missing_value", maturity=maturity, date=date_str)
                    return None

                try:
                    yield_pct = float(value_str)
                except (ValueError, TypeError):
                    logger.warning(
                        "treasury_poller_invalid_value",
                        maturity=maturity,
                        date=date_str,
                        value=value_str,
                    )
                    return None

                return {
                    "date": date_str,
                    "maturity": maturity,
                    "yield_pct": yield_pct,
                }

            except AlphaVantagePremiumError:
                # Permanently skip this maturity — the free tier cannot access it
                logger.warning(
                    "treasury_poller_premium_blocked",
                    maturity=maturity,
                    reason="Premium endpoint requires Alpha Vantage subscription — maturity disabled",
                )
                self._premium_blocked_maturities.add(maturity)
                return None

            except AlphaVantageRateLimitError:
                if attempt < _RATE_LIMIT_MAX_RETRIES:
                    logger.warning(
                        "treasury_poller_rate_limited",
                        maturity=maturity,
                        attempt=attempt + 1,
                        max_retries=_RATE_LIMIT_MAX_RETRIES,
                        backoff_seconds=backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _RATE_LIMIT_BACKOFF_MAX)
                else:
                    logger.error(
                        "treasury_poller_rate_limit_exhausted",
                        maturity=maturity,
                        attempts=_RATE_LIMIT_MAX_RETRIES + 1,
                    )
                    return None

            except Exception as e:
                logger.error(
                    "treasury_poller_fetch_error",
                    maturity=maturity,
                    error=str(e),
                    exc_info=True,
                )
                return None

        return None

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
                    # Pass the parsed date as the canonical event timestamp so
                    # the event_id hash includes it. The previous code patched
                    # ``envelope["ts_event"]`` *after* the hash was already
                    # computed from ``datetime.now(UTC)``, so the event_id
                    # changed on every replay and defeated dedup.
                    ts_event_override=ts_event,
                )

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
    # Telemetry
    # ─────────────────────────────────────────────────────────────────────

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return lightweight runtime telemetry for admin surfaces."""
        return super().get_runtime_snapshot() | {
            "maturities": self._maturities,
            "premium_blocked_maturities": sorted(self._premium_blocked_maturities),
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
