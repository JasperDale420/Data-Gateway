"""Tests for the Treasury yield poller."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.core.treasury_poller import (
    _INTER_REQUEST_DELAY_SECONDS,
    _RATE_LIMIT_BACKOFF_BASE,
    _RATE_LIMIT_MAX_RETRIES,
    HEBER_STREAM,
    TreasuryYieldPoller,
    get_treasury_poller,
    start_treasury_poller,
    stop_treasury_poller,
)
from gateway.providers.alphavantage import AlphaVantagePremiumError, AlphaVantageRateLimitError

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _make_av_response(date: str = "2025-06-15", value: str = "4.25") -> dict[str, Any]:
    """Build a mock Alpha Vantage TREASURY_YIELD response."""
    return {
        "name": "Treasury Yield",
        "interval": "daily",
        "unit": "percent",
        "data": [
            {"date": date, "value": value},
            {"date": "2025-06-14", "value": "4.24"},
            {"date": "2025-06-13", "value": "4.23"},
        ],
    }


def _make_empty_response() -> dict[str, Any]:
    return {"name": "Treasury Yield", "interval": "daily", "unit": "percent", "data": []}


def _make_missing_value_response() -> dict[str, Any]:
    return {
        "name": "Treasury Yield",
        "interval": "daily",
        "unit": "percent",
        "data": [{"date": "2025-06-15", "value": "."}],
    }


@pytest.fixture
def mock_provider():
    """Create a mock Alpha Vantage provider."""
    provider = AsyncMock()
    provider.get_economic_indicator = AsyncMock(return_value=_make_av_response())
    return provider


@pytest.fixture
def mock_sink_registry():
    """Create a mock data sink registry."""
    sink = AsyncMock()
    sink.publish_all = AsyncMock()
    return sink


@pytest.fixture
def poller():
    """Create a TreasuryYieldPoller with default settings."""
    return TreasuryYieldPoller(maturities=["10year", "2year"], poll_interval_seconds=86400)


# ─────────────────────────────────────────────────────────────────────────────
# Construction
# ─────────────────────────────────────────────────────────────────────────────


class TestConstruction:
    def test_default_maturities(self):
        p = TreasuryYieldPoller()
        assert p._maturities == ["2year", "10year"]

    def test_custom_maturities(self):
        p = TreasuryYieldPoller(maturities=["5year", "30year"])
        assert p._maturities == ["5year", "30year"]

    def test_invalid_maturities_filtered(self):
        p = TreasuryYieldPoller(maturities=["10year", "invalid", "99year"])
        assert p._maturities == ["10year"]

    def test_all_invalid_maturities_falls_back(self):
        p = TreasuryYieldPoller(maturities=["bogus"])
        assert p._maturities == ["2year", "10year"]

    def test_minimum_poll_interval(self):
        p = TreasuryYieldPoller(poll_interval_seconds=100)
        assert p._poll_interval == 3600  # enforced minimum

    def test_normal_poll_interval(self):
        p = TreasuryYieldPoller(poll_interval_seconds=43200)
        assert p._poll_interval == 43200


# ─────────────────────────────────────────────────────────────────────────────
# Fetching yields
# ─────────────────────────────────────────────────────────────────────────────


class TestFetchYields:
    @pytest.mark.asyncio
    async def test_fetch_yields_success(self, poller, mock_provider):
        poller._provider = mock_provider

        results = await poller._fetch_yields()

        assert len(results) == 2
        # First maturity
        assert results[0]["maturity"] == "10year"
        assert results[0]["date"] == "2025-06-15"
        assert results[0]["yield_pct"] == 4.25
        # Second maturity
        assert results[1]["maturity"] == "2year"

        # Verify correct API calls
        assert mock_provider.get_economic_indicator.call_count == 2
        calls = mock_provider.get_economic_indicator.call_args_list
        assert calls[0].kwargs == {"indicator": "TREASURY_YIELD", "interval": "daily", "maturity": "10year"}
        assert calls[1].kwargs == {"indicator": "TREASURY_YIELD", "interval": "daily", "maturity": "2year"}

    @pytest.mark.asyncio
    async def test_fetch_yields_no_provider(self, poller):
        poller._provider = None
        results = await poller._fetch_yields()
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_yields_empty_response(self, poller, mock_provider):
        mock_provider.get_economic_indicator = AsyncMock(return_value=_make_empty_response())
        poller._provider = mock_provider

        results = await poller._fetch_yields()
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_yields_missing_value_skipped(self, poller, mock_provider):
        mock_provider.get_economic_indicator = AsyncMock(return_value=_make_missing_value_response())
        poller._provider = mock_provider

        results = await poller._fetch_yields()
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_yields_invalid_float_skipped(self, poller, mock_provider):
        bad_response = _make_av_response(value="not_a_number")
        mock_provider.get_economic_indicator = AsyncMock(return_value=bad_response)
        poller._provider = mock_provider

        results = await poller._fetch_yields()
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_yields_partial_failure(self, poller, mock_provider):
        """If one maturity fails, the other should still succeed."""
        call_count = 0

        async def _side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs.get("maturity") == "10year":
                raise RuntimeError("API error")
            return _make_av_response()

        mock_provider.get_economic_indicator = AsyncMock(side_effect=_side_effect)
        poller._provider = mock_provider

        results = await poller._fetch_yields()
        assert len(results) == 1
        assert results[0]["maturity"] == "2year"

    @pytest.mark.asyncio
    async def test_premium_error_blocks_maturity(self, poller, mock_provider):
        """Premium endpoint errors should permanently block the maturity."""

        async def _side_effect(**kwargs):
            if kwargs.get("maturity") == "10year":
                raise AlphaVantagePremiumError("Premium endpoint requires Alpha Vantage subscription")
            return _make_av_response()

        mock_provider.get_economic_indicator = AsyncMock(side_effect=_side_effect)
        poller._provider = mock_provider

        with patch("gateway.core.treasury_poller.asyncio.sleep", new_callable=AsyncMock):
            results = await poller._fetch_yields()

        assert len(results) == 1
        assert results[0]["maturity"] == "2year"
        assert "10year" in poller._premium_blocked_maturities

        # Second poll should skip the premium-blocked maturity entirely
        mock_provider.get_economic_indicator.reset_mock()
        with patch("gateway.core.treasury_poller.asyncio.sleep", new_callable=AsyncMock):
            results = await poller._fetch_yields()

        assert len(results) == 1
        # Should only call for 2year, not 10year
        assert mock_provider.get_economic_indicator.call_count == 1

    @pytest.mark.asyncio
    async def test_rate_limit_backoff_and_retry(self, poller, mock_provider):
        """Rate-limit errors should trigger exponential backoff retries."""
        call_count = 0

        async def _side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs.get("maturity") == "10year" and call_count <= 2:
                raise AlphaVantageRateLimitError("Rate limit exceeded")
            return _make_av_response()

        mock_provider.get_economic_indicator = AsyncMock(side_effect=_side_effect)
        poller._provider = mock_provider

        sleep_calls = []

        async def _mock_sleep(seconds):
            sleep_calls.append(seconds)

        with patch("gateway.core.treasury_poller.asyncio.sleep", side_effect=_mock_sleep):
            results = await poller._fetch_yields()

        # Both maturities should succeed (10year on 3rd attempt)
        assert len(results) == 2

        # Verify backoff delays were applied for the rate-limited calls
        backoff_sleeps = [s for s in sleep_calls if s >= _RATE_LIMIT_BACKOFF_BASE]
        assert len(backoff_sleeps) == 2  # two rate-limit retries
        assert backoff_sleeps[0] == _RATE_LIMIT_BACKOFF_BASE
        assert backoff_sleeps[1] == _RATE_LIMIT_BACKOFF_BASE * 2

    @pytest.mark.asyncio
    async def test_rate_limit_exhausted(self, poller, mock_provider):
        """After max retries, rate-limited maturity should be skipped."""

        async def _side_effect(**kwargs):
            if kwargs.get("maturity") == "10year":
                raise AlphaVantageRateLimitError("Rate limit exceeded")
            return _make_av_response()

        mock_provider.get_economic_indicator = AsyncMock(side_effect=_side_effect)
        poller._provider = mock_provider

        with patch("gateway.core.treasury_poller.asyncio.sleep", new_callable=AsyncMock):
            results = await poller._fetch_yields()

        assert len(results) == 1
        assert results[0]["maturity"] == "2year"
        # 10year should have been called MAX_RETRIES + 1 times then given up
        ten_year_calls = [
            c for c in mock_provider.get_economic_indicator.call_args_list if c.kwargs.get("maturity") == "10year"
        ]
        assert len(ten_year_calls) == _RATE_LIMIT_MAX_RETRIES + 1

    @pytest.mark.asyncio
    async def test_inter_request_delay(self, poller, mock_provider):
        """Verify delay is inserted between maturity fetches."""
        poller._provider = mock_provider

        sleep_calls = []

        async def _mock_sleep(seconds):
            sleep_calls.append(seconds)

        with patch("gateway.core.treasury_poller.asyncio.sleep", side_effect=_mock_sleep):
            await poller._fetch_yields()

        # Should have one inter-request delay between the two maturities
        pacing_sleeps = [s for s in sleep_calls if s == _INTER_REQUEST_DELAY_SECONDS]
        assert len(pacing_sleeps) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Publishing
# ─────────────────────────────────────────────────────────────────────────────


class TestPollAndPublish:
    @pytest.mark.asyncio
    async def test_publish_envelopes(self, poller, mock_provider, mock_sink_registry):
        poller._provider = mock_provider

        await poller._poll_and_publish(mock_sink_registry)

        # Should publish 2 envelopes (one per maturity)
        assert mock_sink_registry.publish_all.call_count == 2

        # Verify first call
        call_args = mock_sink_registry.publish_all.call_args_list[0]
        topic = call_args.args[0]
        envelope = call_args.args[1]

        assert topic == HEBER_STREAM
        assert envelope["provider"] == "alphavantage"
        assert envelope["feed"] == "treasury_yields"
        assert envelope["source"] == "rest"
        assert envelope["instrument_type"] == "macro"
        assert envelope["instrument_key"] == "macro:treasury_yield:10year"
        assert envelope["symbol"] == "TREASURY_10YEAR"
        assert envelope["payload"]["date"] == "2025-06-15"
        assert envelope["payload"]["maturity"] == "10year"
        assert envelope["payload"]["yield_pct"] == 4.25
        assert envelope["ts_event"] == "2025-06-15T00:00:00+00:00"

    @pytest.mark.asyncio
    async def test_publish_updates_telemetry(self, poller, mock_provider, mock_sink_registry):
        poller._provider = mock_provider

        assert poller._last_poll_time is None
        assert poller._last_poll_count == 0

        await poller._poll_and_publish(mock_sink_registry)

        assert poller._last_poll_time is not None
        assert poller._last_poll_count == 2

    @pytest.mark.asyncio
    async def test_publish_no_yields_skips(self, poller, mock_provider, mock_sink_registry):
        mock_provider.get_economic_indicator = AsyncMock(return_value=_make_empty_response())
        poller._provider = mock_provider

        await poller._poll_and_publish(mock_sink_registry)

        mock_sink_registry.publish_all.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_error_logged_not_raised(self, poller, mock_provider, mock_sink_registry):
        """A publish error for one maturity should not prevent the other."""
        poller._provider = mock_provider

        call_count = 0

        async def _publish_side_effect(topic, envelope):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Redis error")

        mock_sink_registry.publish_all = AsyncMock(side_effect=_publish_side_effect)

        # Should not raise
        await poller._poll_and_publish(mock_sink_registry)

        # Both calls were attempted
        assert mock_sink_registry.publish_all.call_count == 2


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle
# ─────────────────────────────────────────────────────────────────────────────


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_no_provider(self, poller):
        """Start should abort gracefully if no alphavantage provider."""
        mock_registry = MagicMock()
        mock_registry.get.return_value = None

        with patch("gateway.core.globals.get_registry", return_value=mock_registry):
            await poller.start()

        assert not poller._running
        assert poller._task is None

    @pytest.mark.asyncio
    async def test_start_no_api_key(self, poller):
        """Start should abort gracefully if API key is not configured."""
        provider = AsyncMock()
        provider.api_key_configured = False

        mock_registry = MagicMock()
        mock_registry.get.return_value = provider

        with patch("gateway.core.globals.get_registry", return_value=mock_registry):
            await poller.start()

        assert not poller._running
        assert poller._task is None

    @pytest.mark.asyncio
    async def test_start_and_stop(self, poller, mock_provider):
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_provider

        with patch("gateway.core.globals.get_registry", return_value=mock_registry):
            await poller.start()

        assert poller._running
        assert poller._task is not None

        await poller.stop()

        assert not poller._running

    @pytest.mark.asyncio
    async def test_double_start_is_noop(self, poller, mock_provider):
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_provider

        with patch("gateway.core.globals.get_registry", return_value=mock_registry):
            await poller.start()
            first_task = poller._task
            await poller.start()  # second start should be noop
            assert poller._task is first_task

        await poller.stop()

    def test_runtime_snapshot(self, poller):
        snapshot = poller.get_runtime_snapshot()
        assert snapshot["running"] is False
        assert snapshot["poll_interval_seconds"] == 86400
        assert snapshot["maturities"] == ["10year", "2year"]
        assert snapshot["premium_blocked_maturities"] == []
        assert snapshot["last_poll_time"] is None
        assert snapshot["last_poll_count"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────


class TestModuleSingleton:
    @pytest.mark.asyncio
    async def test_start_stop_singleton(self, mock_provider):
        """Test the module-level start/stop/get functions."""
        import gateway.core.treasury_poller as mod

        # Ensure clean state
        mod._treasury_poller = None

        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_provider

        with patch("gateway.core.globals.get_registry", return_value=mock_registry):
            p = await start_treasury_poller(maturities=["10year"], poll_interval_seconds=3600)

        assert get_treasury_poller() is p
        assert p._running

        await stop_treasury_poller()
        assert get_treasury_poller() is None

    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_provider):
        """Starting twice returns the same instance."""
        import gateway.core.treasury_poller as mod

        mod._treasury_poller = None

        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_provider

        with patch("gateway.core.globals.get_registry", return_value=mock_registry):
            p1 = await start_treasury_poller(maturities=["10year"])
            p2 = await start_treasury_poller(maturities=["2year"])

        assert p1 is p2

        await stop_treasury_poller()


# ─────────────────────────────────────────────────────────────────────────────
# Envelope integration
# ─────────────────────────────────────────────────────────────────────────────


class TestEnvelopeIntegration:
    def test_treasury_yields_feed_unique_fields(self):
        """Verify treasury_yields is registered in FEED_UNIQUE_FIELDS."""
        from gateway.core.envelope import FEED_UNIQUE_FIELDS

        assert "treasury_yields" in FEED_UNIQUE_FIELDS
        fields = FEED_UNIQUE_FIELDS["treasury_yields"]
        field_names = [f[0] for f in fields]
        assert "date" in field_names
        assert "maturity" in field_names
        assert "yield_pct" in field_names

    def test_wrap_event_for_treasury_yield(self):
        """Verify wrap_event produces a valid envelope for treasury data."""
        from gateway.core.envelope import wrap_event

        payload = {
            "date": "2025-06-15",
            "maturity": "10year",
            "yield_pct": 4.25,
        }

        envelope = wrap_event(
            event=payload,
            provider="alphavantage",
            feed="treasury_yields",
            source="rest",
            instrument_type_override="macro",
            instrument_key_override="macro:treasury_yield:10year",
            symbol_override="TREASURY_10YEAR",
        )

        assert envelope["provider"] == "alphavantage"
        assert envelope["feed"] == "treasury_yields"
        assert envelope["instrument_type"] == "macro"
        assert envelope["instrument_key"] == "macro:treasury_yield:10year"
        assert envelope["symbol"] == "TREASURY_10YEAR"
        assert envelope["payload"]["yield_pct"] == 4.25
        assert "event_id" in envelope
        assert len(envelope["event_id"]) == 32  # BLAKE2b 16-byte = 32 hex chars


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────


class TestConfig:
    def test_maturity_list_default(self):
        from gateway.config import Settings

        s = Settings(treasury_poller_maturities="2year,10year")
        assert s.treasury_poller_maturity_list == ["2year", "10year"]

    def test_maturity_list_custom(self):
        from gateway.config import Settings

        s = Settings(treasury_poller_maturities="3month,5year,30year")
        assert s.treasury_poller_maturity_list == ["3month", "5year", "30year"]

    def test_maturity_list_invalid_filtered(self):
        from gateway.config import Settings

        s = Settings(treasury_poller_maturities="10year,bogus,2year")
        assert s.treasury_poller_maturity_list == ["10year", "2year"]

    def test_maturity_list_empty_falls_back(self):
        from gateway.config import Settings

        s = Settings(treasury_poller_maturities="")
        assert s.treasury_poller_maturity_list == ["2year", "10year"]

    def test_maturity_list_all_invalid_falls_back(self):
        from gateway.config import Settings

        s = Settings(treasury_poller_maturities="bad,worse")
        assert s.treasury_poller_maturity_list == ["2year", "10year"]
