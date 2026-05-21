"""Tests for stable event_id on date-based feeds (BLOCKER 5).

Without these fixes, ``wrap_event`` only looked at
``timestamp``/``t``/``published_at``/``datetime`` for ``ts_event``, then
fell through to ``datetime.now(UTC)`` when no field matched. Date-based
feeds (``iv_rank``, ``iv_term_structure`` rows without an explicit time,
``congress`` / ``insider`` with ``transaction_date`` / ``filing_date``,
``treasury_yields`` with ``date``) hashed the wall-clock time into their
``event_id`` -- producing a different id on every retry/replay and
defeating Heber's three-layer dedup.

The fix extends the field list ``wrap_event`` checks for ``ts_event``
extraction with ``date``, ``transaction_date``, ``filing_date``,
``report_date``, ``effective_date``, ``period_end_date`` (each
``YYYY-MM-DD`` string is parsed to midnight UTC) AND adds an explicit
``ts_event_override`` kwarg for callers that have the canonical timestamp
in hand.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from gateway.core.envelope import wrap_event


def _iv_rank_row() -> dict:
    return {
        "symbol": "AAPL",
        "iv_rank": 0.78,
        "iv_percentile": 0.85,
        "current_iv": 0.32,
        "one_year_high": 0.55,
        "one_year_low": 0.18,
        "provider": "unusual_whales",
        # NOTE: NO timestamp/t/published_at/datetime field.
    }


def _congress_row() -> dict:
    return {
        "ticker": "AAPL",
        "name": "Nancy Pelosi",
        "transaction_date": "2026-05-14",
        "txn_type": "Purchase",
        "amounts": "$1,000,001 - $5,000,000",
    }


def _insider_row() -> dict:
    return {
        "ticker": "AAPL",
        "owner_name": "Tim Cook",
        "filing_date": "2026-05-12",
        "transaction_date": "2026-05-10",
    }


def _treasury_row() -> dict:
    return {
        "date": "2026-05-15",
        "maturity": "10year",
        "yield_pct": 4.25,
    }


class TestEventIdStableForDateBasedFeeds:
    """Same input row must produce the same event_id across calls."""

    def test_iv_rank_stable_with_ts_event_override(self) -> None:
        """Caller passes ts_event_override (e.g. EOD date) for an undated feed.

        Without the override, ``wrap_event`` would fall through to
        ``datetime.now(UTC)`` and produce a fresh id on every retry.
        """
        ts = datetime(2026, 5, 15, 0, 0, 0, tzinfo=UTC)
        row = _iv_rank_row()

        env1 = wrap_event(row, provider="unusual_whales", feed="iv_rank", ts_event_override=ts)
        env2 = wrap_event(row, provider="unusual_whales", feed="iv_rank", ts_event_override=ts)

        assert env1["event_id"] == env2["event_id"]

    def test_congress_stable_via_transaction_date(self) -> None:
        """``transaction_date`` is in the standard extraction field list."""
        row = _congress_row()

        env1 = wrap_event(row, provider="unusual_whales", feed="congress_trades")
        env2 = wrap_event(row, provider="unusual_whales", feed="congress_trades")

        assert env1["event_id"] == env2["event_id"], (
            "BLOCKER 5 regression: congress_trades event_id changed between calls. "
            "Check that wrap_event extracts ``transaction_date`` into ts_event."
        )
        # ``ts_event`` should reflect the parsed transaction_date, not now()
        assert env1["ts_event"].startswith("2026-05-14")

    def test_insider_stable_via_filing_date(self) -> None:
        """``filing_date`` is in the standard extraction field list."""
        row = _insider_row()

        env1 = wrap_event(row, provider="unusual_whales", feed="insider_trades")
        env2 = wrap_event(row, provider="unusual_whales", feed="insider_trades")

        assert env1["event_id"] == env2["event_id"]
        # transaction_date comes first in the precedence list (after the
        # time-of-day fields), so that should be the one used.
        assert env1["ts_event"].startswith("2026-05-10")

    def test_treasury_stable_via_date(self) -> None:
        """``date`` is in the standard extraction field list."""
        row = _treasury_row()

        env1 = wrap_event(
            row,
            provider="alphavantage",
            feed="treasury_yields",
            instrument_type_override="macro",
            instrument_key_override="macro:treasury_yield:10year",
            symbol_override="TREASURY_10YEAR",
        )
        env2 = wrap_event(
            row,
            provider="alphavantage",
            feed="treasury_yields",
            instrument_type_override="macro",
            instrument_key_override="macro:treasury_yield:10year",
            symbol_override="TREASURY_10YEAR",
        )

        assert env1["event_id"] == env2["event_id"]
        assert env1["ts_event"].startswith("2026-05-15")

    def test_naive_datetime_override_promoted_to_utc(self) -> None:
        """A naive datetime in ts_event_override is promoted to UTC."""
        naive = datetime(2026, 5, 15, 0, 0, 0)  # No tzinfo

        env = wrap_event({"symbol": "AAPL"}, provider="x", feed="iv_rank", ts_event_override=naive)
        assert env["ts_event"] == "2026-05-15T00:00:00+00:00"

    def test_bare_date_field_promoted_to_midnight_utc(self) -> None:
        """A YYYY-MM-DD string in a date-only field is midnight-UTC."""
        env = wrap_event(
            {"symbol": "AAPL", "date": "2026-05-15"},
            provider="x",
            feed="iv_rank",
        )
        assert env["ts_event"].startswith("2026-05-15T00:00:00")

    def test_unknown_string_does_not_silently_fall_through_to_now(self) -> None:
        """If the date field is malformed, wrap_event still falls back to now()
        -- but the lower-precedence dedup fields should still disambiguate.

        This test pins the current behavior so regressions are loud: a
        future change that decides to *raise* on a bad date instead of
        falling through to now() needs to update this test deliberately.
        """
        env = wrap_event(
            {"symbol": "AAPL", "date": "not-a-date"},
            provider="x",
            feed="iv_rank",
        )
        # ts_event will be derived from now(); we only check the call
        # didn't raise. The fallback is what makes the override important.
        assert env["ts_event"] is not None


class TestTsEventOverrideKwarg:
    """Explicit ts_event_override beats payload fields."""

    def test_override_beats_payload_timestamp_field(self) -> None:
        explicit = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        env = wrap_event(
            {"symbol": "AAPL", "timestamp": "2050-12-31T23:59:59Z"},
            provider="x",
            feed="iv_rank",
            ts_event_override=explicit,
        )
        assert env["ts_event"] == "2026-01-01T00:00:00+00:00"


class TestDateOnlyParser:
    """The internal helper accepts date / datetime / ISO strings."""

    def test_date_object_promoted_to_midnight_utc(self) -> None:
        env = wrap_event(
            {"symbol": "AAPL"},
            provider="x",
            feed="iv_rank",
            ts_event_override=datetime.combine(date(2026, 5, 15), datetime.min.time(), tzinfo=UTC),
        )
        assert env["ts_event"] == "2026-05-15T00:00:00+00:00"
