"""Event-id uniqueness for UnusualWhales company financial statements.

A single call to /api/stock/{ticker}/{balance-sheets,income-statements,cash-flows}
returns ~102 rows — one per fiscal period. Each row must hash to a distinct
event_id or Heber's dedup collapses the whole history into one Bronze/Silver row
(the failure mode fixed earlier for oi_change and insider_trades). The row's
period identity therefore has to be in FEED_UNIQUE_FIELDS.
"""

from __future__ import annotations

from gateway.core.envelope import wrap_event

FEEDS = ("income_statement", "balance_sheet", "cash_flow")


def _row(fiscal_date_ending: str, report_type: str = "quarterly") -> dict:
    # Real UW financial-statement row shape (verified 2026-07-03): the period is
    # keyed by fiscal_date_ending + report_type; the ticker is `ticker`.
    return {
        "ticker": "AAPL",
        "fiscal_date_ending": fiscal_date_ending,
        "report_type": report_type,
        "total_revenue": "111184000000",
        "total_assets": "371082000000",
    }


def _wrap(feed: str, row: dict) -> dict:
    return wrap_event(
        event=row,
        provider="unusual_whales",
        feed=feed,
        source="rest",
        symbol_override="AAPL",
        instrument_type_override="equity",
        instrument_key_override="equity:AAPL",
    )


def test_distinct_fiscal_periods_get_distinct_event_ids() -> None:
    for feed in FEEDS:
        periods = ["2024-12-31", "2024-09-30", "2024-06-30", "2005-03-31"]
        ids = {_wrap(feed, _row(p))["event_id"] for p in periods}
        assert len(ids) == len(periods), f"{feed}: fiscal periods collapsed to {len(ids)} ids"


def test_same_ticker_annual_and_quarterly_do_not_collapse() -> None:
    for feed in FEEDS:
        q = _wrap(feed, _row("2024-12-31", report_type="quarterly"))["event_id"]
        a = _wrap(feed, _row("2024-12-31", report_type="annual"))["event_id"]
        assert q != a, f"{feed}: annual and quarterly for same period collapsed"


def test_same_period_is_stable_across_calls() -> None:
    # fiscal_date_ending is in wrap_event's ts_event field chain, so ts_event is
    # derived from the row (not now()) and the event_id is stable across re-fetches.
    for feed in FEEDS:
        first = _wrap(feed, _row("2024-12-31"))["event_id"]
        second = _wrap(feed, _row("2024-12-31"))["event_id"]
        assert first == second, f"{feed}: event_id not stable across calls"
