"""Tests for AlpacaCorporateMixin._parse_corporate_actions (BLOCKER 4).

The shared `NormalizedCorporateAction` schema requires `ex_date`, but
several Alpaca branches do not carry a vendor-supplied `ex_date` field.
Each branch must derive the closest semantic substitute or synthesize
today's UTC date with a structured warning.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from gateway.providers.alpaca.corporate import AlpacaCorporateMixin


@pytest.fixture
def parser() -> AlpacaCorporateMixin:
    return AlpacaCorporateMixin()


def _action_types(parsed: list[Any]) -> list[str]:
    return [a.action_type for a in parsed]


def _exact_ex_dates(parsed: list[Any]) -> list[str]:
    return [a.ex_date for a in parsed]


# ─────────────────────────────────────────────────────────────────────
# Branches with vendor `ex_date` — passes through unchanged
# ─────────────────────────────────────────────────────────────────────


class TestVendorExDate:
    def test_reverse_split_passthrough(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "reverse_splits": [
                    {
                        "id": "rs1",
                        "symbol": "AAPL",
                        "ex_date": "2026-02-01",
                        "process_date": "2026-02-02",
                        "new_rate": 1,
                        "old_rate": 4,
                    }
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-02-01"]

    def test_cash_dividend_passthrough(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "cash_dividends": [
                    {"symbol": "AAPL", "ex_date": "2026-02-10", "rate": "0.25"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-02-10"]

    def test_spin_off_passthrough(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "spin_offs": [
                    {"source_symbol": "AAPL", "ex_date": "2026-03-01"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-03-01"]


# ─────────────────────────────────────────────────────────────────────
# Branches that must DERIVE ex_date from another vendor field
# ─────────────────────────────────────────────────────────────────────


class TestDerivedExDate:
    def test_unit_split_uses_effective_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "unit_splits": [
                    {
                        "id": "us1",
                        "old_symbol": "AAPL",
                        "effective_date": "2026-04-01",
                        "process_date": "2026-04-02",
                        "payable_date": "2026-04-03",
                        "new_rate": 1,
                        "old_rate": 1,
                    }
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-04-01"]
        assert _action_types(out) == ["unit_split"]

    def test_cash_merger_uses_effective_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "cash_mergers": [
                    {
                        "acquiree_symbol": "AAPL",
                        "effective_date": "2026-05-15",
                        "process_date": "2026-05-16",
                        "rate": "100",
                    }
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-05-15"]

    def test_stock_merger_uses_effective_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "stock_mergers": [
                    {"acquiree_symbol": "AAPL", "effective_date": "2026-06-01"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-06-01"]

    def test_stock_and_cash_merger_uses_effective_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "stock_and_cash_mergers": [
                    {"acquiree_symbol": "AAPL", "effective_date": "2026-07-01"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-07-01"]

    def test_redemption_uses_process_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "redemptions": [
                    {"symbol": "XYZ", "process_date": "2026-08-01", "payable_date": "2026-08-05"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-08-01"]

    def test_name_change_uses_process_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "name_changes": [
                    {"old_symbol": "OLD", "new_symbol": "NEW", "process_date": "2026-09-01"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-09-01"]

    def test_worthless_removal_uses_process_date(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "worthless_removals": [
                    {"symbol": "DEAD", "process_date": "2026-10-01"},
                ]
            }
        )
        assert _exact_ex_dates(out) == ["2026-10-01"]


# ─────────────────────────────────────────────────────────────────────
# Synthesized ex_date — last-resort fallback to today's UTC date
# ─────────────────────────────────────────────────────────────────────


class TestSynthesizedExDate:
    def test_missing_all_dates_falls_back_to_today(self, parser: AlpacaCorporateMixin) -> None:
        out = parser._parse_corporate_actions(
            {
                "name_changes": [
                    {"old_symbol": "X", "new_symbol": "Y"},  # no dates at all
                ]
            }
        )
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        assert _exact_ex_dates(out) == [today]


# ─────────────────────────────────────────────────────────────────────
# Full sweep — every action_type round-trips with a non-empty ex_date
# even when the vendor payload is minimal.
# ─────────────────────────────────────────────────────────────────────


class TestAllBranchesProduceExDate:
    def test_full_sweep(self, parser: AlpacaCorporateMixin) -> None:
        payload: dict[str, Any] = {
            "reverse_splits": [{"symbol": "A", "process_date": "2026-01-01"}],
            "forward_splits": [{"symbol": "B", "process_date": "2026-01-02"}],
            "unit_splits": [{"old_symbol": "C", "effective_date": "2026-01-03"}],
            "cash_dividends": [{"symbol": "D", "record_date": "2026-01-04"}],
            "stock_dividends": [{"symbol": "E", "record_date": "2026-01-05"}],
            "cash_mergers": [{"acquiree_symbol": "F", "effective_date": "2026-01-06"}],
            "stock_mergers": [{"acquiree_symbol": "G", "effective_date": "2026-01-07"}],
            "stock_and_cash_mergers": [{"acquiree_symbol": "H", "effective_date": "2026-01-08"}],
            "redemptions": [{"symbol": "I", "process_date": "2026-01-09"}],
            "spin_offs": [{"source_symbol": "J", "process_date": "2026-01-10"}],
            "rights_distributions": [{"source_symbol": "K", "process_date": "2026-01-11"}],
            "name_changes": [{"new_symbol": "L", "process_date": "2026-01-12"}],
            "worthless_removals": [{"symbol": "M", "process_date": "2026-01-13"}],
        }
        out = parser._parse_corporate_actions(payload)
        assert len(out) == 13
        # No bare `None`/empty strings
        assert all(a.ex_date for a in out), [a.ex_date for a in out]
        assert all(isinstance(a.ex_date, str) for a in out)
