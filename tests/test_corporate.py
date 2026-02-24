"""Unit tests for Corporate Actions and Adjustment Factors."""

from datetime import date
from decimal import Decimal

import pytest

from gateway.core.adjustments import (
    AdjustmentFactor,
    AdjustmentService,
    get_adjustment_service,
)
from gateway.core.corporate_actions import (
    ActionType,
    CorporateAction,
    CorporateActionsService,
    get_corporate_actions_service,
)


@pytest.fixture(autouse=True)
def _enable_stub_data(monkeypatch):
    """Corporate unit tests use built-in stub fixtures."""

    class _Settings:
        allow_stub_data = True

    monkeypatch.setattr("gateway.core.corporate_actions.get_settings", lambda: _Settings())


class TestCorporateAction:
    """Test CorporateAction dataclass."""

    def test_split_action_to_dict(self):
        action = CorporateAction(
            type=ActionType.SPLIT,
            ex_date=date(2020, 8, 31),
            record_date=date(2020, 8, 24),
            ratio=4.0,
            description="4-for-1 stock split",
        )
        d = action.to_dict()
        assert d["type"] == "split"
        assert d["ex_date"] == "2020-08-31"
        assert d["ratio"] == 4.0

    def test_dividend_action_to_dict(self):
        action = CorporateAction(
            type=ActionType.DIVIDEND,
            ex_date=date(2024, 2, 9),
            pay_date=date(2024, 2, 15),
            amount=Decimal("0.24"),
            currency="USD",
            frequency="quarterly",
        )
        d = action.to_dict()
        assert d["type"] == "dividend"
        assert d["amount"] == 0.24
        assert d["frequency"] == "quarterly"


class TestCorporateActionsService:
    """Test CorporateActionsService."""

    @pytest.mark.asyncio
    async def test_get_actions_aapl(self):
        service = CorporateActionsService()
        actions = await service.get_actions(
            "AAPL",
            start=date(2020, 1, 1),
            end=date(2024, 12, 31),
        )
        # Should include known AAPL split
        assert len(actions) >= 1
        split = next((a for a in actions if a.type == ActionType.SPLIT), None)
        assert split is not None
        assert split.ratio == 4.0

    @pytest.mark.asyncio
    async def test_get_splits(self):
        service = CorporateActionsService()
        splits = await service.get_splits(
            "TSLA",
            start=date(2020, 1, 1),
            end=date(2024, 12, 31),
        )
        assert all(s.type == ActionType.SPLIT for s in splits)

    @pytest.mark.asyncio
    async def test_get_actions_date_range_filter(self):
        service = CorporateActionsService()
        # Query range that excludes known splits
        actions = await service.get_actions(
            "AAPL",
            start=date(2021, 1, 1),
            end=date(2021, 12, 31),
        )
        # 2020 AAPL split should not be included
        assert len(actions) == 0

    def test_singleton(self):
        s1 = get_corporate_actions_service()
        s2 = get_corporate_actions_service()
        assert s1 is s2


class TestAdjustmentFactor:
    """Test AdjustmentFactor dataclass."""

    def test_to_dict(self):
        factor = AdjustmentFactor(
            date=date(2020, 8, 31),
            cumulative_factor=Decimal("0.25"),
            split_factor=Decimal("0.25"),
            dividend_factor=Decimal("1.0"),
            event="4:1 split",
        )
        d = factor.to_dict()
        assert d["cumulative_factor"] == 0.25
        assert d["event"] == "4:1 split"


class TestAdjustmentService:
    """Test AdjustmentService."""

    @pytest.mark.asyncio
    async def test_get_factors(self):
        service = AdjustmentService()
        factors = await service.get_factors(
            "AAPL",
            start=date(2020, 1, 1),
            end=date(2024, 12, 31),
        )
        # Should have initial factor + per-action factors
        assert len(factors) >= 1

    def test_compute_factors_from_split(self):
        service = AdjustmentService()
        actions = [
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2020, 8, 31),
                ratio=4.0,
                description="4:1 split",
            ),
        ]
        factors = service.compute_factors_from_actions(
            actions,
            start=date(2020, 1, 1),
            end=date(2024, 12, 31),
        )
        # 4:1 split means factor of 0.25
        split_factor = next((f for f in factors if f.event and "split" in f.event.lower()), None)
        assert split_factor is not None
        assert split_factor.cumulative_factor == Decimal("0.25")

    def test_apply_adjustment(self):
        service = AdjustmentService()
        factors = [
            AdjustmentFactor(
                date=date(2020, 1, 1),
                cumulative_factor=Decimal("1.0"),
                split_factor=Decimal("1.0"),
                dividend_factor=Decimal("1.0"),
            ),
            AdjustmentFactor(
                date=date(2020, 8, 31),
                cumulative_factor=Decimal("0.25"),
                split_factor=Decimal("0.25"),
                dividend_factor=Decimal("1.0"),
            ),
        ]

        # Before split - no adjustment
        price = service.apply_adjustment(Decimal("400"), date(2020, 7, 1), factors)
        assert price == Decimal("400")

        # After split - factor applied
        price = service.apply_adjustment(Decimal("400"), date(2020, 9, 1), factors)
        assert price == Decimal("100")  # 400 * 0.25

    def test_adjust_bars(self):
        service = AdjustmentService()
        bars = [
            {"timestamp": "2020-07-01", "close": 400, "volume": 1000},
            {"timestamp": "2020-09-01", "close": 100, "volume": 4000},
        ]
        factors = [
            AdjustmentFactor(
                date=date(2020, 1, 1),
                cumulative_factor=Decimal("1.0"),
                split_factor=Decimal("1.0"),
                dividend_factor=Decimal("1.0"),
            ),
            AdjustmentFactor(
                date=date(2020, 8, 31),
                cumulative_factor=Decimal("0.25"),
                split_factor=Decimal("0.25"),
                dividend_factor=Decimal("1.0"),
            ),
        ]

        adjusted = service.adjust_bars(bars, factors)
        assert adjusted[0]["adjusted"] is True
        assert adjusted[1]["adjusted"] is True

    def test_singleton(self):
        s1 = get_adjustment_service()
        s2 = get_adjustment_service()
        assert s1 is s2
