"""Tests for gateway-strict schema validators.

Covers BLOCKER 1 (naive datetime rejection), BLOCKER 2 (price/strike/qty
positivity), and BLOCKER 3 (ResponseMeta explicit provider).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from gateway.schemas import (
    NormalizedBar,
    NormalizedDarkpoolTrade,
    NormalizedFlowAlert,
    NormalizedForexRate,
    NormalizedGreekExposure,
    NormalizedInsiderTrade,
    NormalizedMarketTide,
    NormalizedNetPremiumTick,
    NormalizedNewsArticle,
    NormalizedOptionContract,
    NormalizedPoliticianTrade,
    NormalizedQuote,
    NormalizedSectorTide,
    NormalizedTrade,
    ResponseMeta,
)
from gateway.schemas._validators import (
    NAIVE_DT_MSG,
    NEGATIVE_QTY_MSG,
    NON_POSITIVE_MSG,
)

UTC_TS = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
NAIVE_TS = datetime(2026, 1, 1, 0, 0, 0)


# ─────────────────────────────────────────────────────────────────────
# BLOCKER 1 — naive datetime rejection
# ─────────────────────────────────────────────────────────────────────


class TestNaiveDatetimeRejected:
    """Every datetime field on every gateway-strict schema rejects tzinfo=None."""

    def test_bar_naive_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            NormalizedBar(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                open=Decimal("1"),
                high=Decimal("2"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("10"),
            )
        assert NAIVE_DT_MSG in str(exc_info.value)

    def test_bar_aware_accepted(self) -> None:
        bar = NormalizedBar(
            symbol="AAPL",
            timestamp=UTC_TS,
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("10"),
        )
        assert bar.timestamp.tzinfo is not None

    def test_quote_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedQuote(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                bid_price=Decimal("1"),
                ask_price=Decimal("2"),
                bid_size=Decimal("100"),
                ask_size=Decimal("100"),
            )

    def test_trade_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedTrade(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                price=Decimal("1"),
                size=Decimal("1"),
            )

    def test_forex_rate_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedForexRate(
                pair="EUR/USD",
                timestamp=NAIVE_TS,
                bid=Decimal("1.05"),
                ask=Decimal("1.06"),
            )

    def test_flow_alert_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedFlowAlert(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                strike=Decimal("100"),
                expiry="2026-01-15",
                put_call="call",
                premium=Decimal("1"),
                volume=10,
                open_interest=10,
                side="ask",
            )

    def test_option_contract_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedOptionContract(
                contract_symbol="AAPL250117C00200000",
                underlying="AAPL",
                expiration="2025-01-17",
                strike=Decimal("200"),
                option_type="call",
                bid=Decimal("1"),
                ask=Decimal("2"),
                last=Decimal("1.5"),
                volume=10,
                open_interest=5,
                provider="alpaca",
                timestamp=NAIVE_TS,
            )

    def test_greek_exposure_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedGreekExposure(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                call_gamma=Decimal("0.1"),
            )

    def test_market_tide_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedMarketTide(
                timestamp=NAIVE_TS,
                net_call_premium=Decimal("100"),
                net_put_premium=Decimal("50"),
                net_volume=100,
            )

    def test_sector_tide_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedSectorTide(
                sector="Technology",
                timestamp=NAIVE_TS,
                net_call_premium=Decimal("100"),
                net_put_premium=Decimal("50"),
                net_volume=100,
            )

    def test_net_premium_tick_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedNetPremiumTick(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                net_call_premium=Decimal("100"),
                net_put_premium=Decimal("50"),
            )

    def test_darkpool_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedDarkpoolTrade(
                symbol="AAPL",
                timestamp=NAIVE_TS,
                price=Decimal("1"),
                size=10,
                premium=Decimal("10"),
            )

    def test_insider_trade_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedInsiderTrade(
                symbol="AAPL",
                filer_name="Doe, John",
                transaction_date=NAIVE_TS,
                transaction_type="P",
                shares=Decimal("100"),
                price=Decimal("1"),
                value=Decimal("100"),
            )

    def test_politician_trade_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedPoliticianTrade(
                symbol="AAPL",
                politician_name="Senator X",
                transaction_date=NAIVE_TS,
                transaction_type="purchase",
                amount_range="$1,001 - $15,000",
            )

    def test_news_naive_rejected(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            NormalizedNewsArticle(
                id="news-1",
                headline="Test",
                summary="Body",
                source="Reuters",
                url="https://example.com",
                published_at=NAIVE_TS,
                symbols=["AAPL"],
                provider="alpaca",
            )


# ─────────────────────────────────────────────────────────────────────
# BLOCKER 2 — positivity constraints
# ─────────────────────────────────────────────────────────────────────


class TestPositivityConstraints:
    def test_bar_negative_open_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedBar(
                symbol="AAPL",
                timestamp=UTC_TS,
                open=Decimal("-1"),
                high=Decimal("2"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("10"),
            )

    def test_bar_zero_open_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedBar(
                symbol="AAPL",
                timestamp=UTC_TS,
                open=Decimal("0"),
                high=Decimal("2"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("10"),
            )

    def test_bar_negative_volume_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedBar(
                symbol="AAPL",
                timestamp=UTC_TS,
                open=Decimal("1"),
                high=Decimal("2"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("-10"),
            )

    def test_bar_zero_volume_accepted(self) -> None:
        """volume=0 is legitimate (halted/low-liquidity bar)."""
        bar = NormalizedBar(
            symbol="AAPL",
            timestamp=UTC_TS,
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("0"),
        )
        assert bar.volume == Decimal("0")

    def test_trade_negative_price_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedTrade(
                symbol="AAPL",
                timestamp=UTC_TS,
                price=Decimal("-1"),
                size=Decimal("5"),
            )

    def test_trade_negative_size_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedTrade(
                symbol="AAPL",
                timestamp=UTC_TS,
                price=Decimal("1"),
                size=Decimal("-5"),
            )

    def test_quote_negative_bid_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedQuote(
                symbol="AAPL",
                timestamp=UTC_TS,
                bid_price=Decimal("-1"),
                ask_price=Decimal("1"),
                bid_size=Decimal("0"),
                ask_size=Decimal("0"),
            )

    def test_option_contract_zero_strike_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedOptionContract(
                contract_symbol="AAPL250117C00000000",
                underlying="AAPL",
                expiration="2025-01-17",
                strike=Decimal("0"),
                option_type="call",
                bid=Decimal("1"),
                ask=Decimal("2"),
                last=Decimal("1.5"),
                volume=10,
                open_interest=5,
                provider="alpaca",
                timestamp=UTC_TS,
            )

    def test_option_contract_negative_bid_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedOptionContract(
                contract_symbol="AAPL250117C00200000",
                underlying="AAPL",
                expiration="2025-01-17",
                strike=Decimal("200"),
                option_type="call",
                bid=Decimal("-1"),
                ask=Decimal("2"),
                last=Decimal("1.5"),
                volume=10,
                open_interest=5,
                provider="alpaca",
                timestamp=UTC_TS,
            )

    def test_option_contract_negative_volume_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedOptionContract(
                contract_symbol="AAPL250117C00200000",
                underlying="AAPL",
                expiration="2025-01-17",
                strike=Decimal("200"),
                option_type="call",
                bid=Decimal("1"),
                ask=Decimal("2"),
                last=Decimal("1.5"),
                volume=-1,
                open_interest=5,
                provider="alpaca",
                timestamp=UTC_TS,
            )

    def test_option_contract_negative_oi_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedOptionContract(
                contract_symbol="AAPL250117C00200000",
                underlying="AAPL",
                expiration="2025-01-17",
                strike=Decimal("200"),
                option_type="call",
                bid=Decimal("1"),
                ask=Decimal("2"),
                last=Decimal("1.5"),
                volume=10,
                open_interest=-2,
                provider="alpaca",
                timestamp=UTC_TS,
            )

    def test_flow_alert_zero_strike_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedFlowAlert(
                symbol="AAPL",
                timestamp=UTC_TS,
                strike=Decimal("0"),
                expiry="2026-01-15",
                put_call="call",
                premium=Decimal("1"),
                volume=10,
                open_interest=10,
                side="ask",
            )

    def test_flow_alert_negative_volume_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NEGATIVE_QTY_MSG):
            NormalizedFlowAlert(
                symbol="AAPL",
                timestamp=UTC_TS,
                strike=Decimal("100"),
                expiry="2026-01-15",
                put_call="call",
                premium=Decimal("1"),
                volume=-10,
                open_interest=10,
                side="ask",
            )

    def test_greek_exposure_negative_strike_rejected(self) -> None:
        """`strike` is optional on NormalizedGreekExposure but must still be
        `> 0` when present -- untested branch of `_positive_strike`."""
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedGreekExposure(
                symbol="AAPL",
                timestamp=UTC_TS,
                call_gamma=Decimal("0.1"),
                strike=Decimal("-1"),
            )

    def test_greek_exposure_zero_strike_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedGreekExposure(
                symbol="AAPL",
                timestamp=UTC_TS,
                call_gamma=Decimal("0.1"),
                strike=Decimal("0"),
            )

    def test_greek_exposure_without_strike_accepted(self) -> None:
        """Aggregate (non-per-contract) greek exposure rows omit strike entirely."""
        row = NormalizedGreekExposure(
            symbol="AAPL",
            timestamp=UTC_TS,
            call_gamma=Decimal("0.1"),
        )
        assert row.strike is None

    def test_forex_rate_negative_bid_rejected(self) -> None:
        """NormalizedForexRate requires bid/ask/mid/OHLC to be `> 0` -- untested branch."""
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedForexRate(
                pair="EUR/USD",
                timestamp=UTC_TS,
                bid=Decimal("-1.05"),
                ask=Decimal("1.06"),
            )

    def test_forex_rate_zero_ask_rejected(self) -> None:
        with pytest.raises(ValidationError, match=NON_POSITIVE_MSG):
            NormalizedForexRate(
                pair="EUR/USD",
                timestamp=UTC_TS,
                bid=Decimal("1.05"),
                ask=Decimal("0"),
            )

    def test_forex_rate_positive_values_accepted(self) -> None:
        rate = NormalizedForexRate(
            pair="EUR/USD",
            timestamp=UTC_TS,
            bid=Decimal("1.05"),
            ask=Decimal("1.06"),
        )
        assert rate.bid == Decimal("1.05")
        assert rate.ask == Decimal("1.06")


# ─────────────────────────────────────────────────────────────────────
# BLOCKER 3 — ResponseMeta requires explicit provider
# ─────────────────────────────────────────────────────────────────────


class TestResponseMetaProviderRequired:
    def test_missing_provider_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            ResponseMeta()
        # The missing field must be `provider` (not silently defaulted to "alpaca")
        assert "provider" in str(exc_info.value).lower()

    def test_explicit_provider_accepted(self) -> None:
        meta = ResponseMeta(provider="finnhub")
        assert meta.provider == "finnhub"
        # And confirm there is no default-Alpaca silent override
        assert meta.provider != "alpaca"

    def test_alpaca_provider_still_accepted_explicitly(self) -> None:
        meta = ResponseMeta(provider="alpaca", symbol="AAPL", count=10)
        assert meta.provider == "alpaca"
        assert meta.symbol == "AAPL"
        assert meta.count == 10
