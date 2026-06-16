"""Tests for `UWMarketMixin.get_realized_volatility` (BLOCKER 6).

The previous wiring called `stock.get_candles.sync` and tried to read
`realized_vol_30d/60d/90d` straight off candle rows. That produced an
empty `NormalizedVolatilityStats` because candles have no such fields.

The fix routes through the dedicated UW endpoint
`get_realized_volatility.sync`, which returns a daily series of
`{date, price, realized_volatility, implied_volatility}`. When the
vendor exposes pre-aggregated 30d/60d/90d windows the gateway uses
those directly; otherwise it falls back to a client-side close-to-close
log-return computation so the schema slots are never silently empty.
"""

from __future__ import annotations

import math
import sys
import types
from decimal import Decimal
from typing import Any

import pytest

from gateway.providers.uw import UnusualWhalesProvider
from gateway.providers.uw.market import _compute_realized_vol

# ─────────────────────────────────────────────────────────────────────
# Realized-vol math helper
# ─────────────────────────────────────────────────────────────────────


class TestComputeRealizedVol:
    def test_returns_none_when_window_too_long(self) -> None:
        assert _compute_realized_vol([100.0, 101.0], 30) is None

    def test_zero_or_negative_price_yields_none(self) -> None:
        prices = [100.0] * 30 + [0.0, 101.0]
        assert _compute_realized_vol(prices, 30) is None

    def test_known_series_matches_manual_calculation(self) -> None:
        # Deterministic series: alternate +1% / -1% close-to-close moves.
        prices = [100.0]
        for i in range(40):
            prices.append(prices[-1] * (1.01 if i % 2 == 0 else 1 / 1.01))
        result = _compute_realized_vol(prices, 30)
        assert result is not None
        # Manual: log returns ~ +/-0.00995, std ~ 0.00995, annualised by sqrt(252).
        expected = 0.00995 * math.sqrt(252)
        assert abs(float(result) - expected) < 5e-3


# ─────────────────────────────────────────────────────────────────────
# get_realized_volatility — endpoint dispatch + parsing
# ─────────────────────────────────────────────────────────────────────


def _install_fake_endpoint(monkeypatch: pytest.MonkeyPatch, response: Any) -> dict[str, Any]:
    """Stub `unusualwhales.api.stock.get_realized_volatility.sync`."""
    captured: dict[str, Any] = {}

    def _fake_sync(*, client: Any, ticker: str, date: str) -> Any:  # noqa: ANN401
        captured["client"] = client
        captured["ticker"] = ticker
        captured["date"] = date
        return response

    # `unusualwhales.api.stock.get_realized_volatility` is imported lazily
    # inside the method, so we materialise the module hierarchy first.
    pkg_api_stock = sys.modules.setdefault("unusualwhales.api.stock", types.ModuleType("unusualwhales.api.stock"))
    fake_module = types.ModuleType("unusualwhales.api.stock.get_realized_volatility")
    fake_module.sync = _fake_sync  # type: ignore[attr-defined]
    monkeypatch.setattr(pkg_api_stock, "get_realized_volatility", fake_module, raising=False)
    monkeypatch.setitem(sys.modules, "unusualwhales.api.stock.get_realized_volatility", fake_module)
    return captured


@pytest.mark.asyncio
async def test_returns_none_when_endpoint_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = UnusualWhalesProvider()
    provider._client = object()  # truthy sentinel
    _install_fake_endpoint(monkeypatch, None)
    out = await provider.get_realized_volatility("AAPL", date="2026-02-01")
    assert out is None


@pytest.mark.asyncio
async def test_returns_none_on_error_message_response(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = UnusualWhalesProvider()
    provider._client = object()

    class _ErrorMessage:
        message = "Not found"

    _install_fake_endpoint(monkeypatch, _ErrorMessage())
    out = await provider.get_realized_volatility("AAPL", date="2026-02-01")
    assert out is None


@pytest.mark.asyncio
async def test_computes_rv_from_price_series_when_vendor_omits_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pure-price series produces non-empty 30d realized vol slots."""
    provider = UnusualWhalesProvider()
    provider._client = object()

    rows = [{"date": f"2026-01-{i:02d}", "price": 100.0 + i} for i in range(1, 95)]
    captured = _install_fake_endpoint(monkeypatch, rows)

    out = await provider.get_realized_volatility("AAPL")
    assert out is not None
    assert out.symbol == "AAPL"
    assert out.provider == "unusual_whales"
    assert out.realized_vol_30d is not None
    assert out.realized_vol_60d is not None
    assert out.realized_vol_90d is not None
    # date default is today's UTC date
    assert captured["ticker"] == "AAPL"


@pytest.mark.asyncio
async def test_prefers_vendor_pre_aggregated_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the vendor emits realized_vol_30d on the latest row, use it directly."""
    provider = UnusualWhalesProvider()
    provider._client = object()

    rows = [{"date": f"2026-01-{i:02d}", "price": 100.0} for i in range(1, 95)]
    rows[-1]["realized_vol_30d"] = "0.25"
    _install_fake_endpoint(monkeypatch, rows)

    out = await provider.get_realized_volatility("AAPL")
    assert out is not None
    assert out.realized_vol_30d == Decimal("0.25")


@pytest.mark.asyncio
async def test_calls_endpoint_with_uppercased_ticker(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = UnusualWhalesProvider()
    provider._client = object()
    captured = _install_fake_endpoint(monkeypatch, [])
    await provider.get_realized_volatility("aapl", date="2026-02-01")
    assert captured["ticker"] == "AAPL"
    assert captured["date"] == "2026-02-01"
