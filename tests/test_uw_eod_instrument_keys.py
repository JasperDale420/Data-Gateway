"""Tests for per-underlying analytics instrument-key correctness (BLOCKER 4).

`_infer_instrument_type` in `gateway.core.envelope` flags any payload with
``strike`` or ``expiry`` fields as ``instrument_type=option``. For
options-flow data this is correct, but for **per-underlying analytics
that happen to include an ``expiry`` field** (``iv_term_structure``,
``historic_option_volume``) it produces malformed ``option:{ticker}`` keys
(no OCC suffix). Heber's writer-side validator rejects these and 100% of
records drop on Bronze→Silver normalization.

Per-underlying pollers must pass ``instrument_type_override="equity"``
and ``instrument_key_override=f"equity:{ticker.upper()}"`` to
``wrap_event``. These tests pin that contract for every affected feed.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from gateway.core.uw_poller import UWPoller


class _RecordingPublishEnvelopes:
    """Captures envelopes passed to ``_publish_envelopes`` for assertion."""

    def __init__(self) -> None:
        self.captured: list[dict[str, Any]] = []

    async def __call__(self, **kwargs) -> tuple[int, int]:
        self.captured.extend(kwargs["envelopes"])
        return len(kwargs["envelopes"]), 0


@pytest.fixture
def recording_poller(monkeypatch):
    """A UWPoller with `_publish_envelopes` swapped for a recorder."""
    poller = UWPoller(eod_enabled=True)
    recorder = _RecordingPublishEnvelopes()
    monkeypatch.setattr(poller, "_publish_envelopes", recorder)
    poller._captured = recorder.captured  # type: ignore[attr-defined]
    return poller


@pytest.mark.asyncio
async def test_historic_option_volume_uses_equity_instrument_key(recording_poller):
    """Per-underlying historic_option_volume must emit equity:{TICKER}, not option:{ticker}.

    Without the override, ``_infer_instrument_type`` sees ``expiry`` in
    the row and tags every envelope as ``instrument_type=option`` with
    ``instrument_key=option:AAPL`` (malformed, no OCC suffix) -- Heber
    rejects 100% of rows.
    """
    recording_poller._provider = AsyncMock()
    recording_poller._provider.get_historic_option_volume = AsyncMock(
        return_value=[
            {
                "symbol": "AAPL",
                "timestamp": "2026-05-15T20:00:00Z",
                "date": "2026-05-15",
                "expiry": "2026-06-20",  # <-- triggers the option-type inference bug
                "volume": 12345,
                "open_interest": 5000,
                "call_volume": 7000,
                "put_volume": 5345,
                "premium": 250000.0,
            },
            {
                "symbol": "AAPL",
                "timestamp": "2026-05-15T20:00:00Z",
                "date": "2026-05-15",
                "expiry": "2026-07-18",
                "volume": 8000,
                "open_interest": 3000,
                "call_volume": 4500,
                "put_volume": 3500,
                "premium": 150000.0,
            },
        ]
    )

    published = await recording_poller._poll_eod_option_volume(sink_registry=None, ticker="aapl")

    assert published == 2
    assert len(recording_poller._captured) == 2  # type: ignore[attr-defined]

    for envelope in recording_poller._captured:  # type: ignore[attr-defined]
        assert envelope["instrument_type"] == "equity", (
            f"BLOCKER 4 regression: per-underlying historic_option_volume "
            f"got instrument_type={envelope['instrument_type']!r}, expected 'equity'. "
            "Heber rejects 'option' keys without an OCC suffix."
        )
        assert envelope["instrument_key"] == "equity:AAPL", (
            f"BLOCKER 4 regression: instrument_key={envelope['instrument_key']!r}, expected 'equity:AAPL'."
        )
        assert envelope["symbol"] == "AAPL"
        # The expiry field must still be preserved in the payload (it's how
        # Heber's downstream surface bucketed volume per expiry).
        assert envelope["payload"]["expiry"] in {"2026-06-20", "2026-07-18"}


@pytest.mark.asyncio
async def test_iv_term_structure_uses_equity_instrument_key(recording_poller):
    """Pre-existing fix for IV term structure -- pinned here too for the audit pass."""

    class _Row:
        def __init__(self, expiry: str) -> None:
            self._d = {
                "symbol": "AAPL",
                "expiry": expiry,
                "iv": 0.25,
                "days_to_expiry": 30,
                "call_iv": 0.26,
                "put_iv": 0.24,
                "provider": "unusual_whales",
            }

        def model_dump(self) -> dict[str, Any]:
            return dict(self._d)

    recording_poller._provider = AsyncMock()
    recording_poller._provider.get_iv_term_structure = AsyncMock(return_value=[_Row("2026-06-20"), _Row("2026-07-18")])

    await recording_poller._poll_eod_iv_term_structure(sink_registry=None, ticker="aapl")

    assert len(recording_poller._captured) == 2  # type: ignore[attr-defined]
    for envelope in recording_poller._captured:  # type: ignore[attr-defined]
        assert envelope["instrument_type"] == "equity"
        assert envelope["instrument_key"] == "equity:AAPL"
        assert envelope["symbol"] == "AAPL"


def _has_no_expiry_field(payload: dict[str, Any]) -> bool:
    """The wrap_event option-inference trigger is ``strike`` OR ``expiry``."""
    return "expiry" not in payload and "strike" not in payload


def test_per_underlying_uw_models_dont_carry_expiry_in_dump():
    """Per-underlying schemas (gex/iv_rank/oi_change/short_*/ftds) must not
    surface an ``expiry`` field in their ``model_dump()``.

    These pollers DON'T pass the equity override -- they rely on
    ``_infer_instrument_type`` defaulting to ``equity`` because the
    payload doesn't carry the option-triggering fields. If a schema is
    ever extended with ``expiry`` or ``strike``, this test fires and the
    poller must add the override.
    """
    # Use the minimum-required-field constructors. If a model gains an
    # ``expiry`` field in the future, attribute lookup on a fresh instance
    # would surface it in model_dump().
    from gateway.schemas import (  # noqa: PLC0415
        NormalizedFTD,
        NormalizedIVRank,
        NormalizedOIChange,
        NormalizedShortData,
    )

    iv_rank = NormalizedIVRank(symbol="AAPL", iv_rank=0.5, provider="unusual_whales")
    assert _has_no_expiry_field(iv_rank.model_dump()), (
        "NormalizedIVRank gained an expiry/strike field -- update _poll_eod_iv_rank with the equity override."
    )

    oi_change = NormalizedOIChange(
        symbol="AAPL",
        date="2026-05-15",
        call_oi=1000,
        put_oi=500,
        call_oi_change=10,
        put_oi_change=5,
        provider="unusual_whales",
    )
    assert _has_no_expiry_field(oi_change.model_dump()), (
        "NormalizedOIChange gained an expiry/strike field -- update _poll_eod_oi_change with the equity override."
    )

    short_data = NormalizedShortData(symbol="AAPL", date="2026-05-15", short_interest=1000, provider="unusual_whales")
    assert _has_no_expiry_field(short_data.model_dump()), (
        "NormalizedShortData gained an expiry/strike field -- "
        "update _poll_eod_short_interest/_short_volume with the equity override."
    )

    ftd = NormalizedFTD(symbol="AAPL", date="2026-05-15", quantity=1000, provider="unusual_whales")
    assert _has_no_expiry_field(ftd.model_dump()), (
        "NormalizedFTD gained an expiry/strike field -- update _poll_eod_ftds with the equity override."
    )
