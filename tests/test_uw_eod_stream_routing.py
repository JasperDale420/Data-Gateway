"""EOD publishes must route to the backfill stream, not the live firehose.

The 16:30 ET EOD run publishes a ~300k+ event burst. On the live
``heber:events`` stream (MAXLEN ~500k) that burst competes with the
market-hours firehose: when the live consumer is behind, the oldest
unread entries are MAXLEN-evicted — on 2026-07-20 this permanently lost
oi_change, iv_rank, iv_term_structure and historic_option_volume for the
day and truncated greek_exposure. The dedicated backfill stream exists
precisely to isolate bulk re-runnable data from live events, and EOD
snapshots are re-runnable by date. These tests pin every EOD poll to the
backfill topic and the default publish path to the live topic.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from gateway.core.redis_sink import BACKFILL_STREAM_TOPIC
from gateway.core.uw_poller import HEBER_STREAM, UWPoller


class _FakeModel:
    """Minimal stand-in for a provider pydantic model."""

    def __init__(self, **fields: Any) -> None:
        self._fields = fields

    def model_dump(self) -> dict[str, Any]:
        return dict(self._fields)


class _RecordingPublish:
    """Captures every ``_publish_envelopes`` call's kwargs."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, **kwargs: Any) -> tuple[int, int]:
        self.calls.append(kwargs)
        return len(kwargs["envelopes"]), 0


def _indexed_sink() -> AsyncMock:
    sink = AsyncMock()
    sink.publish_all_batch_indexed = AsyncMock(side_effect=lambda msgs: set(range(len(msgs))))
    return sink


async def test_publish_envelopes_defaults_to_live_stream():
    """Without an explicit topic, envelopes go to the live heber:events stream."""
    poller = UWPoller()
    poller._redis_dedupe = None
    sink = _indexed_sink()

    await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=[{"event_id": "e1", "feed": "flow_alerts"}],
        dedupe_prefix="uw:test",
        missing_event_log="uw_test_missing_event_id",
    )

    (msgs,), _kw = sink.publish_all_batch_indexed.call_args
    assert [topic for topic, _ in msgs] == [HEBER_STREAM]


async def test_publish_envelopes_honors_topic_override():
    """An explicit topic routes every message to that stream."""
    poller = UWPoller()
    poller._redis_dedupe = None
    sink = _indexed_sink()

    await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=[{"event_id": "e1", "feed": "oi_change"}, {"event_id": "e2", "feed": "oi_change"}],
        dedupe_prefix="uw:test",
        missing_event_log="uw_test_missing_event_id",
        topic=BACKFILL_STREAM_TOPIC,
    )

    (msgs,), _kw = sink.publish_all_batch_indexed.call_args
    assert [topic for topic, _ in msgs] == [BACKFILL_STREAM_TOPIC, BACKFILL_STREAM_TOPIC]


# (method, fetch result, takes a ticker argument)
EOD_POLLS = [
    ("_poll_eod_greek_exposure", [_FakeModel(symbol="AAPL")], True),
    ("_poll_eod_iv_rank", _FakeModel(symbol="AAPL"), True),
    ("_poll_eod_iv_term_structure", [_FakeModel(symbol="AAPL", expiry="2026-08-21")], True),
    ("_poll_eod_oi_change", [_FakeModel(symbol="AAPL")], True),
    ("_poll_eod_option_volume", [{"symbol": "AAPL", "expiry": "2026-08-21"}], True),
    ("_poll_eod_short_interest", [_FakeModel(symbol="AAPL")], True),
    ("_poll_eod_short_volume", [_FakeModel(symbol="AAPL")], True),
    ("_poll_eod_ftds", [_FakeModel(symbol="AAPL")], True),
    ("_poll_eod_congress_trades", [{"ticker": "AAPL", "filed_at_date": "2026-07-20"}], False),
    ("_poll_eod_insiders", [{"ticker": "AAPL", "filing_date": "2026-07-20"}], False),
]


@pytest.mark.parametrize(("method", "result", "takes_ticker"), EOD_POLLS)
async def test_eod_polls_publish_to_backfill_stream(monkeypatch, method, result, takes_ticker):
    """Every EOD poll must publish with the backfill-stream topic."""
    poller = UWPoller(eod_enabled=True)
    poller._provider = AsyncMock()

    async def _fake_fetch(_label: str, _fetch: Any) -> Any:
        return result

    monkeypatch.setattr(poller, "_fetch_with_retry", _fake_fetch)
    recorder = _RecordingPublish()
    monkeypatch.setattr(poller, "_publish_envelopes", recorder)

    fn = getattr(poller, method)
    if takes_ticker:
        await fn(AsyncMock(), "AAPL")
    else:
        await fn(AsyncMock())

    assert recorder.calls, f"{method} never called _publish_envelopes"
    topics = [call.get("topic") for call in recorder.calls]
    assert topics == [BACKFILL_STREAM_TOPIC] * len(topics), (
        f"{method} published to {topics}; EOD bulk must go to {BACKFILL_STREAM_TOPIC} "
        "so it cannot evict unread live events from the capped live stream"
    )
