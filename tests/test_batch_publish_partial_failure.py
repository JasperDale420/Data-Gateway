"""Tests for partial-batch-failure dedup safety (BLOCKER 2).

Polling pollers previously took the integer return value of
``DataSinkRegistry.publish_all_batch`` and sliced ``to_publish[:published]``
to mark events as seen.  Redis pipelines with ``transaction=False`` can fail
at arbitrary indices, so the "first N succeeded" assumption was wrong:
unpublished events were marked seen and permanently suppressed.

The fix introduces a per-message-results path
(``publish_all_batch_results`` returning ``list[bool]``) and updates the
four affected pollers (quotes, trades, crypto, news) to mark only the
successful entries as seen.  These tests pin both the new sink-side API
and the poller-side dedup-marking behavior.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.core.crypto_poller import AlpacaCryptoPoller
from gateway.core.data_sink import DataSink, DataSinkRegistry
from gateway.core.news_poller import AlpacaNewsPoller
from gateway.core.quotes_poller import AlpacaQuotesPoller
from gateway.core.trades_poller import AlpacaTradesPoller


class _BatchResultsSink(DataSink):
    """Sink that exposes per-message success for batch publishes."""

    def __init__(self, results_factory) -> None:
        self._name = "batch_results"
        self._results_factory = results_factory
        self.published: list[tuple[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def record_publish_metrics(self) -> bool:
        return False

    async def publish(self, topic: str, data: Any) -> bool:  # type: ignore[override]
        self.published.append((topic, data))
        return True

    async def publish_batch_results(self, messages) -> list[bool]:
        results = self._results_factory(messages)
        # Tolerate length mismatch so the registry's defensive padding path
        # can be exercised by tests; only iterate the overlap.
        for (topic, data), ok in zip(messages, results, strict=False):
            if ok:
                self.published.append((topic, data))
        return results

    async def health_check(self) -> bool:
        return True

    async def close(self) -> None:
        return None


# ── Registry: publish_all_batch_results aggregates per-message ─────────────


class TestPublishAllBatchResults:
    @pytest.mark.asyncio
    async def test_returns_aligned_booleans(self) -> None:
        """Result list is aligned 1:1 with the input messages."""
        sink = _BatchResultsSink(lambda msgs: [True, True, False, True, True])
        registry = DataSinkRegistry()
        registry.register(sink)

        messages = [("heber:events", {"event_id": f"e{i}"}) for i in range(5)]
        results = await registry.publish_all_batch_results(messages)

        assert results == [True, True, False, True, True]
        published_ids = [d["event_id"] for _, d in sink.published]
        assert published_ids == ["e0", "e1", "e3", "e4"]

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty_list(self) -> None:
        sink = _BatchResultsSink(lambda msgs: [True] * len(msgs))
        registry = DataSinkRegistry()
        registry.register(sink)

        assert await registry.publish_all_batch_results([]) == []

    @pytest.mark.asyncio
    async def test_disabled_registry_returns_all_false(self) -> None:
        sink = _BatchResultsSink(lambda msgs: [True] * len(msgs))
        registry = DataSinkRegistry()
        registry.register(sink)
        registry.disable()

        messages = [("heber:events", {"event_id": "e1"})]
        results = await registry.publish_all_batch_results(messages)
        assert results == [False]

    @pytest.mark.asyncio
    async def test_publish_all_batch_int_matches_results_count(self) -> None:
        """publish_all_batch (legacy int return) must equal sum of new results."""
        sink = _BatchResultsSink(lambda msgs: [True, False, True])
        registry = DataSinkRegistry()
        registry.register(sink)

        messages = [("heber:events", {"event_id": f"e{i}"}) for i in range(3)]
        count = await registry.publish_all_batch(messages)
        assert count == 2  # exactly the True entries

    @pytest.mark.asyncio
    async def test_length_mismatch_pads_with_false(self) -> None:
        """Defensive: sink returning fewer booleans than messages -> False fill."""
        sink = _BatchResultsSink(lambda msgs: [True])  # only 1 entry for 3 messages
        registry = DataSinkRegistry()
        registry.register(sink)

        messages = [("heber:events", {"event_id": f"e{i}"}) for i in range(3)]
        results = await registry.publish_all_batch_results(messages)
        assert results == [True, False, False]


# ── Pollers: only successful entries get marked seen ───────────────────────


def _make_quote(symbol: str) -> Any:
    q = MagicMock()
    q.model_dump = MagicMock(
        return_value={
            "symbol": symbol,
            "bid_price": 100.0,
            "ask_price": 100.1,
            "bid_size": 10,
            "ask_size": 10,
            "timestamp": "2026-04-29T15:30:00Z",
        }
    )
    return q


def _make_trade(symbol: str) -> Any:
    t = MagicMock()
    t.model_dump = MagicMock(
        return_value={
            "symbol": symbol,
            "price": 100.0,
            "size": 10,
            "trade_id": f"{symbol}-trade-1",
            "timestamp": "2026-04-29T15:30:00Z",
        }
    )
    return t


def _make_article(article_id: str) -> Any:
    article = MagicMock()
    article.model_dump = MagicMock(
        return_value={
            "article_id": article_id,
            "headline": f"news {article_id}",
            "summary": "",
            "url": f"https://example/{article_id}",
            "source": "Reuters",
            "author": "",
            "published_at": "2026-04-29T15:30:00Z",
            "symbols": ["AAPL"],
        }
    )
    return article


@pytest.fixture
def partial_failure_sink_registry():
    """Mock sink registry where index 2 of every 5-element batch fails."""
    sink = AsyncMock()
    sink.publish_all = AsyncMock()

    def _results(msgs):
        # Mark the third (index 2) as failed, all others succeed.
        return [i != 2 for i in range(len(msgs))]

    sink.publish_all_batch_results = AsyncMock(side_effect=_results)
    # Legacy method returns count == successes; pollers prefer the new path.
    sink.publish_all_batch = AsyncMock(side_effect=lambda msgs: sum(_results(msgs)))
    return sink


@pytest.mark.asyncio
async def test_quotes_poller_only_marks_successful_entries(partial_failure_sink_registry) -> None:
    """Quotes poller must not mark failed indices as seen."""
    poller = AlpacaQuotesPoller(symbols=["A", "B", "C", "D", "E"], poll_interval_seconds=15)
    poller._redis_dedupe = None
    poller._provider = AsyncMock()
    poller._provider.get_quotes = AsyncMock(return_value=[_make_quote(s) for s in ["A", "B", "C", "D", "E"]])

    await poller._poll_quotes(partial_failure_sink_registry)

    # Re-poll: only the FAILED quote (index 2 == "C") should re-publish.
    partial_failure_sink_registry.publish_all_batch_results.reset_mock()

    # Switch behavior: now everything succeeds so we can see what was retried.
    partial_failure_sink_registry.publish_all_batch_results = AsyncMock(side_effect=lambda msgs: [True] * len(msgs))

    await poller._poll_quotes(partial_failure_sink_registry)

    # The second call should batch only the previously-failed event.
    assert partial_failure_sink_registry.publish_all_batch_results.called
    args, _kw = partial_failure_sink_registry.publish_all_batch_results.call_args
    second_msgs = args[0]
    # Exactly one envelope -- the symbol "C" quote that failed the first time.
    assert len(second_msgs) == 1
    _topic, env = second_msgs[0]
    assert env["symbol"] == "C", (
        f"Expected the failed 'C' quote to retry, got symbol={env['symbol']!r}. "
        "BLOCKER 2 regression: failed events being marked seen would suppress this retry."
    )


@pytest.mark.asyncio
async def test_trades_poller_only_marks_successful_entries(partial_failure_sink_registry) -> None:
    poller = AlpacaTradesPoller(symbols=["A", "B", "C", "D", "E"], poll_interval_seconds=15)
    poller._redis_dedupe = None
    poller._provider = AsyncMock()
    poller._provider.get_latest_trades = AsyncMock(return_value=[_make_trade(s) for s in ["A", "B", "C", "D", "E"]])

    await poller._poll_trades(partial_failure_sink_registry)

    partial_failure_sink_registry.publish_all_batch_results = AsyncMock(side_effect=lambda msgs: [True] * len(msgs))
    await poller._poll_trades(partial_failure_sink_registry)

    args, _kw = partial_failure_sink_registry.publish_all_batch_results.call_args
    second_msgs = args[0]
    assert len(second_msgs) == 1
    assert second_msgs[0][1]["symbol"] == "C"


@pytest.mark.asyncio
async def test_news_poller_only_marks_successful_entries() -> None:
    sink = AsyncMock()
    sink.publish_all = AsyncMock()

    def _results(msgs):
        return [i != 2 for i in range(len(msgs))]

    sink.publish_all_batch_results = AsyncMock(side_effect=_results)
    sink.publish_all_batch = AsyncMock(side_effect=lambda msgs: sum(_results(msgs)))

    poller = AlpacaNewsPoller(symbols=None, poll_interval_seconds=60, fetch_limit=50)
    poller._redis_dedupe = None
    poller._provider = AsyncMock()
    poller._provider.get_news = AsyncMock(return_value=[_make_article(f"a{i}") for i in range(5)])

    await poller._poll_news(sink)

    sink.publish_all_batch_results = AsyncMock(side_effect=lambda msgs: [True] * len(msgs))
    await poller._poll_news(sink)

    args, _kw = sink.publish_all_batch_results.call_args
    second_msgs = args[0]
    assert len(second_msgs) == 1
    # The failed article on first poll was index 2 -> article_id "a2".
    assert second_msgs[0][1]["payload"]["article_id"] == "a2"


@pytest.mark.asyncio
async def test_uw_poller_only_marks_successful_entries() -> None:
    """UW poller must not mark failed indices as seen either.

    The previous "mark all seen before publish" pattern in UWPoller relied
    on a sink-side buffer that ``_publish_chunk`` does NOT populate, so
    failed events were permanently suppressed -- same bug class as the
    Alpaca pollers.
    """
    from gateway.core.uw_poller import HEBER_STREAM, UWPoller

    sink = AsyncMock()
    sink.publish_all = AsyncMock()

    def _results(msgs):
        return [i != 2 for i in range(len(msgs))]

    sink.publish_all_batch_results = AsyncMock(side_effect=_results)
    sink.publish_all_batch = AsyncMock(side_effect=lambda msgs: sum(_results(msgs)))
    # UW poller uses the indexed batch API (it threads the exact succeeded
    # indices into the flow WS fan-out for push/poll event-id parity); mirror
    # ``_results`` as the set of succeeded local indices.
    sink.publish_all_batch_indexed = AsyncMock(
        side_effect=lambda msgs: {i for i in range(len(msgs)) if i != 2}
    )

    poller = UWPoller()
    poller._redis_dedupe = None
    envelopes = [{"event_id": f"uw-e{i}", "feed": "flow_alerts"} for i in range(5)]

    published, _ = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
    )
    assert published == 4

    # Only the 4 successful event_ids should be marked seen.
    seen = set(poller._seen_ids.keys())
    assert seen == {"uw-e0", "uw-e1", "uw-e3", "uw-e4"}, (
        f"BLOCKER 2 (UW poller) regression: marked seen = {seen}, "
        "expected {{'uw-e0', 'uw-e1', 'uw-e3', 'uw-e4'}}. "
        "Failed event 'uw-e2' must NOT be marked seen or it will be "
        "permanently suppressed on subsequent polls."
    )
    assert "uw-e2" not in seen

    # Sanity: messages were passed through the indexed per-message API.
    args, _kw = sink.publish_all_batch_indexed.call_args
    msgs = args[0]
    assert all(topic == HEBER_STREAM for topic, _ in msgs)


@pytest.mark.asyncio
async def test_crypto_poller_only_marks_successful_entries() -> None:
    sink = AsyncMock()
    sink.publish_all = AsyncMock()

    def _results(msgs):
        return [i != 0 for i in range(len(msgs))]

    sink.publish_all_batch_results = AsyncMock(side_effect=_results)
    sink.publish_all_batch = AsyncMock(side_effect=lambda msgs: sum(_results(msgs)))

    poller = AlpacaCryptoPoller(pairs=["BTC/USD", "ETH/USD"], poll_interval_seconds=15)
    poller._redis_dedupe = None
    poller._provider = AsyncMock()
    poller._provider.get_crypto_latest_bars = AsyncMock(
        return_value={
            "BTC/USD": {
                "t": "2026-04-29T15:30:00Z",
                "o": 50000,
                "h": 50100,
                "l": 49900,
                "c": 50050,
                "v": 1,
                "n": 1,
                "vw": 50000,
            },
            "ETH/USD": {
                "t": "2026-04-29T15:30:00Z",
                "o": 3000,
                "h": 3100,
                "l": 2900,
                "c": 3050,
                "v": 1,
                "n": 1,
                "vw": 3000,
            },
        }
    )
    poller._provider.get_crypto_latest_trades = AsyncMock(return_value={})

    await poller._poll_crypto(sink)

    # First publish batch was bars for [BTC/USD, ETH/USD]; index 0 (BTC) failed.
    sink.publish_all_batch_results = AsyncMock(side_effect=lambda msgs: [True] * len(msgs))
    await poller._poll_crypto(sink)

    # Retry should re-publish exactly the BTC bar (failed previously).
    # Note: the second call's first batch is the bars batch.
    call_args_list = sink.publish_all_batch_results.call_args_list
    bars_retry = call_args_list[0][0][0]
    assert len(bars_retry) == 1
    assert "BTC" in bars_retry[0][1]["instrument_key"]


# ── DataSinkRegistry round-trip with real-ish sink ─────────────────────────


class TestDataSinkRegistryRoundTrip:
    @pytest.mark.asyncio
    async def test_index_2_of_5_failure(self) -> None:
        """Reproduce the exact bug scenario from BLOCKER 2."""
        sink = _BatchResultsSink(lambda msgs: [True, True, False, True, True])
        registry = DataSinkRegistry()
        registry.register(sink)

        # Caller has 5 envelopes with event_ids 0..4
        to_publish = [({"event_id": f"e{i}"}, f"e{i}", f"cache:{i}") for i in range(5)]
        messages = [("heber:events", env) for env, _, _ in to_publish]
        results = await registry.publish_all_batch_results(messages)

        # Apply the same dedup-marking pattern the pollers now use
        marked_seen: list[str] = []
        for (_env, event_id, _cache_key), ok in zip(to_publish, results, strict=True):
            if ok and event_id:
                marked_seen.append(event_id)

        assert marked_seen == ["e0", "e1", "e3", "e4"], (
            "BLOCKER 2: only successful events must be marked seen. "
            f"Got {marked_seen}, expected ['e0', 'e1', 'e3', 'e4'] -- "
            "the old code marked ['e0', 'e1', 'e2', 'e3'] by slicing to_publish[:4]."
        )
        assert "e2" not in marked_seen
