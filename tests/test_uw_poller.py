from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

import gateway.core.uw_poller as uw_poller_module
from gateway.core.uw_eod_state import UwEodRunState, UwEodStateStore
from gateway.core.uw_poller import HEBER_STREAM, UWPoller, get_uw_poller_snapshot


class _FakeSinkRegistry:
    def __init__(self, delay_seconds: float = 0.0) -> None:
        self.delay_seconds = delay_seconds
        self.calls: list[tuple[str, dict]] = []
        self._inflight = 0
        self.max_inflight = 0

    async def publish_all(self, stream: str, envelope: dict) -> None:
        self._inflight += 1
        self.max_inflight = max(self.max_inflight, self._inflight)
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        self.calls.append((stream, envelope))
        self._inflight -= 1


class _FakeRedisDedupe:
    def __init__(self, duplicate_keys: set[str] | None = None) -> None:
        self.duplicate_keys = duplicate_keys or set()
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, bool, int | None]] = []

    async def get(self, key: str):
        self.get_calls.append(key)
        return True if key in self.duplicate_keys else None

    async def set(self, key: str, value: bool, ttl: int | None = None) -> bool:
        self.set_calls.append((key, value, ttl))
        return True

    async def mget(self, keys: list[str]) -> dict[str, bool]:
        result: dict[str, bool] = {}
        for key in keys:
            self.get_calls.append(key)
            if key in self.duplicate_keys:
                result[key] = True
        return result

    async def set_many(self, items: list[tuple[str, bool]], ttl: int | None = None) -> int:
        for key, value in items:
            self.set_calls.append((key, value, ttl))
        return len(items)


@pytest.mark.asyncio
async def test_publish_envelopes_dedupes_seen_and_redis_hits() -> None:
    poller = UWPoller()
    redis = _FakeRedisDedupe(duplicate_keys={"uw:flow:e2"})
    poller._redis_dedupe = cast(Any, redis)
    poller._mark_seen("e1")
    sink = _FakeSinkRegistry()

    envelopes = [
        {"event_id": "e1", "feed": "flow_alerts"},
        {"event_id": "e2", "feed": "flow_alerts"},
        {"event_id": "e3", "feed": "flow_alerts"},
        {"feed": "flow_alerts"},  # missing event_id still publishes
    ]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
    )

    assert published == 2
    assert duplicates == 2
    assert len(sink.calls) == 2
    assert all(stream == HEBER_STREAM for stream, _ in sink.calls)
    assert "e3" in poller._seen_ids
    assert [key for key, _, _ in redis.set_calls] == ["uw:flow:e3"]


@pytest.mark.asyncio
async def test_publish_envelopes_respects_max_inflight_limit() -> None:
    poller = UWPoller()
    poller._redis_dedupe = None
    poller._publish_max_inflight = 2
    sink = _FakeSinkRegistry(delay_seconds=0.01)

    envelopes = [{"event_id": f"e{i}", "feed": "flow_alerts"} for i in range(8)]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
    )

    assert published == 8
    assert duplicates == 0
    assert sink.max_inflight <= 2


class _FailingSinkRegistry:
    """Sink whose batch publish always raises (simulates Redis pipeline failure)."""

    def __init__(self) -> None:
        self.batch_calls = 0

    async def publish_all_batch(self, messages: list[tuple[str, dict]]) -> int:
        self.batch_calls += 1
        raise RuntimeError("pipeline down")


class _CountingSinkRegistry:
    """Sink that records every successfully published envelope."""

    def __init__(self) -> None:
        self.published: list[dict] = []

    async def publish_all_batch(self, messages: list[tuple[str, dict]]) -> int:
        for _stream, envelope in messages:
            self.published.append(envelope)
        return len(messages)


@pytest.mark.asyncio
async def test_publish_envelopes_does_not_mark_on_publish_failure() -> None:
    """A failed batch publish must NOT mark events seen, so a later cycle re-publishes."""
    poller = UWPoller()
    poller._redis_dedupe = None
    failing = _FailingSinkRegistry()

    envelopes = [{"event_id": "e1", "feed": "congress_trades"}]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=failing,
        envelopes=envelopes,
        dedupe_prefix="uw:congress",
        missing_event_log="uw_congress_missing_event_id",
    )

    assert published == 0
    assert duplicates == 0
    assert "e1" not in poller._seen_ids  # not marked → eligible for re-publish

    # A later cycle with a healthy sink re-publishes the same event.
    healthy = _CountingSinkRegistry()
    published2, _ = await poller._publish_envelopes(
        sink_registry=healthy,
        envelopes=envelopes,
        dedupe_prefix="uw:congress",
        missing_event_log="uw_congress_missing_event_id",
    )
    assert published2 == 1
    assert [e["event_id"] for e in healthy.published] == ["e1"]
    assert "e1" in poller._seen_ids


@pytest.mark.asyncio
async def test_publish_envelopes_does_not_mark_ambiguous_partial_batch() -> None:
    """A count-only partial batch cannot prove which events landed.

    Marking the first N as seen would permanently suppress an event that may
    not have reached Redis. In this ambiguous fallback, prefer possible
    duplicate re-publish over possible loss.
    """
    poller = UWPoller()
    poller._redis_dedupe = None

    class _PartialSink:
        async def publish_all_batch(self, messages: list[tuple[str, dict]]) -> int:
            return 1  # one landed, but this API does not say which one

    envelopes = [
        {"event_id": "p1", "feed": "insider_trades"},
        {"event_id": "p2", "feed": "insider_trades"},
    ]

    published, _ = await poller._publish_envelopes(
        sink_registry=_PartialSink(),
        envelopes=envelopes,
        dedupe_prefix="uw:insider",
        missing_event_log="uw_insider_missing_event_id",
    )

    assert published == 1
    assert "p1" not in poller._seen_ids
    assert "p2" not in poller._seen_ids


@pytest.mark.asyncio
async def test_publish_envelopes_marks_exact_indexed_successes() -> None:
    """Indexed batch publishing marks and taps only the exact landed events."""
    poller = UWPoller()
    poller._redis_dedupe = None
    delivered: list[str] = []

    class _IndexedPartialSink:
        async def publish_all_batch_indexed(self, messages: list[tuple[str, dict]]) -> set[int]:
            return {1}

    async def _tap(envelope: dict) -> None:
        delivered.append(envelope["event_id"])

    envelopes = [
        {"event_id": "i0", "feed": "flow_alerts"},
        {"event_id": "i1", "feed": "flow_alerts"},
        {"event_id": "i2", "feed": "flow_alerts"},
    ]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=_IndexedPartialSink(),
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
        on_published=_tap,
    )

    assert published == 1
    assert duplicates == 0
    assert set(poller._seen_ids) == {"i1"}
    assert delivered == ["i1"]


@pytest.mark.asyncio
async def test_flow_envelopes_use_atomic_writer_and_watch_admission_when_live_is_durable() -> None:
    poller = UWPoller()
    poller._redis_dedupe = None

    class _DurableFlowSink:
        def __init__(self) -> None:
            self.messages: list[tuple[str, dict]] = []

        def has_durable_admission_for(self, topic: str) -> bool:
            return topic == HEBER_STREAM

        async def publish_flow_with_watch_batch_indexed(self, messages: list[tuple[str, dict]]) -> set[int]:
            self.messages.extend(messages)
            return set(range(len(messages)))

    sink = _DurableFlowSink()
    envelopes = [{"event_id": "flow-1", "feed": "flow_alerts"}]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
        on_published=lambda _envelope: asyncio.sleep(0),
    )

    assert (published, duplicates) == (1, 0)
    assert sink.messages == [(HEBER_STREAM, envelopes[0])]


@pytest.mark.asyncio
async def test_durable_live_flow_writes_watch_copy_without_a_websocket_tap() -> None:
    poller = UWPoller()
    poller._redis_dedupe = None

    class _DurableFlowSink:
        def __init__(self) -> None:
            self.messages: list[tuple[str, dict]] = []

        def has_durable_admission_for(self, topic: str) -> bool:
            return topic == HEBER_STREAM

        async def publish_flow_with_watch_batch_indexed(self, messages: list[tuple[str, dict]]) -> set[int]:
            self.messages.extend(messages)
            return set(range(len(messages)))

        async def publish_all_batch_indexed(self, _messages: list[tuple[str, dict]]) -> set[int]:
            raise AssertionError("durable flow must use atomic writer-and-watch admission")

    sink = _DurableFlowSink()
    envelopes = [{"event_id": "flow-without-ws", "feed": "flow_alerts"}]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
        on_published=None,
    )

    assert (published, duplicates) == (1, 0)
    assert sink.messages == [(HEBER_STREAM, envelopes[0])]


def test_build_feed_envelopes_skips_malformed_record_without_dropping_batch() -> None:
    """Strict envelope failures skip only that UW item, not the whole poll batch."""
    poller = UWPoller()

    records = [
        SimpleNamespace(model_dump=lambda: {"ticker": "AAPL", "timestamp": "2026-06-10T15:30:00Z"}),
        SimpleNamespace(
            model_dump=lambda: {
                "ticker": "AAPL",
                "option_chain": "AAPL260619C00190000",
                "timestamp": "2026-06-10T15:31:00Z",
            }
        ),
    ]

    envelopes, out_of_order = poller._build_feed_envelopes(
        records,
        feed="flow_alerts",
        out_of_order_log="uw_flow_out_of_order_ts",
    )

    assert out_of_order == 0
    assert len(envelopes) == 1
    assert envelopes[0]["instrument_key"] == "option:OCC:AAPL260619C00190000"


@pytest.mark.asyncio
async def test_registry_publish_all_batch_indexed_reports_exact_partial_success() -> None:
    """The real DataSinkRegistry forwards exact succeeded indices from the sink.

    G2 end-to-end: a partial Redis failure (e1 fails, e2 succeeds) must surface
    as the exact index {1}, not a count, so the poller fans out the event Heber
    actually received and not the "first N" approximation.
    """
    from gateway.core.data_sink import DataSinkRegistry

    class _PartialIndexedSink:
        name = "partial_redis"
        record_publish_metrics = True

        async def publish_batch_indexed(self, messages: list[tuple[str, dict]]) -> set[int]:
            # e1 (index 0) failed in the Redis pipeline; e2 (index 1) landed.
            return {1}

        async def health_check(self) -> bool:
            return True

    registry = DataSinkRegistry()
    registry._sinks.append(_PartialIndexedSink())  # type: ignore[arg-type]

    indices = await registry.publish_all_batch_indexed(
        [(HEBER_STREAM, {"event_id": "e1"}), (HEBER_STREAM, {"event_id": "e2"})]
    )
    assert indices == {1}


@pytest.mark.asyncio
async def test_eod_congress_event_ids_are_content_stable_across_runs() -> None:
    """Re-fetching the same congress trade must yield the same event_id."""
    poller = UWPoller()

    trade = {
        "ticker": "AAPL",
        "name": "Jane Doe",
        "transaction_date": "2026-06-09",
        "filed_at_date": "2026-06-10",
    }

    class _Provider:
        async def get_congress_trades(self, limit: int = 200):  # noqa: ARG002
            return [dict(trade)]

    poller._provider = _Provider()

    run_event_ids: list[str] = []

    async def _capture(**kwargs):
        run_event_ids.extend(e["event_id"] for e in kwargs["envelopes"])
        return len(kwargs["envelopes"]), 0

    poller._publish_envelopes = _capture  # type: ignore[method-assign]
    await poller._poll_eod_congress_trades(sink_registry=_FakeSinkRegistry())
    await poller._poll_eod_congress_trades(sink_registry=_FakeSinkRegistry())

    assert len(run_event_ids) == 2
    assert run_event_ids[0] == run_event_ids[1]


@pytest.mark.asyncio
async def test_eod_congress_fetch_retries_transient_error() -> None:
    """A single transient 5xx on the EOD congress fetch is retried, not dropped."""
    import httpx

    poller = UWPoller()

    async def _publish(**kwargs):
        return len(kwargs["envelopes"]), 0

    poller._publish_envelopes = _publish  # type: ignore[method-assign]

    calls = {"n": 0}

    def _make_503() -> httpx.HTTPStatusError:
        request = httpx.Request("GET", "https://api.unusualwhales.com/api/congress/recent-trades")
        response = httpx.Response(503, request=request)
        return httpx.HTTPStatusError("503", request=request, response=response)

    class _FlakyProvider:
        async def get_congress_trades(self, limit: int = 200):  # noqa: ARG002
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_503()
            return [{"ticker": "AAPL", "name": "Jane Doe", "transaction_date": "2026-06-09"}]

    poller._provider = _FlakyProvider()

    published = await poller._poll_eod_congress_trades(sink_registry=_FakeSinkRegistry())

    assert calls["n"] == 2  # retried once after the 503
    assert published == 1


@pytest.mark.asyncio
async def test_eod_historic_option_volume_uses_equity_instrument_key() -> None:
    """Per-underlying option-volume rows carry expiry but are not OCC contracts."""
    poller = UWPoller()

    class _Provider:
        async def get_historic_option_volume(self, ticker: str):
            return [
                {
                    "symbol": ticker,
                    "date": "2026-06-10",
                    "expiry": "2026-06-19",
                    "volume": 1000,
                    "timestamp": "2026-06-10T21:00:00Z",
                }
            ]

    poller._provider = _Provider()
    poller._redis_dedupe = None
    sink = _FakeSinkRegistry()

    published = await poller._poll_eod_option_volume(sink_registry=sink, ticker="SPY")

    assert published == 1
    envelope = sink.calls[0][1]
    assert envelope["instrument_type"] == "equity"
    assert envelope["instrument_key"] == "equity:SPY"


@pytest.mark.asyncio
async def test_poll_darkpool_emits_canonical_darkpool_feed(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = UWPoller()

    class _FakeProvider:
        async def get_darkpool_recent(self, limit: int = 200):  # noqa: ARG002
            return [
                SimpleNamespace(
                    model_dump=lambda: {
                        "event_id": "dp-1",
                        "symbol": "AAPL",
                        "ts_event": "2026-02-10T14:30:00Z",
                    }
                )
            ]

    captured: dict[str, Any] = {}

    async def _capture_publish(**kwargs):
        captured["envelopes"] = kwargs["envelopes"]
        return len(kwargs["envelopes"]), 0

    poller._provider = _FakeProvider()
    monkeypatch.setattr(poller, "_publish_envelopes", _capture_publish)

    await poller._poll_darkpool(sink_registry=_FakeSinkRegistry(), limit=1)

    envelopes = captured["envelopes"]
    assert len(envelopes) == 1
    assert envelopes[0]["feed"] == "darkpool"


def test_uw_poller_publish_limit_reads_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        uw_poller_module,
        "get_settings",
        lambda: SimpleNamespace(
            cache_redis_enabled=False,
            cache_redis_url="",
            uw_poller_publish_max_inflight=7,
            uw_eod_state_path="/tmp/uw-eod-state-test.json",
            uw_eod_claim_stale_after_seconds=3600,
        ),
    )

    poller = UWPoller()
    assert poller._publish_max_inflight == 7


def test_should_poll_eod_uses_persistent_completed_state(tmp_path) -> None:
    path = tmp_path / "uw_eod_state.json"
    today = uw_poller_module.datetime.now(uw_poller_module.ET).strftime("%Y-%m-%d")
    store = UwEodStateStore(path, stale_after_seconds=3600)
    assert store.claim(today) is True
    store.mark_completed(today, totals={})

    poller = UWPoller(eod_hour=0, eod_minute=0)
    poller._calendar = SimpleNamespace(is_trading_day=lambda _date: True)
    poller._eod_state = store

    assert poller._should_poll_eod() is False


def test_should_poll_eod_skips_active_running_marker_without_claim_log(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "uw_eod_state.json"
    today = uw_poller_module.datetime.now(uw_poller_module.ET).strftime("%Y-%m-%d")
    store = UwEodStateStore(path, stale_after_seconds=3600)
    assert store.claim(today) is True

    poller = UWPoller(eod_hour=0, eod_minute=0)
    poller._calendar = SimpleNamespace(is_trading_day=lambda _date: True)
    poller._eod_state = UwEodStateStore(path, stale_after_seconds=3600)

    info_messages: list[str] = []

    def _capture_info(message: str, **_kwargs) -> None:
        info_messages.append(message)

    monkeypatch.setattr(uw_poller_module.logger, "info", _capture_info)

    assert poller._should_poll_eod() is False
    assert "uw_eod_skipped_persistent_state" not in info_messages


@pytest.mark.asyncio
async def test_poll_eod_snapshots_skips_active_same_day_state_after_restart(tmp_path) -> None:
    path = tmp_path / "uw_eod_state.json"
    today = uw_poller_module.datetime.now(uw_poller_module.ET).strftime("%Y-%m-%d")
    first_instance = UwEodStateStore(path, stale_after_seconds=3600)
    assert first_instance.claim(today) is True

    class _TickerUniverse:
        all_tickers = ["SPY"]

        async def refresh_dynamic(self, _provider) -> None:
            raise AssertionError("persistent EOD claim should skip before refreshing tickers")

    poller = UWPoller()
    poller._provider = object()  # type: ignore[assignment]
    poller._ticker_universe = cast(Any, _TickerUniverse())
    poller._eod_state = UwEodStateStore(path, stale_after_seconds=3600)

    await poller._poll_eod_snapshots(sink_registry=_FakeSinkRegistry())


@pytest.mark.asyncio
async def test_poll_eod_snapshots_marks_persistent_state_completed_after_success(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "uw_eod_state.json"
    today = uw_poller_module.datetime.now(uw_poller_module.ET).strftime("%Y-%m-%d")

    class _TickerUniverse:
        all_tickers = ["SPY"]

        async def refresh_dynamic(self, _provider) -> None:
            return None

    async def _per_ticker(_sink_registry, _ticker: str) -> int:
        return 1

    async def _market_wide(_sink_registry) -> int:
        return 2

    poller = UWPoller()
    poller._provider = object()  # type: ignore[assignment]
    poller._ticker_universe = cast(Any, _TickerUniverse())
    poller._eod_state = UwEodStateStore(path, stale_after_seconds=3600)

    for method_name in (
        "_poll_eod_greek_exposure",
        "_poll_eod_iv_rank",
        "_poll_eod_iv_term_structure",
        "_poll_eod_oi_change",
        "_poll_eod_option_volume",
        "_poll_eod_short_interest",
        "_poll_eod_short_volume",
        "_poll_eod_ftds",
    ):
        monkeypatch.setattr(poller, method_name, _per_ticker)
    monkeypatch.setattr(poller, "_poll_eod_congress_trades", _market_wide)
    monkeypatch.setattr(poller, "_poll_eod_insiders", _market_wide)

    await poller._poll_eod_snapshots(sink_registry=_FakeSinkRegistry())

    state = UwEodRunState.model_validate_json(path.read_text())
    assert state.trading_date == today
    assert state.status == "completed"
    assert state.completed_at is not None
    assert state.totals["greek_exposure"] == {"published": 1, "errors": 0}
    assert state.totals["congress_trades"] == {"published": 2, "errors": 0}
    assert poller._last_eod_date == today


def test_uw_poller_runtime_snapshot_includes_tuning_fields() -> None:
    poller = UWPoller()
    poller._running = True
    poller._seen_ids = {"a": uw_poller_module.datetime.now(uw_poller_module.UTC)}

    snapshot = poller.get_runtime_snapshot()

    assert snapshot["running"] is True
    assert snapshot["publish_max_inflight"] >= 1
    assert snapshot["dedupe_cache_entries"] == 1
    assert snapshot["dedupe_cache_ttl_seconds"] == 7200
    assert snapshot["poll_intervals_seconds"]["flow"] == 300
    assert snapshot["poll_intervals_seconds"]["darkpool"] == poller._get_darkpool_interval()
    assert snapshot["poll_intervals_seconds"]["tide"] == 3600


def test_get_uw_poller_snapshot_returns_disabled_payload_when_not_started() -> None:
    uw_poller_module._uw_poller = None

    snapshot = get_uw_poller_snapshot()

    assert snapshot["running"] is False
    assert snapshot["enabled"] is False


def test_sector_tide_polls_independently_of_market_tide() -> None:
    """Sector tide must have its own timer so market_tide doesn't block it."""
    poller = UWPoller()

    # Initially both should be ready to poll
    assert poller._should_poll_tide() is True
    assert poller._should_poll_sector_tide() is True

    # Simulate market_tide polling (sets _last_tide_poll)
    poller._last_tide_poll = uw_poller_module.datetime.now(uw_poller_module.UTC)

    # Market tide should now be blocked (just polled)
    assert poller._should_poll_tide() is False

    # Sector tide should still be ready (independent timer)
    assert poller._should_poll_sector_tide() is True

    # Now simulate sector_tide polling
    poller._last_sector_tide_poll = uw_poller_module.datetime.now(uw_poller_module.UTC)

    # Now both should be blocked
    assert poller._should_poll_tide() is False
    assert poller._should_poll_sector_tide() is False


@pytest.mark.asyncio
async def test_runtime_snapshot_includes_sink_available() -> None:
    """get_runtime_snapshot should include sink_available field."""
    poller = UWPoller()
    snapshot = poller.get_runtime_snapshot()
    assert "sink_available" in snapshot
    assert snapshot["sink_available"] is False  # No sink configured by default


@pytest.mark.asyncio
async def test_on_flow_envelope_tap_fires_only_for_published_flow_envelopes() -> None:
    """The flow tap is invoked once per envelope actually written to Redis.

    Deduped envelopes (seen / redis hit) are NOT tapped — push must mirror the
    heber:events publish set exactly so Orion's parity holds.
    """
    poller = UWPoller()
    poller._mark_seen("dup1")  # pre-seen → deduped, must not be tapped
    poller._redis_dedupe = None
    sink = _FakeSinkRegistry()

    tapped: list[dict] = []

    async def _tap(envelope: dict) -> None:
        tapped.append(envelope)

    poller.on_flow_envelope = _tap

    envelopes = [
        {"event_id": "dup1", "feed": "flow_alerts", "symbol": "AAPL"},
        {"event_id": "new1", "feed": "flow_alerts", "symbol": "TSLA"},
        {"event_id": "new2", "feed": "flow_alerts", "symbol": "NVDA"},
    ]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
        on_published=poller.on_flow_envelope,
    )

    assert published == 2
    assert duplicates == 1
    assert [e["event_id"] for e in tapped] == ["new1", "new2"]


@pytest.mark.asyncio
async def test_tap_failure_does_not_break_publish_accounting() -> None:
    poller = UWPoller()
    poller._redis_dedupe = None
    sink = _FakeSinkRegistry()

    async def _boom(_envelope: dict) -> None:
        raise RuntimeError("fan-out down")

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=[{"event_id": "e1", "feed": "flow_alerts", "symbol": "AAPL"}],
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
        on_published=_boom,
    )

    assert published == 1
    assert duplicates == 0


@pytest.mark.asyncio
async def test_darkpool_publish_passes_no_flow_tap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the flow path forwards on_published; darkpool must pass None."""
    poller = UWPoller()

    class _FakeProvider:
        async def get_darkpool_recent(self, limit: int = 200):  # noqa: ARG002
            return [SimpleNamespace(model_dump=lambda: {"symbol": "AAPL", "ts_event": "2026-02-10T14:30:00Z"})]

    captured: dict[str, Any] = {}

    async def _capture_publish(**kwargs):
        captured["on_published"] = kwargs.get("on_published")
        return len(kwargs["envelopes"]), 0

    poller._provider = cast(Any, _FakeProvider())
    monkeypatch.setattr(poller, "_publish_envelopes", _capture_publish)

    await poller._poll_darkpool(sink_registry=_FakeSinkRegistry(), limit=1)
    assert captured["on_published"] is None


@pytest.mark.asyncio
async def test_flow_publish_forwards_flow_tap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The flow path forwards poller.on_flow_envelope to _publish_envelopes."""
    poller = UWPoller()

    async def _tap(_env: dict) -> None:
        return None

    poller.on_flow_envelope = _tap

    class _FakeProvider:
        async def get_flow_alerts(self, limit: int = 200):  # noqa: ARG002
            return [
                SimpleNamespace(
                    model_dump=lambda: {
                        "ticker": "AAPL",
                        "option_chain": "AAPL240119C00190000",
                        "timestamp": "2026-02-10T14:30:00Z",
                    }
                )
            ]

    captured: dict[str, Any] = {}

    async def _capture_publish(**kwargs):
        captured["on_published"] = kwargs.get("on_published")
        return len(kwargs["envelopes"]), 0

    poller._provider = cast(Any, _FakeProvider())
    monkeypatch.setattr(poller, "_publish_envelopes", _capture_publish)

    await poller._poll_flow_alerts(sink_registry=_FakeSinkRegistry(), limit=1)
    assert captured["on_published"] is _tap


@pytest.mark.asyncio
async def test_eod_per_ticker_fetch_retries_transient_error() -> None:
    """A transient 5xx on a per-ticker EOD fetch (greek_exposure) is retried.

    Guards the fix that routed the 8 per-ticker EOD feeds through
    _fetch_with_retry, closing scattered per-ticker daily holes in Heber Gold.
    """
    import httpx

    poller = UWPoller()

    async def _publish(**kwargs):
        return len(kwargs["envelopes"]), 0

    poller._publish_envelopes = _publish  # type: ignore[method-assign]

    calls = {"n": 0}

    def _make_503() -> httpx.HTTPStatusError:
        request = httpx.Request("GET", "https://api.unusualwhales.com/api/stock/AAPL/greek-exposure")
        response = httpx.Response(503, request=request)
        return httpx.HTTPStatusError("503", request=request, response=response)

    class _Row:
        def model_dump(self) -> dict:
            return {"ticker": "AAPL", "call_gex": 1.0}

    class _FlakyProvider:
        async def get_greek_exposure(self, ticker: str):  # noqa: ARG002
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_503()
            return [_Row()]

    poller._provider = _FlakyProvider()

    published = await poller._poll_eod_greek_exposure(sink_registry=_FakeSinkRegistry(), ticker="AAPL")

    assert calls["n"] == 2  # retried once after the 503
    assert published == 1


def test_envelope_build_skip_is_metered(monkeypatch) -> None:
    """A record that fails to wrap is skipped AND metered, so steady partial UW
    loss from schema drift surfaces on the dropped-message alert, not just logs.
    """
    poller = UWPoller()
    calls: list[tuple[str, str]] = []

    def _fake_wrap(*, event, provider, feed, source):
        if event.get("bad"):
            raise ValueError("schema drift")
        return {"ts_event": "2026-06-25T00:00:00Z", "payload": event}

    monkeypatch.setattr("gateway.core.uw_poller.wrap_event", _fake_wrap)
    monkeypatch.setattr(
        "gateway.core.uw_poller.record_message_dropped",
        lambda reason, feed="unknown": calls.append((reason, feed)),
    )

    records = [{"bad": True}, {"bad": False}]
    envelopes, _ = poller._build_feed_envelopes(records, feed="darkpool", out_of_order_log="x", use_model_dump=False)

    assert len(envelopes) == 1  # the bad record was skipped, the good one kept
    assert calls == [("envelope_build_skipped", "darkpool")]


@pytest.mark.asyncio
async def test_publish_envelopes_records_per_feed_published() -> None:
    """The per-feed published counter increments so a feed silently going to zero
    during market hours (e.g. darkpool window overflow) is visible per feed."""
    from gateway.core.metrics import FEED_PUBLISHED

    poller = UWPoller()
    poller._redis_dedupe = None  # isolate from real Redis dedup state (deterministic)
    envelopes = [{"event_id": f"dp-feedmetric-{i}", "feed": "darkpool", "payload": {}} for i in range(3)]
    before = FEED_PUBLISHED.labels(feed="darkpool")._value.get()

    published, _ = await poller._publish_envelopes(
        sink_registry=_FakeSinkRegistry(),
        envelopes=envelopes,
        dedupe_prefix="uw:dp",
        missing_event_log="x",
    )

    assert published == 3
    assert FEED_PUBLISHED.labels(feed="darkpool")._value.get() == before + 3


def test_dedup_cache_has_hard_size_cap() -> None:
    """The in-process dedup cache is LRU-capped so a burst can't grow it unbounded
    between TTL cleanups (an evicted id just re-publishes once; Heber dedups it)."""
    from gateway.core.base_poller import DedupMixin

    mixin = DedupMixin()
    mixin._init_dedup(cache_ttl_seconds=3600)
    mixin._MAX_SEEN_IDS = 5  # shrink for the test

    for i in range(20):
        mixin._mark_seen(f"id{i}")

    assert len(mixin._seen_ids) == 5  # capped
    assert mixin._is_duplicate("id19")  # most-recent kept
    assert not mixin._is_duplicate("id0")  # oldest evicted


@pytest.mark.asyncio
async def test_eod_cancel_releases_running_claim(tmp_path) -> None:
    """A shutdown that cancels EOD mid-run must release the persistent 'running'
    claim so the next startup re-runs today's EOD (not deferred until stale)."""
    poller = UWPoller()
    poller._eod_state = UwEodStateStore(tmp_path / "eod.json", stale_after_seconds=3600)
    today = uw_poller_module.datetime.now(uw_poller_module.ET).strftime("%Y-%m-%d")

    assert poller._eod_state.claim(today) is True
    assert poller._eod_state.should_defer(today) is True  # running blocks re-run

    async def _cancelled(_sink) -> None:
        raise asyncio.CancelledError()

    poller._poll_eod_snapshots = _cancelled  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await poller._run_eod_with_logging(sink_registry=None, previous_eod_date=None)

    assert poller._eod_state.should_defer(today) is False  # claim released → retry allowed
