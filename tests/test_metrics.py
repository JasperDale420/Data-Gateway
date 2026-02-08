"""Tests for metrics helpers."""

import gateway.core.metrics as metrics
from gateway.api import metrics as metrics_api


def test_normalize_path_uses_expected_placeholders() -> None:
    path = "/api/v1/alpaca/bars/AAPL/123456789/2025-01-01"
    normalized = metrics._normalize_path(path)
    assert normalized == "/api/v1/alpaca/bars/{symbol}/{id}/{id}"


def test_normalize_path_cache_is_bounded() -> None:
    metrics._PATH_NORMALIZATION_CACHE.clear()

    max_size = metrics._PATH_NORMALIZATION_CACHE_MAX
    for i in range(max_size + 20):
        metrics._normalize_path(f"/api/v1/finnhub/quote/SYM{i:05d}")

    assert len(metrics._PATH_NORMALIZATION_CACHE) <= max_size


def test_update_memory_metrics_if_due_is_throttled(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_update() -> None:
        calls["count"] += 1

    monkeypatch.setattr(metrics, "update_memory_metrics", _fake_update)
    monkeypatch.setattr(metrics, "_LAST_MEMORY_METRICS_UPDATE_MONOTONIC", 0.0)

    assert metrics.update_memory_metrics_if_due(now_monotonic=100.0) is True
    assert metrics.update_memory_metrics_if_due(now_monotonic=105.0) is False
    assert metrics.update_memory_metrics_if_due(now_monotonic=111.0) is True
    assert calls["count"] == 2


def test_update_memory_metrics_if_due_force_bypasses_throttle(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_update() -> None:
        calls["count"] += 1

    monkeypatch.setattr(metrics, "update_memory_metrics", _fake_update)
    monkeypatch.setattr(metrics, "_LAST_MEMORY_METRICS_UPDATE_MONOTONIC", 100.0)

    assert metrics.update_memory_metrics_if_due(force=True, now_monotonic=101.0) is True
    assert calls["count"] == 1


async def test_metrics_endpoint_calls_throttled_updater(monkeypatch) -> None:
    called = {"update": 0}

    def _fake_update(*, force: bool = False, now_monotonic: float | None = None) -> bool:
        called["update"] += 1
        assert force is False
        assert now_monotonic is None
        return True

    monkeypatch.setattr(metrics_api, "update_memory_metrics_if_due", _fake_update)
    monkeypatch.setattr(metrics_api, "generate_latest", lambda: b"# test 1\n")
    monkeypatch.setattr(metrics_api, "CONTENT_TYPE_LATEST", "text/plain; version=0.0.4")

    response = await metrics_api.get_metrics()

    assert called["update"] == 1
    assert response.body == b"# test 1\n"
    assert response.media_type == "text/plain; version=0.0.4"


def test_stream_sink_dispatch_snapshot_tracks_updates() -> None:
    before = metrics.get_stream_sink_dispatch_snapshot()
    before_scheduled = int(before["events"].get("scheduled", 0))
    before_completed = int(before["events"].get("completed", 0))

    metrics.set_stream_sink_dispatch_limits_metrics(max_inflight_publish=7, max_pending_tasks=21)
    metrics.set_stream_sink_pending_tasks(5)
    metrics.record_stream_sink_dispatch_event("scheduled")
    metrics.record_stream_sink_dispatch_event("completed")

    after = metrics.get_stream_sink_dispatch_snapshot()
    assert after["limits"]["max_inflight_publish"] == 7
    assert after["limits"]["max_pending_tasks"] == 21
    assert after["pending_tasks"] == 5
    assert int(after["events"].get("scheduled", 0)) == before_scheduled + 1
    assert int(after["events"].get("completed", 0)) == before_completed + 1
