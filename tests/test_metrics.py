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


def test_stream_fanout_snapshot_tracks_updates() -> None:
    before = metrics.get_stream_fanout_snapshot()
    before_delivered = int(before["events"].get("delivered", 0))

    metrics.set_stream_fanout_limits_metrics(max_inflight=9, batch_size=4)
    metrics.record_stream_fanout_batch_size(4)
    metrics.record_stream_fanout_batch_size(2)
    metrics.record_stream_fanout_dispatch_event("delivered")

    after = metrics.get_stream_fanout_snapshot()
    assert after["limits"]["max_inflight"] == 9
    assert after["limits"]["batch_size"] == 4
    assert after["batches"]["count"] >= 2
    assert after["batches"]["total_clients"] >= 6
    assert after["batches"]["max_batch_size"] >= 4
    assert int(after["events"].get("delivered", 0)) == before_delivered + 1


def test_stream_sink_dispatch_snapshot_includes_derived_calibration_metrics() -> None:
    before = metrics.get_stream_sink_dispatch_snapshot()
    before_scheduled = int(before["events"].get("scheduled", 0))
    before_completed = int(before["events"].get("completed", 0))
    before_dropped = int(before["events"].get("dropped_backpressure", 0))

    metrics.set_stream_sink_dispatch_limits_metrics(max_inflight_publish=5, max_pending_tasks=20)
    metrics.set_stream_sink_pending_tasks(10)
    metrics.record_stream_sink_dispatch_event("scheduled")
    metrics.record_stream_sink_dispatch_event("scheduled")
    metrics.record_stream_sink_dispatch_event("completed")
    metrics.record_stream_sink_dispatch_event("dropped_backpressure")

    after = metrics.get_stream_sink_dispatch_snapshot()
    derived = after["derived"]

    expected_scheduled = before_scheduled + 2
    expected_completed = before_completed + 1
    expected_dropped = before_dropped + 1
    expected_completion_rate = expected_completed / expected_scheduled
    expected_drop_rate = expected_dropped / expected_scheduled

    assert derived["pending_utilization"] == 0.5
    assert derived["completion_rate"] == expected_completion_rate
    assert derived["drop_rate"] == expected_drop_rate


def test_stream_fanout_snapshot_includes_derived_calibration_metrics() -> None:
    before = metrics.get_stream_fanout_snapshot()
    before_count = int(before["batches"].get("count", 0))
    before_total_clients = int(before["batches"].get("total_clients", 0))
    before_delivered = int(before["events"].get("delivered", 0))
    before_error = int(before["events"].get("error", 0))

    metrics.set_stream_fanout_limits_metrics(max_inflight=9, batch_size=6)
    metrics.record_stream_fanout_batch_size(6)
    metrics.record_stream_fanout_batch_size(3)
    metrics.record_stream_fanout_dispatch_event("delivered")
    metrics.record_stream_fanout_dispatch_event("delivered")
    metrics.record_stream_fanout_dispatch_event("error")

    after = metrics.get_stream_fanout_snapshot()
    derived = after["derived"]

    expected_count = before_count + 2
    expected_total_clients = before_total_clients + 9
    expected_delivered = before_delivered + 2
    expected_error = before_error + 1
    expected_avg_batch_size = expected_total_clients / expected_count
    expected_error_rate = expected_error / (expected_delivered + expected_error)

    assert derived["avg_batch_size"] == expected_avg_batch_size
    assert derived["batch_fill_ratio"] == expected_avg_batch_size / 6
    assert derived["error_rate"] == expected_error_rate


def test_provider_health_check_snapshot_tracks_updates_and_derivatives() -> None:
    before = metrics.get_provider_health_check_snapshot()
    before_ok = before.get("ok", {})
    before_ok_total = int(before_ok.get("count", 0))
    before_ok_success = int(before_ok.get("success_count", 0))
    before_ok_error = int(before_ok.get("error_count", 0))

    metrics.record_provider_health_check("ok", healthy=True, duration=0.2)
    metrics.record_provider_health_check("ok", healthy=False, duration=0.1)

    after = metrics.get_provider_health_check_snapshot()
    ok = after["ok"]
    derived = ok["derived"]

    assert int(ok["count"]) == before_ok_total + 2
    assert int(ok["success_count"]) == before_ok_success + 1
    assert int(ok["error_count"]) == before_ok_error + 1
    assert float(ok["last_duration_seconds"]) == 0.1
    assert derived["avg_duration_seconds"] == ok["total_duration_seconds"] / ok["count"]
    assert derived["error_rate"] == ok["error_count"] / ok["count"]


def test_record_provider_quote_batch_size_accepts_non_negative_values() -> None:
    metrics.record_provider_quote_batch_size("alphavantage", 5)
    metrics.record_provider_quote_batch_size("finnhub", 0)
    metrics.record_provider_quote_batch_size("finnhub", -3)


def test_provider_quote_batch_snapshot_tracks_updates_and_derivatives() -> None:
    metrics.record_provider_quote_batch_size("alpaca", 10)
    metrics.record_provider_quote_batch_size("alpaca", 20)
    metrics.record_provider_quote_batch_size("alpaca", 5)

    snapshot = metrics.get_provider_quote_batch_snapshot()
    alpaca = snapshot["alpaca"]
    derived = alpaca["derived"]

    assert alpaca["count"] >= 3
    assert alpaca["total_symbols"] >= 35
    assert alpaca["max_batch_size"] >= 20
    assert derived["avg_batch_size"] == alpaca["total_symbols"] / alpaca["count"]


def test_provider_quote_batch_snapshot_includes_calibration_guidance() -> None:
    metrics.record_provider_quote_batch_size("finnhub", 100)
    metrics.record_provider_quote_batch_size("finnhub", 120)

    snapshot = metrics.get_provider_quote_batch_snapshot()
    derived = snapshot["finnhub"]["derived"]

    assert derived["batch_level"] in {"warning", "critical"}
    assert any("max_symbols" in hint for hint in derived["recommendations"])


def test_stream_sink_dispatch_snapshot_includes_calibration_guidance() -> None:
    before = metrics.get_stream_sink_dispatch_snapshot()
    before_scheduled = int(before["events"].get("scheduled", 0))
    before_completed = int(before["events"].get("completed", 0))

    metrics.set_stream_sink_dispatch_limits_metrics(max_inflight_publish=8, max_pending_tasks=100)
    metrics.set_stream_sink_pending_tasks(95)
    metrics.record_stream_sink_dispatch_event("scheduled")
    metrics.record_stream_sink_dispatch_event("dropped_backpressure")

    snapshot = metrics.get_stream_sink_dispatch_snapshot()
    derived = snapshot["derived"]
    expected_completion_rate = (before_completed) / (before_scheduled + 1)

    assert derived["backpressure_level"] in {"warning", "critical"}
    assert derived["completion_gap"] == max(0.0, 1.0 - expected_completion_rate)
    assert any("max_pending_tasks" in hint for hint in derived["recommendations"])


def test_stream_fanout_snapshot_includes_calibration_guidance() -> None:
    metrics.set_stream_fanout_limits_metrics(max_inflight=32, batch_size=4)
    metrics.record_stream_fanout_batch_size(4)
    metrics.record_stream_fanout_dispatch_event("delivered")
    metrics.record_stream_fanout_dispatch_event("error")

    snapshot = metrics.get_stream_fanout_snapshot()
    derived = snapshot["derived"]

    assert derived["fanout_level"] in {"warning", "critical"}
    assert any("stream_fanout_max_inflight" in hint for hint in derived["recommendations"])


def test_provider_health_check_snapshot_includes_calibration_guidance() -> None:
    metrics.record_provider_health_check("slow_provider", healthy=False, duration=1.5)
    snapshot = metrics.get_provider_health_check_snapshot()
    derived = snapshot["slow_provider"]["derived"]

    assert derived["health_level"] in {"warning", "critical"}
    assert derived["latency_level"] in {"warning", "critical"}
    assert any("provider" in hint.lower() for hint in derived["recommendations"])
