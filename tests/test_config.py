from __future__ import annotations

from gateway.config import Settings


def test_data_sink_defaults_cover_opening_bell_burst_capacity() -> None:
    settings = Settings(_env_file=None)

    assert settings.data_sink_queue_size == 16384
    assert settings.data_sink_worker_count == 16
    assert settings.data_sink_redis_pool_size == 32


def test_rest_sink_excluded_feed_defaults_cover_heavy_uw_feeds() -> None:
    settings = Settings(_env_file=None)

    excluded_feeds = settings.rest_sink_excluded_feed_set

    assert "greek_exposure" in excluded_feeds
    assert "flow_alerts" in excluded_feeds
    assert "darkpool" in excluded_feeds


def test_data_sink_dedup_cache_uses_bounded_redis_pool_settings() -> None:
    from gateway.main import _create_data_sink_dedup_cache

    settings = Settings(
        _env_file=None,
        GATEWAY_DATA_SINK_REDIS_URL="redis://redis:6379",
        data_sink_redis_pool_size=24,
        data_sink_operation_timeout_seconds=2.5,
    )

    cache = _create_data_sink_dedup_cache(settings)

    assert cache._redis_url == "redis://redis:6379"
    assert cache._max_connections == 24
    assert cache._operation_timeout_seconds == 2.5
