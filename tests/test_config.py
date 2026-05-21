from __future__ import annotations

from gateway.config import Settings


def test_data_sink_defaults_cover_opening_bell_burst_capacity() -> None:
    settings = Settings(_env_file=None)

    assert settings.data_sink_queue_size == 16384
    assert settings.data_sink_worker_count == 16
    assert settings.data_sink_redis_pool_size == 32
