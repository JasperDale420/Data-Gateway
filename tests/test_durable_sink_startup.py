from pathlib import Path

import pytest

from gateway.config import Settings
from gateway.core.backfill_manifest import RedisBackfillManifestStore
from gateway.main import _initialize_data_sink


@pytest.mark.asyncio
async def test_durable_transport_uses_redis_only_for_backfill_control(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        data_sink_enabled=True,
        durable_outbox_enabled=True,
        durable_outbox_path=tmp_path / "events.sqlite3",
        GATEWAY_DATA_SINK_REDIS_URL="redis://redis:6379/0",
        jetstream_enabled=True,
        jetstream_username="gateway",
        jetstream_password="secret",  # pragma: allowlist secret
    )

    registry, publisher, manifest = await _initialize_data_sink(settings)

    assert registry is not None
    assert registry.has_durable_admission is False
    assert registry.has_durable_admission_for("heber:events") is False
    assert registry.has_durable_admission_for("heber:events:backfill") is True
    assert registry._dedup_cache is not None
    assert publisher is not None
    assert isinstance(manifest, RedisBackfillManifestStore)

    await registry.close_all()
    await publisher.close()
