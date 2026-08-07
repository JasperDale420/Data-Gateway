import os
from pathlib import Path

import pytest

from gateway.config import Settings
from gateway.core.backfill_manifest import RedisBackfillManifestStore
from gateway.main import _initialize_data_sink


@pytest.fixture(autouse=True)
def _isolate_gateway_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip GATEWAY_* out of the process environment for this module.

    ``gateway.main`` calls ``load_dotenv()`` at import time, so the developer's
    ``.env`` is already in ``os.environ`` by the time a test runs. ``_env_file=None``
    only disables pydantic-settings' own dotenv read, not the environment, so
    without this fixture these tests assert against local config instead of the
    declared defaults.
    """
    for key in list(os.environ):
        if key.startswith("GATEWAY_"):
            monkeypatch.delenv(key, raising=False)


@pytest.mark.asyncio
async def test_durable_transport_uses_redis_only_for_backfill_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    async def verify(self, lanes: str) -> None:
        assert lanes == "backfill"

    monkeypatch.setattr("gateway.core.jetstream.JetStreamOutboxPublisher.verify_startup", verify)
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


@pytest.mark.asyncio
async def test_durable_transport_verifies_jetstream_before_exposing_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(
        _env_file=None,
        data_sink_enabled=True,
        durable_outbox_enabled=True,
        durable_outbox_path=tmp_path / "events.sqlite3",
        GATEWAY_DATA_SINK_REDIS_URL="redis://redis:6379/0",
        jetstream_enabled=True,
        jetstream_lanes="both",
        jetstream_username="gateway",
        jetstream_password="secret",  # pragma: allowlist secret
    )
    verified: list[str] = []

    async def verify(self, lanes: str) -> None:
        verified.append(lanes)

    monkeypatch.setattr("gateway.core.jetstream.JetStreamOutboxPublisher.verify_startup", verify)

    registry, publisher, _manifest = await _initialize_data_sink(settings)

    assert verified == ["both"]
    assert registry is not None
    assert publisher is not None
    await registry.close_all()
    await publisher.close()


@pytest.mark.asyncio
async def test_startup_cleanup_failure_does_not_mask_the_verification_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing cleanup step must not skip the next one or replace the real cause.

    ``verify_startup()`` failing is what explains a bad startup — a stream
    contract or auth problem. If closing the outbox also fails, that secondary
    error must not surface in its place, and the publisher must still be closed
    rather than left half-open.
    """
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
    publisher_closed: list[bool] = []

    async def verify(self, lanes: str) -> None:
        raise RuntimeError("jetstream stream contract mismatch")

    async def failing_close(self) -> None:
        raise RuntimeError("outbox close failed")

    async def record_publisher_close(self) -> None:
        publisher_closed.append(True)

    monkeypatch.setattr("gateway.core.jetstream.JetStreamOutboxPublisher.verify_startup", verify)
    monkeypatch.setattr("gateway.core.durable_outbox_sink.DurableOutboxSink.close", failing_close)
    monkeypatch.setattr("gateway.core.jetstream.JetStreamOutboxPublisher.close", record_publisher_close)

    with pytest.raises(RuntimeError, match="jetstream stream contract mismatch"):
        await _initialize_data_sink(settings)

    assert publisher_closed == [True]
