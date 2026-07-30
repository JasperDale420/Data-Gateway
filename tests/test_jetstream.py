from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from nats.js.errors import NotFoundError
from pydantic import ValidationError

from gateway.config import Settings
from gateway.core.durable_outbox import OutboxEntry
from gateway.core.jetstream import (
    JetStreamOutboxPublisher,
    build_stream_configs,
    ensure_streams,
    subject_for_entry,
)


def test_jetstream_settings_are_bounded_and_disabled_by_default() -> None:
    settings = Settings(_env_file=None)

    assert settings.jetstream_enabled is False
    assert settings.jetstream_lanes == "backfill"
    assert settings.jetstream_url == "nats://jetstream:4222"
    assert settings.jetstream_live_max_bytes == 256 * 1024**3
    assert settings.jetstream_backfill_max_bytes == 256 * 1024**3
    assert settings.jetstream_watch_max_bytes == 8 * 1024**3
    assert settings.jetstream_max_message_bytes == 8 * 1024**2
    assert settings.durable_outbox_enabled is False
    assert settings.durable_outbox_max_bytes == 20 * 1024**3
    assert settings.backfill_max_chunk_records == 5_000

    with pytest.raises(ValidationError):
        Settings(_env_file=None, jetstream_live_max_bytes=0)
    with pytest.raises(ValidationError, match="JetStream requires"):
        Settings(_env_file=None, jetstream_enabled=True)
    with pytest.raises(ValidationError, match="durable outbox"):
        Settings(
            _env_file=None,
            jetstream_enabled=True,
            jetstream_username="gateway",
            jetstream_password="secret",  # pragma: allowlist secret
            GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:6379/0",
        )
    with pytest.raises(ValidationError):
        Settings(_env_file=None, jetstream_lanes="invalid")
    with pytest.raises(ValidationError, match="data sink"):
        Settings(
            _env_file=None,
            durable_outbox_enabled=True,
            jetstream_enabled=True,
            jetstream_username="gateway",
            jetstream_password="secret",  # pragma: allowlist secret
            GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:6379/0",
            data_sink_enabled=False,
        )

    enabled = Settings(
        _env_file=None,
        data_sink_enabled=True,
        durable_outbox_enabled=True,
        GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:6379/0",
        jetstream_enabled=True,
        jetstream_username="gateway",
        jetstream_password="secret",  # pragma: allowlist secret
    )
    assert enabled.jetstream_password.get_secret_value() == "secret"  # pragma: allowlist secret

    with pytest.raises(ValidationError, match="durable outbox requires JetStream"):
        Settings(_env_file=None, durable_outbox_enabled=True)
    with pytest.raises(ValidationError, match="data sink"):
        Settings(
            _env_file=None,
            durable_outbox_enabled=True,
            jetstream_enabled=True,
            jetstream_username="gateway",
            jetstream_password="secret",  # pragma: allowlist secret
            GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:6379/0",
            data_sink_enabled=False,
        )
    with pytest.raises(ValidationError, match="Redis control-plane URL"):
        Settings(
            _env_file=None,
            durable_outbox_enabled=True,
            jetstream_enabled=True,
            jetstream_username="gateway",
            jetstream_password="secret",  # pragma: allowlist secret
            GATEWAY_DATA_SINK_REDIS_URL="",
        )


def test_build_stream_configs_separates_authoritative_and_watch_traffic() -> None:
    settings = Settings(
        _env_file=None,
        jetstream_live_max_bytes=10_000,
        jetstream_backfill_max_bytes=20_000,
        jetstream_watch_max_bytes=3_000,
        jetstream_max_message_bytes=1_024,
    )

    configs = {config.name: config for config in build_stream_configs(settings)}

    assert set(configs) == {"HEBER_LIVE", "HEBER_BACKFILL", "HEBER_WATCH"}
    assert configs["HEBER_LIVE"].subjects == ["heber.live.>"]
    assert configs["HEBER_LIVE"].retention.value == "workqueue"
    assert configs["HEBER_LIVE"].discard.value == "new"
    assert configs["HEBER_LIVE"].max_age == 0
    assert configs["HEBER_LIVE"].max_bytes == 10_000
    assert configs["HEBER_LIVE"].max_msg_size == 1_024

    assert configs["HEBER_BACKFILL"].subjects == ["heber.backfill.>"]
    assert configs["HEBER_BACKFILL"].retention.value == "workqueue"
    assert configs["HEBER_BACKFILL"].discard.value == "new"
    assert configs["HEBER_BACKFILL"].max_age == 0
    assert configs["HEBER_BACKFILL"].max_bytes == 20_000

    assert configs["HEBER_WATCH"].subjects == ["heber.watch.>"]
    assert configs["HEBER_WATCH"].retention.value == "limits"
    assert configs["HEBER_WATCH"].discard.value == "old"
    assert configs["HEBER_WATCH"].max_age == 86_400
    assert configs["HEBER_WATCH"].max_bytes == 3_000


async def test_ensure_streams_creates_missing_streams_and_updates_existing_ones() -> None:
    manager = AsyncMock()
    manager.stream_info.side_effect = [NotFoundError(), object(), object()]
    configs = build_stream_configs(Settings(_env_file=None))

    await ensure_streams(manager, configs)

    assert manager.stream_info.await_count == 3
    manager.add_stream.assert_awaited_once_with(config=configs[0])
    assert [call.kwargs["config"] for call in manager.update_stream.await_args_list] == list(configs[1:])


def _entry(topic: str, feed: str = "trade/condition") -> OutboxEntry:
    return OutboxEntry(
        sequence=1,
        topic=topic,
        event_id="evt-1",
        payload=f'{{"event_id":"evt-1","feed":"{feed}"}}'.encode(),
        payload_digest="digest",
        attempts=0,
        last_error=None,
        created_at="2026-07-29T00:00:00Z",
    )


def test_subject_mapping_keeps_live_and_backfill_work_queues_separate() -> None:
    assert subject_for_entry(_entry("heber:events")) == ("heber.live.trade_condition", "HEBER_LIVE")
    assert subject_for_entry(_entry("heber:events:backfill")) == (
        "heber.backfill.trade_condition",
        "HEBER_BACKFILL",
    )


async def test_outbox_publisher_waits_for_matching_jetstream_puback() -> None:
    jetstream = AsyncMock()
    jetstream.publish.return_value = SimpleNamespace(stream="HEBER_LIVE")
    connection = SimpleNamespace(
        is_closed=False,
        jetstream=lambda: jetstream,
        drain=AsyncMock(),
    )
    connector = AsyncMock(return_value=connection)
    settings = Settings(
        _env_file=None,
        data_sink_enabled=True,
        durable_outbox_enabled=True,
        GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:6379/0",
        jetstream_enabled=True,
        jetstream_username="gateway",
        jetstream_password="secret",  # pragma: allowlist secret
    )
    publisher = JetStreamOutboxPublisher(settings, connector=connector)
    entry = _entry("heber:events", feed="trades")

    assert await publisher(entry) is True

    connector.assert_awaited_once()
    jetstream.publish.assert_awaited_once_with(
        "heber.live.trades",
        entry.payload,
        stream="HEBER_LIVE",
        headers={"Nats-Msg-Id": "evt-1"},
    )
    await publisher.close()


async def test_stream_bootstrap_failure_discards_connection_before_retry() -> None:
    failing_jetstream = AsyncMock()
    failing_jetstream.stream_info.side_effect = RuntimeError("permission denied")
    failing_connection = SimpleNamespace(
        is_closed=False,
        jetstream=lambda: failing_jetstream,
        close=AsyncMock(),
    )
    healthy_jetstream = AsyncMock()
    healthy_jetstream.publish.return_value = SimpleNamespace(stream="HEBER_LIVE")
    healthy_connection = SimpleNamespace(
        is_closed=False,
        jetstream=lambda: healthy_jetstream,
        drain=AsyncMock(),
    )
    connector = AsyncMock(side_effect=[failing_connection, healthy_connection])
    settings = Settings(
        _env_file=None,
        data_sink_enabled=True,
        durable_outbox_enabled=True,
        GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:6379/0",
        jetstream_enabled=True,
        jetstream_username="gateway",
        jetstream_password="secret",  # pragma: allowlist secret
    )
    publisher = JetStreamOutboxPublisher(settings, connector=connector)
    entry = _entry("heber:events", feed="trades")

    with pytest.raises(RuntimeError, match="permission denied"):
        await publisher(entry)
    assert await publisher(entry) is True

    assert connector.await_count == 2
    failing_connection.close.assert_awaited_once()
    await publisher.close()


def test_compose_jetstream_is_authenticated_internal_and_host_persisted() -> None:
    compose = yaml.safe_load(Path("docker-compose.yml").read_text())
    service = compose["services"]["jetstream"]
    server_config = compose["configs"]["jetstream-server"]["content"]

    assert service["image"] == "nats:2.14.3-alpine"
    assert service["profiles"] == ["jetstream"]
    assert service["ports"] == ["127.0.0.1:4222:4222"]
    assert "./state/jetstream:/data" in service["volumes"]
    assert "data-gateway-outbox:/app/state/outbox" in compose["services"]["gateway"]["volumes"]
    assert "data-gateway-outbox" in compose["volumes"]
    assert "sync_interval: always" in server_config
    assert "max_payload: 8388608" in server_config
    assert "user: $$NATS_USER" in server_config
    assert "password: $$NATS_PASSWORD" in server_config
    gateway_environment = compose["services"]["gateway"]["environment"]
    assert "GATEWAY_JETSTREAM_USERNAME=${GATEWAY_JETSTREAM_USERNAME:-}" in gateway_environment
    assert "GATEWAY_JETSTREAM_PASSWORD=${GATEWAY_JETSTREAM_PASSWORD:-}" in gateway_environment
    assert "GATEWAY_JETSTREAM_LANES=${GATEWAY_JETSTREAM_LANES:-backfill}" in gateway_environment
    assert "GATEWAY_DURABLE_OUTBOX_PATH=${GATEWAY_DURABLE_OUTBOX_PATH:-/app/state/outbox/events.sqlite3}" in (
        gateway_environment
    )
    assert "GATEWAY_DURABLE_OUTBOX_MAX_BYTES=${GATEWAY_DURABLE_OUTBOX_MAX_BYTES:-21474836480}" in (gateway_environment)
    assert "GATEWAY_DURABLE_OUTBOX_RETRY_SECONDS=${GATEWAY_DURABLE_OUTBOX_RETRY_SECONDS:-1.0}" in (gateway_environment)
