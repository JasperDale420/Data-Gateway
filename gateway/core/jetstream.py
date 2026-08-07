"""JetStream stream definitions and durable-outbox publication."""

import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

import nats
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError
from nats.js.manager import JetStreamManager

from gateway.config import Settings
from gateway.core.durable_outbox import OutboxEntry
from gateway.core.logger import logger
from gateway.core.metrics import set_durable_outbox_broker_connected

Connector = Callable[..., Awaitable[Any]]
_MAX_ASYNC_PUBACKS = 32
_UNSAFE_SUBJECT_TOKEN = re.compile(r"[^A-Za-z0-9_-]+")


def build_stream_configs(settings: Settings) -> tuple[StreamConfig, ...]:
    """Build the three bounded streams used by Gateway and Heber."""
    return (
        StreamConfig(
            name="HEBER_LIVE",
            subjects=["heber.live.>"],
            retention=RetentionPolicy.WORK_QUEUE,
            discard=DiscardPolicy.NEW,
            max_age=0,
            max_bytes=settings.jetstream_live_max_bytes,
            storage=StorageType.FILE,
            num_replicas=1,
            max_msg_size=settings.jetstream_max_message_bytes,
        ),
        StreamConfig(
            name="HEBER_BACKFILL",
            subjects=["heber.backfill.>"],
            retention=RetentionPolicy.WORK_QUEUE,
            discard=DiscardPolicy.NEW,
            max_age=0,
            max_bytes=settings.jetstream_backfill_max_bytes,
            storage=StorageType.FILE,
            num_replicas=1,
            max_msg_size=settings.jetstream_max_message_bytes,
        ),
        StreamConfig(
            name="HEBER_WATCH",
            subjects=["heber.watch.>"],
            retention=RetentionPolicy.WORK_QUEUE,
            discard=DiscardPolicy.NEW,
            max_age=0,
            max_bytes=settings.jetstream_watch_max_bytes,
            storage=StorageType.FILE,
            num_replicas=1,
            max_msg_size=settings.jetstream_max_message_bytes,
        ),
    )


async def ensure_streams(manager: JetStreamManager, configs: tuple[StreamConfig, ...]) -> None:
    """Create missing streams without mutating an existing stream contract."""
    for config in configs:
        try:
            await manager.stream_info(config.name or "")
        except NotFoundError:
            await manager.add_stream(config=config)


def subject_for_entry(entry: OutboxEntry) -> tuple[str, str]:
    """Map a legacy logical topic to one bounded JetStream work queue."""
    try:
        payload = json.loads(entry.payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    feed = payload.get("feed") if isinstance(payload, dict) else None
    token = _UNSAFE_SUBJECT_TOKEN.sub("_", str(feed or "unknown")).strip("_") or "unknown"
    if entry.topic == "heber:events:backfill":
        return f"heber.backfill.{token}", "HEBER_BACKFILL"
    if entry.topic == "heber:watch":
        return f"heber.watch.{token}", "HEBER_WATCH"
    return f"heber.live.{token}", "HEBER_LIVE"


class JetStreamOutboxPublisher:
    """Publish one outbox row and return only after the matching broker PubAck."""

    def __init__(self, settings: Settings, *, connector: Connector = nats.connect) -> None:
        self._settings = settings
        self._connector = connector
        self._connection: Any = None
        self._jetstream: Any = None
        self._connect_lock = asyncio.Lock()
        self._broker_connection = "not_connected"
        self._last_connection_error: str | None = None

    async def _on_disconnected(self) -> None:
        self._broker_connection = "disconnected"
        set_durable_outbox_broker_connected(False)
        logger.warning("jetstream_outbox_disconnected")

    async def _on_reconnected(self) -> None:
        self._broker_connection = "connected"
        self._last_connection_error = None
        set_durable_outbox_broker_connected(True)
        logger.info("jetstream_outbox_reconnected")

    async def _on_error(self, error: Exception) -> None:
        self._last_connection_error = f"{type(error).__name__}: {error}"
        logger.warning("jetstream_outbox_connection_error", error=self._last_connection_error)

    async def _connect(self) -> Any:
        if self._connection is not None and not self._connection.is_closed:
            return self._jetstream
        async with self._connect_lock:
            if self._connection is None or self._connection.is_closed:
                connection = await self._connector(
                    servers=self._settings.jetstream_url,
                    user=self._settings.jetstream_username,
                    password=self._settings.jetstream_password.get_secret_value(),
                    name="data-gateway-outbox",
                    disconnected_cb=self._on_disconnected,
                    reconnected_cb=self._on_reconnected,
                    error_cb=self._on_error,
                )
                jetstream = connection.jetstream(publish_async_max_pending=_MAX_ASYNC_PUBACKS)
                # nosemgrep: empire-no-bare-exception -- connection cleanup, not a handler: any stream-provisioning failure must close the half-open connection and reset broker state, then `raise` re-raises the original unchanged
                try:
                    await ensure_streams(jetstream, build_stream_configs(self._settings))
                except Exception:
                    with suppress(Exception):
                        await connection.close()
                    self._broker_connection = "disconnected"
                    set_durable_outbox_broker_connected(False)
                    raise
                self._connection = connection
                self._jetstream = jetstream
                self._broker_connection = "connected"
                self._last_connection_error = None
                set_durable_outbox_broker_connected(True)
        return self._jetstream

    def transport_status(self) -> dict[str, str | None]:
        """Return broker state observed through the NATS connection callbacks."""
        connection = self._connection
        state = "closed" if connection is not None and connection.is_closed else self._broker_connection
        return {
            "broker_connection": state,
            "last_connection_error": self._last_connection_error,
        }

    async def __call__(self, entry: OutboxEntry) -> bool:
        subject, stream = subject_for_entry(entry)
        jetstream = await self._connect()
        acknowledgement_future = await jetstream.publish_async(
            subject,
            entry.payload,
            stream=stream,
            headers={"Nats-Msg-Id": entry.event_id},
        )
        acknowledgement = await acknowledgement_future
        return acknowledgement.stream == stream

    async def verify_startup(self, lanes: str) -> None:
        """Connect, authenticate, provision, and verify selected streams."""
        jetstream = await self._connect()
        selected = {"HEBER_BACKFILL"}
        if lanes in {"live", "both"}:
            selected.add("HEBER_LIVE")
        # Flow watch is only used with a durable live writer, but provision and
        # validate it alongside that writer so dual admission cannot start half
        # configured.
        if lanes in {"live", "both"}:
            selected.add("HEBER_WATCH")
        expected = {config.name: config for config in build_stream_configs(self._settings)}
        for name in selected:
            info = await jetstream.stream_info(name)
            config = info.config
            wanted = expected[name]
            if (
                config.subjects != wanted.subjects
                or config.storage != wanted.storage
                or config.num_replicas != wanted.num_replicas
                or config.retention != wanted.retention
                or config.discard != wanted.discard
                or config.max_age != wanted.max_age
                or config.max_bytes != wanted.max_bytes
                or config.max_msg_size != wanted.max_msg_size
            ):
                raise RuntimeError(f"JetStream stream {name} does not match the durable transport contract")

    async def close(self) -> None:
        connection = self._connection
        self._connection = None
        self._jetstream = None
        self._broker_connection = "closed"
        set_durable_outbox_broker_connected(False)
        if connection is not None and not connection.is_closed:
            await connection.drain()
