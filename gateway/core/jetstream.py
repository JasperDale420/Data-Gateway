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

Connector = Callable[..., Awaitable[Any]]
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
            retention=RetentionPolicy.LIMITS,
            discard=DiscardPolicy.OLD,
            max_age=float(settings.jetstream_watch_max_age_seconds),
            max_bytes=settings.jetstream_watch_max_bytes,
            storage=StorageType.FILE,
            num_replicas=1,
            max_msg_size=settings.jetstream_max_message_bytes,
        ),
    )


async def ensure_streams(manager: JetStreamManager, configs: tuple[StreamConfig, ...]) -> None:
    """Create missing streams and enforce current settings on existing streams."""
    for config in configs:
        try:
            await manager.stream_info(config.name or "")
        except NotFoundError:
            await manager.add_stream(config=config)
        else:
            await manager.update_stream(config=config)


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
    return f"heber.live.{token}", "HEBER_LIVE"


class JetStreamOutboxPublisher:
    """Publish one outbox row and return only after the matching broker PubAck."""

    def __init__(self, settings: Settings, *, connector: Connector = nats.connect) -> None:
        self._settings = settings
        self._connector = connector
        self._connection: Any = None
        self._jetstream: Any = None
        self._connect_lock = asyncio.Lock()

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
                )
                jetstream = connection.jetstream()
                try:
                    await ensure_streams(jetstream, build_stream_configs(self._settings))
                except Exception:
                    with suppress(Exception):
                        await connection.close()
                    raise
                self._connection = connection
                self._jetstream = jetstream
        return self._jetstream

    async def __call__(self, entry: OutboxEntry) -> bool:
        subject, stream = subject_for_entry(entry)
        jetstream = await self._connect()
        acknowledgement = await jetstream.publish(
            subject,
            entry.payload,
            stream=stream,
            headers={"Nats-Msg-Id": entry.event_id},
        )
        return acknowledgement.stream == stream

    async def close(self) -> None:
        connection = self._connection
        self._connection = None
        self._jetstream = None
        if connection is not None and not connection.is_closed:
            await connection.drain()
