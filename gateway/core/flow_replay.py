"""Durable Redis replay log for UW flow WebSocket delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import orjson

from gateway.core.logger import logger


@dataclass(frozen=True)
class ReplayEntry:
    """One persisted flow envelope and its Redis stream cursor."""

    cursor: str
    envelope: dict[str, Any]


class FlowReplayStore(Protocol):
    """Storage boundary used by :class:`FlowFanout`."""

    @property
    def available(self) -> bool: ...

    async def append(self, envelope: dict[str, Any]) -> str: ...

    async def high_watermark(self) -> str: ...

    async def read(self, after: str, through: str) -> list[ReplayEntry]: ...

    async def close(self) -> None: ...


class RedisFlowReplayStore:
    """Append-only, bounded flow replay stream backed by Redis."""

    def __init__(self, redis_url: str, *, stream: str, max_len: int, max_replay_events: int) -> None:
        self._redis_url = redis_url
        self._stream = stream
        self._max_len = max_len
        self._max_replay_events = max_replay_events
        self._redis: Any = None
        self._healthy = False

    @property
    def available(self) -> bool:
        return self._healthy and self._redis is not None

    async def initialize(self) -> bool:
        """Connect and prove Redis is usable before advertising replay."""
        # nosemgrep: empire-no-bare-exception -- probe boundary: any failure marks the store unavailable; logged with exc_info
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(self._redis_url, decode_responses=False)
            await self._redis.ping()
            self._healthy = True
            logger.info("flow_replay_store_ready", stream=self._stream, max_len=self._max_len)
            return True
        except Exception:
            self._healthy = False
            logger.error("flow_replay_store_unavailable", stream=self._stream, exc_info=True)
            return False

    async def append(self, envelope: dict[str, Any]) -> str:
        if not self.available:
            raise RuntimeError("flow replay store is unavailable")
        # nosemgrep: empire-no-bare-exception -- append failure disables the store and re-raises; logged with exc_info
        try:
            cursor = await self._redis.xadd(
                self._stream,
                {b"envelope": orjson.dumps(envelope, default=str)},
                maxlen=self._max_len,
                approximate=True,
            )
        except Exception:
            self._healthy = False
            logger.error(
                "flow_replay_append_failed_entries_disabled",
                event_id=envelope.get("event_id"),
                exc_info=True,
            )
            raise
        return cursor.decode() if isinstance(cursor, bytes) else str(cursor)

    async def high_watermark(self) -> str:
        if not self.available:
            raise RuntimeError("flow replay store is unavailable")
        latest = await self._redis.xrevrange(self._stream, count=1)
        if not latest:
            return "0-0"
        cursor = latest[0][0]
        return cursor.decode() if isinstance(cursor, bytes) else str(cursor)

    async def read(self, after: str, through: str) -> list[ReplayEntry]:
        if not self.available:
            raise RuntimeError("flow replay store is unavailable")
        if after == "$" or after == through:
            return []
        stream_info = await self._redis.xinfo_stream(self._stream)
        first_entry = stream_info.get(b"first-entry") or stream_info.get("first-entry")
        if first_entry:
            raw_first_cursor = first_entry[0]
            first_cursor = raw_first_cursor.decode() if isinstance(raw_first_cursor, bytes) else str(raw_first_cursor)
            if _cursor_parts(after) < _cursor_parts(first_cursor):
                raise RuntimeError(
                    f"requested cursor {after} is older than retained history starting at {first_cursor}"
                )
        rows = await self._redis.xrange(
            self._stream,
            min=f"({after}",
            max=through,
            count=self._max_replay_events + 1,
        )
        if len(rows) > self._max_replay_events:
            raise RuntimeError(
                f"flow replay exceeds safety limit of {self._max_replay_events} events; entries remain disabled"
            )
        entries: list[ReplayEntry] = []
        for raw_cursor, fields in rows:
            cursor = raw_cursor.decode() if isinstance(raw_cursor, bytes) else str(raw_cursor)
            raw_envelope = fields.get(b"envelope") or fields.get("envelope")
            if raw_envelope is None:
                raise RuntimeError(f"flow replay entry {cursor} has no envelope")
            envelope = orjson.loads(raw_envelope)
            if not isinstance(envelope, dict):
                raise RuntimeError(f"flow replay entry {cursor} is malformed")
            entries.append(ReplayEntry(cursor=cursor, envelope=envelope))
        return entries

    async def close(self) -> None:
        client = self._redis
        self._redis = None
        self._healthy = False
        if client is not None:
            await client.aclose()


def _cursor_parts(cursor: str) -> tuple[int, int]:
    try:
        milliseconds, sequence = cursor.split("-", 1)
        return int(milliseconds), int(sequence)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid Redis stream cursor: {cursor!r}") from exc
