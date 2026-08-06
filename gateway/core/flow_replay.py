"""Durable Redis replay log for UW flow WebSocket delivery."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import orjson

from gateway.core.logger import logger


@dataclass(frozen=True)
class ReplayEntry:
    """One persisted flow envelope and its Redis stream cursor."""

    cursor: str
    envelope: dict[str, Any]


class ReplayHistoryTrimmedError(RuntimeError):
    """The requested cursor precedes retained history — resume can never succeed."""


class ReplayGapError(RuntimeError):
    """A durability gap lies inside the requested range — resume can never succeed."""


class FlowReplayStore(Protocol):
    """Storage boundary used by :class:`FlowFanout`."""

    @property
    def available(self) -> bool: ...

    async def ensure_available(self) -> bool: ...

    async def append(self, envelope: dict[str, Any]) -> str: ...

    async def high_watermark(self) -> str: ...

    async def read(self, after: str, through: str) -> list[ReplayEntry]: ...

    async def close(self) -> None: ...


class RedisFlowReplayStore:
    """Append-only, bounded flow replay stream backed by Redis."""

    _REPROBE_COOLDOWN_SECONDS = 5.0

    def __init__(
        self,
        redis_url: str,
        *,
        stream: str,
        max_len: int,
        max_replay_events: int,
        clock: Callable[[], float] | None = None,
        gap_sentinel_path: Path | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._stream = stream
        self._max_len = max_len
        self._max_replay_events = max_replay_events
        self._redis: Any = None
        self._healthy = False
        self._clock = clock or time.monotonic
        self._reprobe_not_before = 0.0
        self._probe_lock: asyncio.Lock | None = None
        # True when at least one primary-published event could not be appended
        # to the replay stream — a durability gap that a resuming reader must
        # not silently cross. Bounded durably by a marker entry on recovery.
        # The sentinel file (on the persistent outbox volume) carries the
        # latch across process restarts: the failure moment is exactly when
        # Redis cannot record it, so local disk is the durable medium.
        self._gap_sentinel_path = gap_sentinel_path
        self._gap_pending = bool(gap_sentinel_path is not None and gap_sentinel_path.exists())

    def _set_gap_pending(self, pending: bool) -> None:
        self._gap_pending = pending
        if self._gap_sentinel_path is None:
            return
        try:
            if pending:
                self._gap_sentinel_path.parent.mkdir(parents=True, exist_ok=True)
                self._gap_sentinel_path.touch()
                # fsync the file and its directory so the latch survives a
                # crash, not just a clean restart — the sentinel exists
                # precisely because Redis cannot record the gap right now.
                fd = os.open(self._gap_sentinel_path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                dir_fd = os.open(self._gap_sentinel_path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            else:
                self._gap_sentinel_path.unlink(missing_ok=True)
        except OSError:
            if pending:
                # The in-memory latch still guarantees a marker within this
                # process; only the restart window is exposed. Escalate loudly
                # — this means the persistent volume itself is failing.
                logger.critical(
                    "flow_replay_gap_sentinel_write_failed",
                    sentinel=str(self._gap_sentinel_path),
                    exc_info=True,
                )
            else:
                logger.error(
                    "flow_replay_gap_sentinel_clear_failed",
                    sentinel=str(self._gap_sentinel_path),
                    exc_info=True,
                )

    @property
    def available(self) -> bool:
        return self._healthy and self._redis is not None

    async def initialize(self) -> bool:
        """Connect and prove Redis is usable before advertising replay.

        Candidate-before-swap: the current client (if any) is only replaced —
        and then closed — after the new connection proves usable, so a failed
        probe cannot destroy a working client or leak the candidate.
        """
        candidate: Any = None
        # nosemgrep: empire-no-bare-exception -- probe boundary: any failure marks the store unavailable; logged with exc_info
        try:
            import redis.asyncio as aioredis

            candidate = aioredis.from_url(self._redis_url, decode_responses=False)
            await candidate.ping()
        except Exception:
            self._healthy = False
            if candidate is not None:
                with contextlib.suppress(Exception):
                    await candidate.aclose()
            logger.error("flow_replay_store_unavailable", stream=self._stream, exc_info=True)
            return False
        previous, self._redis = self._redis, candidate
        self._healthy = True
        if previous is not None:
            with contextlib.suppress(Exception):
                await previous.aclose()
        logger.info("flow_replay_store_ready", stream=self._stream, max_len=self._max_len)
        return True

    async def ensure_available(self) -> bool:
        """Re-probe an unhealthy store (single-flight, rate-limited).

        A failed operation latches ``_healthy = False``; without this re-probe
        only a process restart could recover, so one Redis blip disabled
        replay — and every resumable flow subscription — until an operator
        restarted the gateway.
        """
        if self.available:
            return True
        if self._probe_lock is None:
            self._probe_lock = asyncio.Lock()
        async with self._probe_lock:
            if self.available:
                return True
            now = self._clock()
            if now < self._reprobe_not_before:
                return False
            self._reprobe_not_before = now + self._REPROBE_COOLDOWN_SECONDS
            return await self.initialize()

    def _mark_unhealthy(self, client: Any) -> None:
        """Latch unhealthy only when the failing client is still current — a
        stale failure racing a successful re-probe must not clobber it."""
        if client is self._redis:
            self._healthy = False

    async def append(self, envelope: dict[str, Any]) -> str:
        if not self.available and not await self.ensure_available():
            self._set_gap_pending(True)
            raise RuntimeError("flow replay store is unavailable")
        client = self._redis
        if self._gap_pending:
            # Durably bound the gap before the next event lands so a resuming
            # reader crossing this point fails closed instead of silently
            # missing the events dropped while Redis was down.
            # nosemgrep: empire-no-bare-exception -- marker failure re-latches and re-raises; logged with exc_info
            try:
                await client.xadd(
                    self._stream,
                    {b"replay_gap": b"1"},
                    maxlen=self._max_len,
                    approximate=True,
                )
            except Exception:
                self._mark_unhealthy(client)
                logger.error("flow_replay_gap_marker_failed", exc_info=True)
                raise
            self._set_gap_pending(False)
            logger.warning("flow_replay_gap_marker_recorded", stream=self._stream)
        # nosemgrep: empire-no-bare-exception -- append failure disables the store and re-raises; logged with exc_info
        try:
            cursor = await client.xadd(
                self._stream,
                {b"envelope": orjson.dumps(envelope, default=str)},
                maxlen=self._max_len,
                approximate=True,
            )
        except Exception:
            self._mark_unhealthy(client)
            self._set_gap_pending(True)
            logger.error(
                "flow_replay_append_failed_entries_disabled",
                event_id=envelope.get("event_id"),
                exc_info=True,
            )
            raise
        return cursor.decode() if isinstance(cursor, bytes) else str(cursor)

    async def high_watermark(self) -> str:
        if not self.available and not await self.ensure_available():
            raise RuntimeError("flow replay store is unavailable")
        client = self._redis
        # nosemgrep: empire-no-bare-exception -- redis failure latches unhealthy so retrying subscribers re-probe; re-raised
        try:
            latest = await client.xrevrange(self._stream, count=1)
        except Exception:
            self._mark_unhealthy(client)
            raise
        if not latest:
            return "0-0"
        cursor = latest[0][0]
        return cursor.decode() if isinstance(cursor, bytes) else str(cursor)

    async def read(self, after: str, through: str) -> list[ReplayEntry]:
        if not self.available and not await self.ensure_available():
            raise RuntimeError("flow replay store is unavailable")
        if after == "$" or after == through:
            return []
        from redis.exceptions import RedisError

        client = self._redis
        # redis-py raises redis.exceptions.ConnectionError/TimeoutError, which
        # do NOT subclass the builtins — catching only builtins would silently
        # skip the unhealthy latch and defeat subscriber-driven recovery.
        try:
            return await self._read_range(client, after, through)
        except (RedisError, ConnectionError, OSError, TimeoutError):
            self._mark_unhealthy(client)
            raise

    async def _read_range(self, client: Any, after: str, through: str) -> list[ReplayEntry]:
        stream_info = await client.xinfo_stream(self._stream)
        first_entry = stream_info.get(b"first-entry") or stream_info.get("first-entry")
        if first_entry:
            raw_first_cursor = first_entry[0]
            first_cursor = raw_first_cursor.decode() if isinstance(raw_first_cursor, bytes) else str(raw_first_cursor)
            if _cursor_parts(after) < _cursor_parts(first_cursor):
                raise ReplayHistoryTrimmedError(
                    f"requested cursor {after} is older than retained history starting at {first_cursor}"
                )
        rows = await client.xrange(
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
            if fields.get(b"replay_gap") or fields.get("replay_gap"):
                # A durability gap lies inside the requested range: events were
                # dropped here while Redis was down, so this replay cannot
                # guarantee completeness. Fail closed — the client must take a
                # fresh resume point instead of silently missing events.
                raise ReplayGapError(f"flow replay range crosses a durability gap at {cursor}")
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
