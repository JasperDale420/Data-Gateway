"""Durable, ordered admission for events awaiting broker publication."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class OutboxIntegrityError(RuntimeError):
    """Raised when an event identity is reused with different content."""


class OutboxCapacityError(RuntimeError):
    """Raised when durable admission would exceed the configured disk budget."""


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """One event durably waiting for broker acknowledgement."""

    sequence: int
    topic: str
    event_id: str
    payload: bytes
    payload_digest: str
    attempts: int
    last_error: str | None
    created_at: str


@dataclass(frozen=True, slots=True)
class OutboxStorageSize:
    """Physical bytes occupied by the SQLite database and its WAL."""

    database_bytes: int
    wal_bytes: int

    @property
    def total_bytes(self) -> int:
        return self.database_bytes + self.wal_bytes


class SQLiteOutbox:
    """A small SQLite outbox with durable commits and FIFO reads."""

    def __init__(self, path: str | Path, *, max_bytes: int | None = None) -> None:
        self.path = Path(path)
        if max_bytes is not None and max_bytes < 1:
            raise ValueError("outbox max_bytes must be positive")
        self._max_bytes = max_bytes
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        try:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    payload_digest TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL DEFAULT (
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    UNIQUE (topic, event_id)
                )
                """
            )
            self._connection.commit()
        except BaseException:
            self._connection.close()
            raise

    def __enter__(self) -> SQLiteOutbox:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    @staticmethod
    def _serialize(data: dict[str, Any] | str | bytes) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        if isinstance(data, dict):
            return json.dumps(
                data,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        raise TypeError("outbox payload must be a dict, str, or bytes")

    @staticmethod
    def _event_id(data: dict[str, Any] | str | bytes, event_id: str | None) -> str:
        identity = event_id
        if identity is None and isinstance(data, dict):
            value = data.get("event_id")
            if isinstance(value, str):
                identity = value
        if not isinstance(identity, str) or not identity.strip():
            raise ValueError("outbox admission requires a nonempty event_id")
        return identity

    def admit(
        self,
        topic: str,
        data: dict[str, Any] | str | bytes,
        *,
        event_id: str | None = None,
    ) -> bool:
        """Commit an event before publication.

        Returns ``True`` for a new row and ``False`` for an exact duplicate.
        Reusing the same topic and event ID with different bytes fails closed.
        """
        return self._admit_prepared(
            [
                (
                    topic,
                    self._event_id(data, event_id),
                    self._serialize(data),
                )
            ]
        )[0]

    def admit_many(
        self,
        messages: Sequence[tuple[str, dict[str, Any] | str | bytes]],
    ) -> list[bool]:
        """Commit a batch in one transaction and report newly inserted rows."""
        prepared = [(topic, self._event_id(data, None), self._serialize(data)) for topic, data in messages]
        return self._admit_prepared(prepared)

    def _admit_prepared(
        self,
        messages: list[tuple[str, str, bytes]],
    ) -> list[bool]:
        for topic, _identity, _payload in messages:
            if not isinstance(topic, str) or not topic.strip():
                raise ValueError("outbox admission requires a nonempty topic")

        inserted: list[bool] = []
        with self._lock, self._connection:
            additional_bytes = 0
            for topic, identity, payload in messages:
                existing = self._connection.execute(
                    """
                    SELECT payload, payload_digest
                    FROM outbox
                    WHERE topic = ? AND event_id = ?
                    """,
                    (topic, identity),
                ).fetchone()
                digest = hashlib.sha256(payload).hexdigest()
                if existing is not None:
                    if bytes(existing[0]) != payload or existing[1] != digest:
                        raise OutboxIntegrityError(
                            f"event_id {identity!r} on topic {topic!r} was reused with different content"
                        )
                    continue
                # Bound the pending backlog, not SQLite's reusable high-water
                # pages; otherwise a once-full database stays "full" forever.
                additional_bytes += len(payload) + 512

            if self._max_bytes is not None and self._pending_bytes() + additional_bytes > self._max_bytes:
                raise OutboxCapacityError(f"outbox admission would exceed configured limit of {self._max_bytes} bytes")
            if shutil.disk_usage(self.path.parent).free <= additional_bytes:
                raise OutboxCapacityError("outbox admission would exhaust available disk space")

            for topic, identity, payload in messages:
                digest = hashlib.sha256(payload).hexdigest()
                try:
                    self._connection.execute(
                        """
                        INSERT INTO outbox (topic, event_id, payload, payload_digest)
                        VALUES (?, ?, ?, ?)
                        """,
                        (topic, identity, payload, digest),
                    )
                except sqlite3.IntegrityError:
                    existing = self._connection.execute(
                        """
                        SELECT payload, payload_digest
                        FROM outbox
                        WHERE topic = ? AND event_id = ?
                        """,
                        (topic, identity),
                    ).fetchone()
                    if existing is None or bytes(existing[0]) != payload or existing[1] != digest:
                        raise OutboxIntegrityError(
                            f"event_id {identity!r} on topic {topic!r} was reused with different content"
                        ) from None
                    inserted.append(False)
                else:
                    inserted.append(True)
        return inserted

    def pending(self, *, limit: int = 1000) -> list[OutboxEntry]:
        """Return the oldest pending entries in admission order."""
        if limit < 1:
            raise ValueError("pending limit must be at least 1")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT sequence, topic, event_id, payload, payload_digest,
                       attempts, last_error, created_at
                FROM outbox
                ORDER BY sequence
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            OutboxEntry(
                sequence=row[0],
                topic=row[1],
                event_id=row[2],
                payload=bytes(row[3]),
                payload_digest=row[4],
                attempts=row[5],
                last_error=row[6],
                created_at=row[7],
            )
            for row in rows
        ]

    def acknowledge(self, sequence: int) -> bool:
        """Delete one entry after the broker has durably acknowledged it."""
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM outbox WHERE sequence = ?",
                (sequence,),
            )
        return cursor.rowcount == 1

    def record_failure(self, sequence: int, error: str) -> bool:
        """Record one failed publication attempt without removing the event."""
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                UPDATE outbox
                SET attempts = attempts + 1, last_error = ?
                WHERE sequence = ?
                """,
                (error, sequence),
            )
        return cursor.rowcount == 1

    def storage_size(self) -> OutboxStorageSize:
        """Return physical database and WAL file sizes."""

        def file_size(path: Path) -> int:
            try:
                return path.stat().st_size
            except FileNotFoundError:
                return 0

        return OutboxStorageSize(
            database_bytes=file_size(self.path),
            wal_bytes=file_size(Path(f"{self.path}-wal")),
        )

    def has_capacity(self) -> bool:
        """Return whether the configured disk budget has room for another write."""
        with self._lock:
            return self._max_bytes is None or self._pending_bytes() + 513 <= self._max_bytes

    def capacity_utilization(self) -> float:
        """Return pending-backlog utilization of the configured budget."""
        if self._max_bytes is None:
            return 0.0
        with self._lock:
            return min(self._pending_bytes() / self._max_bytes, 1.0)

    def _pending_bytes(self) -> int:
        row = self._connection.execute("SELECT COALESCE(SUM(length(payload) + 512), 0) FROM outbox").fetchone()
        return int(row[0])

    def close(self) -> None:
        """Close the SQLite connection."""
        with self._lock:
            self._connection.close()
