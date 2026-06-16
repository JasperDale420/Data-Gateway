"""Persistent run-state guard for Unusual Whales EOD polling."""

import fcntl
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from gateway.core.logger import logger


class UwEodRunState(BaseModel):
    """Last known UW EOD run state for one trading date."""

    trading_date: str
    status: Literal["running", "completed"]
    started_at: str
    completed_at: str | None = None
    totals: dict[str, Any] = Field(default_factory=dict)


class UwEodStateStore:
    """Atomic JSON-backed state store for daily UW EOD runs."""

    def __init__(self, path: Path, stale_after_seconds: int):
        self._path = Path(path)
        self._stale_after_seconds = stale_after_seconds

    def should_skip(self, trading_date: str) -> bool:
        """Return True only when this trading date has already completed."""
        state = self._read_state()
        return state is not None and state.trading_date == trading_date and state.status == "completed"

    def should_defer(self, trading_date: str) -> bool:
        """Return True when this trading date is completed or already running."""
        state = self._read_state()
        if state is None or state.trading_date != trading_date:
            return False
        if state.status == "completed":
            return True
        return not self._is_running_stale(state)

    def claim(self, trading_date: str) -> bool:
        """Claim an EOD run unless the same trading date is done or actively running."""
        with self._claim_lock() as acquired:
            if not acquired:
                return False

            state = self._read_state()
            if state is not None and state.trading_date == trading_date:
                if state.status == "completed":
                    return False
                if not self._is_running_stale(state):
                    return False

            self._write_state(
                UwEodRunState(
                    trading_date=trading_date,
                    status="running",
                    started_at=datetime.now(UTC).isoformat(),
                )
            )
            return True

    def mark_completed(self, trading_date: str, totals: dict) -> None:
        """Persist completion for the trading date, preserving the original start time."""
        with self._claim_lock() as _acquired:
            state = self._read_state()
            started_at = (
                state.started_at
                if state is not None and state.trading_date == trading_date
                else datetime.now(UTC).isoformat()
            )
            self._write_state(
                UwEodRunState(
                    trading_date=trading_date,
                    status="completed",
                    started_at=started_at,
                    completed_at=datetime.now(UTC).isoformat(),
                    totals=totals,
                )
            )

    @contextmanager
    def _claim_lock(self, blocking: bool = True) -> Iterator[bool]:
        """Hold an exclusive lock for read-check-write state transitions."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self._path.with_name(f"{self._path.name}.lock")
        lock_file = lock_path.open("a+", encoding="utf-8")
        acquired = False
        try:
            operation = fcntl.LOCK_EX
            if not blocking:
                operation |= fcntl.LOCK_NB
            try:
                fcntl.flock(lock_file.fileno(), operation)
            except BlockingIOError:
                yield False
                return
            acquired = True
            yield True
        finally:
            if acquired:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()

    def _read_state(self) -> UwEodRunState | None:
        if not self._path.exists():
            return None
        try:
            state = UwEodRunState.model_validate_json(self._path.read_text())
            self._parse_datetime(state.started_at)
            if state.completed_at is not None:
                self._parse_datetime(state.completed_at)
            return state
        except (OSError, UnicodeError, ValueError, ValidationError) as exc:
            logger.warning(
                "uw_eod_state_read_failed",
                path=str(self._path),
                error=str(exc),
                exc_info=True,
            )
            return None

    def _write_state(self, state: UwEodRunState) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                delete=False,
            ) as tmp:
                tmp_path = tmp.name
                tmp.write(state.model_dump_json(indent=2))
                tmp.write("\n")
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tmp_path, self._path)
        finally:
            if tmp_path is not None and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _is_running_stale(self, state: UwEodRunState) -> bool:
        started_at = self._parse_datetime(state.started_at)
        return (datetime.now(UTC) - started_at).total_seconds() > self._stale_after_seconds

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
