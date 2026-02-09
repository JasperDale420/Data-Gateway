"""Historical Replay Mode for backtesting.

Simulates real-time data arrival for strategy backtesting as specified in PRD.
"""

import asyncio
import contextlib
import json
import os
import tempfile
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

from gateway.config import get_settings

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Replay Session State
# ─────────────────────────────────────────────────────────────────────────────


class ReplayState(str, Enum):
    """State of a replay session."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"


# ─────────────────────────────────────────────────────────────────────────────
# Replay Request/Config
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ReplayConfig:
    """Configuration for a replay session."""

    name: str
    symbols: list[str]
    feeds: list[str]  # bars, quotes, trades
    start: datetime
    end: datetime
    speed: float = 1.0  # Playback speed multiplier
    include_premarket: bool = False

    def validate(self) -> list[str]:
        """Validate config. Returns list of error messages."""
        errors = []
        if not self.name:
            errors.append("name is required")
        if not self.symbols:
            errors.append("symbols list cannot be empty")
        if len(self.symbols) > 100:
            errors.append(f"Maximum 100 symbols allowed, got {len(self.symbols)}")
        if not self.feeds:
            errors.append("feeds list cannot be empty")
        if self.start >= self.end:
            errors.append("start must be before end")
        if self.speed <= 0:
            errors.append("speed must be positive")
        if self.speed > 100:
            errors.append("speed cannot exceed 100x")
        return errors


# ─────────────────────────────────────────────────────────────────────────────
# Replay Message
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ReplayMessage:
    """A message in the replay stream."""

    feed: str
    symbol: str
    data: dict[str, Any]
    market_timestamp: datetime

    def to_ws_message(
        self,
        sequence: int,
        session_progress: float,
        replay_speed: float,
    ) -> dict[str, Any]:
        """Convert to WebSocket message format."""
        return {
            "type": "data",
            "feed": self.feed,
            "symbol": self.symbol,
            "data": self.data,
            "meta": {
                "market_timestamp": self.market_timestamp.isoformat(),
                "replay_wall_clock": datetime.now(UTC).isoformat(),
                "replay_speed": replay_speed,
                "sequence": sequence,
                "session_progress": round(session_progress, 4),
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# Replay Session
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ReplaySession:
    """A historical replay session."""

    session_id: str
    config: ReplayConfig
    client_id: str
    state: ReplayState = ReplayState.PENDING

    # Progress tracking
    current_timestamp: datetime | None = None
    sequence: int = 0
    messages_sent: int = 0

    # Timing
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    ended_at: datetime | None = None

    # Control
    speed: float = 1.0
    _pause_event: asyncio.Event = field(default_factory=asyncio.Event)
    _stop_flag: bool = field(default=False)
    _task: asyncio.Task[None] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.speed = self.config.speed
        self._pause_event.set()  # Not paused initially

    @property
    def progress(self) -> float:
        """Calculate session progress (0.0 to 1.0)."""
        if not self.current_timestamp:
            return 0.0

        total = (self.config.end - self.config.start).total_seconds()
        if total == 0:
            return 1.0

        elapsed = (self.current_timestamp - self.config.start).total_seconds()
        return max(0.0, min(1.0, elapsed / total))

    @property
    def estimated_duration_seconds(self) -> float:
        """Estimated real-time duration at current speed."""
        market_duration = (self.config.end - self.config.start).total_seconds()
        return market_duration / self.speed

    def to_dict(self) -> dict[str, Any]:
        """Convert to response dict."""
        return {
            "session_id": self.session_id,
            "name": self.config.name,
            "state": self.state.value,
            "symbols": self.config.symbols,
            "feeds": self.config.feeds,
            "start": self.config.start.isoformat(),
            "end": self.config.end.isoformat(),
            "speed": self.speed,
            "progress": round(self.progress, 4),
            "messages_sent": self.messages_sent,
            "sequence": self.sequence,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }

    def pause(self) -> None:
        """Pause the replay session."""
        self._pause_event.clear()
        self.state = ReplayState.PAUSED
        logger.info("replay_paused", session_id=self.session_id)

    def resume(self, speed: float | None = None) -> None:
        """Resume the replay session."""
        if speed is not None and speed > 0:
            self.speed = speed
        self._pause_event.set()
        self.state = ReplayState.RUNNING
        logger.info("replay_resumed", session_id=self.session_id, speed=self.speed)

    def stop(self) -> None:
        """Stop the replay session."""
        self._stop_flag = True
        self._pause_event.set()  # Unblock if paused
        self.state = ReplayState.STOPPED
        self.ended_at = datetime.now(UTC)
        logger.info("replay_stopped", session_id=self.session_id)

    def seek(self, timestamp: datetime) -> None:
        """Seek to a specific timestamp."""
        if self.config.start <= timestamp <= self.config.end:
            self.current_timestamp = timestamp
            logger.info(
                "replay_seek",
                session_id=self.session_id,
                timestamp=timestamp.isoformat(),
            )

    async def wait_if_paused(self) -> bool:
        """Wait if paused. Returns False if stopped."""
        await self._pause_event.wait()
        return not self._stop_flag


# ─────────────────────────────────────────────────────────────────────────────
# Replay Session Manager
# ─────────────────────────────────────────────────────────────────────────────


class ReplaySessionManager:
    """Manages historical replay sessions."""

    MAX_SESSIONS = 10

    def __init__(self) -> None:
        settings = get_settings()
        self._sessions: dict[str, ReplaySession] = {}
        self._lock = asyncio.Lock()
        self._data_loader: Callable[..., Any] | None = None
        self._spool_lists_to_disk = settings.replay_messages_spool_to_disk
        self._spool_list_threshold = max(1, settings.replay_messages_max_in_memory)

    def set_data_loader(self, loader: Callable[..., Any]) -> None:
        """Set the function used to load historical data."""
        self._data_loader = loader

    def has_data_loader(self) -> bool:
        """Check whether a data loader is configured."""
        return self._data_loader is not None

    async def create_session(self, config: ReplayConfig, client_id: str) -> ReplaySession:
        """Create a new replay session.

        Args:
            config: Replay configuration
            client_id: Authenticated client ID

        Returns:
            Created session
        """
        session_id = f"replay-{uuid.uuid4().hex[:12]}"

        session = ReplaySession(
            session_id=session_id,
            config=config,
            client_id=client_id,
        )

        async with self._lock:
            if len(self._sessions) >= self.MAX_SESSIONS:
                raise ValueError("Maximum concurrent replay sessions reached")

            self._sessions[session_id] = session

        logger.info(
            "replay_session_created",
            session_id=session_id,
            name=config.name,
            symbols=len(config.symbols),
            speed=config.speed,
        )

        return session

    def get_session(self, session_id: str) -> ReplaySession | None:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    async def list_sessions(self, client_id: str | None = None) -> list[ReplaySession]:
        """List sessions, optionally filtered by client."""
        sessions = list(self._sessions.values())
        if client_id is None:
            return sessions
        return [session for session in sessions if session.client_id == client_id]

    async def list_sessions_page(
        self,
        *,
        client_id: str | None = None,
        state: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[ReplaySession], int, bool, int | None]:
        """List replay sessions with offset/limit pagination metadata.

        Returns:
            (paged_sessions, total_matches, has_more, next_offset)
        """
        sessions = await self.list_sessions(client_id=client_id)
        if state:
            sessions = [session for session in sessions if session.state.value == state]

        total_matches = len(sessions)
        page_offset = max(0, offset)
        page_limit = None if limit is None else max(1, limit)

        if page_offset:
            sessions = sessions[page_offset:]
        if page_limit is not None:
            sessions = sessions[:page_limit]

        consumed = page_offset + len(sessions)
        has_more = consumed < total_matches
        next_offset = consumed if has_more else None
        return sessions, total_matches, has_more, next_offset

    async def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        session = self._sessions.get(session_id)
        if session:
            session.stop()
            async with self._lock:
                del self._sessions[session_id]
            return True
        return False

    async def start_session(
        self,
        session_id: str,
        message_callback: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Start replaying a session.

        Args:
            session_id: Session to start
            message_callback: Async function called with each message
        """
        session = self._sessions.get(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")

        if session.state not in (ReplayState.PENDING, ReplayState.PAUSED):
            raise ValueError(f"Cannot start session in state: {session.state.value}")

        session.state = ReplayState.RUNNING
        session.started_at = datetime.now(UTC)
        session.current_timestamp = session.config.start

        # Store task reference
        session._task = asyncio.create_task(self._run_replay(session, message_callback))

    async def _run_replay(
        self,
        session: ReplaySession,
        message_callback: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Run the replay loop."""
        try:
            # Load historical data
            loaded_messages = await self._load_data(session)
            total_messages = len(loaded_messages) if isinstance(loaded_messages, list) else None
            messages = self._iter_loaded_messages(loaded_messages)

            first_message = await anext(messages, None)
            if first_message is None:
                logger.warning("replay_no_data", session_id=session.session_id)
                session.state = ReplayState.COMPLETED
                session.ended_at = datetime.now(UTC)
                return

            logger_payload: dict[str, Any] = {"session_id": session.session_id}
            if total_messages is not None:
                logger_payload["total_messages"] = total_messages
            else:
                logger_payload["total_messages"] = "streaming"
            logger.info("replay_starting", **logger_payload)

            prev_timestamp = first_message.market_timestamp

            async for msg in self._yield_with_first(first_message, messages):
                # Check for stop/pause
                if not await session.wait_if_paused():
                    break

                # Calculate delay based on market time gap and speed
                time_gap = (msg.market_timestamp - prev_timestamp).total_seconds()
                real_delay = time_gap / session.speed

                if real_delay > 0:
                    await asyncio.sleep(real_delay)

                # Update session state
                session.sequence += 1
                session.messages_sent += 1
                session.current_timestamp = msg.market_timestamp
                prev_timestamp = msg.market_timestamp

                # Send message
                ws_msg = msg.to_ws_message(
                    sequence=session.sequence,
                    session_progress=session.progress,
                    replay_speed=session.speed,
                )

                await message_callback(ws_msg)

            if session.state == ReplayState.RUNNING:
                session.state = ReplayState.COMPLETED
                session.ended_at = datetime.now(UTC)
                logger.info(
                    "replay_completed",
                    session_id=session.session_id,
                    messages_sent=session.messages_sent,
                )

        except Exception as e:
            session.state = ReplayState.FAILED
            session.ended_at = datetime.now(UTC)
            logger.error(
                "replay_failed",
                session_id=session.session_id,
                error=str(e),
            )

    def _iter_loaded_messages(
        self,
        loaded: list[ReplayMessage] | AsyncIterable[ReplayMessage] | Iterable[ReplayMessage],
    ) -> AsyncIterator[ReplayMessage]:
        """Normalize loaded replay data to an async iterator.

        List inputs keep existing compatibility behavior. Async/sync iterables enable
        streaming loaders that avoid full materialization in replay startup paths.
        """
        if isinstance(loaded, list):
            return self._iter_list_messages(loaded)
        if hasattr(loaded, "__aiter__"):
            return self._iter_async_iterable(loaded)  # type: ignore[arg-type]
        return self._iter_sync_iterable(loaded)

    async def _iter_list_messages(
        self, messages: list[ReplayMessage]
    ) -> AsyncIterator[ReplayMessage]:
        """Iterate list-backed messages, sorting only when required."""
        if not self._is_sorted_by_market_timestamp(messages):
            messages = sorted(messages, key=lambda msg: msg.market_timestamp)
        if self._spool_lists_to_disk and len(messages) > self._spool_list_threshold:
            spool_path = self._spool_messages_to_tempfile(messages)
            messages.clear()
            try:
                async for msg in self._iter_spooled_messages(spool_path):
                    yield msg
            finally:
                with contextlib.suppress(FileNotFoundError):
                    os.remove(spool_path)
            return

        for msg in messages:
            yield msg

    async def _iter_async_iterable(
        self,
        messages: AsyncIterable[ReplayMessage],
    ) -> AsyncIterator[ReplayMessage]:
        async for msg in messages:
            yield msg

    async def _iter_sync_iterable(
        self,
        messages: Iterable[ReplayMessage],
    ) -> AsyncIterator[ReplayMessage]:
        for msg in messages:
            yield msg

    def _is_sorted_by_market_timestamp(self, messages: list[ReplayMessage]) -> bool:
        if len(messages) < 2:
            return True
        return all(
            previous.market_timestamp <= current.market_timestamp
            for previous, current in zip(messages, messages[1:], strict=False)
        )

    def _spool_messages_to_tempfile(self, messages: list[ReplayMessage]) -> str:
        """Persist replay messages to a temp JSONL file and return path."""
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix="replay-messages-",
            suffix=".jsonl",
            delete=False,
        ) as spool_file:
            for msg in messages:
                spool_file.write(
                    json.dumps(
                        {
                            "feed": msg.feed,
                            "symbol": msg.symbol,
                            "data": msg.data,
                            "market_timestamp": msg.market_timestamp.isoformat(),
                        }
                    )
                )
                spool_file.write("\n")
            return spool_file.name

    async def _iter_spooled_messages(self, spool_path: str) -> AsyncIterator[ReplayMessage]:
        """Iterate replay messages from a temp JSONL spool file."""
        with open(spool_path, encoding="utf-8") as spool_file:
            for line in spool_file:
                payload = json.loads(line)
                ts = datetime.fromisoformat(payload["market_timestamp"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                yield ReplayMessage(
                    feed=payload["feed"],
                    symbol=payload["symbol"],
                    data=payload["data"],
                    market_timestamp=ts,
                )

    async def _yield_with_first(
        self,
        first: ReplayMessage,
        messages: AsyncIterator[ReplayMessage],
    ) -> AsyncIterator[ReplayMessage]:
        yield first
        async for msg in messages:
            yield msg

    async def _load_data(
        self,
        session: ReplaySession,
    ) -> list[ReplayMessage] | AsyncIterable[ReplayMessage] | Iterable[ReplayMessage]:
        """Load historical data for replay.

        Override or set data_loader for production use.
        """
        if self._data_loader:
            return await self._data_loader(session)

        settings = get_settings()
        if settings.allow_stub_data:
            # Default: generate mock data for testing
            return self._generate_mock_data(session)
        raise RuntimeError("Replay data loader not configured")

    def _generate_mock_data(self, session: ReplaySession) -> list[ReplayMessage]:
        """Generate mock replay data for testing."""
        messages = []
        current = session.config.start

        # Generate one message per minute for bars
        while current < session.config.end:
            for symbol in session.config.symbols:
                if "bars" in session.config.feeds:
                    messages.append(
                        ReplayMessage(
                            feed="stock_bars",
                            symbol=symbol,
                            data={
                                "open": 150.0,
                                "high": 151.0,
                                "low": 149.5,
                                "close": 150.5,
                                "volume": 100000,
                            },
                            market_timestamp=current,
                        )
                    )

            # Advance by 1 minute
            current = current.replace(
                minute=current.minute + 1 if current.minute < 59 else 0,
                hour=current.hour + 1 if current.minute >= 59 else current.hour,
            )

            # Limit mock data
            if len(messages) >= 100:
                break

        return messages


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_manager: ReplaySessionManager | None = None


def get_replay_manager() -> ReplaySessionManager:
    """Get the singleton replay session manager."""
    global _manager
    if _manager is None:
        _manager = ReplaySessionManager()
    return _manager
