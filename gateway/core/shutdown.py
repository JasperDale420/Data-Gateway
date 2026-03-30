"""Graceful shutdown coordination (PRD §Graceful Shutdown).

Tracks shutdown state so health endpoints can return 503 during drain
and the lifespan handler can execute the 8-step shutdown sequence.
"""

import time

from gateway.core.logger import logger


class ShutdownCoordinator:
    """Singleton coordinating the graceful shutdown sequence."""

    _instance: "ShutdownCoordinator | None" = None

    def __init__(self) -> None:
        self._is_shutting_down: bool = False
        self._drain_started_at: float = 0.0
        self._drain_seconds: int = 30

    @classmethod
    def get_instance(cls) -> "ShutdownCoordinator":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (testing only)."""
        cls._instance = None

    @property
    def is_shutting_down(self) -> bool:
        return self._is_shutting_down

    @property
    def drain_seconds(self) -> int:
        return self._drain_seconds

    def drain_remaining(self) -> float:
        """Seconds remaining in the drain period."""
        if not self._is_shutting_down:
            return 0.0
        elapsed = time.monotonic() - self._drain_started_at
        return max(0.0, self._drain_seconds - elapsed)

    def start_shutdown(self, drain_seconds: int = 30) -> None:
        """Begin the graceful shutdown sequence."""
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        self._drain_seconds = drain_seconds
        self._drain_started_at = time.monotonic()
        logger.info(
            "shutdown_coordinator_started",
            drain_seconds=drain_seconds,
        )
