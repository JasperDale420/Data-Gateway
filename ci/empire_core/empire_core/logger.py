"""Unified structured logging for all Empire services.

Usage:
    from empire_core.logger import setup_logging, get_logger

    # Call once at service startup
    setup_logging("cerberus")

    # In any module
    logger = get_logger(__name__)
    logger.info("order_placed", symbol="AAPL", qty=100)

    # Bind trace/correlation IDs
    from empire_core.logger import bind_context, clear_context
    bind_context(trace_id="req-abc-123", correlation_id="corr-456")
    logger.info("processing")  # trace_id and correlation_id auto-injected
    clear_context()

    # Structured error helpers
    from empire_core.logger import log_error
    log_error(logger, exc, "bronze_write", feed="quotes", symbol="AAPL")
"""

from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

import structlog

_configured = False
_service_name: str = "unknown"


class _StdoutProxy:
    """Proxy that always writes to the current ``sys.stdout``.

    ``logging.StreamHandler(sys.stdout)`` captures the *reference* to
    ``sys.stdout`` at handler-creation time.  If the handler is created
    during pytest collection (when pytest has already redirected stdout),
    the handler is forever locked to the stale fd and ``capsys`` /
    ``capfd`` can never intercept the output.

    This proxy solves the problem by delegating every write to whatever
    ``sys.stdout`` points to *right now*, so test-capture fixtures work
    correctly regardless of import order.
    """

    @staticmethod
    def write(s: str) -> int:
        return sys.stdout.write(s)

    @staticmethod
    def flush() -> None:
        sys.stdout.flush()


# ---------------------------------------------------------------------------
# Structlog processors
# ---------------------------------------------------------------------------


def _upcase_level(logger: Any, method_name: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Uppercase the log level (e.g. 'info' -> 'INFO')."""
    if "level" in event_dict:
        event_dict["level"] = event_dict["level"].upper()
    return event_dict


def _rename_event_to_message(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Rename structlog's 'event' key to 'message' for consistency."""
    if "event" in event_dict:
        event_dict["message"] = event_dict.pop("event")
    return event_dict


def _inject_service_name(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Inject service_name into every log entry."""
    if "service" not in event_dict:
        event_dict["service"] = _service_name
    return event_dict


# ---------------------------------------------------------------------------
# File handlers
# ---------------------------------------------------------------------------


def _daily_namer(default_name: str) -> str:
    """Rename rotated log files from suffix-date to date-in-name format.

    Transforms e.g. ``logs/3roses_2026-03-12.log.2026-03-11``
    into ``logs/3roses_2026-03-11.log``.
    """
    match = re.search(r"\.(\d{4}-\d{2}-\d{2})$", default_name)
    if not match:
        return default_name
    date_suffix = match.group(1)
    base = default_name[: match.start()]
    # base is e.g. "logs/3roses_2026-03-12.log"
    # Replace the date in the stem with the rotated date
    stem_match = re.search(r"_\d{4}-\d{2}-\d{2}\.log$", base)
    if stem_match:
        prefix = base[: stem_match.start()]
        return f"{prefix}_{date_suffix}.log"
    return default_name


def _make_daily_file_handler(
    log_dir: Path, service_name: str, backup_count: int, *, level: int = logging.DEBUG
) -> logging.Handler:
    """Create a file handler that writes to ``{service}_{YYYY-MM-DD}.log``.

    Rotates at midnight; old files keep their date in the filename.
    """
    from datetime import date
    from logging.handlers import TimedRotatingFileHandler

    today = date.today().isoformat()
    filename = log_dir / f"{service_name}_{today}.log"

    handler = TimedRotatingFileHandler(
        filename=filename,
        when="midnight",
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.namer = _daily_namer
    handler.setLevel(level)
    return handler


def _make_daily_error_handler(log_dir: Path, service_name: str, backup_count: int) -> logging.Handler:
    """Create a file handler for WARNING+ logs only.

    Writes to ``{service}_errors_{YYYY-MM-DD}.log``.
    """
    from datetime import date
    from logging.handlers import TimedRotatingFileHandler

    today = date.today().isoformat()
    filename = log_dir / f"{service_name}_errors_{today}.log"

    handler = TimedRotatingFileHandler(
        filename=filename,
        when="midnight",
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.namer = _daily_namer
    handler.setLevel(logging.WARNING)
    return handler


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def setup_logging(
    service_name: str,
    level: str = "INFO",
    *,
    log_file: bool = True,
    error_log: bool = True,
    log_dir: str | None = None,
    backup_count: int = 14,
    force: bool = False,
) -> None:
    """Configure structlog and stdlib logging for a service.

    Args:
        service_name: Name of the service (used in log filenames and auto-injected).
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        log_file: Whether to write logs to a daily rotating file.
        error_log: Whether to write a separate WARNING+ error log file.
        log_dir: Directory for log files. Defaults to EMPIRE_LOG_DIR env or "./logs".
        backup_count: Number of rotated log files to keep (default 14 days).
        force: Force reconfiguration (used in tests).
    """
    global _configured, _service_name
    force = force or "PYTEST_CURRENT_TEST" in os.environ
    if _configured and not force:
        return

    _service_name = service_name

    # Allow env var overrides
    level = os.environ.get("EMPIRE_LOG_LEVEL", level).upper()
    log_format = os.environ.get("EMPIRE_LOG_FORMAT", "json").lower()
    use_json = log_format not in ("human", "dev", "text")

    # EMPIRE_LOG_BACKUP_COUNT overrides the default 14-day daily-log
    # retention. Services with verbose output or longer post-mortem
    # windows (e.g. Data-Gateway) can bump this without touching code.
    env_backup = os.environ.get("EMPIRE_LOG_BACKUP_COUNT")
    if env_backup is not None:
        try:
            parsed = int(env_backup)
            if parsed > 0:
                backup_count = parsed
        except ValueError:
            # Fall through to the caller-provided default; warn once.
            logging.getLogger(__name__).warning(
                "Invalid EMPIRE_LOG_BACKUP_COUNT=%r; using default %d",
                env_backup,
                backup_count,
            )

    # Choose output renderer
    if use_json:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _upcase_level,
            _inject_service_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _rename_event_to_message,
            renderer,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    handlers: list[logging.Handler] = [logging.StreamHandler(_StdoutProxy())]

    if log_file:
        resolved_dir = log_dir or os.environ.get("EMPIRE_LOG_DIR", "logs")
        path = Path(resolved_dir)
        path.mkdir(parents=True, exist_ok=True)
        handlers.append(_make_daily_file_handler(path, service_name, backup_count))
        if error_log:
            handlers.append(_make_daily_error_handler(path, service_name, backup_count))

    logging.basicConfig(
        format="%(message)s",
        handlers=handlers,
        level=getattr(logging, level, logging.INFO),
        force=force,
    )

    # Suppress noisy HTTP library loggers
    for noisy in ("httpx", "httpcore", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger bound to the given name."""
    return structlog.get_logger(name)


# ---------------------------------------------------------------------------
# Context helpers (trace_id, correlation_id, etc.)
# ---------------------------------------------------------------------------


def bind_context(**kwargs: Any) -> None:
    """Bind key-value pairs to structlog's contextvars.

    All subsequent log calls in the same async/thread context will include
    these fields automatically. Common fields: trace_id, correlation_id, run_id.

    Example::

        bind_context(trace_id="req-abc-123")
        logger.info("processing")  # includes trace_id automatically
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()


def unbind_context(*keys: str) -> None:
    """Remove specific keys from the bound context."""
    structlog.contextvars.unbind_contextvars(*keys)


# ---------------------------------------------------------------------------
# Structured error helpers
# ---------------------------------------------------------------------------


def log_error(
    logger: structlog.stdlib.BoundLogger,
    error: Exception,
    operation: str,
    **context: Any,
) -> None:
    """Log an error with structured context and full traceback.

    Automatically extracts error_type and error_message. If the exception
    is an EmpireError, also extracts error_code and details.

    Args:
        logger: The structlog logger instance.
        error: The exception to log.
        operation: What operation failed (e.g. "bronze_write", "order_submit").
        **context: Additional structured fields (symbol, feed, order_id, etc.).
    """
    fields: dict[str, Any] = {
        "operation": operation,
        "error_type": type(error).__name__,
        "error_message": str(error),
        **context,
    }

    # Extract EmpireError fields if available
    if hasattr(error, "code"):
        fields["error_code"] = error.code
    if hasattr(error, "details") and error.details:
        fields["error_details"] = error.details

    logger.error("error", exc_info=True, **fields)


def log_retry(
    logger: structlog.stdlib.BoundLogger,
    operation: str,
    attempt: int,
    max_retries: int,
    delay_seconds: float,
    error: str,
) -> None:
    """Log a retry attempt."""
    logger.warning(
        "retry",
        operation=operation,
        attempt=attempt,
        max_retries=max_retries,
        delay_seconds=delay_seconds,
        error=error,
    )
