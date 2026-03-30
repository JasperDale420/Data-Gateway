"""Centralized logger shim — delegates to empire_core.logger.

All gateway modules should import from here:
    from gateway.core.logger import logger
"""

from empire_core.logger import (
    bind_context,
    clear_context,
    get_logger,
    log_error,
    log_retry,
    setup_logging,
)

logger = get_logger("data-gateway")

__all__ = [
    "bind_context",
    "clear_context",
    "get_logger",
    "log_error",
    "log_retry",
    "logger",
    "setup_logging",
]
