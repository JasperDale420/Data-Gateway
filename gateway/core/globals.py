"""Application-wide singleton state.

Holds get/set accessors for global infrastructure objects (provider registry,
stream multiplexer, data sink registry) that are initialized during app
lifespan and consumed across both API and core layers.

This module exists to break the circular dependency where core modules
(e.g. uw_poller) need the sink registry but shouldn't import from the API
layer. All singleton state lives here; gateway.api.deps re-exports the
accessors for backward compatibility with existing API-layer callers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gateway.core.data_sink import DataSinkRegistry
    from gateway.core.flow_fanout import FlowFanout
    from gateway.core.registry import ProviderRegistry
    from gateway.core.stream import StreamMultiplexer


# ── Provider Registry ────────────────────────────────────────────────────────

_registry: ProviderRegistry | None = None


def set_registry(registry: ProviderRegistry) -> None:
    """Set the global provider registry (called during startup)."""
    global _registry
    _registry = registry


def get_registry() -> ProviderRegistry:
    """Get the provider registry."""
    if _registry is None:
        raise RuntimeError("Provider registry not initialized")
    return _registry


# ── Stream Multiplexer ───────────────────────────────────────────────────────

_multiplexer: StreamMultiplexer | None = None


def set_multiplexer(multiplexer: StreamMultiplexer) -> None:
    """Set the global stream multiplexer (called during startup)."""
    global _multiplexer
    _multiplexer = multiplexer


def get_multiplexer() -> StreamMultiplexer:
    """Get the stream multiplexer."""
    if _multiplexer is None:
        raise RuntimeError("Stream multiplexer not initialized")
    return _multiplexer


# ── Flow Fan-out ─────────────────────────────────────────────────────────────

_flow_fanout: FlowFanout | None = None


def set_flow_fanout(flow_fanout: FlowFanout | None) -> None:
    """Set the global UW flow fan-out (called during startup)."""
    global _flow_fanout
    _flow_fanout = flow_fanout


def get_flow_fanout() -> FlowFanout | None:
    """Get the UW flow fan-out (may be None if disabled / not initialized)."""
    return _flow_fanout


# ── Data Sink Registry ───────────────────────────────────────────────────────

_sink_registry: DataSinkRegistry | None = None


def set_sink_registry(registry: DataSinkRegistry) -> None:
    """Set the global data sink registry (called during startup)."""
    global _sink_registry
    _sink_registry = registry


def get_sink_registry() -> DataSinkRegistry | None:
    """Get the data sink registry (may be None if not configured)."""
    return _sink_registry
