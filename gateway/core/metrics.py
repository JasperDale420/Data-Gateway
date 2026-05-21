"""Prometheus metrics for gateway observability."""

import time
from copy import deepcopy
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, Info

_PATH_NORMALIZATION_CACHE_MAX = 4096
_PATH_NORMALIZATION_CACHE_PRUNE_TARGET = _PATH_NORMALIZATION_CACHE_MAX // 4
_PATH_NORMALIZATION_CACHE: dict[str, str] = {}
_MEMORY_METRICS_REFRESH_INTERVAL_SECONDS = 10.0
_LAST_MEMORY_METRICS_UPDATE_MONOTONIC = 0.0

# Gateway info
GATEWAY_INFO = Info(
    "gateway",
    "Gateway service information",
)

# Request metrics
REQUEST_COUNT = Counter(
    "gateway_requests_total",
    "Total number of HTTP requests",
    ["method", "path", "status"],
)

REQUEST_DURATION = Histogram(
    "gateway_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0),
)

SYMBOLOGY_BATCH_SIZE = Histogram(
    "gateway_symbology_batch_size",
    "Number of symbols in symbology batch requests",
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)

# Cache metrics
CACHE_HITS = Counter(
    "gateway_cache_hits_total",
    "Total cache hits",
    ["cache_type"],
)

CACHE_MISSES = Counter(
    "gateway_cache_misses_total",
    "Total cache misses",
    ["cache_type"],
)

CACHE_SIZE = Gauge(
    "gateway_cache_size",
    "Current cache size",
    ["cache_type"],
)

# WebSocket metrics
WEBSOCKET_CONNECTIONS = Gauge(
    "gateway_websocket_connections",
    "Current WebSocket connections",
)

WEBSOCKET_MESSAGES = Counter(
    "gateway_websocket_messages_total",
    "Total WebSocket messages",
    ["direction"],  # inbound, outbound
)

WEBSOCKET_SUBSCRIPTIONS = Gauge(
    "gateway_websocket_subscriptions",
    "Current WebSocket subscriptions",
    ["feed"],  # bars, quotes, trades, news
)

# Provider metrics
PROVIDER_REQUESTS = Counter(
    "gateway_provider_requests_total",
    "Total requests to upstream providers",
    ["provider", "status"],  # success, error
)

PROVIDER_LATENCY = Histogram(
    "gateway_provider_latency_seconds",
    "Upstream provider request latency",
    ["provider"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

PROVIDER_QUOTE_BATCH_SIZE = Histogram(
    "gateway_provider_quote_batch_size",
    "Number of symbols requested in provider multi-quote calls",
    ["provider"],
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)

PROVIDER_HEALTH = Gauge(
    "gateway_provider_healthy",
    "Provider health status (1=healthy, 0=unhealthy)",
    ["provider"],
)

PROVIDER_HEALTH_CHECK_DURATION = Histogram(
    "gateway_provider_health_check_duration_seconds",
    "Provider health check duration in seconds",
    ["provider", "status"],  # status: success, error
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

PROVIDER_SYNC_CALL_WAIT = Histogram(
    "gateway_provider_sync_call_wait_seconds",
    "Wait time before provider sync call execution",
    ["provider"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

PROVIDER_SYNC_CALL_EXEC = Histogram(
    "gateway_provider_sync_call_exec_seconds",
    "Execution time for provider sync calls",
    ["provider"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

PROVIDER_SYNC_CALL_INFLIGHT = Gauge(
    "gateway_provider_sync_call_inflight",
    "Current in-flight provider sync calls",
    ["provider"],
)

# Rate limiting metrics
RATE_LIMIT_EXCEEDED = Counter(
    "gateway_rate_limit_exceeded_total",
    "Total rate limit exceeded events",
    ["client_id"],
)

# Process metrics
PROCESS_MEMORY_BYTES = Gauge(
    "gateway_process_memory_bytes",
    "Process memory usage in bytes",
    ["type"],  # rss, vms
)

PROCESS_MEMORY_PERCENT = Gauge(
    "gateway_process_memory_percent",
    "Process memory usage as percentage of total system memory",
)

# ─────────────────────────────────────────────────────────────────────────────
# Data Sink & Envelope Metrics
# ─────────────────────────────────────────────────────────────────────────────

ENVELOPE_CREATED = Counter(
    "gateway_envelopes_created_total",
    "Total EventEnvelopes created",
    ["provider", "feed"],
)

SINK_PUBLISH = Counter(
    "gateway_sink_publish_total",
    "Total data sink publish operations",
    ["sink", "topic", "status"],  # status: success, error
)

# Failed-event buffer instrumentation. Without these, the only signal that the
# RedisStreamsSink is dropping events on overflow is the per-eviction WARNING
# log line — which we proved on 2026-05-05 fires hundreds of times per second
# silently for hours when no operator is watching `docker logs`. The Counter
# drives an alert; the Gauge provides a quick "how full is the buffer right
# now" probe for dashboards / readiness checks.
SINK_BUFFER_EVICTIONS = Counter(
    "gateway_sink_buffer_evictions_total",
    "Failed-event buffer evictions (oldest event dropped because buffer was full)",
    ["sink"],
)

SINK_BUFFER_SIZE = Gauge(
    "gateway_sink_buffer_size",
    "Current number of events in the failed-event buffer",
    ["sink"],
)

# Bounded-queue + worker-pool gauges/counters. The previous semaphore
# implementation dropped on saturation silently and only emitted a
# `data_sink_backpressure_drop` WARNING per drop. The new path uses a
# bounded asyncio.Queue: producers block with a short timeout and only
# drop if the timeout itself fires — i.e. the queue is full AND workers
# can't drain it within `data_sink_producer_block_timeout_seconds`. That
# is a true emergency (sink stalled or hard-overloaded) and warrants its
# own counter + alert, distinct from steady-state semaphore behaviour.
SINK_QUEUE_SIZE = Gauge(
    "gateway_sink_queue_size",
    "Current depth of the per-sink dispatch queue",
    ["sink"],
)

SINK_QUEUE_CAPACITY = Gauge(
    "gateway_sink_queue_capacity",
    "Configured capacity of the per-sink dispatch queue",
    ["sink"],
)

SINK_WORKER_COUNT = Gauge(
    "gateway_sink_worker_count",
    "Configured number of workers draining the per-sink dispatch queue",
    ["sink"],
)

SINK_PRODUCER_TIMEOUT_DROPS = Counter(
    "gateway_sink_producer_timeout_drops_total",
    "Events dropped because the producer-side queue-put timed out — emergency-only",
    ["sink"],
)

STREAM_SINK_DISPATCH_EVENTS = Counter(
    "gateway_stream_sink_dispatch_events_total",
    "Stream-to-sink dispatch lifecycle events",
    ["status"],  # scheduled, completed, failed, cancelled
)

STREAM_SINK_PENDING_TASKS = Gauge(
    "gateway_stream_sink_pending_tasks",
    "Current number of queued stream-to-sink dispatch tasks (registry queue depth)",
)

STREAM_SINK_DISPATCH_LIMIT = Gauge(
    "gateway_stream_sink_dispatch_limit",
    "Configured stream-to-sink dispatch limits (mirrors registry queue/worker config)",
    ["limit_type"],  # max_inflight_publish, max_pending_tasks
)

STREAM_FANOUT_EVENTS = Counter(
    "gateway_stream_fanout_events_total",
    "WebSocket stream fanout dispatch events",
    ["status"],  # delivered, error
)

STREAM_FANOUT_BATCH_SIZE = Histogram(
    "gateway_stream_fanout_batch_size",
    "Number of clients in each fanout dispatch batch",
    buckets=(1, 2, 4, 8, 16, 32, 64, 128, 256, 512),
)

STREAM_FANOUT_LIMIT = Gauge(
    "gateway_stream_fanout_limit",
    "Configured stream fanout limits",
    ["limit_type"],  # max_inflight, batch_size
)

OPTION_CAPTURE_CYCLES = Counter(
    "gateway_option_capture_cycles_total",
    "Option capture cycle outcomes",
    ["status"],  # success, partial_failure, skipped_market_closed
)

OPTION_CAPTURE_SNAPSHOTS_PUBLISHED = Counter(
    "gateway_option_capture_snapshots_published_total",
    "Option chain snapshots published to the sink",
)

OPTION_CAPTURE_FAILED_SYMBOLS = Counter(
    "gateway_option_capture_failed_symbols_total",
    "Option capture symbol failures across cycles",
)

OPTION_CAPTURE_SYMBOL_CONTRACTS = Gauge(
    "gateway_option_capture_symbol_contracts",
    "Contracts seen and tracked by option capture",
    ["symbol", "kind"],  # snapshot_contracts, ws_tracked_contracts
)

OPTION_CAPTURE_QUALITY_RATIO = Gauge(
    "gateway_option_capture_quality_ratio",
    "Option capture field coverage ratios",
    ["symbol", "metric"],  # greeks_coverage, iv_coverage, nonzero_open_interest, bid_ask_coverage
)

OPTION_CAPTURE_SNAPSHOT_AGE = Gauge(
    "gateway_option_capture_snapshot_age_seconds",
    "Age of the latest snapshot payload observed for each underlying",
    ["symbol"],
)

OPTION_CAPTURE_WS_EVENTS = Counter(
    "gateway_option_capture_ws_events_total",
    "Option capture websocket subscription updates",
    ["event"],  # subscribe_calls, unsubscribe_calls, contracts_added, contracts_removed
)

_STREAM_SINK_DISPATCH_SNAPSHOT: dict[str, Any] = {
    "limits": {"max_inflight_publish": 1, "max_pending_tasks": 1},
    "pending_tasks": 0,
    "events": {},
}

_STREAM_FANOUT_SNAPSHOT: dict[str, Any] = {
    "limits": {"max_inflight": 1, "batch_size": 1},
    "events": {},
    "batches": {"count": 0, "total_clients": 0, "max_batch_size": 0},
}

_OPTION_CAPTURE_SNAPSHOT: dict[str, Any] = {
    "cycles": {"count": 0, "published": 0, "failed_symbols": 0, "skipped_market_closed": 0},
    "websocket": {
        "subscribe_calls": 0,
        "unsubscribe_calls": 0,
        "contracts_added": 0,
        "contracts_removed": 0,
    },
    "symbols": {},
}

_PROVIDER_HEALTH_CHECK_SNAPSHOT: dict[str, dict[str, Any]] = {}
_PROVIDER_QUOTE_BATCH_SNAPSHOT: dict[str, dict[str, Any]] = {}

# ─────────────────────────────────────────────────────────────────────────────
# SLI Metrics (PRD 11.1.2-4)
# ─────────────────────────────────────────────────────────────────────────────

# Availability SLI (11.1.2) - uptime tracking
UPTIME_SECONDS = Gauge(
    "gateway_uptime_seconds",
    "Gateway uptime in seconds since start",
)

HEALTH_STATUS = Gauge(
    "gateway_health_status",
    "Gateway health status (1=healthy, 0=degraded, -1=unhealthy)",
)

# Error rate tracking for SLI
ERROR_COUNT = Counter(
    "gateway_errors_total",
    "Total error count by type",
    ["error_type"],  # provider_error, rate_limit, auth_failure, internal
)

# Latency SLI (11.1.3) - exposed via existing REQUEST_DURATION histogram
# p50, p99 calculated by Prometheus from histogram buckets

# Message delivery rate SLI (11.1.4)
MESSAGE_DELIVERED = Counter(
    "gateway_messages_delivered_total",
    "Total messages successfully delivered to clients",
    ["feed"],
)

MESSAGE_DROPPED = Counter(
    "gateway_messages_dropped_total",
    "Total messages dropped due to errors",
    ["reason"],  # client_disconnected, buffer_full, timeout
)

# Alpha Vantage route/cache metrics
ALPHAVANTAGE_ROUTE_CACHE = Counter(
    "gateway_alphavantage_route_cache_total",
    "Alpha Vantage route cache events",
    ["endpoint", "status", "cache_mode"],  # status: hit, miss
)

ROUTE_CACHE_EVENTS = Counter(
    "gateway_route_cache_total",
    "Route-level cache hit/miss events",
    ["route", "status", "cache_mode"],  # status: hit, miss
)

ALPHAVANTAGE_PAYLOAD_BYTES = Histogram(
    "gateway_alphavantage_payload_bytes",
    "Alpha Vantage cached payload size in bytes",
    ["endpoint", "cache_mode"],
    buckets=(256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216),
)

# Memory pressure metric (for 11.2.4 alerting)
MEMORY_PRESSURE = Gauge(
    "gateway_memory_pressure",
    "Memory pressure indicator (0-100, percentage of target)",
)


def init_metrics(version: str = "0.1.0") -> None:
    """Initialize gateway info metrics."""
    GATEWAY_INFO.info(
        {
            "version": version,
            "service": "data-gateway",
        }
    )


def update_memory_metrics() -> None:
    """Update process memory metrics.

    Uses resource module (available on Unix) or psutil if available.
    """
    try:
        import resource

        # Get memory usage in bytes (Unix only)
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # maxrss is in kilobytes on Linux, bytes on macOS
        import platform

        if platform.system() == "Darwin":
            rss = usage.ru_maxrss  # bytes on macOS
        else:
            rss = usage.ru_maxrss * 1024  # KB to bytes on Linux

        PROCESS_MEMORY_BYTES.labels(type="rss").set(rss)

    except ImportError:
        pass  # resource module not available on Windows

    try:
        import psutil

        process = psutil.Process()
        mem_info = process.memory_info()
        PROCESS_MEMORY_BYTES.labels(type="rss").set(mem_info.rss)
        PROCESS_MEMORY_BYTES.labels(type="vms").set(mem_info.vms)
        PROCESS_MEMORY_PERCENT.set(process.memory_percent())
    except ImportError:
        pass  # psutil not installed


def update_memory_metrics_if_due(
    *,
    force: bool = False,
    now_monotonic: float | None = None,
) -> bool:
    """Update memory metrics at most once per refresh interval.

    Returns True when an update was performed, otherwise False.
    """
    global _LAST_MEMORY_METRICS_UPDATE_MONOTONIC

    now = now_monotonic if now_monotonic is not None else time.monotonic()
    if not force:
        last_update = _LAST_MEMORY_METRICS_UPDATE_MONOTONIC
        if last_update > 0 and (now - last_update) < _MEMORY_METRICS_REFRESH_INTERVAL_SECONDS:
            return False

    update_memory_metrics()
    _LAST_MEMORY_METRICS_UPDATE_MONOTONIC = now
    return True


def record_symbology_batch_size(batch_size: int) -> None:
    """Record symbol count for symbology batch requests."""
    SYMBOLOGY_BATCH_SIZE.observe(max(0, batch_size))


def record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
    """Record route-level cache hit/miss events."""
    ROUTE_CACHE_EVENTS.labels(route=route, status=status, cache_mode=cache_mode).inc()


def record_request(method: str, path: str, status: int, duration: float) -> None:
    """Record HTTP request metrics."""
    # Normalize path to avoid high cardinality
    normalized_path = _normalize_path(path)
    REQUEST_COUNT.labels(method=method, path=normalized_path, status=str(status)).inc()
    REQUEST_DURATION.labels(method=method, path=normalized_path).observe(duration)


def record_cache_hit(cache_type: str = "memory") -> None:
    """Record cache hit."""
    CACHE_HITS.labels(cache_type=cache_type).inc()


def record_cache_miss(cache_type: str = "memory") -> None:
    """Record cache miss."""
    CACHE_MISSES.labels(cache_type=cache_type).inc()


def record_provider_request(provider: str, success: bool, duration: float) -> None:
    """Record upstream provider request."""
    status = "success" if success else "error"
    PROVIDER_REQUESTS.labels(provider=provider, status=status).inc()
    PROVIDER_LATENCY.labels(provider=provider).observe(duration)


def record_provider_quote_batch_size(provider: str, batch_size: int) -> None:
    """Record requested symbol count for provider multi-quote calls."""
    bounded_size = max(0, batch_size)
    PROVIDER_QUOTE_BATCH_SIZE.labels(provider=provider).observe(bounded_size)
    snapshot = _PROVIDER_QUOTE_BATCH_SNAPSHOT.setdefault(
        provider,
        {"count": 0, "total_symbols": 0, "max_batch_size": 0},
    )
    snapshot["count"] = int(snapshot.get("count", 0)) + 1
    snapshot["total_symbols"] = int(snapshot.get("total_symbols", 0)) + bounded_size
    snapshot["max_batch_size"] = max(int(snapshot.get("max_batch_size", 0)), bounded_size)


def get_provider_quote_batch_snapshot() -> dict[str, dict[str, Any]]:
    """Get provider multi-quote batch telemetry snapshot for admin status surfaces."""
    snapshot = deepcopy(_PROVIDER_QUOTE_BATCH_SNAPSHOT)
    for provider_data in snapshot.values():
        count = float(int(provider_data.get("count", 0)))
        total_symbols = float(int(provider_data.get("total_symbols", 0)))
        max_batch_size = float(int(provider_data.get("max_batch_size", 0)))
        avg_batch_size = _safe_ratio(total_symbols, count)
        batch_level = max(
            _threshold_level(avg_batch_size, warning_at=50, critical_at=100),
            _threshold_level(max_batch_size, warning_at=100, critical_at=250),
            key=lambda level: {"healthy": 0, "warning": 1, "critical": 2}[level],
        )
        recommendations: list[str] = []
        if avg_batch_size >= 50:
            recommendations.append("Consider lowering client max_symbols or splitting large quote requests.")
        if max_batch_size >= 100:
            recommendations.append("Review provider max_symbols_per_request and tune per-provider quote batching.")
        provider_data["derived"] = {
            "avg_batch_size": avg_batch_size,
            "batch_level": batch_level,
            "recommendations": recommendations,
        }
    return snapshot


def set_provider_health(provider: str, healthy: bool) -> None:
    """Set provider health status."""
    PROVIDER_HEALTH.labels(provider=provider).set(1 if healthy else 0)


def record_provider_health_check(provider: str, healthy: bool, duration: float) -> None:
    """Record provider health check duration."""
    status = "success" if healthy else "error"
    PROVIDER_HEALTH_CHECK_DURATION.labels(provider=provider, status=status).observe(duration)

    snapshot = _PROVIDER_HEALTH_CHECK_SNAPSHOT.setdefault(
        provider,
        {
            "count": 0,
            "success_count": 0,
            "error_count": 0,
            "total_duration_seconds": 0.0,
            "last_duration_seconds": 0.0,
        },
    )
    snapshot["count"] = int(snapshot.get("count", 0)) + 1
    if healthy:
        snapshot["success_count"] = int(snapshot.get("success_count", 0)) + 1
    else:
        snapshot["error_count"] = int(snapshot.get("error_count", 0)) + 1
    snapshot["total_duration_seconds"] = float(snapshot.get("total_duration_seconds", 0.0)) + max(0.0, duration)
    snapshot["last_duration_seconds"] = max(0.0, duration)


def get_provider_health_check_snapshot() -> dict[str, dict[str, Any]]:
    """Get provider health-check telemetry snapshot for admin status surfaces."""
    snapshot = deepcopy(_PROVIDER_HEALTH_CHECK_SNAPSHOT)
    for provider_data in snapshot.values():
        count = float(int(provider_data.get("count", 0)))
        error_count = float(int(provider_data.get("error_count", 0)))
        total_duration = float(provider_data.get("total_duration_seconds", 0.0))
        avg_duration_seconds = _safe_ratio(total_duration, count)
        error_rate = _safe_ratio(error_count, count)
        health_level = _threshold_level(error_rate, warning_at=0.05, critical_at=0.2)
        latency_level = _threshold_level(avg_duration_seconds, warning_at=0.3, critical_at=1.0)
        recommendations: list[str] = []
        if health_level != "healthy":
            recommendations.append("Review failing provider health checks and upstream error budgets.")
        if latency_level != "healthy":
            recommendations.append("Profile provider health endpoint latency and adjust check intervals.")
        provider_data["derived"] = {
            "avg_duration_seconds": avg_duration_seconds,
            "error_rate": error_rate,
            "health_level": health_level,
            "latency_level": latency_level,
            "recommendations": recommendations,
        }
    return snapshot


def record_provider_sync_call_wait(provider: str, duration: float) -> None:
    """Record sync-call queue wait duration."""
    PROVIDER_SYNC_CALL_WAIT.labels(provider=provider).observe(duration)


def record_provider_sync_call_exec(provider: str, duration: float) -> None:
    """Record sync-call execution duration."""
    PROVIDER_SYNC_CALL_EXEC.labels(provider=provider).observe(duration)


def inc_provider_sync_call_inflight(provider: str) -> None:
    """Increment in-flight sync-call gauge."""
    PROVIDER_SYNC_CALL_INFLIGHT.labels(provider=provider).inc()


def dec_provider_sync_call_inflight(provider: str) -> None:
    """Decrement in-flight sync-call gauge."""
    PROVIDER_SYNC_CALL_INFLIGHT.labels(provider=provider).dec()


def record_alphavantage_route_cache(endpoint: str, status: str, cache_mode: str) -> None:
    """Record Alpha Vantage route cache hit/miss event."""
    ALPHAVANTAGE_ROUTE_CACHE.labels(endpoint=endpoint, status=status, cache_mode=cache_mode).inc()


def record_alphavantage_payload_bytes(endpoint: str, cache_mode: str, payload_bytes: int) -> None:
    """Record Alpha Vantage payload size in bytes."""
    ALPHAVANTAGE_PAYLOAD_BYTES.labels(endpoint=endpoint, cache_mode=cache_mode).observe(max(payload_bytes, 0))


def record_rate_limit_exceeded(client_id: str) -> None:
    """Record rate limit exceeded event."""
    # Truncate client ID to avoid cardinality explosion
    safe_id = client_id[:20] if len(client_id) > 20 else client_id
    RATE_LIMIT_EXCEEDED.labels(client_id=safe_id).inc()


def _normalize_path(path: str) -> str:
    """Normalize path to reduce cardinality.

    Replaces variable path segments with placeholders.
    """
    cached = _PATH_NORMALIZATION_CACHE.get(path)
    if cached is not None:
        return cached

    parts = path.split("/")
    normalized = []

    for part in parts:
        if not part:
            continue
        # Replace symbols, IDs, etc. with placeholders
        if _looks_like_symbol(part):
            normalized.append("{symbol}")
        elif _looks_like_id(part):
            normalized.append("{id}")
        elif _looks_like_date(part):
            normalized.append("{date}")
        else:
            normalized.append(part)

    normalized_path = "/" + "/".join(normalized)
    if len(_PATH_NORMALIZATION_CACHE) >= _PATH_NORMALIZATION_CACHE_MAX:
        _prune_path_normalization_cache()
    _PATH_NORMALIZATION_CACHE[path] = normalized_path
    return normalized_path


def _prune_path_normalization_cache() -> None:
    """Prune oldest cached normalized paths instead of clearing all entries."""
    if not _PATH_NORMALIZATION_CACHE:
        return
    prune_count = min(_PATH_NORMALIZATION_CACHE_PRUNE_TARGET, len(_PATH_NORMALIZATION_CACHE))
    for key in list(_PATH_NORMALIZATION_CACHE)[:prune_count]:
        _PATH_NORMALIZATION_CACHE.pop(key, None)


def _looks_like_symbol(s: str) -> bool:
    """Check if string looks like a stock symbol."""
    return len(s) <= 5 and s.isupper() and s.isalpha()


def _looks_like_id(s: str) -> bool:
    """Check if string looks like an ID."""
    # UUIDs, long alphanumeric strings
    if len(s) > 8 and any(c.isdigit() for c in s):
        return True
    # Pure numbers
    return bool(s.isdigit() and len(s) > 4)


def _looks_like_date(s: str) -> bool:
    """Check if string looks like a date."""
    # YYYY-MM-DD format
    return bool(len(s) == 10 and s[4] == "-" and s[7] == "-")


# ─────────────────────────────────────────────────────────────────────────────
# SLI Helper Functions (PRD 11.1.2-4)
# ─────────────────────────────────────────────────────────────────────────────

_start_time: float = 0.0


def init_uptime() -> None:
    """Initialize uptime tracking."""
    import time

    global _start_time
    _start_time = time.time()
    UPTIME_SECONDS.set(0)
    HEALTH_STATUS.set(1)  # Start healthy


def update_uptime() -> None:
    """Update uptime metric."""
    import time

    if _start_time > 0:
        UPTIME_SECONDS.set(time.time() - _start_time)


def set_health_status(status: int) -> None:
    """Set health status (1=healthy, 0=degraded, -1=unhealthy)."""
    HEALTH_STATUS.set(status)


def record_error(error_type: str) -> None:
    """Record an error by type."""
    ERROR_COUNT.labels(error_type=error_type).inc()


def record_message_delivered(feed: str) -> None:
    """Record successful message delivery."""
    MESSAGE_DELIVERED.labels(feed=feed).inc()


def record_message_dropped(reason: str) -> None:
    """Record dropped message."""
    MESSAGE_DROPPED.labels(reason=reason).inc()


def update_memory_pressure(current_mb: float, target_mb: float) -> None:
    """Update memory pressure indicator."""
    if target_mb > 0:
        pressure = (current_mb / target_mb) * 100
        MEMORY_PRESSURE.set(min(pressure, 100))


# ─────────────────────────────────────────────────────────────────────────────
# Data Sink & Envelope Helper Functions
# ─────────────────────────────────────────────────────────────────────────────


def record_envelope_created(provider: str, feed: str) -> None:
    """Record EventEnvelope creation."""
    ENVELOPE_CREATED.labels(provider=provider, feed=feed).inc()


def record_sink_publish(sink: str, topic: str, success: bool) -> None:
    """Record data sink publish result."""
    status = "success" if success else "error"
    SINK_PUBLISH.labels(sink=sink, topic=topic, status=status).inc()


def record_sink_buffer_eviction(sink: str) -> None:
    """Increment the buffer-eviction counter — drives the silent-data-loss alert."""
    SINK_BUFFER_EVICTIONS.labels(sink=sink).inc()


def set_sink_buffer_size(sink: str, size: int) -> None:
    """Update the current failed-event buffer size for ``sink``."""
    SINK_BUFFER_SIZE.labels(sink=sink).set(max(0, size))


def set_sink_queue_size(sink: str, size: int) -> None:
    """Update the current dispatch-queue depth for ``sink``."""
    SINK_QUEUE_SIZE.labels(sink=sink).set(max(0, size))


def set_sink_queue_capacity(sink: str, capacity: int) -> None:
    """Set the configured dispatch-queue capacity for ``sink``."""
    SINK_QUEUE_CAPACITY.labels(sink=sink).set(max(0, capacity))


def set_sink_worker_count(sink: str, count: int) -> None:
    """Set the configured worker count draining ``sink``'s dispatch queue."""
    SINK_WORKER_COUNT.labels(sink=sink).set(max(0, count))


def record_sink_producer_timeout_drop(sink: str) -> None:
    """Increment the producer-side queue-put timeout counter (emergency drop)."""
    SINK_PRODUCER_TIMEOUT_DROPS.labels(sink=sink).inc()


def record_stream_sink_dispatch_event(status: str) -> None:
    """Record stream-to-sink scheduler lifecycle events."""
    STREAM_SINK_DISPATCH_EVENTS.labels(status=status).inc()
    events = _STREAM_SINK_DISPATCH_SNAPSHOT["events"]
    if isinstance(events, dict):
        events[status] = int(events.get(status, 0)) + 1


def set_stream_sink_pending_tasks(count: int) -> None:
    """Set current pending stream-to-sink task count."""
    pending = max(0, count)
    STREAM_SINK_PENDING_TASKS.set(pending)
    _STREAM_SINK_DISPATCH_SNAPSHOT["pending_tasks"] = pending


def set_stream_sink_dispatch_limits_metrics(
    *,
    max_inflight_publish: int,
    max_pending_tasks: int,
) -> None:
    """Set configured stream-to-sink scheduler limits."""
    STREAM_SINK_DISPATCH_LIMIT.labels(limit_type="max_inflight_publish").set(max(1, max_inflight_publish))
    STREAM_SINK_DISPATCH_LIMIT.labels(limit_type="max_pending_tasks").set(max(1, max_pending_tasks))
    limits = _STREAM_SINK_DISPATCH_SNAPSHOT["limits"]
    if isinstance(limits, dict):
        limits["max_inflight_publish"] = max(1, max_inflight_publish)
        limits["max_pending_tasks"] = max(1, max_pending_tasks)


def _safe_ratio(numerator: float, denominator: float) -> float:
    """Return a bounded ratio and avoid division-by-zero in derived telemetry."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _threshold_level(value: float, *, warning_at: float, critical_at: float) -> str:
    """Return health level from simple warning/critical thresholds."""
    if value >= critical_at:
        return "critical"
    if value >= warning_at:
        return "warning"
    return "healthy"


def get_stream_sink_dispatch_snapshot() -> dict[str, Any]:
    """Get stream-to-sink dispatch telemetry snapshot for admin status surfaces.

    "limits" mirror the registry's bounded-queue/worker-pool configuration:
    - max_inflight_publish maps to ``data_sink_worker_count``
    - max_pending_tasks  maps to ``data_sink_queue_size``

    Drop accounting now lives on the registry as
    ``gateway_sink_producer_timeout_drops_total`` (per-sink emergency
    counter); this snapshot only tracks dispatch-call lifecycle
    (scheduled / completed / failed / cancelled).
    """
    snapshot = deepcopy(_STREAM_SINK_DISPATCH_SNAPSHOT)
    limits = snapshot.get("limits", {})
    events = snapshot.get("events", {})

    max_pending_tasks = float(int(limits.get("max_pending_tasks", 0)))
    pending_tasks = float(int(snapshot.get("pending_tasks", 0)))
    scheduled = float(int(events.get("scheduled", 0)))
    completed = float(int(events.get("completed", 0)))

    pending_utilization = _safe_ratio(pending_tasks, max_pending_tasks)
    completion_rate = _safe_ratio(completed, scheduled)
    backpressure_level = max(
        _threshold_level(pending_utilization, warning_at=0.7, critical_at=0.9),
        _threshold_level(max(0.0, 1.0 - completion_rate), warning_at=0.05, critical_at=0.2),
        key=lambda level: {"healthy": 0, "warning": 1, "critical": 2}[level],
    )
    recommendations: list[str] = []
    if pending_utilization >= 0.7:
        recommendations.append(
            "Increase data_sink_queue_size or data_sink_worker_count, or inspect sink publish latency."
        )
    if completion_rate < 0.95:
        recommendations.append("Investigate sink publish failures/timeouts and callback backpressure.")

    snapshot["derived"] = {
        "pending_utilization": pending_utilization,
        "completion_rate": completion_rate,
        # Producer-timeout drops live on the registry now; preserve the
        # field for back-compat consumers but always emit 0.0 — operators
        # should watch gateway_sink_producer_timeout_drops_total instead.
        "drop_rate": 0.0,
        "completion_gap": max(0.0, 1.0 - completion_rate),
        "backpressure_level": backpressure_level,
        "recommendations": recommendations,
    }
    return snapshot


def record_stream_fanout_dispatch_event(status: str) -> None:
    """Record stream fanout dispatch lifecycle events."""
    STREAM_FANOUT_EVENTS.labels(status=status).inc()
    events = _STREAM_FANOUT_SNAPSHOT["events"]
    if isinstance(events, dict):
        events[status] = int(events.get(status, 0)) + 1


def record_stream_fanout_batch_size(batch_size: int) -> None:
    """Record fanout client batch size for each dispatch batch."""
    bounded_size = max(0, batch_size)
    STREAM_FANOUT_BATCH_SIZE.observe(bounded_size)
    batches = _STREAM_FANOUT_SNAPSHOT["batches"]
    if isinstance(batches, dict):
        batches["count"] = int(batches.get("count", 0)) + 1
        batches["total_clients"] = int(batches.get("total_clients", 0)) + bounded_size
        batches["max_batch_size"] = max(int(batches.get("max_batch_size", 0)), bounded_size)


def set_stream_fanout_limits_metrics(*, max_inflight: int, batch_size: int) -> None:
    """Set configured stream fanout limits."""
    STREAM_FANOUT_LIMIT.labels(limit_type="max_inflight").set(max(1, max_inflight))
    STREAM_FANOUT_LIMIT.labels(limit_type="batch_size").set(max(1, batch_size))
    limits = _STREAM_FANOUT_SNAPSHOT["limits"]
    if isinstance(limits, dict):
        limits["max_inflight"] = max(1, max_inflight)
        limits["batch_size"] = max(1, batch_size)


def get_stream_fanout_snapshot() -> dict[str, Any]:
    """Get stream fanout telemetry snapshot for admin status surfaces."""
    snapshot = deepcopy(_STREAM_FANOUT_SNAPSHOT)
    limits = snapshot.get("limits", {})
    events = snapshot.get("events", {})
    batches = snapshot.get("batches", {})

    count = float(int(batches.get("count", 0)))
    total_clients = float(int(batches.get("total_clients", 0)))
    configured_batch_size = float(int(limits.get("batch_size", 0)))
    delivered = float(int(events.get("delivered", 0)))
    errored = float(int(events.get("error", 0)))

    avg_batch_size = _safe_ratio(total_clients, count)
    batch_fill_ratio = _safe_ratio(avg_batch_size, configured_batch_size)
    error_rate = _safe_ratio(errored, delivered + errored)
    fanout_level = max(
        _threshold_level(batch_fill_ratio, warning_at=0.8, critical_at=0.95),
        _threshold_level(error_rate, warning_at=0.005, critical_at=0.02),
        key=lambda level: {"healthy": 0, "warning": 1, "critical": 2}[level],
    )
    recommendations: list[str] = []
    if batch_fill_ratio >= 0.8:
        recommendations.append("Increase stream_fanout_batch_size or shard high-fanout symbols.")
    if error_rate >= 0.005:
        recommendations.append("Increase stream_fanout_max_inflight and inspect client send latency.")

    snapshot["derived"] = {
        "avg_batch_size": avg_batch_size,
        "batch_fill_ratio": batch_fill_ratio,
        "error_rate": error_rate,
        "fanout_level": fanout_level,
        "recommendations": recommendations,
    }
    return snapshot


def record_option_capture_cycle(*, published: int, failed_symbols: int, skipped_market_closed: bool) -> None:
    """Record option capture cycle outcome counters and snapshot totals."""
    status = "skipped_market_closed" if skipped_market_closed else "partial_failure" if failed_symbols else "success"
    OPTION_CAPTURE_CYCLES.labels(status=status).inc()
    OPTION_CAPTURE_SNAPSHOTS_PUBLISHED.inc(max(0, published))
    OPTION_CAPTURE_FAILED_SYMBOLS.inc(max(0, failed_symbols))

    cycles = _OPTION_CAPTURE_SNAPSHOT["cycles"]
    if isinstance(cycles, dict):
        cycles["count"] = int(cycles.get("count", 0)) + 1
        cycles["published"] = int(cycles.get("published", 0)) + max(0, published)
        cycles["failed_symbols"] = int(cycles.get("failed_symbols", 0)) + max(0, failed_symbols)
        if skipped_market_closed:
            cycles["skipped_market_closed"] = int(cycles.get("skipped_market_closed", 0)) + 1


def record_option_capture_symbol_metrics(
    *,
    symbol: str,
    snapshot_contracts: int,
    ws_tracked_contracts: int,
    snapshot_age_seconds: float,
    greeks_coverage_ratio: float,
    iv_coverage_ratio: float,
    nonzero_open_interest_ratio: float,
    bid_ask_coverage_ratio: float,
) -> None:
    """Record per-symbol option capture quality gauges and admin snapshot state."""
    safe_symbol = symbol.strip().upper()
    OPTION_CAPTURE_SYMBOL_CONTRACTS.labels(symbol=safe_symbol, kind="snapshot_contracts").set(
        max(0, snapshot_contracts)
    )
    OPTION_CAPTURE_SYMBOL_CONTRACTS.labels(symbol=safe_symbol, kind="ws_tracked_contracts").set(
        max(0, ws_tracked_contracts)
    )
    OPTION_CAPTURE_QUALITY_RATIO.labels(symbol=safe_symbol, metric="greeks_coverage").set(
        max(0.0, min(1.0, greeks_coverage_ratio))
    )
    OPTION_CAPTURE_QUALITY_RATIO.labels(symbol=safe_symbol, metric="iv_coverage").set(
        max(0.0, min(1.0, iv_coverage_ratio))
    )
    OPTION_CAPTURE_QUALITY_RATIO.labels(symbol=safe_symbol, metric="nonzero_open_interest").set(
        max(0.0, min(1.0, nonzero_open_interest_ratio))
    )
    OPTION_CAPTURE_QUALITY_RATIO.labels(symbol=safe_symbol, metric="bid_ask_coverage").set(
        max(0.0, min(1.0, bid_ask_coverage_ratio))
    )
    OPTION_CAPTURE_SNAPSHOT_AGE.labels(symbol=safe_symbol).set(max(0.0, snapshot_age_seconds))

    symbols = _OPTION_CAPTURE_SNAPSHOT["symbols"]
    if isinstance(symbols, dict):
        symbols[safe_symbol] = {
            "snapshot_contracts": max(0, snapshot_contracts),
            "ws_tracked_contracts": max(0, ws_tracked_contracts),
            "snapshot_age_seconds": max(0.0, snapshot_age_seconds),
            "greeks_coverage_ratio": max(0.0, min(1.0, greeks_coverage_ratio)),
            "iv_coverage_ratio": max(0.0, min(1.0, iv_coverage_ratio)),
            "nonzero_open_interest_ratio": max(0.0, min(1.0, nonzero_open_interest_ratio)),
            "bid_ask_coverage_ratio": max(0.0, min(1.0, bid_ask_coverage_ratio)),
        }


def record_option_capture_ws_update(*, added: int = 0, removed: int = 0) -> None:
    """Record websocket add/remove activity for option capture."""
    websocket = _OPTION_CAPTURE_SNAPSHOT["websocket"]
    if added > 0:
        OPTION_CAPTURE_WS_EVENTS.labels(event="subscribe_calls").inc()
        OPTION_CAPTURE_WS_EVENTS.labels(event="contracts_added").inc(added)
        if isinstance(websocket, dict):
            websocket["subscribe_calls"] = int(websocket.get("subscribe_calls", 0)) + 1
            websocket["contracts_added"] = int(websocket.get("contracts_added", 0)) + added
    if removed > 0:
        OPTION_CAPTURE_WS_EVENTS.labels(event="unsubscribe_calls").inc()
        OPTION_CAPTURE_WS_EVENTS.labels(event="contracts_removed").inc(removed)
        if isinstance(websocket, dict):
            websocket["unsubscribe_calls"] = int(websocket.get("unsubscribe_calls", 0)) + 1
            websocket["contracts_removed"] = int(websocket.get("contracts_removed", 0)) + removed


def get_option_capture_snapshot() -> dict[str, Any]:
    """Return option capture telemetry snapshot for tests and admin surfaces."""
    return deepcopy(_OPTION_CAPTURE_SNAPSHOT)


def httpx_event_hooks(provider: str) -> dict[str, list]:
    """Create httpx event hooks to record provider metrics."""
    import time

    async def _on_request(request) -> None:
        request.extensions["gateway_metrics_start"] = time.perf_counter()

    async def _on_response(response) -> None:
        start = response.request.extensions.get("gateway_metrics_start")
        duration = time.perf_counter() - start if start else 0.0
        record_provider_request(provider, response.is_success, duration)

    return {"request": [_on_request], "response": [_on_response]}
