"""Prometheus metrics for gateway observability."""

from prometheus_client import Counter, Gauge, Histogram, Info

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

PROVIDER_ERRORS = Counter(
    "gateway_provider_errors_total",
    "Total provider request errors by type",
    ["provider", "error_type"],  # timeout, connection, auth, rate_limit, server_error, unknown
)

PROVIDER_LATENCY = Histogram(
    "gateway_provider_latency_seconds",
    "Upstream provider request latency",
    ["provider"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

PROVIDER_HEALTH = Gauge(
    "gateway_provider_healthy",
    "Provider health status (1=healthy, 0=unhealthy)",
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


def record_provider_error(provider: str, error_type: str) -> None:
    """Record provider request error with specific type.

    Args:
        provider: Provider name (e.g., 'alpaca', 'finnhub')
        error_type: Error classification:
            - 'timeout': Request timed out
            - 'connection': Connection failed
            - 'auth': Authentication error (401/403)
            - 'rate_limit': Provider rate limit hit (429)
            - 'server_error': Server error (5xx)
            - 'client_error': Client error (4xx)
            - 'unknown': Unclassified error
    """
    PROVIDER_ERRORS.labels(provider=provider, error_type=error_type).inc()


def set_provider_health(provider: str, healthy: bool) -> None:
    """Set provider health status."""
    PROVIDER_HEALTH.labels(provider=provider).set(1 if healthy else 0)


def record_rate_limit_exceeded(client_id: str) -> None:
    """Record rate limit exceeded event."""
    # Truncate client ID to avoid cardinality explosion
    safe_id = client_id[:20] if len(client_id) > 20 else client_id
    RATE_LIMIT_EXCEEDED.labels(client_id=safe_id).inc()


def _normalize_path(path: str) -> str:
    """Normalize path to reduce cardinality.

    Replaces variable path segments with placeholders.
    """
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

    return "/" + "/".join(normalized)


def _looks_like_symbol(s: str) -> bool:
    """Check if string looks like a stock symbol."""
    return len(s) <= 5 and s.isupper() and s.isalpha()


def _looks_like_id(s: str) -> bool:
    """Check if string looks like an ID."""
    # UUIDs, long alphanumeric strings
    if len(s) > 8 and any(c.isdigit() for c in s):
        return True
    # Pure numbers
    if s.isdigit() and len(s) > 4:
        return True
    return False


def _looks_like_date(s: str) -> bool:
    """Check if string looks like a date."""
    # YYYY-MM-DD format
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return True
    return False


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


def _classify_http_error(status_code: int) -> str:
    """Classify HTTP status code into error type."""
    if status_code == 401 or status_code == 403:
        return "auth"
    elif status_code == 429:
        return "rate_limit"
    elif 400 <= status_code < 500:
        return "client_error"
    elif 500 <= status_code < 600:
        return "server_error"
    else:
        return "unknown"


def httpx_event_hooks(provider: str) -> dict[str, list]:
    """Create httpx event hooks to record provider metrics."""
    import time

    async def _on_request(request) -> None:
        request.extensions["gateway_metrics_start"] = time.perf_counter()

    async def _on_response(response) -> None:
        start = response.request.extensions.get("gateway_metrics_start")
        duration = time.perf_counter() - start if start else 0.0
        is_success = response.is_success
        record_provider_request(provider, is_success, duration)

        # Record specific error type for failed requests
        if not is_success:
            error_type = _classify_http_error(response.status_code)
            record_provider_error(provider, error_type)

    async def _on_error(request, error) -> None:
        # Handle network/transport errors
        start = request.extensions.get("gateway_metrics_start")
        duration = time.perf_counter() - start if start else 0.0
        record_provider_request(provider, success=False, duration=duration)

        # Classify the error type based on exception
        error_type = "unknown"
        error_name = type(error).__name__.lower()

        if "timeout" in error_name or "timeout" in str(error).lower():
            error_type = "timeout"
        elif "connect" in error_name or "connection" in str(error).lower():
            error_type = "connection"
        elif "auth" in error_name:
            error_type = "auth"

        record_provider_error(provider, error_type)

    return {
        "request": [_on_request],
        "response": [_on_response],
        "request_exception": [_on_error],
    }
