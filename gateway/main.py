"""Data Gateway - FastAPI application."""

# Load .env into os.environ BEFORE any other imports
from dotenv import load_dotenv

load_dotenv()

import asyncio
import os
import socket
import sys
from contextlib import asynccontextmanager, suppress

import orjson

# uvloop removed — incompatible with container environment (causes deadlocks).
# Standard asyncio event loop is used instead.
from empire_core.logger import setup_logging

setup_logging("data-gateway")


def _check_port_available(host: str, port: int) -> None:
    """Fail fast if another process is already listening on our port.

    Prevents silent credential conflicts where two gateway instances
    compete for the same Alpaca WebSocket connection pool.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
    except OSError:
        print(
            f"FATAL: port {port} is already in use. "
            "Another gateway instance (bare-metal or Docker) may be running. "
            "Kill it first: lsof -ti :{port} | xargs kill -9",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        sock.close()


# Only check port when running under uvicorn as a single worker (not during
# test imports or multi-worker mode where the master process binds the port).
if "uvicorn" in sys.modules:
    from gateway.config import get_settings as _get_boot_settings

    _boot = _get_boot_settings()
    # Skip port check when running as a forked worker (WEB_CONCURRENCY or
    # parent process already holds the socket).
    if os.environ.get("WEB_CONCURRENCY") is None and os.getppid() != 1:
        _check_port_available(_boot.host, _boot.port)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from gateway import __version__
from gateway.api import (
    adjustments_router,
    admin_router,
    alpaca_router,
    alphavantage_router,
    backfill_router,
    bulk_router,
    calendar_router,
    catalog_router,
    corporate_router,
    finnhub_router,
    health_router,
    legacy_adjustments_router,
    legacy_corporate_router,
    legacy_symbology_router,
    market_router,
    news_router,
    replay_router,
    sec_router,
    symbology_router,
    uw_router,
    websocket_router,
    yf_router,
)
from gateway.api.admin import attach_error_buffer_handler
from gateway.api.deps import get_connection_manager
from gateway.api.errors import gateway_http_exception_handler
from gateway.api.metrics import router as metrics_router
from gateway.api.middleware import (
    CacheMiddleware,
    EventEnvelopeMiddleware,
    GlobalRateLimitMiddleware,
    InputValidationMiddleware,
    RateLimitMiddleware,
    RequestMetricsMiddleware,
    SecurityHeadersMiddleware,
)
from gateway.config import get_settings
from gateway.core.globals import set_flow_fanout, set_multiplexer, set_registry
from gateway.core.logger import logger
from gateway.core.metrics import (
    init_metrics,
    init_uptime,
    record_stream_sink_dispatch_event,
    set_stream_sink_dispatch_limits_metrics,
    update_uptime,
)
from gateway.core.registry import ProviderRegistry
from gateway.core.stream import StreamMultiplexer

attach_error_buffer_handler()


async def _on_stream_data(client_id: str, data_type: str, envelope: dict) -> None:
    """Per-client WS delivery callback (used by the FALLBACK fanout path only).

    The envelope contains:
    - event_id: Idempotency hash for deduplication
    - provider, feed, instrument_key: Routing metadata
    - payload: The raw normalized event data

    For backward compatibility, we include both 'envelope' and 'data' fields.

    IMPORTANT: This callback is ONLY invoked in StreamMultiplexer's fallback
    fanout path. In production the multiplexer is wired with
    ``on_broadcast=connections.broadcast_to_connection_ids`` which takes the
    fast-path and never calls on_data per client. Per-envelope side-effects
    (sink publish, dedup, etc.) MUST live in ``_on_stream_envelope`` (wired
    as ``on_envelope`` on the multiplexer) — not here — so they fire exactly
    once per envelope regardless of which fanout path is taken.

    Historical note (2026-05-21): sink publish previously lived inline in
    this function. Because production uses the broadcast fast-path, sink
    publish was silently bypassed for ALL streaming events — Heber received
    zero `source:stream` events while operators believed PR #32's
    bounded-queue refactor was protecting that path. Fixed by extracting
    sink dispatch to ``_on_stream_envelope``.
    """
    connections = get_connection_manager()
    connection = connections.get(client_id)
    if connection and connection.websocket:
        try:
            await connection.websocket.send_json(
                {
                    "type": "data",
                    "feed": data_type,
                    "symbol": envelope.get("symbol", ""),
                    "event_id": envelope.get("event_id"),
                    "envelope": envelope,  # Full envelope for downstream consumers
                    "data": envelope.get("payload", {}),  # Backward compat: raw event data
                }
            )
        except Exception as e:
            logger.error(
                "stream_send_error",
                client_id=client_id,
                event_id=envelope.get("event_id", "unknown"),
                error=str(e),
            )


async def _on_stream_envelope(envelope: dict) -> None:
    """Per-envelope sink-dispatch callback. Fires ONCE per upstream envelope,
    regardless of which fanout path the multiplexer takes (broadcast
    fast-path OR fallback per-client). This is the single source of truth
    for streaming → Heber publishing.

    The registry's bounded queue is the single in-process gate — producers
    block briefly only if the queue is saturated, and drops show up as
    producer-timeout drops (``gateway_sink_producer_timeout_drops_total``).
    """
    sink_registry = _stream_sink_registry
    if sink_registry is None:
        return
    record_stream_sink_dispatch_event("scheduled")
    try:
        # Pre-serialize once so the Redis sink doesn't have to re-encode.
        try:
            payload: dict | str = orjson.dumps(envelope, default=str).decode()
        except Exception:
            payload = envelope
        await sink_registry.publish_all(STREAM_SINK_TOPIC, payload, source="poller", feed=envelope.get("feed"))
        record_stream_sink_dispatch_event("completed")
    except asyncio.CancelledError:
        record_stream_sink_dispatch_event("cancelled")
        raise
    except Exception as exc:
        record_stream_sink_dispatch_event("failed")
        logger.warning(
            "stream_sink_publish_failed",
            event_id=envelope.get("event_id", "unknown"),
            error=str(exc),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Stream-to-Sink Dispatch
# ─────────────────────────────────────────────────────────────────────────────

_stream_sink_registry = None
STREAM_SINK_TOPIC = "heber:events"


def _set_stream_sink_registry(sink_registry) -> None:
    global _stream_sink_registry
    _stream_sink_registry = sink_registry


def _create_data_sink_dedup_cache(settings):
    from gateway.core.cache import RedisCache

    return RedisCache(
        redis_url=settings.data_sink_redis_url,
        default_ttl=86400,  # 24h TTL for dedup keys
        max_connections=settings.data_sink_redis_pool_size,
        # Tie the dedup pool to the sink worker count so a saturated dedup pool
        # can never starve the workers (the "Too many connections" flood) — the
        # REST/poller dict path runs set_nx inline per published event.
        worker_count=settings.data_sink_worker_count,
        operation_timeout_seconds=settings.data_sink_operation_timeout_seconds,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    import signal

    settings = get_settings()
    # Expose the registry's bounded-queue limits on the stream-sink
    # dispatch snapshot so admin/health surfaces still report queue
    # capacity + worker count. The "max_inflight_publish" label maps to
    # the registry worker count (concurrency for sink publishes) and
    # "max_pending_tasks" maps to the registry queue depth.
    set_stream_sink_dispatch_limits_metrics(
        max_inflight_publish=settings.data_sink_worker_count,
        max_pending_tasks=settings.data_sink_queue_size,
    )
    _set_stream_sink_registry(None)

    # Startup
    logger.info(
        "gateway_starting",
        version=__version__,
        debug=settings.debug,
    )

    # Initialize metrics
    init_metrics(version=__version__)
    init_uptime()

    async def _uptime_loop() -> None:
        try:
            while True:
                update_uptime()
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass

    uptime_task = asyncio.create_task(_uptime_loop())

    # Initialize provider registry. In production (debug=False) we enforce
    # `required: true` providers — gateway refuses to boot if a critical
    # provider fails to initialize, instead of silently degrading. Local dev
    # (debug=True) keeps the lenient behavior so developers can run without
    # all 7 provider keys.
    registry = ProviderRegistry()
    await registry.load_from_config(settings.providers_config_path, strict_required=not settings.debug)
    set_registry(registry)

    # Initialize stream multiplexer (only if credentials are set)
    multiplexer = None
    if settings.alpaca_api_key and settings.alpaca_secret_key:
        connections = get_connection_manager()
        # Parse comma-separated eager-connect list. "stocks" is the default —
        # so the first 9:30 ET subscribe by a trading bot doesn't pay the
        # upstream Alpaca cold-start (~30s of TLS + auth + first-bar drain).
        eager_types = [t.strip().lower() for t in (settings.stream_eager_connect_types or "").split(",") if t.strip()]
        multiplexer = StreamMultiplexer(
            api_key=settings.alpaca_api_key,
            api_secret=settings.alpaca_secret_key,
            on_data=_on_stream_data,
            use_iex=settings.stream_use_iex,
            options_feed=settings.stream_options_feed,
            lazy_connect=settings.stream_lazy_connect,
            fanout_max_inflight=settings.stream_fanout_max_inflight,
            fanout_batch_size=settings.stream_fanout_batch_size,
            on_broadcast=connections.broadcast_to_connection_ids,
            on_envelope=_on_stream_envelope,
            eager_connect_types=eager_types,
        )
        set_multiplexer(multiplexer)
        await multiplexer.start()
        logger.info(
            "multiplexer_initialized",
            lazy_connect=settings.stream_lazy_connect,
            eager_connect_types=eager_types,
            options_feed=settings.stream_options_feed,
        )
    else:
        logger.warning("multiplexer_skipped", reason="Missing Alpaca credentials")

    # Initialize data sink for Heber integration
    sink_registry = None
    if settings.data_sink_enabled and settings.data_sink_redis_url:
        from gateway.core.data_sink import DataSinkRegistry
        from gateway.core.globals import set_sink_registry
        from gateway.core.redis_sink import RedisStreamsSink

        sink_registry = DataSinkRegistry(
            queue_size=settings.data_sink_queue_size,
            worker_count=settings.data_sink_worker_count,
            producer_block_timeout_seconds=settings.data_sink_producer_block_timeout_seconds,
        )
        redis_sink = RedisStreamsSink(
            redis_url=settings.data_sink_redis_url,
            max_len=settings.data_sink_max_stream_len,
            operation_timeout_seconds=settings.data_sink_operation_timeout_seconds,
            pool_size=settings.data_sink_redis_pool_size,
            # Tie the XADD pool to the worker count so a worker_count set above
            # pool_size can't serialize the workers on connection acquisition.
            worker_count=settings.data_sink_worker_count,
        )
        sink_registry.register(redis_sink)

        # Enable dedup cache to prevent duplicate events in Heber
        dedup_cache = _create_data_sink_dedup_cache(settings)
        sink_registry.set_dedup_cache(dedup_cache)

        set_sink_registry(sink_registry)
        _set_stream_sink_registry(sink_registry)
        logger.info("data_sink_initialized", sink="redis_streams", dedup_enabled=True)
    elif settings.data_sink_enabled:
        logger.warning("data_sink_skipped", reason="Missing GATEWAY_DATA_SINK_REDIS_URL")

    # Configure backfill engine with provider and sink registries
    from gateway.core.backfill import get_backfill_engine

    backfill_engine = get_backfill_engine(
        lightweight_concurrency=settings.backfill_lightweight_concurrency,
        heavyweight_concurrency=settings.backfill_heavyweight_concurrency,
    )
    backfill_engine.configure(
        provider_registry=registry,
        sink_registry=sink_registry,
    )
    logger.info("backfill_engine_configured")

    # SIGHUP handler for hot config reload (PRD 6.5.4)
    def handle_sighup(signum, frame):
        logger.info("sighup_received", action="reloading_config")
        # Clear settings cache to reload on next access
        get_settings.cache_clear()
        # Reload client authenticator
        from gateway.api.deps import get_authenticator

        auth = get_authenticator()
        auth.reload()
        logger.info("config_reloaded")

    # Register SIGHUP handler (Unix only)
    try:
        signal.signal(signal.SIGHUP, handle_sighup)
        logger.info("sighup_handler_registered")
    except ValueError:
        # signal.signal() raises ValueError when called from a non-main thread
        # (e.g. during pytest). This is expected and not a platform limitation.
        logger.debug("sighup_handler_skipped", reason="Not in main thread (expected during tests)")
    except OSError:
        logger.warning("sighup_handler_failed", reason="Not supported on this platform")

    # Construct the UW flow WS fan-out (additive; inert until a client
    # subscribes). Registered as the poller's flow tap below so published flow
    # envelopes are also delivered over WS without touching the Redis publish.
    flow_fanout = None
    if settings.ws_flow_fanout_enabled:
        from gateway.core.flow_fanout import FlowFanout

        flow_fanout = FlowFanout(get_connection_manager())
        set_flow_fanout(flow_fanout)
        logger.info("flow_fanout_initialized")

    # Start UW background poller (if data sink is enabled)
    uw_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url:
        from gateway.core.uw_poller import start_uw_poller

        uw_poller = await start_uw_poller(
            poll_interval_seconds=300,  # 5 minutes
            flow_enabled=True,
            darkpool_enabled=True,
            market_tide_enabled=True,
            eod_enabled=settings.uw_eod_enabled,
            eod_hour=settings.uw_eod_hour,
            eod_minute=settings.uw_eod_minute,
            eod_concurrency=settings.uw_eod_concurrency,
        )
        # Tap the poller's flow path so published flow envelopes fan out over WS.
        # Mark the fan-out producer-wired so a flow subscribe ACKs "ok" only
        # when envelopes can actually arrive (the subscribe handler warns
        # otherwise — e.g. when the data sink / Redis poller never started).
        if flow_fanout is not None:
            uw_poller.on_flow_envelope = flow_fanout.deliver
            flow_fanout.mark_producer_wired()
        logger.info(
            "uw_poller_initialized",
            interval_seconds=300,
            eod_enabled=settings.uw_eod_enabled,
            flow_fanout=flow_fanout is not None,
        )

    # Start Treasury yield background poller (if data sink is enabled and AlphaVantage is ready)
    treasury_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url:
        av_provider = registry.get("alphavantage")
        if av_provider is not None and getattr(av_provider, "_api_key", ""):
            from gateway.core.treasury_poller import start_treasury_poller

            treasury_poller = await start_treasury_poller(
                maturities=settings.treasury_poller_maturity_list,
                poll_interval_seconds=86400,
            )
            logger.info(
                "treasury_poller_initialized",
                maturities=settings.treasury_poller_maturity_list,
            )
        else:
            reason = (
                "AlphaVantage provider not available" if av_provider is None else "AlphaVantage API key not configured"
            )
            logger.warning("treasury_poller_skipped", reason=reason)

    # Start Alpaca quotes REST-fallback poller (ensures equity quotes flow to Heber
    # without requiring a WebSocket client to subscribe).
    quotes_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url and settings.quotes_poller_enabled:
        if registry.get("alpaca") is not None:
            from gateway.core.quotes_poller import start_quotes_poller

            quotes_poller = await start_quotes_poller(
                symbols=settings.quotes_poller_symbol_list,
                poll_interval_seconds=settings.quotes_poller_interval_seconds,
            )
            logger.info(
                "quotes_poller_initialized",
                symbols=len(settings.quotes_poller_symbol_list) if settings.quotes_poller_symbol_list else "default",
                interval_seconds=settings.quotes_poller_interval_seconds,
            )
        else:
            logger.warning("quotes_poller_skipped", reason="Alpaca provider not available")

    # Start Alpaca trades REST-fallback poller (same rationale as quotes_poller).
    trades_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url and settings.trades_poller_enabled:
        if registry.get("alpaca") is not None:
            from gateway.core.trades_poller import start_trades_poller

            trades_poller = await start_trades_poller(
                symbols=settings.trades_poller_symbol_list,
                poll_interval_seconds=settings.trades_poller_interval_seconds,
            )
            logger.info(
                "trades_poller_initialized",
                symbols=len(settings.trades_poller_symbol_list) if settings.trades_poller_symbol_list else "default",
                interval_seconds=settings.trades_poller_interval_seconds,
            )
        else:
            logger.warning("trades_poller_skipped", reason="Alpaca provider not available")

    # Start Alpaca crypto poller (24/7 — no market-hours gate).
    crypto_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url and settings.crypto_poller_enabled:
        if registry.get("alpaca") is not None:
            from gateway.core.crypto_poller import start_crypto_poller

            crypto_poller = await start_crypto_poller(
                pairs=settings.crypto_poller_pair_list,
                poll_interval_seconds=settings.crypto_poller_interval_seconds,
            )
            logger.info(
                "crypto_poller_initialized",
                pairs=len(settings.crypto_poller_pair_list) if settings.crypto_poller_pair_list else "default",
                interval_seconds=settings.crypto_poller_interval_seconds,
            )
        else:
            logger.warning("crypto_poller_skipped", reason="Alpaca provider not available")

    # Start Alpaca news poller (REST-based; news WS only delivers when a client subscribes).
    news_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url and settings.news_poller_enabled:
        if registry.get("alpaca") is not None:
            from gateway.core.news_poller import start_news_poller

            news_poller = await start_news_poller(
                symbols=settings.news_poller_symbol_list,
                poll_interval_seconds=settings.news_poller_interval_seconds,
                fetch_limit=settings.news_poller_fetch_limit,
            )
            logger.info(
                "news_poller_initialized",
                symbols=settings.news_poller_symbol_list or "all",
                interval_seconds=settings.news_poller_interval_seconds,
            )
        else:
            logger.warning("news_poller_skipped", reason="Alpaca provider not available")

    option_capture_service = None
    if settings.option_capture_enabled:
        from gateway.core.option_capture import start_option_capture_service

        option_capture_service = await start_option_capture_service(
            registry=registry,
            multiplexer=multiplexer,
            sink_registry=sink_registry,
            settings=settings,
        )
        logger.info(
            "option_capture_initialized",
            symbols=settings.option_capture_symbol_list,
            interval_seconds=settings.option_capture_interval_seconds,
            market_hours_only=settings.option_capture_market_hours_only,
            websocket_enabled=settings.option_capture_ws_enabled,
        )

    yield

    # ── PRD §Graceful Shutdown: 8-step sequence ────────────────────────────

    # Step 1: Mark as shutting down (health endpoints → 503)
    from gateway.core.shutdown import ShutdownCoordinator

    coordinator = ShutdownCoordinator.get_instance()
    drain_seconds = settings.shutdown_drain_seconds
    coordinator.start_shutdown(drain_seconds=drain_seconds)
    logger.info("shutdown_starting", drain_seconds=drain_seconds)

    # Step 2: Notify connected clients
    connections = get_connection_manager()
    notified = await connections.broadcast_shutdown(timeout_seconds=drain_seconds)
    logger.info("shutdown_clients_notified", count=notified)

    # Step 3: Drain period — continue delivering queued messages
    if drain_seconds > 0:
        await asyncio.sleep(drain_seconds)

    # Step 4: Stop streaming PRODUCERS first (option capture, multiplexer).
    # Order is critical: we must stop everything that emits events into
    # ``sink_registry`` *before* draining and closing the sink. Otherwise the
    # registry flips ``_enabled=False`` and any in-flight tail event from a
    # producer's final iteration is silently dropped (`data_sink.publish_all`
    # early-returns on a disabled registry).
    if option_capture_service:
        from gateway.core.option_capture import stop_option_capture_service

        await stop_option_capture_service()

    if multiplexer:
        logger.info("multiplexer_shutdown_starting")
        try:
            await asyncio.wait_for(multiplexer.stop(), timeout=15.0)
            logger.info("multiplexer_shutdown_complete")
        except TimeoutError:
            logger.error("multiplexer_shutdown_timeout", timeout_seconds=15)
        except Exception as e:
            logger.error("multiplexer_shutdown_error", error=str(e))

    # Step 5: Stop polling PRODUCERS (treasury, quotes, trades, crypto, news,
    # UW) and the backfill engine + provider registry. Pollers can publish
    # one last batch on shutdown — keeping the sink open while they stop
    # guarantees those events get a chance to land in Redis.
    if treasury_poller:
        from gateway.core.treasury_poller import stop_treasury_poller

        await stop_treasury_poller()

    if quotes_poller:
        from gateway.core.quotes_poller import stop_quotes_poller

        await stop_quotes_poller()

    if trades_poller:
        from gateway.core.trades_poller import stop_trades_poller

        await stop_trades_poller()

    if crypto_poller:
        from gateway.core.crypto_poller import stop_crypto_poller

        await stop_crypto_poller()

    if news_poller:
        from gateway.core.news_poller import stop_news_poller

        await stop_news_poller()

    if uw_poller:
        from gateway.core.uw_poller import stop_uw_poller

        await stop_uw_poller()

    # Clear the flow fan-out global so a subsequent in-process restart rebuilds
    # it against the fresh connection manager.
    set_flow_fanout(None)

    from gateway.core.backfill import get_backfill_engine

    await get_backfill_engine().shutdown()
    await registry.shutdown()

    # Step 6: Close client connections with 1001 Going Away
    await connections.close_all(code=1001, reason="Going Away")

    # Step 7: Drain queues and close sink connections — now that all
    # producers have stopped, ``sink_registry.close_all()`` can safely drain
    # the bounded queue and tear down workers without losing tail events.
    if sink_registry:
        await sink_registry.close_all()

    uptime_task.cancel()
    with suppress(asyncio.CancelledError):
        await uptime_task
    logger.info("gateway_shutdown_complete")
    _set_stream_sink_registry(None)
    # Step 8: Reset coordinator state for next startup
    ShutdownCoordinator.reset()


def create_app() -> FastAPI:
    """Application factory."""
    settings = get_settings()

    app = FastAPI(
        title="Data Gateway",
        description="Unified financial data gateway for the Empire Trading Framework",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
    )
    app.add_exception_handler(HTTPException, gateway_http_exception_handler)

    # Middleware (order matters: first added = outermost)
    # Security headers should be outermost (applied last, seen first by client)
    app.add_middleware(SecurityHeadersMiddleware)
    # Request metrics should wrap the stack to capture end-to-end timing
    app.add_middleware(RequestMetricsMiddleware)
    # Input validation for request size/limits
    app.add_middleware(InputValidationMiddleware)
    # EventEnvelope wraps responses LAST (outermost) so it sees final cached data
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=settings.cache_max_body_bytes)
    app.add_middleware(
        CacheMiddleware,
        default_ttl=settings.cache_default_ttl,
        max_size=settings.cache_max_size,
        max_body_bytes=settings.cache_max_body_bytes,
    )
    app.add_middleware(RateLimitMiddleware, default_limit=settings.rate_limit_default)
    # Global rate limit (PRD 7.5.1-2) before per-client limits
    app.add_middleware(
        GlobalRateLimitMiddleware,
        trust_proxy_headers=settings.behind_trusted_proxy,
        trusted_proxy_cidrs=settings.trusted_proxy_cidrs,
    )
    # CORS: API uses X-Gateway-Key header auth, not cookies, so credentials are
    # not required. The CORS spec FORBIDS allow_origins=["*"] with credentials=True
    # (browsers reject; non-browser clients bypass silently). Keep credentials off
    # whenever using a wildcard origin.
    cors_origins = ["*"] if settings.debug else []
    cors_credentials = "*" not in cors_origins
    if "*" in cors_origins and cors_credentials:
        raise RuntimeError("CORS misconfig: allow_origins=['*'] with allow_credentials=True is forbidden")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=cors_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(health_router)
    app.include_router(websocket_router)
    app.include_router(alpaca_router)
    app.include_router(market_router)
    app.include_router(admin_router)
    app.include_router(uw_router)
    app.include_router(news_router)
    app.include_router(yf_router)
    app.include_router(sec_router)
    app.include_router(finnhub_router)
    app.include_router(alphavantage_router)
    app.include_router(metrics_router)
    app.include_router(bulk_router)
    app.include_router(replay_router)
    app.include_router(calendar_router)
    app.include_router(symbology_router)
    app.include_router(legacy_symbology_router)
    app.include_router(corporate_router)
    app.include_router(adjustments_router)
    app.include_router(legacy_corporate_router)
    app.include_router(legacy_adjustments_router)
    app.include_router(catalog_router)
    app.include_router(backfill_router)

    # Root endpoint
    @app.get("/")
    async def root():
        return {
            "name": "Data Gateway",
            "version": __version__,
            "status": "ok",
        }

    return app


# Create app instance
app = create_app()
