"""Data Gateway - FastAPI application."""

# Load .env into os.environ BEFORE any other imports
from dotenv import load_dotenv

load_dotenv()

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from gateway import __version__
from gateway.api import (
    adjustments_router,
    admin_router,
    alpaca_router,
    alphavantage_router,
    bulk_router,
    calendar_router,
    catalog_router,
    corporate_router,
    finnhub_router,
    health_router,
    news_router,
    quality_router,
    replay_router,
    sec_router,
    symbology_router,
    uw_router,
    websocket_router,
    yf_router,
)
from gateway.api.deps import get_connection_manager, set_multiplexer, set_registry
from gateway.api.metrics import router as metrics_router
from gateway.api.middleware import (
    CacheMiddleware,
    EventEnvelopeMiddleware,
    GlobalRateLimitMiddleware,
    RateLimitMiddleware,
    SecurityHeadersMiddleware,
)
from gateway.config import get_settings
from gateway.core.metrics import init_metrics
from gateway.core.registry import ProviderRegistry
from gateway.core.stream import StreamMultiplexer

# Configure structlog
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def _on_stream_data(client_id: str, data_type: str, envelope: dict) -> None:
    """Callback when stream data arrives. Sends envelope to connected client.

    The envelope contains:
    - event_id: Idempotency hash for deduplication
    - provider, feed, instrument_key: Routing metadata
    - payload: The raw normalized event data

    For backward compatibility, we include both 'envelope' and 'data' fields.
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

    # Publish to data sink for Heber storage (non-blocking)
    from gateway.api.deps import get_sink_registry

    sink_registry = get_sink_registry()
    if sink_registry:
        # Publish all events to single Heber stream for unified ingestion
        await sink_registry.publish_all("heber:events", envelope)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    import asyncio
    import signal

    settings = get_settings()

    # Startup
    logger.info(
        "gateway_starting",
        version=__version__,
        debug=settings.debug,
    )

    # Initialize metrics
    init_metrics(version=__version__)

    # Initialize provider registry
    registry = ProviderRegistry()
    await registry.load_from_config(settings.providers_config_path)
    set_registry(registry)

    # Initialize stream multiplexer (only if credentials are set)
    multiplexer = None
    if settings.alpaca_api_key and settings.alpaca_secret_key:
        multiplexer = StreamMultiplexer(
            api_key=settings.alpaca_api_key,
            api_secret=settings.alpaca_secret_key,
            on_data=_on_stream_data,
            use_iex=settings.stream_use_iex,
            lazy_connect=settings.stream_lazy_connect,
        )
        set_multiplexer(multiplexer)
        await multiplexer.start()
        logger.info("multiplexer_initialized", lazy_connect=settings.stream_lazy_connect)
    else:
        logger.warning("multiplexer_skipped", reason="Missing Alpaca credentials")

    # Initialize data sink for Heber integration
    sink_registry = None
    if settings.data_sink_enabled and settings.data_sink_redis_url:
        from gateway.api.deps import set_sink_registry
        from gateway.core.data_sink import DataSinkRegistry
        from gateway.core.redis_sink import RedisStreamsSink

        sink_registry = DataSinkRegistry()
        redis_sink = RedisStreamsSink(
            redis_url=settings.data_sink_redis_url,
            max_len=settings.data_sink_max_stream_len,
        )
        sink_registry.register(redis_sink)
        set_sink_registry(sink_registry)
        logger.info("data_sink_initialized", sink="redis_streams")
    elif settings.data_sink_enabled:
        logger.warning("data_sink_skipped", reason="Missing GATEWAY_DATA_SINK_REDIS_URL")

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
    except (ValueError, OSError):
        logger.warning("sighup_handler_failed", reason="Not supported on this platform")

    # Start UW background poller (if data sink is enabled)
    uw_poller = None
    if settings.data_sink_enabled and settings.data_sink_redis_url:
        from gateway.core.uw_poller import start_uw_poller

        uw_poller = await start_uw_poller(
            poll_interval_seconds=300,  # 5 minutes
            flow_enabled=True,
            darkpool_enabled=True,
            market_tide_enabled=True,
        )
        logger.info("uw_poller_initialized", interval_seconds=300)

    yield

    # Shutdown with graceful drain (PRD 6.5, 11.3.4)
    drain_seconds = settings.shutdown_drain_seconds
    logger.info("shutdown_starting", drain_seconds=drain_seconds)

    # PRIORITY: Stop multiplexer FIRST to release Alpaca WebSocket connections immediately
    # This prevents "connection limit exceeded" errors on restart
    if multiplexer:
        logger.info("multiplexer_shutdown_starting")
        try:
            await asyncio.wait_for(multiplexer.stop(), timeout=15.0)
            logger.info("multiplexer_shutdown_complete")
        except TimeoutError:
            logger.error("multiplexer_shutdown_timeout", timeout_seconds=15)
        except Exception as e:
            logger.error("multiplexer_shutdown_error", error=str(e))

    # Stop accepting new connections
    connections = get_connection_manager()
    logger.info("shutdown_connections", active=connections.active_count)

    # Wait for drain period (allow in-flight requests to complete)
    if drain_seconds > 0:
        await asyncio.sleep(drain_seconds)

    # Shutdown UW poller
    if uw_poller:
        from gateway.core.uw_poller import stop_uw_poller

        await stop_uw_poller()

    # Shutdown remaining components
    await registry.shutdown()
    logger.info("gateway_shutdown_complete")


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

    # Middleware (order matters: first added = outermost)
    # Security headers should be outermost (applied last, seen first by client)
    app.add_middleware(SecurityHeadersMiddleware)
    # EventEnvelope wraps responses LAST (outermost) so it sees final cached data
    app.add_middleware(EventEnvelopeMiddleware)
    app.add_middleware(CacheMiddleware, default_ttl=settings.cache_default_ttl)
    app.add_middleware(RateLimitMiddleware, default_limit=settings.rate_limit_default)
    # Global rate limit (PRD 7.5.1-2) before per-client limits
    app.add_middleware(GlobalRateLimitMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.debug else [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(health_router)
    app.include_router(websocket_router)
    app.include_router(alpaca_router)
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
    app.include_router(corporate_router)
    app.include_router(adjustments_router)
    app.include_router(quality_router)
    app.include_router(catalog_router)

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
