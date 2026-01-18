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
from gateway.api.middleware import CacheMiddleware, EventEnvelopeMiddleware, RateLimitMiddleware
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
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
        )
        set_multiplexer(multiplexer)
        await multiplexer.start()
        logger.info("multiplexer_initialized")
    else:
        logger.warning("multiplexer_skipped", reason="Missing Alpaca credentials")

    yield

    # Shutdown
    if multiplexer:
        await multiplexer.stop()
    await registry.shutdown()
    logger.info("gateway_shutting_down")


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
    # EventEnvelope wraps responses LAST (outermost) so it sees final cached data
    app.add_middleware(EventEnvelopeMiddleware)
    app.add_middleware(CacheMiddleware, default_ttl=settings.cache_default_ttl)
    app.add_middleware(RateLimitMiddleware, default_limit=settings.rate_limit_default)
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
