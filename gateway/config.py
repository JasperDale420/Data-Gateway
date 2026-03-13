"""Gateway configuration using pydantic-settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="GATEWAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    app_name: str = "Data Gateway"
    debug: bool = False
    log_level: str = "INFO"
    allow_stub_data: bool = False

    # Server
    host: str = "0.0.0.0"
    port: int = 8080

    # Authentication
    auth_timeout_seconds: int = 10
    clients_config_path: Path = Path("config/clients.yaml")

    # Providers
    providers_config_path: Path = Path("config/providers.yaml")

    # Cache
    cache_max_size: int = Field(default=10000, ge=100)
    cache_default_ttl: int = Field(default=300, ge=1)  # seconds
    cache_max_body_bytes: int = Field(default=524288, ge=1024)  # 512KB
    cache_redis_url: str = Field(default="", alias="GATEWAY_CACHE_REDIS_URL")
    cache_redis_enabled: bool = Field(default=False, alias="GATEWAY_CACHE_REDIS_ENABLED")

    # WebSocket
    ws_heartbeat_interval: int = Field(default=30, ge=5)  # seconds
    ws_max_message_size: int = Field(default=65536, ge=1024)  # 64KB

    # Streaming
    stream_use_iex: bool = False  # Use IEX instead of SIP for stocks
    stream_options_feed: str = "opra"  # Options stream feed: opra or indicative
    stream_lazy_connect: bool = True  # Connect to streams on-demand for efficiency
    stream_reconnect_max_retries: int = Field(default=10, ge=1)
    stream_reconnect_base_delay: float = Field(default=1.0, ge=0.1)
    stream_reconnect_max_delay: float = Field(default=16.0, ge=1.0)
    stream_fanout_max_inflight: int = Field(default=100, ge=1)
    stream_fanout_batch_size: int = Field(default=32, ge=1)

    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_default: int = Field(default=600, ge=1)  # requests per minute

    # Per-provider rate limits (override hardcoded defaults via env)
    alpaca_rate_limit_per_minute: int = Field(default=10000, ge=1)
    alpaca_rate_limit_per_second: int = Field(default=75, ge=1)

    # Alpaca (loaded from env, not prefixed)
    alpaca_api_key: str = Field(default="", alias="APCA_API_KEY_ID")
    alpaca_secret_key: str = Field(default="", alias="APCA_API_SECRET_KEY")
    alpaca_base_url: str = Field(default="https://paper-api.alpaca.markets", alias="APCA_API_BASE_URL")

    # Unusual Whales
    uw_api_key: str = Field(default="", alias="UNUSUAL_WHALES_API_KEY")

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 2: Backend Engineering Limits (PRD 6.2, 6.3, 6.5)
    # ─────────────────────────────────────────────────────────────────────────

    # Connection Limits (6.3.3-5)
    ws_idle_timeout: int = Field(default=300, ge=60)  # 5 min idle timeout
    ws_max_duration: int = Field(default=86400, ge=3600)  # 24h max connection
    ws_max_clients: int = Field(default=1000, ge=10)  # Max concurrent clients

    # Memory Limits (6.2.1-4)
    memory_target_mb: int = Field(default=512, ge=256)  # 512MB target
    memory_hard_limit_mb: int = Field(default=1024, ge=512)  # 1GB hard limit
    gc_threshold_pct: int = Field(default=80, ge=50, le=95)  # GC at 80%

    # Graceful Shutdown (6.5.3-4)
    shutdown_drain_seconds: int = Field(default=30, ge=5, le=60)  # 30s drain

    # ─────────────────────────────────────────────────────────────────────────
    # Data Sink (for Heber integration)
    # ─────────────────────────────────────────────────────────────────────────

    data_sink_enabled: bool = False
    data_sink_redis_url: str = Field(default="", alias="GATEWAY_DATA_SINK_REDIS_URL")
    data_sink_max_stream_len: int = Field(default=100_000, ge=1000)
    data_sink_operation_timeout_seconds: float = Field(default=5.0, ge=0.5)
    data_sink_redis_pool_size: int = Field(default=8, ge=1, le=32)
    data_sink_stream_publish_max_inflight: int = Field(default=32, ge=1)
    data_sink_stream_publish_max_pending: int = Field(default=512, ge=1)

    # Backfill concurrency (per-provider, split by feed weight)
    backfill_lightweight_concurrency: int = Field(default=5, ge=1)
    backfill_heavyweight_concurrency: int = Field(default=2, ge=1)

    # Bulk Jobs
    bulk_results_max_in_memory: int = Field(default=25_000, ge=100)
    bulk_results_spool_to_disk: bool = True

    # UW Poller
    uw_poller_publish_max_inflight: int = Field(default=16, ge=1)

    # UW EOD Polling (daily snapshots for per-ticker endpoints)
    uw_eod_enabled: bool = False
    uw_eod_hour: int = Field(default=16, ge=0, le=23)  # 4:00 PM ET
    uw_eod_minute: int = Field(default=30, ge=0, le=59)  # 4:30 PM ET
    uw_core_tickers: str = ""  # comma-separated override, empty = use defaults
    uw_dynamic_ticker_count: int = Field(default=20, ge=0)
    uw_eod_concurrency: int = Field(default=5, ge=1, le=20)

    # Alpaca option chain capture
    option_capture_enabled: bool = False
    option_capture_symbols: str = "SPY,QQQ,IWM"
    option_capture_interval_seconds: int = Field(default=60, ge=1)
    option_capture_market_hours_only: bool = True
    option_capture_snapshot_timeout_seconds: float = Field(default=10.0, ge=0.5)
    option_capture_ws_enabled: bool = True
    option_capture_ws_contract_limit_per_symbol: int = Field(default=40, ge=1)

    # Replay
    replay_messages_max_in_memory: int = Field(default=50_000, ge=100)
    replay_messages_spool_to_disk: bool = True

    # Endpoint Rate Limits (PRD 7.5.3)
    bulk_rate_limit_per_hour: int = Field(default=10, ge=1)
    replay_max_concurrent_sessions: int = Field(default=5, ge=1)

    @property
    def option_capture_symbol_list(self) -> list[str]:
        """Parse configured option capture symbols into a stable uppercase list."""
        symbols: list[str] = []
        for raw_symbol in self.option_capture_symbols.split(","):
            symbol = raw_symbol.strip().upper()
            if not symbol or symbol in symbols:
                continue
            symbols.append(symbol)
        return symbols

    @field_validator("stream_options_feed", mode="before")
    @classmethod
    def _normalize_stream_options_feed(cls, value: str) -> str:
        normalized = str(value or "opra").strip().lower()
        if normalized not in {"opra", "indicative"}:
            raise ValueError("stream_options_feed must be 'opra' or 'indicative'")
        return normalized


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
