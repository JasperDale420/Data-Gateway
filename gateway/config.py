"""Gateway configuration using pydantic-settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
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

    # WebSocket
    ws_heartbeat_interval: int = Field(default=30, ge=5)  # seconds
    ws_max_message_size: int = Field(default=65536, ge=1024)  # 64KB

    # Streaming
    stream_use_iex: bool = False  # Use IEX instead of SIP for stocks
    stream_lazy_connect: bool = True  # Connect to streams on-demand for efficiency
    stream_reconnect_max_retries: int = Field(default=10, ge=1)
    stream_reconnect_base_delay: float = Field(default=1.0, ge=0.1)
    stream_reconnect_max_delay: float = Field(default=16.0, ge=1.0)

    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_default: int = Field(default=600, ge=1)  # requests per minute

    # Alpaca (loaded from env, not prefixed)
    alpaca_api_key: str = Field(default="", alias="APCA_API_KEY_ID")
    alpaca_secret_key: str = Field(default="", alias="APCA_API_SECRET_KEY")
    alpaca_base_url: str = Field(
        default="https://paper-api.alpaca.markets", alias="APCA_API_BASE_URL"
    )

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


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
