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
    # Comma-separated stream types (stocks_sip, stocks_iex, options, crypto, news)
    # to connect eagerly at Gateway startup, even when stream_lazy_connect is True.
    # Default eagerly connects the stocks feed so the first trading-bot subscribe
    # at 9:30 ET doesn't pay the upstream Alpaca cold-start cost (~30s of TLS +
    # auth + first-bar drain). Verified safe with the Algo Trader Plus plan
    # (multi-connection); on a Basic plan, set this to "" to keep all streams lazy.
    stream_eager_connect_types: str = "stocks"
    stream_reconnect_max_retries: int = Field(default=10, ge=1)
    stream_reconnect_base_delay: float = Field(default=1.0, ge=0.1)
    stream_reconnect_max_delay: float = Field(default=16.0, ge=1.0)
    stream_fanout_max_inflight: int = Field(default=100, ge=1)
    stream_fanout_batch_size: int = Field(default=32, ge=1)

    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_default: int = Field(default=600, ge=1)  # requests per minute
    behind_trusted_proxy: bool = False  # Only trust X-Forwarded-For when behind a known proxy
    # Comma-separated CIDRs for trusted intermediate proxies (e.g. "10.0.0.0/8,172.16.0.0/12").
    # When `behind_trusted_proxy=True` AND this is set, X-Forwarded-For is parsed
    # rightmost-to-leftmost and the first IP NOT in this list is treated as the real
    # client. Without this, the leftmost (attacker-controlled) IP is used — which
    # makes per-IP rate limits and IP block lists trivially bypassable.
    trusted_proxy_cidrs: str = ""

    @field_validator("trusted_proxy_cidrs")
    @classmethod
    def _validate_trusted_proxy_cidrs(cls, v: str) -> str:
        if not v:
            return v
        import ipaddress

        for cidr in v.split(","):
            cidr = cidr.strip()
            if cidr:
                try:
                    ipaddress.ip_network(cidr, strict=False)
                except ValueError as exc:
                    raise ValueError(f"trusted_proxy_cidrs: invalid CIDR {cidr!r}: {exc}") from exc
        return v

    # Per-provider rate limits (override hardcoded defaults via env)
    alpaca_rate_limit_per_minute: int = Field(default=10000, ge=1)
    alpaca_rate_limit_per_second: int = Field(default=75, ge=1)
    alpaca_max_concurrent_requests: int = Field(default=25, ge=1)  # see rate_limiter.DEFAULT_ALPACA_MAX_CONCURRENT
    alpaca_trading_call_timeout_seconds: float = Field(default=15.0, ge=0.5)
    alpaca_trading_http_timeout_seconds: float = Field(
        default=30.0,
        ge=1.0,
        description=(
            "Default HTTP-level timeout (seconds) injected into the alpaca-py SDK's "
            "requests session. The SDK creates a bare Session() with no timeout, so "
            "asyncio's wait_for cannot cancel an in-flight thread; without this, "
            "alpaca_trading_call_timeout_seconds returns 504 to the caller but leaks "
            "the trading thread until the OS gives up. Should be > "
            "alpaca_trading_call_timeout_seconds so the wall-clock keeps user-facing "
            "behavior unchanged; the HTTP timeout is the safety net for thread release."
        ),
    )
    alpaca_trading_thread_pool_size: int = Field(
        default=16, ge=2, description="Dedicated thread pool for Alpaca trading SDK calls"
    )
    alpaca_trading_max_inflight: int = Field(
        default=24,
        ge=2,
        description=(
            "Upper bound on concurrent in-flight Alpaca trading calls. When exceeded, "
            "new calls fast-fail with 503 instead of piling up in the executor queue. "
            "Should be >= alpaca_trading_thread_pool_size; the difference is the allowed "
            "short-term queue depth before backpressure kicks in."
        ),
    )

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

    # Strict mode: when True, exceptions during EventEnvelope construction or
    # REST envelope wrapping propagate as 500 instead of returning a degraded
    # fallback envelope / unwrapped body. Recommended for staging/prod where
    # silent corruption is worse than a loud failure. Default False preserves
    # legacy lenient behavior.
    strict_envelopes: bool = False

    data_sink_enabled: bool = False
    data_sink_redis_url: str = Field(default="", alias="GATEWAY_DATA_SINK_REDIS_URL")
    data_sink_max_stream_len: int = Field(default=100_000, ge=1000)
    data_sink_operation_timeout_seconds: float = Field(default=5.0, ge=0.5)
    data_sink_redis_pool_size: int = Field(default=8, ge=1, le=32)
    data_sink_max_inflight_per_sink: int = Field(default=512, ge=64, le=4096)
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
    option_capture_snapshot_timeout_seconds: float = Field(default=90.0, ge=5.0)
    option_capture_symbol_timeout_overrides: str = ""  # comma-separated SYMBOL:SECONDS pairs, e.g. "SPY:45,QQQ:45"
    option_capture_ws_enabled: bool = True
    option_capture_ws_contract_limit_per_symbol: int = Field(default=40, ge=1)

    # Replay
    replay_messages_max_in_memory: int = Field(default=50_000, ge=100)
    replay_messages_spool_to_disk: bool = True

    # Endpoint Rate Limits (PRD 7.5.3)
    bulk_rate_limit_per_hour: int = Field(default=10, ge=1)
    replay_max_concurrent_sessions: int = Field(default=5, ge=1)

    # Treasury yield poller
    treasury_poller_maturities: str = "2year,10year"  # comma-separated, e.g. "2year,10year"

    # Alpaca quotes REST-fallback poller. Enable to publish quotes to Heber without
    # needing a WebSocket client to subscribe.
    quotes_poller_enabled: bool = True
    quotes_poller_interval_seconds: int = Field(default=30, ge=5)
    quotes_poller_symbols: str = ""  # comma-separated; empty = use DEFAULT_QUOTES_SYMBOLS

    @property
    def quotes_poller_symbol_list(self) -> list[str] | None:
        """Parse quotes_poller_symbols; return None to use poller defaults."""
        symbols = [s.strip().upper() for s in self.quotes_poller_symbols.split(",") if s.strip()]
        return symbols or None

    # Alpaca trades REST-fallback poller. Same rationale as quotes_poller.
    trades_poller_enabled: bool = True
    trades_poller_interval_seconds: int = Field(default=30, ge=5)
    trades_poller_symbols: str = ""  # comma-separated; empty = use DEFAULT_TRADES_SYMBOLS

    @property
    def trades_poller_symbol_list(self) -> list[str] | None:
        """Parse trades_poller_symbols; return None to use poller defaults."""
        symbols = [s.strip().upper() for s in self.trades_poller_symbols.split(",") if s.strip()]
        return symbols or None

    # Alpaca crypto poller. Crypto trades 24/7; no market-hours gate.
    crypto_poller_enabled: bool = True
    crypto_poller_interval_seconds: int = Field(default=60, ge=5)
    crypto_poller_pairs: str = ""  # comma-separated BASE/QUOTE pairs; empty = DEFAULT_CRYPTO_PAIRS

    @property
    def crypto_poller_pair_list(self) -> list[str] | None:
        """Parse crypto_poller_pairs; return None to use poller defaults."""
        pairs = [p.strip().upper() for p in self.crypto_poller_pairs.split(",") if p.strip()]
        return pairs or None

    # Emit feed=option_trades envelopes for each contract in the captured chain
    # snapshot. No additional API calls — reuses snapshot data already fetched.
    option_capture_publish_per_contract_trades: bool = True

    # Alpaca news REST poller. Replaces missing WS news subscription path.
    news_poller_enabled: bool = True
    news_poller_interval_seconds: int = Field(default=120, ge=5)
    news_poller_fetch_limit: int = Field(default=50, ge=1, le=50)
    news_poller_symbols: str = ""  # comma-separated; empty = all symbols (market-wide)

    @property
    def news_poller_symbol_list(self) -> list[str] | None:
        """Parse news_poller_symbols; return None for market-wide."""
        symbols = [s.strip().upper() for s in self.news_poller_symbols.split(",") if s.strip()]
        return symbols or None

    @property
    def treasury_poller_maturity_list(self) -> list[str]:
        """Parse configured treasury maturities, filtering invalid values."""
        from gateway.core.treasury_poller import VALID_MATURITIES

        maturities = [m.strip().lower() for m in self.treasury_poller_maturities.split(",") if m.strip()]
        valid = [m for m in maturities if m in VALID_MATURITIES]
        return valid if valid else ["2year", "10year"]

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

    @property
    def option_capture_symbol_timeout_map(self) -> dict[str, float]:
        """Parse per-symbol timeout overrides into {SYMBOL: seconds} dict.

        Format: comma-separated SYMBOL:SECONDS pairs, e.g. "SPY:45,QQQ:45".
        Invalid entries are silently skipped.
        """
        overrides: dict[str, float] = {}
        if not self.option_capture_symbol_timeout_overrides.strip():
            return overrides
        for entry in self.option_capture_symbol_timeout_overrides.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            parts = entry.split(":", 1)
            symbol = parts[0].strip().upper()
            try:
                timeout = float(parts[1].strip())
            except (ValueError, IndexError):
                continue
            if symbol and timeout >= 5.0:
                overrides[symbol] = timeout
        return overrides

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
