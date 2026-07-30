"""Gateway configuration using pydantic-settings."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
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
    stream_fanout_max_inflight: int = Field(default=100, ge=1)
    stream_fanout_batch_size: int = Field(default=32, ge=1)

    # Rate Limiting
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
    alpaca_trading_call_timeout_seconds: float = Field(
        default=15.0,
        ge=0.5,
        description=(
            "Wall-clock timeout (seconds) for READ Alpaca trading calls — "
            "get_account, get_orders, get_position, get_clock, get_calendar, "
            "get_portfolio_history, get_assets, etc. Reads can safely retry "
            "after a 504 (they're idempotent at the broker), so a tighter "
            "timeout here keeps the per-call ceiling predictable. Writes "
            "use the separate ``alpaca_trading_write_call_timeout_seconds`` "
            "knob (default 25s) which permits more slack for opening-bell "
            "broker slowdowns where surfacing a 504 forces the caller into "
            "the idempotency-retry contract — less convenient than just "
            "succeeding once."
        ),
    )
    alpaca_trading_write_call_timeout_seconds: float = Field(
        default=25.0,
        ge=0.5,
        description=(
            "Wall-clock timeout (seconds) for WRITE Alpaca trading calls — "
            "create_order, replace_order, cancel_order, cancel_all_orders, "
            "close_position, close_all_positions. Evidence from 2026-05-15 "
            "opening bell showed 13 write-class timeouts at the previous "
            "shared 15s ceiling (3 create_order, 1 close_position, plus 9 "
            "cancels). Alpaca's broker latency at the market-open burst can "
            "exceed 15s on writes; bumping the write ceiling to 25s lets "
            "those calls complete instead of 504'ing into the idempotency "
            "retry contract (which exists for genuine failures, not for "
            "merely slow successes). Must stay <= "
            "``alpaca_trading_http_timeout_seconds`` (default 30s) so the "
            "HTTP-level safety net still releases threads. Reads keep the "
            "existing 15s default — see ``alpaca_trading_call_timeout_seconds``."
        ),
    )
    alpaca_trading_http_timeout_seconds: float = Field(
        default=30.0,
        ge=1.0,
        description=(
            "Default HTTP-level timeout (seconds) injected into the alpaca-py SDK's "
            "requests session. The SDK creates a bare Session() with no timeout, so "
            "asyncio's wait_for cannot cancel an in-flight thread; without this, "
            "alpaca_trading_call_timeout_seconds returns 504 to the caller but leaks "
            "the trading thread until the OS gives up. Should be >= the larger of "
            "alpaca_trading_call_timeout_seconds and alpaca_trading_write_call_timeout_seconds "
            "so user-facing 504s fire first; the HTTP timeout is the safety net for "
            "thread release."
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

    # Live-trading opt-in. Paper is the fail-safe default: live trading (real
    # capital on the shared account) requires BOTH this flag AND the recognized
    # live APCA_API_BASE_URL, so a missing/empty/typo'd URL can never silently
    # route orders to real money. (GATEWAY_ prefix → GATEWAY_ALLOW_LIVE_TRADING.)
    allow_live_trading: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 2: Backend Engineering Limits (PRD 6.2, 6.3, 6.5)
    # ─────────────────────────────────────────────────────────────────────────

    # Connection Limits (6.3.3-5)
    ws_idle_timeout: int = Field(default=300, ge=60)  # 5 min idle timeout
    ws_max_clients: int = Field(default=1000, ge=10)  # Max concurrent clients

    # Graceful Shutdown (6.5.3-4)
    shutdown_drain_seconds: int = Field(default=30, ge=5, le=60)  # 30s drain

    # ─────────────────────────────────────────────────────────────────────────
    # Data Sink (for Heber integration)
    # ─────────────────────────────────────────────────────────────────────────

    # Strict mode controls the *error surface* on envelope-wrap failure, not
    # whether failures are tolerated. Wrap failures are NEVER silently degraded
    # into an ``unknown:{symbol}`` key (Heber rejects those, dropping the record
    # on Bronze→Silver) — they always increment an alerting counter and raise.
    # When True, the original exception propagates so the REST middleware returns
    # a 500; when False (default), a tagged ``EnvelopeWrapError`` is raised which
    # callers (REST middleware, pollers) catch to drop the single bad event and
    # keep moving.
    strict_envelopes: bool = False

    data_sink_enabled: bool = False
    data_sink_redis_url: str = Field(default="", alias="GATEWAY_DATA_SINK_REDIS_URL")
    data_sink_max_stream_len: int = Field(default=100_000, ge=1000)
    data_sink_operation_timeout_seconds: float = Field(default=5.0, ge=0.5)
    redis_backfill_consumer_group: str = "heber-backfill-writers"

    # JetStream transport. Disabled until the durable outbox and Heber consumer
    # are wired; these bounds are enforced server-side when streams are bootstrapped.
    jetstream_enabled: bool = False
    jetstream_lanes: Literal["backfill", "live", "both"] = "backfill"
    jetstream_url: str = "nats://jetstream:4222"
    jetstream_username: str = ""
    jetstream_password: SecretStr = SecretStr("")
    jetstream_backfill_durable_name: str = "heber-backfill-writers"
    jetstream_live_max_bytes: int = Field(default=256 * 1024**3, ge=1024)
    jetstream_backfill_max_bytes: int = Field(default=256 * 1024**3, ge=1024)
    jetstream_watch_max_bytes: int = Field(default=8 * 1024**3, ge=1024)
    jetstream_max_message_bytes: int = Field(default=8 * 1024**2, ge=1024, le=8 * 1024**2)
    jetstream_watch_max_age_seconds: int = Field(default=86_400, ge=60)
    durable_outbox_enabled: bool = False
    durable_outbox_path: Path = Path("state/outbox/events.sqlite3")
    durable_outbox_max_bytes: int = Field(default=20 * 1024**3, ge=1024**2)
    durable_outbox_retry_seconds: float = Field(default=1.0, ge=0.05, le=60.0)

    @model_validator(mode="after")
    def _require_jetstream_credentials_when_enabled(self) -> "Settings":
        if self.jetstream_enabled and (not self.jetstream_username or not self.jetstream_password.get_secret_value()):
            raise ValueError("JetStream requires GATEWAY_JETSTREAM_USERNAME and GATEWAY_JETSTREAM_PASSWORD")
        if self.durable_outbox_enabled and not self.jetstream_enabled:
            raise ValueError("durable outbox requires JetStream")
        if self.jetstream_enabled and not self.durable_outbox_enabled:
            raise ValueError("JetStream requires the durable outbox")
        if (self.jetstream_enabled or self.durable_outbox_enabled) and not self.data_sink_enabled:
            raise ValueError("JetStream and durable outbox require the data sink to be enabled")
        if self.durable_outbox_enabled and not self.data_sink_redis_url:
            raise ValueError("durable outbox requires GATEWAY_DATA_SINK_REDIS_URL for the Redis control-plane URL")
        return self

    # Pool size cap raised from 32 → 128. The previous cap left the redis_sink-
    # layer min(64, ...) clamp dead and capped operators at half the available
    # connection capacity. 128 connections is well within typical Redis defaults
    # (maxclients=10000 on the redis:7-alpine image used in compose). The
    # default keeps headroom above the sink worker count for reconnect churn and
    # health checks during opening-bell bursts.
    data_sink_redis_pool_size: int = Field(default=32, ge=1, le=128)
    # Failed-event retry buffer capacity. Events that exhaust all publish
    # retries (Redis down / circuit open) are held in an in-memory deque and
    # re-published on the next successful reconnect; the bounded deque silently
    # evicts oldest events when full (metered via
    # gateway_sink_buffer_evictions_total → SinkBufferEvictionsActive alert).
    # At ~1 KB/event this caps memory at roughly capacity KB. The breaker's 15s
    # open window at OPRA quote volume overran the prior fixed 10k in under a
    # second, so the default is raised to 50k (~50 MB). OPRA-heavy deployments
    # that still see evictions during a flap should raise it further.
    data_sink_failed_buffer_capacity: int = Field(default=50_000, ge=1_000, le=1_000_000)

    # Bounded-queue + worker-pool dispatch parameters. Producers `put` into a
    # per-sink bounded asyncio.Queue with a short producer-block timeout; a
    # small worker pool drains the queue and calls sink.publish via the
    # existing circuit-breaker-protected path. The registry queue is the
    # *single* in-process gate for sink dispatch — there is no outer
    # fire-and-forget task gate. Drops happen only if the queue is full
    # AND workers can't drain it within producer_block_timeout_seconds —
    # surfaced via gateway_sink_producer_timeout_drops_total.
    #
    # Replaces the prior data_sink_max_inflight_per_sink semaphore (default
    # 512, raised to 2048 in commit 744bba2) which dropped events at the
    # acquire site with no retry: 32 757 events lost to Heber on 2026-05-15,
    # 5 872 on 2026-05-18 from this exact mechanism.
    #
    # - queue_size: max queued events per sink before producers block.
    #   Opening-bell bursts have hit ~20K events/min historically; 16384
    #   covers ~45s of sustained 350 events/sec without producer block.
    # - worker_count: concurrency for publishes against a single sink.
    #   Should fit within data_sink_redis_pool_size for the Redis sink.
    # - producer_block_timeout_seconds: how long a producer waits on a
    #   full queue before dropping the event. 0.1s is intentionally short
    #   — the producer here is the upstream stream callback running on
    #   the asyncio event loop, and longer blocks would stall the Alpaca
    #   receive loop. A drop is a true emergency (queue is full AND
    #   workers can't drain in 100ms).
    data_sink_queue_size: int = Field(default=16384, ge=64, le=65536)
    # Raised 16 → 24 (still < pool_size 32, leaving headroom for reconnect /
    # health-check churn) to widen drain throughput against the Redis sink and
    # reduce producer-timeout drops during opening-bell bursts. This is a
    # throughput lever, not a fix for event-loop starvation — if drops persist
    # the bottleneck is loop saturation, which needs profiling, not more workers.
    data_sink_worker_count: int = Field(default=24, ge=1, le=64)
    data_sink_producer_block_timeout_seconds: float = Field(default=0.1, ge=0.001, le=5.0)
    rest_sink_excluded_feeds: str = Field(
        default="greek_exposure,iv_rank,iv_term_structure,short_data,ftd,flow_alerts,darkpool",
        description="Comma-separated feed names that REST envelope middleware must not publish into live Heber stream",
    )
    rest_sink_low_priority_max_queue_utilization: float = Field(
        default=0.70,
        ge=0.0,
        le=1.0,
        description="Shed low-priority REST sink publishing when sink queue utilization is at or above this threshold",
    )

    @property
    def rest_sink_excluded_feed_set(self) -> set[str]:
        return {item.strip() for item in self.rest_sink_excluded_feeds.split(",") if item.strip()}

    # Backfill concurrency (per-provider, split by feed weight)
    backfill_lightweight_concurrency: int = Field(default=5, ge=1)
    backfill_heavyweight_concurrency: int = Field(default=2, ge=1)
    backfill_max_chunk_records: int = Field(default=5_000, ge=1)

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
    uw_universe_file: Path = Field(
        default=Path("config/uw_pit_universe.json"),
        description=(
            "JSON file with an 'active' symbol list (Atlas PIT universe export). When it exists, "
            "EOD capture uses that list as its ticker set and disables the dynamic screener rotation. "
            "Missing file falls back to uw_core_tickers / DEFAULT_CORE_TICKERS."
        ),
    )
    uw_dynamic_ticker_count: int = Field(default=20, ge=0)
    uw_eod_concurrency: int = Field(default=5, ge=1, le=20)
    uw_eod_state_path: Path = Field(
        default=Path("/app/logs/state/uw_eod_state.json"),
        description="Persistent JSON state file used to prevent duplicate UW EOD runs across restarts",
    )
    uw_eod_claim_stale_after_seconds: int = Field(
        default=14400,  # 4h — the ~989-ticker PIT universe EOD run can exceed the old 2h window,
        ge=300,  # which would let a mid-run restart launch a duplicate concurrent EOD run
        description="Allow a UW EOD run to be retried when a running claim is older than this many seconds",
    )

    # Alpaca option chain capture
    option_capture_enabled: bool = False
    option_capture_symbols: str = "SPY,QQQ,IWM"
    option_capture_interval_seconds: int = Field(default=60, ge=1)
    option_capture_market_hours_only: bool = True
    option_capture_snapshot_timeout_seconds: float = Field(default=90.0, ge=5.0)
    option_capture_symbol_timeout_overrides: str = ""  # comma-separated SYMBOL:SECONDS pairs, e.g. "SPY:45,QQQ:45"
    option_capture_ws_enabled: bool = True
    option_capture_ws_contract_limit_per_symbol: int = Field(default=40, ge=1)

    # Event-loop stall watchdog (diagnostics): an off-loop thread stack-dumps the
    # event loop when it is blocked longer than the threshold, capturing the
    # synchronous frame behind multi-second on_message_slow stalls / heartbeat drops.
    loop_watchdog_enabled: bool = True
    loop_watchdog_stall_threshold_seconds: float = Field(default=5.0, ge=1.0)

    # Replay
    replay_messages_max_in_memory: int = Field(default=50_000, ge=100)
    replay_messages_spool_to_disk: bool = True

    # Endpoint Rate Limits (PRD 7.5.3)
    bulk_rate_limit_per_hour: int = Field(default=10, ge=1)
    replay_max_concurrent_sessions: int = Field(default=5, ge=1)

    # Treasury yield poller
    treasury_poller_maturities: str = "2year,10year"  # comma-separated, e.g. "2year,10year"

    # UW flow WebSocket fan-out. When enabled, the UW poller's published flow
    # envelopes are also delivered to WS clients subscribed to provider=uw /
    # feed in {flow, flow_alerts}. Additive: inert unless a client subscribes,
    # and never touches the Alpaca multiplexer or the Redis heber:events publish.
    ws_flow_fanout_enabled: bool = True
    ws_flow_replay_stream: str = "gateway:flow_alerts:replay"
    ws_flow_replay_max_len: int = Field(default=100_000, ge=1_000)
    ws_flow_replay_max_events_per_resume: int = Field(default=50_000, ge=100)

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
