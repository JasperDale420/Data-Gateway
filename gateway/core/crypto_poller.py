"""Autonomous Alpaca crypto bars+trades poller for 24/7 crypto data.

Crypto markets trade 24/7, so unlike quotes/trades pollers there is no
market-hours gate. On each tick the poller fetches the latest bars and
the latest trades for a configured set of pairs and publishes both feeds
to Heber as feed=crypto_bars and feed=crypto_trades respectively.

Pattern follows ``gateway.core.base_poller.BasePoller``:
- Runs as an asyncio background task during application lifespan
- Wraps each item in an ``EventEnvelope`` via ``wrap_event``
- Publishes to the data sink for Heber integration
- Deduplicates via in-memory OrderedDict + optional Redis cache
"""

from typing import Any

from gateway.config import get_settings
from gateway.core.base_poller import BasePoller, DedupMixin
from gateway.core.cache import RedisCache
from gateway.core.envelope import wrap_event
from gateway.core.logger import logger

HEBER_STREAM = "heber:events"

DEFAULT_CRYPTO_PAIRS: list[str] = [
    "BTC/USD",
    "ETH/USD",
    "SOL/USD",
    "DOGE/USD",
    "AVAX/USD",
    "LINK/USD",
    # MATIC/USD removed 2026-07-19: Alpaca delisted it in June 2023 (POL
    # migration), so its "latest bar" endpoint forever returns the final
    # 2023-06-23T20:33Z bar — which kept landing in Heber Bronze as a bogus
    # dt=2023-06-23 partition whenever consumer dedupe rotated.
    "DOT/USD",
]

MAX_PAIRS_PER_REQUEST = 50


def _to_canonical_instrument_key(pair: str) -> str:
    """Normalize Alpaca's BASE/QUOTE pair into Heber's crypto:BASE-QUOTE form."""
    return f"crypto:{pair.replace('/', '-').upper()}"


class AlpacaCryptoPoller(DedupMixin, BasePoller):
    """Background poller for crypto latest bars + trades via Alpaca REST."""

    def __init__(
        self,
        pairs: list[str] | None = None,
        poll_interval_seconds: int = 30,
    ):
        settings = get_settings()
        self._pairs = [p.upper() for p in (pairs or DEFAULT_CRYPTO_PAIRS)]

        super().__init__(
            poll_interval_seconds=max(5, poll_interval_seconds),
            poller_name="crypto_poller",
        )

        self._provider: Any = None
        self._init_dedup(cache_ttl_seconds=300)

        self._redis_dedupe: RedisCache | None = None
        if settings.cache_redis_enabled and settings.cache_redis_url:
            self._redis_dedupe = RedisCache(
                redis_url=settings.cache_redis_url,
                default_ttl=self._cache_ttl_seconds,
            )

    # ─────────────────────────────────────────────────────────────────────
    # BasePoller hooks
    # ─────────────────────────────────────────────────────────────────────

    async def _on_start(self) -> bool:
        from gateway.core.globals import get_registry

        registry = get_registry()
        self._provider = registry.get("alpaca")
        if self._provider is None:
            logger.error("crypto_poller_no_alpaca_provider")
            return False

        logger.info(
            "crypto_poller_started",
            interval_seconds=self._poll_interval,
            pairs=len(self._pairs),
        )
        return True

    async def stop(self) -> None:
        await super().stop()
        logger.info("crypto_poller_stopped", cached_ids=len(self._seen_ids))

    async def _poll_once(self) -> None:
        from gateway.core.globals import get_sink_registry

        sink_registry = get_sink_registry()
        if not sink_registry:
            logger.debug("crypto_poller_no_sink")
            return

        # Crypto trades 24/7 — no market-hours gate.
        await self._poll_crypto(sink_registry)
        self._cleanup_cache()

    # ─────────────────────────────────────────────────────────────────────
    # Redis dedup
    # ─────────────────────────────────────────────────────────────────────

    async def _check_redis_duplicates(self, items: list[tuple[str, str]]) -> set[str]:
        if self._redis_dedupe is None or not items:
            return set()
        cache_keys = [ck for _, ck in items]
        try:
            found = await self._redis_dedupe.mget(cache_keys)
        except Exception as e:
            logger.warning("crypto_poller_redis_dedupe_mget_failed", count=len(cache_keys), error=str(e))
            return set()
        return {eid for eid, ck in items if ck in found}

    # ─────────────────────────────────────────────────────────────────────
    # Publish
    # ─────────────────────────────────────────────────────────────────────

    async def _publish_envelopes(
        self,
        sink_registry: Any,
        envelopes: list[dict[str, Any]],
        feed_label: str,
    ) -> tuple[int, int]:
        if not envelopes:
            return 0, 0

        duplicates = 0
        to_publish: list[tuple[dict[str, Any], str, str | None]] = []
        dedupe_items: list[tuple[str, str]] = []

        for envelope in envelopes:
            event_id = str(envelope.get("event_id", ""))
            if not event_id:
                to_publish.append((envelope, "", None))
                continue
            if self._is_duplicate(event_id):
                duplicates += 1
                continue
            cache_key = f"alpaca:{feed_label}:{event_id}"
            dedupe_items.append((event_id, cache_key))
            to_publish.append((envelope, event_id, cache_key))

        redis_dups = await self._check_redis_duplicates(dedupe_items)
        if redis_dups:
            duplicates += len(redis_dups)
            to_publish = [item for item in to_publish if not item[1] or item[1] not in redis_dups]

        if not to_publish:
            return 0, duplicates

        messages: list[tuple[str, dict[str, Any]]] = [(HEBER_STREAM, env) for env, _, _ in to_publish]
        # Use per-message results so partial-batch failures don't mis-mark
        # events. See quotes_poller for the full bug write-up.
        try:
            if hasattr(sink_registry, "publish_all_batch_results"):
                results = await sink_registry.publish_all_batch_results(messages)
            elif hasattr(sink_registry, "publish_all_batch"):
                count = await sink_registry.publish_all_batch(messages)
                results = [count == len(messages)] * len(messages)
            else:
                for topic, env in messages:
                    await sink_registry.publish_all(topic, env)
                results = [True] * len(messages)
        except Exception as exc:
            logger.warning("crypto_poller_batch_publish_failed", count=len(messages), error=str(exc), feed=feed_label)
            results = [False] * len(messages)

        published = sum(1 for ok in results if ok)

        if published > 0:
            redis_items: list[tuple[str, Any]] = []
            for (_env, event_id, cache_key), ok in zip(to_publish, results, strict=True):
                if not ok:
                    continue
                if event_id:
                    self._mark_seen(event_id)
                    if self._redis_dedupe is not None and cache_key:
                        redis_items.append((cache_key, True))
            if redis_items and self._redis_dedupe is not None:
                await self._redis_dedupe.set_many(redis_items, ttl=self._cache_ttl_seconds)

        return published, duplicates

    # ─────────────────────────────────────────────────────────────────────
    # Core polling
    # ─────────────────────────────────────────────────────────────────────

    def _wrap_pair_payload(self, pair: str, payload: dict[str, Any], feed: str) -> dict[str, Any]:
        """Build an EventEnvelope for one pair's bar/trade payload."""
        # Add normalized symbol fields the wrap_event layer expects.
        enriched = dict(payload)
        enriched.setdefault("symbol", pair)
        envelope = wrap_event(
            event=enriched,
            provider="alpaca",
            feed=feed,
            source="rest",
        )
        # Force the canonical crypto instrument_key — wrap_event's symbology
        # may not recognize Alpaca's BASE/QUOTE format.
        envelope["instrument_type"] = "crypto"
        envelope["instrument_key"] = _to_canonical_instrument_key(pair)
        envelope["symbol"] = pair.replace("/", "-").upper()
        return envelope

    async def _poll_crypto(self, sink_registry: Any) -> None:
        """Fetch latest bars + trades for all configured pairs and publish."""
        if self._provider is None:
            return

        # Bars
        try:
            bars_envelopes: list[dict[str, Any]] = []
            for i in range(0, len(self._pairs), MAX_PAIRS_PER_REQUEST):
                batch = self._pairs[i : i + MAX_PAIRS_PER_REQUEST]
                bars = await self._provider.get_crypto_latest_bars(batch)
                for pair, bar in (bars or {}).items():
                    bars_envelopes.append(self._wrap_pair_payload(pair, bar, "crypto_bars"))

            if bars_envelopes:
                published, duplicates = await self._publish_envelopes(sink_registry, bars_envelopes, "crypto_bars")
                logger.info(
                    "crypto_poller_bars_published",
                    fetched=len(bars_envelopes),
                    published=published,
                    duplicates=duplicates,
                )
        except Exception as e:
            logger.error("crypto_poller_bars_error", error=str(e), exc_info=True)

        # Trades
        try:
            trades_envelopes: list[dict[str, Any]] = []
            for i in range(0, len(self._pairs), MAX_PAIRS_PER_REQUEST):
                batch = self._pairs[i : i + MAX_PAIRS_PER_REQUEST]
                trades = await self._provider.get_crypto_latest_trades(batch)
                for pair, trade in (trades or {}).items():
                    trades_envelopes.append(self._wrap_pair_payload(pair, trade, "crypto_trades"))

            if trades_envelopes:
                published, duplicates = await self._publish_envelopes(sink_registry, trades_envelopes, "crypto_trades")
                logger.info(
                    "crypto_poller_trades_published",
                    fetched=len(trades_envelopes),
                    published=published,
                    duplicates=duplicates,
                )
        except Exception as e:
            logger.error("crypto_poller_trades_error", error=str(e), exc_info=True)

    def get_runtime_snapshot(self) -> dict[str, Any]:
        return super().get_runtime_snapshot() | {
            "pairs_count": len(self._pairs),
            "pairs": self._pairs[:10],
            "dedupe_cache_entries": len(self._seen_ids),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────

_crypto_poller: AlpacaCryptoPoller | None = None


def get_crypto_poller() -> AlpacaCryptoPoller | None:
    """Get the global crypto poller instance."""
    return _crypto_poller


async def start_crypto_poller(
    pairs: list[str] | None = None,
    poll_interval_seconds: int = 30,
) -> AlpacaCryptoPoller:
    """Start the global Alpaca crypto poller."""
    global _crypto_poller

    if _crypto_poller is not None:
        return _crypto_poller

    _crypto_poller = AlpacaCryptoPoller(
        pairs=pairs,
        poll_interval_seconds=poll_interval_seconds,
    )
    await _crypto_poller.start()
    return _crypto_poller


async def stop_crypto_poller() -> None:
    """Stop the global Alpaca crypto poller."""
    global _crypto_poller

    if _crypto_poller is not None:
        await _crypto_poller.stop()
        _crypto_poller = None
