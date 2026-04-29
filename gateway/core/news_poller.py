"""Autonomous Alpaca news poller.

The Alpaca news WebSocket stream only delivers articles when a downstream
client actively subscribes. This poller provides a REST-based fallback
that runs every N seconds and publishes news articles to Heber.

Heber's ingest contract treats ``feed=news`` as Bronze-only (no Silver),
which matches what we want — raw news fidelity for downstream consumers.
"""

from typing import Any

from gateway.config import get_settings
from gateway.core.base_poller import BasePoller, DedupMixin
from gateway.core.cache import RedisCache
from gateway.core.envelope import wrap_event
from gateway.core.logger import logger

HEBER_STREAM = "heber:events"

# Alpaca news REST endpoint cap per page.
ALPACA_NEWS_MAX_PER_PAGE = 50


class AlpacaNewsPoller(DedupMixin, BasePoller):
    """Background poller that fetches latest Alpaca news via REST."""

    def __init__(
        self,
        symbols: list[str] | None = None,
        poll_interval_seconds: int = 60,
        fetch_limit: int = 50,
    ):
        settings = get_settings()
        self._symbols = [s.strip().upper() for s in (symbols or []) if s.strip()] or None
        self._fetch_limit = max(1, min(int(fetch_limit), ALPACA_NEWS_MAX_PER_PAGE))

        super().__init__(
            poll_interval_seconds=max(5, poll_interval_seconds),
            poller_name="news_poller",
        )

        self._provider: Any = None
        # News articles are stable by article_id — use a longer dedup TTL.
        self._init_dedup(cache_ttl_seconds=86400)

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
            logger.error("news_poller_no_alpaca_provider")
            return False

        logger.info(
            "news_poller_started",
            interval_seconds=self._poll_interval,
            symbols=self._symbols if self._symbols else "all",
            fetch_limit=self._fetch_limit,
        )
        return True

    async def stop(self) -> None:
        await super().stop()
        logger.info("news_poller_stopped", cached_ids=len(self._seen_ids))

    async def _poll_once(self) -> None:
        from gateway.core.globals import get_sink_registry

        sink_registry = get_sink_registry()
        if not sink_registry:
            logger.debug("news_poller_no_sink")
            return

        # News flows continuously — no market-hours gate.
        await self._poll_news(sink_registry)
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
            logger.warning("news_poller_redis_dedupe_mget_failed", count=len(cache_keys), error=str(e))
            return set()
        return {eid for eid, ck in items if ck in found}

    # ─────────────────────────────────────────────────────────────────────
    # Publish
    # ─────────────────────────────────────────────────────────────────────

    async def _publish_envelopes(
        self,
        sink_registry: Any,
        envelopes: list[dict[str, Any]],
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
            cache_key = f"alpaca:news:{event_id}"
            dedupe_items.append((event_id, cache_key))
            to_publish.append((envelope, event_id, cache_key))

        redis_dups = await self._check_redis_duplicates(dedupe_items)
        if redis_dups:
            duplicates += len(redis_dups)
            to_publish = [item for item in to_publish if not item[1] or item[1] not in redis_dups]

        if not to_publish:
            return 0, duplicates

        messages: list[tuple[str, dict[str, Any]]] = [(HEBER_STREAM, env) for env, _, _ in to_publish]
        try:
            if hasattr(sink_registry, "publish_all_batch"):
                published = await sink_registry.publish_all_batch(messages)
            else:
                for topic, env in messages:
                    await sink_registry.publish_all(topic, env)
                published = len(messages)
        except Exception as exc:
            logger.warning("news_poller_batch_publish_failed", count=len(messages), error=str(exc))
            published = 0

        if published > 0:
            redis_items: list[tuple[str, Any]] = []
            for _env, event_id, cache_key in to_publish[:published]:
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

    async def _poll_news(self, sink_registry: Any) -> None:
        """Fetch latest news articles and publish to Heber."""
        if self._provider is None:
            return

        try:
            articles = await self._provider.get_news(
                symbols=self._symbols,
                limit=self._fetch_limit,
            )

            if not articles:
                logger.debug("news_poller_no_articles")
                return

            envelopes: list[dict[str, Any]] = []
            for article in articles:
                payload = article.model_dump(mode="json") if hasattr(article, "model_dump") else dict(article)
                # Use the first symbol mentioned (or "MARKET" if none) as the
                # instrument identifier so wrap_event produces a valid envelope.
                article_symbols = payload.get("symbols") or []
                primary_symbol = article_symbols[0] if article_symbols else "MARKET"
                envelope = wrap_event(
                    event=payload,
                    provider="alpaca",
                    feed="news",
                    source="rest",
                    instrument_type_override="equity",
                    instrument_key_override=f"equity:{primary_symbol}",
                    symbol_override=primary_symbol,
                )
                envelopes.append(envelope)

            published, duplicates = await self._publish_envelopes(sink_registry, envelopes)

            logger.info(
                "news_poller_published",
                fetched=len(articles),
                published=published,
                duplicates=duplicates,
            )

        except Exception as e:
            logger.error("news_poller_poll_error", error=str(e), exc_info=True)

    def get_runtime_snapshot(self) -> dict[str, Any]:
        return super().get_runtime_snapshot() | {
            "symbols": self._symbols or "all",
            "fetch_limit": self._fetch_limit,
            "dedupe_cache_entries": len(self._seen_ids),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────

_news_poller: AlpacaNewsPoller | None = None


def get_news_poller() -> AlpacaNewsPoller | None:
    """Get the global news poller instance."""
    return _news_poller


async def start_news_poller(
    symbols: list[str] | None = None,
    poll_interval_seconds: int = 60,
    fetch_limit: int = 50,
) -> AlpacaNewsPoller:
    """Start the global Alpaca news poller."""
    global _news_poller

    if _news_poller is not None:
        return _news_poller

    _news_poller = AlpacaNewsPoller(
        symbols=symbols,
        poll_interval_seconds=poll_interval_seconds,
        fetch_limit=fetch_limit,
    )
    await _news_poller.start()
    return _news_poller


async def stop_news_poller() -> None:
    """Stop the global Alpaca news poller."""
    global _news_poller

    if _news_poller is not None:
        await _news_poller.stop()
        _news_poller = None
