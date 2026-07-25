"""Safety contracts for durable, automatically verified market-data replay."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.core.backfill import (
    BACKFILL_CAPABILITIES,
    BackfillEngine,
    BackfillJob,
    BackfillRequest,
    BackfillStatus,
    ChunkResult,
    ReplayCapability,
    ReplayFeedCapability,
    SymbolProgress,
    _ack_mismatch,
    _chunk_id,
    _date_chunks,
    _manifest_hash,
)
from gateway.core.backfill_manifest import (
    HeberChunkAcknowledgement,
    HeberReadiness,
    InMemoryBackfillManifestStore,
    RedisBackfillManifestStore,
)


def _request(
    *,
    provider: str = "alpaca",
    feed: str = "bars",
    symbols: list[str] | None = None,
    day: date = date(2026, 7, 23),
    canary: bool = False,
) -> BackfillRequest:
    return BackfillRequest(
        provider=provider,
        feed=feed,
        symbols=symbols or ["AAPL"],
        start=day,
        end=day,
        canary=canary,
    )


def _ready_store() -> InMemoryBackfillManifestStore:
    store = InMemoryBackfillManifestStore()
    store.readiness = HeberReadiness(
        consumer_healthy=True,
        writer_healthy=True,
        ack_store_ready=True,
        protocol_version=1,
        observed_at=datetime.now(UTC),
    )
    return store


def test_heber_evidence_timestamps_must_be_timezone_aware() -> None:
    with pytest.raises(ValueError, match="observed_at must be timezone-aware"):
        HeberReadiness(
            consumer_healthy=True,
            writer_healthy=True,
            ack_store_ready=True,
            protocol_version=1,
            observed_at=datetime(2026, 7, 23),
        )
    with pytest.raises(ValueError, match="committed_at must be timezone-aware"):
        HeberChunkAcknowledgement(
            job_id="bf-test",
            chunk_id="chunk",
            manifest_hash="manifest",
            record_count=1,
            event_ids_sha256="digest",
            records_sha256="records-digest",
            commit_id="commit",
            committed_at=datetime(2026, 7, 23),
        )


def test_heber_acknowledgement_must_match_committed_payload_digest() -> None:
    published_at = datetime.now(UTC)
    job = BackfillJob(
        job_id="bf-test",
        manifest_hash="manifest",
        request=_request(),
    )
    chunk = ChunkResult(
        chunk_id="chunk",
        symbol="AAPL",
        requested_start=published_at,
        requested_end=published_at,
        record_count=1,
        event_ids_sha256="event-digest",
        records_sha256="gateway-record-digest",
        published_at=published_at,
    )
    acknowledgement = HeberChunkAcknowledgement(
        job_id=job.job_id,
        chunk_id=chunk.chunk_id,
        manifest_hash=job.manifest_hash,
        record_count=1,
        event_ids_sha256="event-digest",
        records_sha256="different-committed-payload",
        commit_id="commit",
        committed_at=published_at + timedelta(seconds=1),
    )

    assert _ack_mismatch(job, chunk, acknowledgement) == ("heber_acknowledgement_mismatch")


def _engine(
    provider: object,
    store: InMemoryBackfillManifestStore,
    *,
    publish_results: list[bool] | None = None,
) -> BackfillEngine:
    provider_registry = MagicMock()
    provider_registry.get.return_value = provider
    sink_registry = MagicMock()
    sink_registry.publish_all_batch_results = AsyncMock(
        side_effect=lambda messages: publish_results or [True] * len(messages)
    )
    capabilities = dict(BACKFILL_CAPABILITIES)
    capabilities[("alpaca", "bars")] = ReplayFeedCapability(capability=ReplayCapability.DATE_BOUNDED)
    engine = BackfillEngine(ack_wait_seconds=0, capabilities=capabilities)
    engine.configure(
        provider_registry=provider_registry,
        sink_registry=sink_registry,
        manifest_store=store,
    )
    return engine


@pytest.mark.asyncio
async def test_flow_alerts_is_unrecoverable_without_bound_provider_contract() -> None:
    provider = MagicMock()
    provider.get_flow_alerts = AsyncMock()
    store = _ready_store()
    engine = _engine(provider, store)

    job = await engine.submit(_request(provider="unusual_whales", feed="flow_alerts", canary=True))

    assert job.status == BackfillStatus.UNRECOVERABLE
    assert job.blocked_reason == "provider_has_no_date_bounded_flow_alerts_contract"
    assert await store.load_job(job.job_id) is not None
    provider.get_flow_alerts.assert_not_called()
    engine._sink_registry.publish_all_batch_results.assert_not_called()


def test_production_capabilities_publish_nothing_without_complete_coverage_proof() -> None:
    assert all(capability.capability != ReplayCapability.DATE_BOUNDED for capability in BACKFILL_CAPABILITIES.values())


@pytest.mark.parametrize("feed", ["short_interest", "short_volume", "ftds"])
@pytest.mark.asyncio
async def test_snapshot_only_market_feeds_are_explicitly_unrecoverable(feed: str) -> None:
    provider = MagicMock()
    store = _ready_store()
    engine = _engine(provider, store)

    job = await engine.submit(_request(provider="unusual_whales", feed=feed))

    assert job.status == BackfillStatus.UNRECOVERABLE
    assert job.blocked_reason == "snapshot_only_source"
    assert await store.load_job(job.job_id) is not None
    provider.assert_not_called()
    engine._sink_registry.publish_all_batch_results.assert_not_called()


@pytest.mark.asyncio
async def test_missing_heber_readiness_blocks_before_fetch_or_publication() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock()
    store = InMemoryBackfillManifestStore()
    engine = _engine(provider, store)

    job = await engine.submit(_request())
    await engine.wait(job.job_id)

    assert job.status == BackfillStatus.FAILED
    assert job.blocked_reason == "heber_readiness_missing"
    provider.get_bars.assert_not_called()
    engine._sink_registry.publish_all_batch_results.assert_not_called()


@pytest.mark.asyncio
async def test_missing_durable_manifest_store_blocks_submission() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock()
    provider_registry = MagicMock()
    provider_registry.get.return_value = provider
    sink_registry = MagicMock()
    engine = BackfillEngine()
    engine.configure(
        provider_registry=provider_registry,
        sink_registry=sink_registry,
        manifest_store=None,
    )

    with pytest.raises(ValueError, match="missing durable manifest store"):
        await engine.submit(_request())

    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_heber_readiness_blocks_before_fetch_or_publication() -> None:
    class InvalidReadinessStore(InMemoryBackfillManifestStore):
        async def read_heber_readiness(self):
            raise ValueError("malformed readiness")

    provider = MagicMock()
    store = InvalidReadinessStore()
    engine = _engine(provider, store)

    job = await engine.submit(_request())
    await engine.wait(job.job_id)

    assert job.status == BackfillStatus.FAILED
    assert job.blocked_reason == "heber_readiness_invalid"
    provider.get_bars.assert_not_called()
    engine._sink_registry.publish_all_batch_results.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (
            {"symbol": "AAPL", "timestamp": "2026-07-24T14:00:00Z"},
            "provider_date_out_of_range",
        ),
        ({"symbol": "AAPL", "close": "200"}, "provider_timestamp_missing"),
        (
            {"symbol": "MSFT", "timestamp": "2026-07-23T14:00:00Z"},
            "provider_symbol_mismatch",
        ),
    ],
)
async def test_invalid_provider_rows_reject_whole_chunk_before_publish(
    row: dict[str, str],
    reason: str,
) -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[row])
    store = _ready_store()
    engine = _engine(provider, store)

    job = await engine.submit(_request())
    await engine.wait(job.job_id)

    assert job.status == BackfillStatus.FAILED
    assert job.blocked_reason == reason
    engine._sink_registry.publish_all_batch_results.assert_not_called()


@pytest.mark.asyncio
async def test_redis_acceptance_without_heber_ack_is_partial_not_verified() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(
        return_value=[
            {
                "symbol": "AAPL",
                "timestamp": "2026-07-23T14:00:00Z",
                "open": "200",
                "high": "201",
                "low": "199",
                "close": "200.5",
                "volume": "1000",
                "timeframe": "1Day",
            }
        ]
    )
    store = _ready_store()
    engine = _engine(provider, store)

    job = await engine.submit(_request())
    await engine.wait(job.job_id)

    assert job.gateway_completed_at is not None
    assert job.ingestion_verified_at is None
    assert job.status == BackfillStatus.PARTIAL
    assert job.blocked_reason == "missing_heber_acknowledgement"


@pytest.mark.asyncio
async def test_invalid_heber_acknowledgement_is_partial_not_verified() -> None:
    class InvalidAckStore(InMemoryBackfillManifestStore):
        async def read_ack(self, job_id: str, chunk_id: str):
            raise ValueError("malformed acknowledgement")

    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}])
    store = InvalidAckStore()
    store.readiness = _ready_store().readiness
    engine = _engine(provider, store)

    job = await engine.submit(_request())
    await engine.wait(job.job_id)

    assert job.status == BackfillStatus.PARTIAL
    assert job.blocked_reason == "heber_acknowledgement_invalid"
    assert job.ingestion_verified_at is None


@pytest.mark.asyncio
async def test_exact_post_commit_ack_is_the_only_path_to_verified() -> None:
    store = _ready_store()
    provider = MagicMock()
    provider.get_bars = AsyncMock(
        return_value=[
            {
                "symbol": "AAPL",
                "timestamp": "2026-07-23T14:00:00Z",
                "open": "200",
                "high": "201",
                "low": "199",
                "close": "200.5",
                "volume": "1000",
                "timeframe": "1Day",
            }
        ]
    )
    engine = _engine(provider, store)

    async def publish_and_ack(messages):
        envelopes = [envelope for _topic, envelope in messages]
        lineage = envelopes[0]["lineage"]
        event_ids = sorted(envelope["event_id"] for envelope in envelopes)
        manifest = await store.load_job(lineage["backfill_job_id"])
        records_sha256 = manifest["chunks"][lineage["backfill_chunk_id"]]["records_sha256"]
        await store.acknowledge(
            job_id=lineage["backfill_job_id"],
            chunk_id=lineage["backfill_chunk_id"],
            manifest_hash=lineage["backfill_manifest_hash"],
            record_count=len(event_ids),
            event_ids_sha256=hashlib.sha256("\n".join(event_ids).encode()).hexdigest(),
            records_sha256=records_sha256,
            commit_id="heber-commit-1",
            committed_at=datetime.now(UTC),
        )
        return [True] * len(messages)

    engine._sink_registry.publish_all_batch_results.side_effect = publish_and_ack

    job = await engine.submit(_request())
    await engine.wait(job.job_id)

    assert job.status == BackfillStatus.VERIFIED
    assert job.ingestion_verified_at is not None
    assert job.blocked_reason is None
    assert all(chunk.acknowledged for chunk in job.chunks.values())


@pytest.mark.asyncio
async def test_retry_reuses_manifest_and_rejects_changed_record_identity() -> None:
    first = {
        "symbol": "AAPL",
        "timestamp": "2026-07-23T14:00:00Z",
        "open": "200",
        "high": "201",
        "low": "199",
        "close": "200.5",
        "volume": "1000",
        "timeframe": "1Day",
    }
    changed = {**first, "close": "199.5"}
    provider = MagicMock()
    provider.get_bars = AsyncMock(side_effect=[[first], [changed]])
    store = _ready_store()
    engine = _engine(provider, store)

    original = await engine.submit(_request())
    await engine.wait(original.job_id)
    first_chunk = next(iter(original.chunks.values()))
    first_digest = first_chunk.event_ids_sha256

    retried = await engine.submit(_request())
    await engine.wait(retried.job_id)

    assert retried.job_id == original.job_id
    assert next(iter(retried.chunks.values())).event_ids_sha256 == first_digest
    assert retried.status == BackfillStatus.FAILED
    assert retried.blocked_reason == "provider_record_identity_changed"


@pytest.mark.asyncio
async def test_persisted_manifest_identity_mismatch_fails_closed() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}])
    store = _ready_store()
    engine = _engine(provider, store)
    job = await engine.submit(_request())
    await engine.wait(job.job_id)
    store.jobs[job.job_id]["manifest_hash"] = "different-manifest"

    with pytest.raises(ValueError, match="Durable manifest mismatch"):
        await engine.submit(_request())

    assert len(job.job_id.removeprefix("bf-")) == 32


@pytest.mark.asyncio
async def test_duplicate_submit_reuses_the_only_live_task() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    provider = MagicMock()

    async def slow_bars(*_args, **_kwargs):
        started.set()
        await release.wait()
        return [{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}]

    provider.get_bars = AsyncMock(side_effect=slow_bars)
    store = _ready_store()
    engine = _engine(provider, store)

    first, second = await asyncio.gather(
        engine.submit(_request()),
        engine.submit(_request()),
    )

    assert second is first
    assert len(engine._running_tasks) == 1
    await started.wait()
    release.set()
    await engine.wait(first.job_id)
    provider.get_bars.assert_awaited_once()


@pytest.mark.asyncio
async def test_restart_automatically_reconciles_late_durable_ack_without_refetch() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}])
    store = _ready_store()
    first_engine = _engine(provider, store)
    first = await first_engine.submit(_request())
    await first_engine.wait(first.job_id)
    chunk = next(iter(first.chunks.values()))
    await store.acknowledge(
        job_id=first.job_id,
        chunk_id=chunk.chunk_id,
        manifest_hash=first.manifest_hash,
        record_count=chunk.record_count,
        event_ids_sha256=chunk.event_ids_sha256,
        records_sha256=chunk.records_sha256,
        commit_id="heber-late-commit",
        committed_at=chunk.published_at + timedelta(seconds=1),
    )
    provider.get_bars.reset_mock()

    restarted_engine = _engine(provider, store)
    await restarted_engine.start()
    restored = await restarted_engine.wait(first.job_id)

    assert restored.status == BackfillStatus.VERIFIED
    assert restored.ingestion_verified_at is not None
    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_restart_marks_interrupted_publication_failed_without_refetch() -> None:
    provider = MagicMock()
    store = _ready_store()
    engine = _engine(provider, store)
    request = _request()
    manifest_hash = _manifest_hash(request)
    start, end = _date_chunks(request.start, request.end, request.chunk_days)[0]
    job_id = f"bf-{manifest_hash[:32]}"
    chunk_id = _chunk_id(job_id, "AAPL", start, end)
    job = BackfillJob(
        job_id=job_id,
        manifest_hash=manifest_hash,
        request=request,
        status=BackfillStatus.RUNNING,
        symbols_progress={"AAPL": SymbolProgress(symbol="AAPL", chunks_total=1)},
        chunks={
            chunk_id: ChunkResult(
                chunk_id=chunk_id,
                symbol="AAPL",
                requested_start=start,
                requested_end=end,
            )
        },
    )
    await store.save_job(
        job_id=job.job_id,
        payload=job.model_dump(mode="json"),
        status=job.status.value,
        created_at=job.created_at,
    )

    await engine.start()
    restored = engine.get_job(job.job_id)

    assert restored is not None
    assert restored.status == BackfillStatus.FAILED
    assert restored.blocked_reason == "gateway_restart_interrupted_before_publication"
    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_restart_rejects_empty_publication_evidence_without_verifying() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}])
    store = _ready_store()
    first_engine = _engine(provider, store)
    first = await first_engine.submit(_request())
    await first_engine.wait(first.job_id)
    stored = store.jobs[first.job_id]
    stored["chunks"] = {}
    stored["status"] = BackfillStatus.AWAITING_ACK.value
    stored["blocked_reason"] = None
    provider.get_bars.reset_mock()

    restarted_engine = _engine(provider, store)
    await restarted_engine.start()
    restored = restarted_engine.get_job(first.job_id)

    assert restored is not None
    assert restored.status == BackfillStatus.FAILED
    assert restored.blocked_reason == "durable_manifest_invalid"
    assert restored.ingestion_verified_at is None
    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_restart_rejects_chunk_whose_embedded_identity_does_not_match_manifest_key() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}])
    store = _ready_store()
    first_engine = _engine(provider, store)
    first = await first_engine.submit(_request())
    await first_engine.wait(first.job_id)
    chunk = next(iter(store.jobs[first.job_id]["chunks"].values()))
    chunk["chunk_id"] = "different-chunk"
    provider.get_bars.reset_mock()

    restarted_engine = _engine(provider, store)
    await restarted_engine.start()
    restored = restarted_engine.get_job(first.job_id)

    assert restored is not None
    assert restored.status == BackfillStatus.FAILED
    assert restored.blocked_reason == "durable_manifest_invalid"
    assert restored.ingestion_verified_at is None
    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_restart_revokes_verified_when_durable_ack_is_missing() -> None:
    provider = MagicMock()
    provider.get_bars = AsyncMock(return_value=[{"symbol": "AAPL", "timestamp": "2026-07-23T14:00:00Z"}])
    store = _ready_store()
    first_engine = _engine(provider, store)

    async def publish_and_ack(messages):
        envelopes = [envelope for _topic, envelope in messages]
        lineage = envelopes[0]["lineage"]
        event_ids = sorted(envelope["event_id"] for envelope in envelopes)
        manifest = await store.load_job(lineage["backfill_job_id"])
        chunk = manifest["chunks"][lineage["backfill_chunk_id"]]
        await store.acknowledge(
            job_id=lineage["backfill_job_id"],
            chunk_id=lineage["backfill_chunk_id"],
            manifest_hash=lineage["backfill_manifest_hash"],
            record_count=len(event_ids),
            event_ids_sha256=hashlib.sha256("\n".join(event_ids).encode()).hexdigest(),
            records_sha256=chunk["records_sha256"],
            commit_id="heber-commit",
            committed_at=datetime.now(UTC),
        )
        return [True] * len(messages)

    first_engine._sink_registry.publish_all_batch_results.side_effect = publish_and_ack
    first = await first_engine.submit(_request())
    await first_engine.wait(first.job_id)
    assert first.status == BackfillStatus.VERIFIED
    store.acks.clear()
    provider.get_bars.reset_mock()

    restarted_engine = _engine(provider, store)
    await restarted_engine.start()
    restored = await restarted_engine.wait(first.job_id)

    assert restored is not None
    assert restored.status == BackfillStatus.PARTIAL
    assert restored.blocked_reason == "missing_heber_acknowledgement"
    assert restored.ingestion_verified_at is None
    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_resubmit_revalidates_verified_manifest_ack_without_refetch() -> None:
    provider = MagicMock()
    store = _ready_store()
    engine = _engine(provider, store)
    request = _request()
    manifest_hash = _manifest_hash(request)
    job_id = f"bf-{manifest_hash[:32]}"
    start, end = _date_chunks(request.start, request.end, request.chunk_days)[0]
    chunk_id = _chunk_id(job_id, "AAPL", start, end)
    published_at = datetime.now(UTC)
    job = BackfillJob(
        job_id=job_id,
        manifest_hash=manifest_hash,
        request=request,
        status=BackfillStatus.VERIFIED,
        symbols_progress={
            "AAPL": SymbolProgress(
                symbol="AAPL",
                status="complete",
                chunks_total=1,
                chunks_complete=1,
                records_published=1,
            )
        },
        chunks={
            chunk_id: ChunkResult(
                chunk_id=chunk_id,
                symbol="AAPL",
                requested_start=start,
                requested_end=end,
                status="verified",
                record_count=1,
                records_published=1,
                event_ids_sha256="event-digest",
                records_sha256="record-digest",
                published_at=published_at,
                acknowledged=True,
                heber_commit_id="missing-commit",
            )
        },
        records_published=1,
        gateway_completed_at=published_at,
        ingestion_verified_at=published_at,
        completed_at=published_at,
    )
    await store.save_job(
        job_id=job.job_id,
        payload=job.model_dump(mode="json"),
        status=job.status.value,
        created_at=job.created_at,
    )

    restored = await engine.submit(request)
    await engine.wait(restored.job_id)

    assert restored.status == BackfillStatus.PARTIAL
    assert restored.blocked_reason == "missing_heber_acknowledgement"
    assert restored.ingestion_verified_at is None
    provider.get_bars.assert_not_called()


@pytest.mark.asyncio
async def test_mixed_symbol_success_is_partial() -> None:
    async def fetch(symbols, *_args, **_kwargs):
        symbol = symbols[0]
        if symbol == "MSFT":
            raise RuntimeError("rate limited")
        return [
            {
                "symbol": symbol,
                "timestamp": "2026-07-23T14:00:00Z",
                "open": "200",
                "high": "201",
                "low": "199",
                "close": "200.5",
                "volume": "1000",
                "timeframe": "1Day",
            }
        ]

    provider = MagicMock()
    provider.get_bars = AsyncMock(side_effect=fetch)
    store = _ready_store()
    engine = _engine(provider, store)

    job = await engine.submit(_request(symbols=["AAPL", "MSFT"]))
    await engine.wait(job.job_id)

    assert job.status == BackfillStatus.PARTIAL
    assert job.ingestion_verified_at is None
    assert job.symbols_progress["MSFT"].status == "failed"


@pytest.mark.asyncio
async def test_canary_requires_one_symbol_one_completed_market_day() -> None:
    provider = MagicMock()
    store = _ready_store()
    engine = _engine(provider, store)

    with pytest.raises(ValueError, match="exactly one symbol"):
        await engine.submit(_request(symbols=["AAPL", "MSFT"], canary=True))

    future_day = date.today() + timedelta(days=1)
    with pytest.raises(ValueError, match="completed market day"):
        await engine.submit(_request(day=future_day, canary=True))


class _RedisSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    async def hset(self, *args, **kwargs):
        self.calls.append(("hset", args, kwargs))

    async def hgetall(self, *args, **kwargs):
        self.calls.append(("hgetall", args, kwargs))
        return {}

    async def zadd(self, *args, **kwargs):
        self.calls.append(("zadd", args, kwargs))

    async def delete(self, *args, **kwargs):
        self.calls.append(("delete", args, kwargs))

    async def zrem(self, *args, **kwargs):
        self.calls.append(("zrem", args, kwargs))

    def pipeline(self, *, transaction: bool):
        self.calls.append(("pipeline", (), {"transaction": transaction}))
        return _RedisPipelineSpy(self)


class _RedisPipelineSpy:
    def __init__(self, redis: _RedisSpy) -> None:
        self.redis = redis

    def hset(self, *args, **kwargs):
        self.redis.calls.append(("hset", args, kwargs))
        return self

    def zadd(self, *args, **kwargs):
        self.redis.calls.append(("zadd", args, kwargs))
        return self

    def delete(self, *args, **kwargs):
        self.redis.calls.append(("delete", args, kwargs))
        return self

    def zrem(self, *args, **kwargs):
        self.redis.calls.append(("zrem", args, kwargs))
        return self

    async def execute(self):
        self.redis.calls.append(("execute", (), {}))
        return []


@pytest.mark.asyncio
async def test_redis_manifest_store_never_sets_ttl() -> None:
    redis = _RedisSpy()
    store = RedisBackfillManifestStore("redis://unused", client=redis)

    await store.save_job(
        job_id="bf-stable",
        payload={"job_id": "bf-stable", "status": "queued"},
        status="queued",
        created_at=datetime(2026, 7, 23, tzinfo=UTC),
    )

    assert [name for name, _args, _kwargs in redis.calls] == ["pipeline", "hset", "zadd", "execute"]
    assert redis.calls[0][2] == {"transaction": True}
    assert not any(name in {"expire", "setex"} for name, _args, _kwargs in redis.calls)


def test_replay_block_alert_contract_is_deduplicated_and_resolves() -> None:
    from gateway.core.metrics import REPLAY_BLOCKED, set_replay_verification_state

    labels = {
        "provider": "unusual_whales",
        "feed": "flow_alerts",
        "scope": "AAPL:2026-07-23",
        "reason": "missing_heber_acknowledgement",
    }
    set_replay_verification_state(verified=False, **labels)
    set_replay_verification_state(verified=False, **labels)
    assert REPLAY_BLOCKED.labels(**labels)._value.get() == 1

    changed = {**labels, "reason": "heber_consumer_unhealthy"}
    set_replay_verification_state(verified=False, **changed)
    assert REPLAY_BLOCKED.labels(**labels)._value.get() == 1
    assert REPLAY_BLOCKED.labels(**changed)._value.get() == 0

    set_replay_verification_state(verified=True, **changed)
    assert REPLAY_BLOCKED.labels(**labels)._value.get() == 0


def test_prometheus_rule_routes_block_and_recovery_through_existing_alerting() -> None:
    alerts = Path("config/prometheus_alerts.yml").read_text()

    assert "KairosReplayIntakeBlocked" in alerts
    assert "gateway_replay_blocked == 1" in alerts
    assert "Kairos intake is blocked" in alerts
    assert "matching durable Heber acknowledgement" in alerts
