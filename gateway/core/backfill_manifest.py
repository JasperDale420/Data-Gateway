"""Non-expiring replay manifests and durable Heber acknowledgement contracts."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, field_validator

from gateway.core.logger import logger

PROTOCOL_VERSION = 1
READINESS_MAX_AGE_SECONDS = 60


class HeberReadiness(BaseModel):
    """Machine-verifiable readiness written by the Heber backfill consumer."""

    consumer_healthy: bool
    writer_healthy: bool
    ack_store_ready: bool
    protocol_version: int
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        return value

    def failure_reason(self, now: datetime | None = None) -> str | None:
        now = now or datetime.now(UTC)
        if abs((now - self.observed_at).total_seconds()) > READINESS_MAX_AGE_SECONDS:
            return "heber_readiness_stale"
        if self.protocol_version != PROTOCOL_VERSION:
            return "heber_ack_protocol_mismatch"
        if not self.consumer_healthy:
            return "heber_consumer_unhealthy"
        if not self.writer_healthy:
            return "heber_writer_unhealthy"
        if not self.ack_store_ready:
            return "heber_ack_store_unavailable"
        return None


class HeberChunkAcknowledgement(BaseModel):
    """Proof written only after Heber has durably committed one replay chunk."""

    job_id: str
    chunk_id: str
    manifest_hash: str
    record_count: int
    event_ids_sha256: str
    records_sha256: str
    commit_id: str
    committed_at: datetime
    status: str = "committed"

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("committed_at must be timezone-aware")
        return value


class InMemoryBackfillManifestStore:
    """Deterministic test store matching the Redis contract."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.acks: dict[tuple[str, str], HeberChunkAcknowledgement] = {}
        self.readiness: HeberReadiness | None = None

    async def save_job(
        self,
        *,
        job_id: str,
        payload: dict[str, Any],
        status: str,
        created_at: datetime,
    ) -> None:
        self.jobs[job_id] = deepcopy(payload)

    async def load_job(self, job_id: str) -> dict[str, Any] | None:
        payload = self.jobs.get(job_id)
        return deepcopy(payload) if payload is not None else None

    async def load_jobs(self) -> list[dict[str, Any]]:
        return [deepcopy(payload) for payload in self.jobs.values()]

    async def delete_job(self, job_id: str) -> None:
        self.jobs.pop(job_id, None)
        for key in [key for key in self.acks if key[0] == job_id]:
            self.acks.pop(key, None)

    async def read_heber_readiness(self) -> HeberReadiness | None:
        return self.readiness

    async def read_ack(
        self,
        job_id: str,
        chunk_id: str,
    ) -> HeberChunkAcknowledgement | None:
        return self.acks.get((job_id, chunk_id))

    async def acknowledge(
        self,
        *,
        job_id: str,
        chunk_id: str,
        manifest_hash: str,
        record_count: int,
        event_ids_sha256: str,
        records_sha256: str,
        commit_id: str,
        committed_at: datetime,
    ) -> None:
        self.acks[(job_id, chunk_id)] = HeberChunkAcknowledgement(
            job_id=job_id,
            chunk_id=chunk_id,
            manifest_hash=manifest_hash,
            record_count=record_count,
            event_ids_sha256=event_ids_sha256,
            records_sha256=records_sha256,
            commit_id=commit_id,
            committed_at=committed_at,
        )

    async def close(self) -> None:
        return None


class RedisBackfillManifestStore:
    """Redis-backed manifests using only non-expiring Hashes and a sorted index."""

    KEY_PREFIX = "gateway:backfill"
    MANIFEST_INDEX = f"{KEY_PREFIX}:manifest:index"
    READINESS_KEY = f"{KEY_PREFIX}:heber:readiness:v{PROTOCOL_VERSION}"

    def __init__(self, redis_url: str, *, client: Any | None = None) -> None:
        self._redis_url = redis_url
        self._redis = client

    async def _client(self) -> Any:
        if self._redis is None:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
        return self._redis

    @classmethod
    def _manifest_key(cls, job_id: str) -> str:
        return f"{cls.KEY_PREFIX}:manifest:{job_id}"

    @classmethod
    def ack_key(cls, job_id: str, chunk_id: str) -> str:
        return f"{cls.KEY_PREFIX}:ack:{job_id}:{chunk_id}"

    async def save_job(
        self,
        *,
        job_id: str,
        payload: dict[str, Any],
        status: str,
        created_at: datetime,
    ) -> None:
        redis = await self._client()
        now = datetime.now(UTC).isoformat()
        pipeline = redis.pipeline(transaction=True)
        pipeline.hset(
            self._manifest_key(job_id),
            mapping={
                "payload": json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                "status": status,
                "updated_at": now,
                "protocol_version": str(PROTOCOL_VERSION),
            },
        )
        pipeline.zadd(self.MANIFEST_INDEX, {job_id: created_at.timestamp()})
        await pipeline.execute()

    async def load_job(self, job_id: str) -> dict[str, Any] | None:
        redis = await self._client()
        raw = await redis.hgetall(self._manifest_key(job_id))
        if not raw:
            return None
        payload = raw.get("payload") or raw.get(b"payload")
        if isinstance(payload, bytes):
            payload = payload.decode()
        return json.loads(payload) if payload else None

    async def load_jobs(self) -> list[dict[str, Any]]:
        redis = await self._client()
        job_ids = await redis.zrange(self.MANIFEST_INDEX, 0, -1)
        jobs = []
        for job_id in job_ids:
            if isinstance(job_id, bytes):
                job_id = job_id.decode()
            payload = await self.load_job(str(job_id))
            if payload is not None:
                jobs.append(payload)
        return jobs

    async def delete_job(self, job_id: str) -> None:
        redis = await self._client()
        payload = await self.load_job(job_id)
        ack_keys = []
        if payload:
            ack_keys = [self.ack_key(job_id, chunk_id) for chunk_id in payload.get("chunks", {})]
        pipeline = redis.pipeline(transaction=True)
        pipeline.delete(self._manifest_key(job_id), *ack_keys)
        pipeline.zrem(self.MANIFEST_INDEX, job_id)
        await pipeline.execute()

    async def read_heber_readiness(self) -> HeberReadiness | None:
        redis = await self._client()
        raw = await redis.hgetall(self.READINESS_KEY)
        return HeberReadiness.model_validate(_decode_hash(raw)) if raw else None

    async def read_ack(
        self,
        job_id: str,
        chunk_id: str,
    ) -> HeberChunkAcknowledgement | None:
        redis = await self._client()
        raw = await redis.hgetall(self.ack_key(job_id, chunk_id))
        return HeberChunkAcknowledgement.model_validate(_decode_hash(raw)) if raw else None

    async def close(self) -> None:
        if self._redis is None:
            return
        try:
            async_close = getattr(self._redis, "aclose", None)
            if async_close is not None:
                await async_close()
            else:
                await self._redis.close()
        except Exception:
            logger.warning("backfill_manifest_store_close_failed", exc_info=True)
        finally:
            self._redis = None


def _decode_hash(raw: dict[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(key, bytes):
            key = key.decode()
        if isinstance(value, bytes):
            value = value.decode()
        decoded[str(key)] = value
    return decoded
