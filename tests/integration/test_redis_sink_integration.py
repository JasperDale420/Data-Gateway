"""Real-Redis integration tests for the Redis Streams sink and dedup cache.

These tests run against a REAL Redis instance (not a mock). They are marked
``integration`` and skip gracefully when no Redis is reachable, so the default
local ``pytest`` run is never broken on a machine without Redis.

Redis URL comes from ``GATEWAY_TEST_REDIS_URL`` (default
``redis://localhost:6379/15``). DB 15 is used as a throwaway test database and
is flushed before and after each test by the ``redis_sink`` fixture.

Coverage:
    * publish -> XLEN increment on the target stream
    * dedup TTL behavior (set_nx first-wins + TTL applied)
    * failed-event buffer fill -> evict -> drain against live Redis
"""

import asyncio

import pytest

from gateway.core.cache import RedisCache
from gateway.core.redis_sink import FAILED_EVENT_BUFFER_CAPACITY

pytestmark = pytest.mark.integration


async def test_publish_increments_xlen(redis_sink, redis_probe):
    """A successful publish appends exactly one entry to the stream."""
    topic = "gateway.test.bars"

    assert await redis_sink.publish(topic, {"event_id": "evt-1", "symbol": "AAPL"}) is True
    assert await redis_probe.xlen(topic) == 1

    assert await redis_sink.publish(topic, {"event_id": "evt-2", "symbol": "MSFT"}) is True
    assert await redis_probe.xlen(topic) == 2

    # The serialized payload is stored under the b"data" field.
    entries = await redis_probe.xrange(topic)
    assert len(entries) == 2
    _entry_id, fields = entries[0]
    assert b"data" in fields


async def test_publish_batch_increments_xlen(redis_sink, redis_probe):
    """publish_batch pipelines all messages onto the stream."""
    topic = "gateway.test.batch"
    messages = [(topic, {"event_id": f"evt-{i}", "i": i}) for i in range(50)]

    published = await redis_sink.publish_batch(messages)

    assert published == 50
    assert await redis_probe.xlen(topic) == 50


async def test_dedup_set_nx_first_wins_with_ttl(redis_cache, redis_probe):
    """set_nx returns True on first write, False on duplicate, and applies TTL."""
    key = "publish:evt-dedup-1"
    prefixed = f"{redis_cache.KEY_PREFIX}{key}"

    assert await redis_cache.set_nx(key, "1", ttl=120) is True
    # Duplicate write must report the key already exists.
    assert await redis_cache.set_nx(key, "1", ttl=120) is False

    # The dedup key must carry a TTL so stale entries self-expire instead of
    # leaking forever (24h in production via DEDUP_TTL_SECONDS).
    ttl = await redis_probe.ttl(prefixed)
    assert 0 < ttl <= 120


async def test_failed_buffer_fill_evict_then_drain(redis_sink, redis_probe):
    """Overfilling the failed buffer evicts oldest events, then drain flushes.

    Fills the bounded ``_failed_buffer`` past capacity so the oldest entries
    are evicted (eviction counter increments), then drains the buffer through
    the live Redis connection and asserts every retained event lands on its
    stream.
    """
    topic = "gateway.test.drain"
    overflow = 5

    # Fill past capacity to force eviction of the oldest entries.
    for i in range(FAILED_EVENT_BUFFER_CAPACITY + overflow):
        redis_sink.buffer_event(topic, str(i))

    stats = redis_sink.get_buffer_stats()
    assert stats["buffer_size"] == FAILED_EVENT_BUFFER_CAPACITY
    assert stats["evicted"] == overflow

    # Establish a real connection, then drain the buffer onto Redis.
    assert await redis_sink.health_check() is True
    await redis_sink._do_drain()

    drained_stats = redis_sink.get_buffer_stats()
    assert drained_stats["buffer_size"] == 0
    assert drained_stats["drained"] == FAILED_EVENT_BUFFER_CAPACITY
    assert await redis_probe.xlen(topic) == FAILED_EVENT_BUFFER_CAPACITY


async def test_buffer_drain_on_reconnect(redis_sink, redis_probe):
    """Events buffered while disconnected are drained on the next reconnect."""
    topic = "gateway.test.reconnect"

    # Connect once, then simulate a transport failure that resets the
    # connection and arms the reconnect path.
    assert await redis_sink.health_check() is True
    await redis_sink._reset_connection("test", ConnectionError("boom"))

    # Buffer some events while disconnected.
    for i in range(10):
        redis_sink.buffer_event(topic, f"reconnect-{i}")

    assert redis_sink.get_buffer_stats()["buffer_size"] == 10

    # Reconnect triggers an async drain of the failed buffer. Publish forces
    # a reconnect; then explicitly drain as well. The reconnect-scheduled
    # background drain races the explicit one: whichever wins snapshot-clears
    # the buffer, and the loser no-ops on the now-empty buffer — possibly while
    # the winner's pipeline is still in flight. So poll until the drained
    # events land instead of asserting a fixed count at a fixed moment.
    assert await redis_sink.publish(topic, "trigger") is True
    await redis_sink._do_drain()

    # 10 buffered + 1 trigger publish, once whichever drain won completes.
    expected = 11
    deadline = asyncio.get_running_loop().time() + 5.0
    while (
        redis_sink.get_buffer_stats()["buffer_size"] != 0 or await redis_probe.xlen(topic) != expected
    ) and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)

    assert redis_sink.get_buffer_stats()["buffer_size"] == 0
    assert await redis_probe.xlen(topic) == expected


async def test_redis_cache_set_nx_module_importable():
    """Guard: RedisCache is importable and exposes set_nx (used for dedup)."""
    assert hasattr(RedisCache, "set_nx")
