"""Tests for key load balancer."""

import pytest


@pytest.fixture
def load_balancer():
    """Fresh load balancer for testing."""
    from gateway.core.balancer import KeyLoadBalancer

    lb = KeyLoadBalancer(cooldown_seconds=1, max_errors=3)
    lb.add_key("key1", "secret1")
    lb.add_key("key2", "secret2")
    return lb


@pytest.mark.asyncio
async def test_round_robin_selection(load_balancer):
    """Keys are selected in round-robin order."""
    key1 = await load_balancer.get_key()
    key2 = await load_balancer.get_key()
    key3 = await load_balancer.get_key()

    assert key1.key_id == "key1"
    assert key2.key_id == "key2"
    assert key3.key_id == "key1"


@pytest.mark.asyncio
async def test_skip_unavailable_key(load_balancer):
    """Unavailable keys are skipped."""
    key1 = await load_balancer.get_key()
    await load_balancer.record_rate_limit(key1, retry_after=100)

    key2 = await load_balancer.get_key()
    assert key2.key_id == "key2"

    key3 = await load_balancer.get_key()
    assert key3.key_id == "key2"


@pytest.mark.asyncio
async def test_all_keys_exhausted(load_balancer):
    """Returns None when all keys are on cooldown."""
    key1 = await load_balancer.get_key()
    await load_balancer.record_rate_limit(key1, retry_after=100)

    key2 = await load_balancer.get_key()
    await load_balancer.record_rate_limit(key2, retry_after=100)

    result = await load_balancer.get_key()
    assert result is None


@pytest.mark.asyncio
async def test_record_success_increments_count(load_balancer):
    """Recording success increments request count."""
    key = await load_balancer.get_key()
    assert key.request_count == 0

    await load_balancer.record_success(key)
    assert key.request_count == 1


def test_get_available_count(load_balancer):
    """Count of available keys."""
    assert load_balancer.get_available_count() == 2


def test_get_stats(load_balancer):
    """Get load balancer statistics."""
    stats = load_balancer.get_stats()
    assert stats["total_keys"] == 2
    assert stats["available_keys"] == 2
    assert len(stats["keys"]) == 2
