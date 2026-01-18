"""Tests for subscription manager."""

import pytest


@pytest.fixture
def subscription_manager():
    """Fresh subscription manager for testing."""
    from gateway.core.multiplexer import SubscriptionManager

    return SubscriptionManager(grace_period_seconds=1)


@pytest.mark.asyncio
async def test_subscribe_new_symbols(subscription_manager):
    """New symbols are returned as newly subscribed."""
    newly_subscribed = await subscription_manager.subscribe(
        client_id="client1",
        symbols=["AAPL", "MSFT"],
        feed="bars",
    )
    assert set(newly_subscribed) == {"AAPL", "MSFT"}


@pytest.mark.asyncio
async def test_subscribe_existing_symbol(subscription_manager):
    """Existing symbols are not returned as newly subscribed."""
    await subscription_manager.subscribe("client1", ["AAPL"], "bars")
    newly_subscribed = await subscription_manager.subscribe("client2", ["AAPL"], "bars")
    assert newly_subscribed == []


@pytest.mark.asyncio
async def test_unsubscribe_with_remaining_clients(subscription_manager):
    """Symbol stays subscribed if other clients remain."""
    await subscription_manager.subscribe("client1", ["AAPL"], "bars")
    await subscription_manager.subscribe("client2", ["AAPL"], "bars")

    pending = await subscription_manager.unsubscribe("client1", ["AAPL"], "bars")
    assert pending == []

    subscribers = subscription_manager.get_subscribers("AAPL", "bars")
    assert "client2" in subscribers


@pytest.mark.asyncio
async def test_unsubscribe_triggers_grace_period(subscription_manager):
    """Last client unsubscribe starts grace period."""
    await subscription_manager.subscribe("client1", ["AAPL"], "bars")
    pending = await subscription_manager.unsubscribe("client1", ["AAPL"], "bars")
    assert pending == ["AAPL"]


@pytest.mark.asyncio
async def test_resubscribe_cancels_grace_period(subscription_manager):
    """Resubscribe during grace period cancels removal."""
    await subscription_manager.subscribe("client1", ["AAPL"], "bars")
    await subscription_manager.unsubscribe("client1", ["AAPL"], "bars")

    # New client subscribes during grace period
    newly_subscribed = await subscription_manager.subscribe("client2", ["AAPL"], "bars")
    assert newly_subscribed == []

    subscribers = subscription_manager.get_subscribers("AAPL", "bars")
    assert "client2" in subscribers


@pytest.mark.asyncio
async def test_get_all_symbols(subscription_manager):
    """Get all subscribed symbols for feed."""
    await subscription_manager.subscribe("client1", ["AAPL", "MSFT"], "bars")
    await subscription_manager.subscribe("client1", ["GOOG"], "quotes")

    bars_symbols = subscription_manager.get_all_symbols("bars")
    assert set(bars_symbols) == {"AAPL", "MSFT"}

    quotes_symbols = subscription_manager.get_all_symbols("quotes")
    assert quotes_symbols == ["GOOG"]


@pytest.mark.asyncio
async def test_remove_client(subscription_manager):
    """Remove client from all subscriptions."""
    await subscription_manager.subscribe("client1", ["AAPL", "MSFT"], "bars")

    await subscription_manager.remove_client("client1")

    stats = subscription_manager.get_stats()
    assert stats["active_subscriptions"] == 0 or stats["pending_unsubscribes"] == 2


def test_get_stats(subscription_manager):
    """Get subscription statistics."""
    stats = subscription_manager.get_stats()
    assert "total_subscriptions" in stats
    assert "active_subscriptions" in stats
    assert "by_feed" in stats
