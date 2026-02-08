"""Tests for subscription manager."""

import pytest

import gateway.core.stream as stream_module
from gateway.core.stream import StreamMultiplexer


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


@pytest.mark.asyncio
async def test_stream_multiplexer_applies_fanout_limits() -> None:
    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        fanout_max_inflight=7,
        fanout_batch_size=5,
    )

    assert multiplexer._fanout_max_inflight == 7
    assert multiplexer._fanout_client_batch_size == 5
    batches = list(multiplexer._iter_client_batches({"a", "b", "c", "d", "e", "f"}))
    assert len(batches) == 2
    assert len(batches[0]) == 5


@pytest.mark.asyncio
async def test_stream_multiplexer_clamps_invalid_fanout_limits() -> None:
    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        fanout_max_inflight=0,
        fanout_batch_size=0,
    )

    assert multiplexer._fanout_max_inflight == 1
    assert multiplexer._fanout_client_batch_size == 1


@pytest.mark.asyncio
async def test_stream_multiplexer_records_fanout_limits_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []

    def _set_limits(*, max_inflight: int, batch_size: int) -> None:
        calls.append((max_inflight, batch_size))

    monkeypatch.setattr(stream_module, "set_stream_fanout_limits_metrics", _set_limits)

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        fanout_max_inflight=9,
        fanout_batch_size=4,
    )

    assert calls == [(9, 4)]


@pytest.mark.asyncio
async def test_iter_client_batches_records_batch_size_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_sizes: list[int] = []

    def _record_batch_size(size: int) -> None:
        batch_sizes.append(size)

    monkeypatch.setattr(stream_module, "record_stream_fanout_batch_size", _record_batch_size)

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        fanout_max_inflight=8,
        fanout_batch_size=3,
    )

    batches = list(multiplexer._iter_client_batches({"a", "b", "c", "d", "e"}))

    assert len(batches) == 2
    assert sorted(batch_sizes) == [2, 3]


@pytest.mark.asyncio
async def test_iter_client_batches_is_lazy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_sizes: list[int] = []

    def _record_batch_size(size: int) -> None:
        batch_sizes.append(size)

    monkeypatch.setattr(stream_module, "record_stream_fanout_batch_size", _record_batch_size)

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        fanout_max_inflight=8,
        fanout_batch_size=3,
    )

    batch_iter = multiplexer._iter_client_batches({"a", "b", "c", "d", "e"})
    assert batch_sizes == []

    first_batch = next(batch_iter)
    assert len(first_batch) == 3
    assert batch_sizes == [3]

    remaining_batches = list(batch_iter)
    assert len(remaining_batches) == 1
    assert len(remaining_batches[0]) == 2
    assert batch_sizes == [3, 2]
