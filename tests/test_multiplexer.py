"""Tests for subscription manager."""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

import gateway.core.stream as stream_module
from gateway.core.auth import Client, ClientPermissions
from gateway.core.connections import ConnectionManager
from gateway.core.stream import AlpacaStreamType, StreamMultiplexer
from gateway.core.stream import SubscriptionManager as StreamSubscriptionManager


@pytest.fixture
def subscription_manager():
    """Fresh subscription manager for testing."""
    from gateway.core.stream import SubscriptionManager

    return SubscriptionManager()


def test_subscribe_new_symbols(subscription_manager):
    """New symbols are returned as newly subscribed."""
    new_bars, _, _, _ = subscription_manager.subscribe(
        client_id="client1",
        bars=["AAPL", "MSFT"],
    )
    assert new_bars == {"AAPL", "MSFT"}


def test_subscribe_existing_symbol(subscription_manager):
    """Existing symbols are not returned as newly subscribed."""
    subscription_manager.subscribe("client1", bars=["AAPL"])
    new_bars, _, _, _ = subscription_manager.subscribe("client2", bars=["AAPL"])
    assert new_bars == set()


def test_unsubscribe_with_remaining_clients(subscription_manager):
    """Symbol stays subscribed if other clients remain."""
    subscription_manager.subscribe("client1", bars=["AAPL"])
    subscription_manager.subscribe("client2", bars=["AAPL"])

    removed_bars, _, _, _ = subscription_manager.unsubscribe("client1", bars=["AAPL"])
    assert removed_bars == set()

    subscribers = subscription_manager.get_clients_for_symbol("AAPL", "bars")
    assert "client2" in subscribers


def test_unsubscribe_last_client_removes_symbol(subscription_manager):
    """Last client unsubscribe returns symbol as removed."""
    subscription_manager.subscribe("client1", bars=["AAPL"])
    removed_bars, _, _, _ = subscription_manager.unsubscribe("client1", bars=["AAPL"])
    assert removed_bars == {"AAPL"}


def test_resubscribe_after_removal(subscription_manager):
    """Resubscribe after last client unsubscribed re-adds symbol."""
    subscription_manager.subscribe("client1", bars=["AAPL"])
    subscription_manager.unsubscribe("client1", bars=["AAPL"])

    # New client subscribes — symbol is new again since it was fully removed
    new_bars, _, _, _ = subscription_manager.subscribe("client2", bars=["AAPL"])
    assert new_bars == {"AAPL"}

    subscribers = subscription_manager.get_clients_for_symbol("AAPL", "bars")
    assert "client2" in subscribers


def test_get_all_symbols(subscription_manager):
    """Get all subscribed symbols for feed via aggregate."""
    subscription_manager.subscribe("client1", bars=["AAPL", "MSFT"])
    subscription_manager.subscribe("client1", quotes=["GOOG"])

    bars_syms, quotes_syms, _, _ = subscription_manager.get_all_subscriptions()
    assert bars_syms == {"AAPL", "MSFT"}
    assert quotes_syms == {"GOOG"}


def test_remove_client(subscription_manager):
    """Remove client from all subscriptions."""
    subscription_manager.subscribe("client1", bars=["AAPL", "MSFT"])

    removed_bars, _, _, _ = subscription_manager.remove_client("client1")
    assert removed_bars == {"AAPL", "MSFT"}

    bars_syms, _, _, _ = subscription_manager.get_all_subscriptions()
    assert bars_syms == set()


def test_empty_manager_has_no_subscriptions(subscription_manager):
    """A fresh manager has no subscriptions."""
    bars, quotes, trades, news = subscription_manager.get_all_subscriptions()
    assert bars == set()
    assert quotes == set()
    assert trades == set()
    assert news == set()


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


@pytest.mark.asyncio
async def test_stream_multiplexer_reuses_cached_validator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_validator_calls = 0
    validation_calls = 0

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            nonlocal validation_calls
            validation_calls += 1
            return SimpleNamespace(valid=False, error_codes=["GW-E9999"])

    def _get_validator() -> _Validator:
        nonlocal get_validator_calls
        get_validator_calls += 1
        return _Validator()

    monkeypatch.setattr(stream_module, "get_validator", _get_validator)

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )
    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(
        Any,
        SimpleNamespace(
            subscriptions=SimpleNamespace(
                get_clients_for_symbol=lambda _symbol, _data_type: ["client-1"],
                get_clients_for_symbol_view=lambda _symbol, _data_type: ["client-1"],
            )
        ),
    )

    await multiplexer._handle_message(AlpacaStreamType.STOCKS_SIP, {"T": "b", "S": "AAPL"})
    await multiplexer._handle_message(AlpacaStreamType.STOCKS_SIP, {"T": "b", "S": "MSFT"})

    assert get_validator_calls == 1
    assert validation_calls == 2


@pytest.mark.asyncio
async def test_stream_multiplexer_skips_validation_without_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _get_validator():
        raise AssertionError("validator should not be resolved without an active connection")

    monkeypatch.setattr(stream_module, "get_validator", _get_validator)

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )

    await multiplexer._handle_message(AlpacaStreamType.STOCKS_SIP, {"T": "b", "S": "AAPL"})


@pytest.mark.asyncio
async def test_stream_multiplexer_skips_validation_without_subscribers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _get_validator():
        raise AssertionError("validator should not be resolved without downstream subscribers")

    monkeypatch.setattr(stream_module, "get_validator", _get_validator)

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )
    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(
        Any,
        SimpleNamespace(
            subscriptions=SimpleNamespace(
                get_clients_for_symbol=lambda _symbol, _data_type: [],
                get_clients_for_symbol_view=lambda _symbol, _data_type: [],
            )
        ),
    )

    await multiplexer._handle_message(AlpacaStreamType.STOCKS_SIP, {"T": "b", "S": "AAPL"})


@pytest.mark.asyncio
async def test_news_symbol_lookup_deduplicates_symbols() -> None:
    calls: list[str] = []

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            calls.append(symbol)
            return []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    class _Connection:
        def __init__(self) -> None:
            self.subscriptions = _Subscriptions()

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )
    multiplexer._connections[AlpacaStreamType.NEWS] = _Connection()  # type: ignore[assignment]

    await multiplexer._handle_message(
        AlpacaStreamType.NEWS,
        {"T": "n", "S": "AAPL", "symbols": ["AAPL", "AAPL", "MSFT"]},
    )

    assert calls == ["*", "AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_stream_multiplexer_single_client_fanout_skips_gather(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    async def _forbidden_gather(*_tasks):
        raise AssertionError("gather should not be used for single-client fanout")

    monkeypatch.setattr(stream_module.asyncio, "gather", _forbidden_gather)

    delivered: list[tuple[str, str]] = []

    async def _on_data(client_id: str, data_type: str, _message: dict) -> None:
        delivered.append((client_id, data_type))

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            if symbol == "AAPL":
                return ["client-1"]
            return []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    await multiplexer._handle_message(AlpacaStreamType.STOCKS_SIP, {"T": "b", "S": "AAPL"})

    assert delivered == [("client-1", "bars")]


@pytest.mark.asyncio
async def test_stream_multiplexer_broadcasts_to_authenticated_connection_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeWebSocket:
        def __init__(self) -> None:
            self.sent_bytes: list[bytes] = []

        async def accept(self) -> None:
            return

        async def send_text(self, _payload: str) -> None:
            raise AssertionError("expected bytes payload")

        async def send_bytes(self, payload: bytes) -> None:
            self.sent_bytes.append(payload)

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    connections = ConnectionManager()
    websocket = _FakeWebSocket()
    connection_id = "conn-1"
    client = Client(
        id="test-client",
        permissions=ClientPermissions(
            providers=["alpaca"],
            feeds=["bars"],
            max_symbols=10,
            rate_limit=60,
        ),
        enabled=True,
    )

    await connections.connect(connection_id, cast(Any, websocket))
    await connections.authenticate(connection_id, client)

    async def _subscribe(_bars=None, _quotes=None, _trades=None, _news=None) -> list[str]:
        return []

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=lambda *_args, **_kwargs: asyncio.sleep(0),
        lazy_connect=False,
        on_broadcast=connections.broadcast_to_connection_ids,
    )
    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(
        Any,
        SimpleNamespace(
            subscriptions=StreamSubscriptionManager(),
            subscribe=_subscribe,
        ),
    )

    subscribe_response = await multiplexer.client_subscribe(
        client_id=connection_id,
        stream_type=AlpacaStreamType.STOCKS_SIP,
        bars=["AAPL"],
    )

    assert subscribe_response["status"] == "ok"

    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {
            "T": "b",
            "S": "AAPL",
            "t": "2026-04-02T13:31:00Z",
            "o": 10.0,
            "h": 10.5,
            "l": 9.9,
            "c": 10.2,
            "v": 1000,
        },
    )

    assert websocket.sent_bytes


@pytest.mark.asyncio
async def test_lazy_connect_returns_false_while_stream_is_still_not_ready() -> None:
    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )
    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(
        Any,
        SimpleNamespace(
            is_connected=False,
            _running=True,
            _connected_event=asyncio.Event(),
        ),
    )

    connected = await multiplexer._ensure_connected(AlpacaStreamType.STOCKS_SIP)

    assert connected is False


def test_stream_subscription_manager_client_view_reuses_index_set() -> None:
    manager = StreamSubscriptionManager()
    manager.subscribe(client_id="client-1", bars=["AAPL"])

    clients_view = manager.get_clients_for_symbol_view("AAPL", "bars")

    assert clients_view == {"client-1"}
    assert isinstance(clients_view, frozenset)  # noqa: SLF001


def test_stream_subscription_manager_client_view_missing_symbol_is_empty() -> None:
    manager = StreamSubscriptionManager()

    clients_view = manager.get_clients_for_symbol_view("MSFT", "bars")

    assert tuple(clients_view) == ()
