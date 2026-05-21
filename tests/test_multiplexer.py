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
            _start_lock=asyncio.Lock(),
        ),
    )

    connected = await multiplexer._ensure_connected(AlpacaStreamType.STOCKS_SIP)

    assert connected is False


@pytest.mark.asyncio
async def test_lazy_connect_does_not_spawn_duplicate_start_tasks_under_race() -> None:
    """Two concurrent subscribers waking up a dormant upstream must produce
    exactly ONE ``conn.start()`` task, not two.

    Regression — codex caught: ``_ensure_connected`` checked ``conn._running``
    but ``_running`` is only assigned inside ``conn.start()``, *after* the
    ``create_task`` returns. Two concurrent callers both observed
    ``_running == False`` and both scheduled ``conn.start()``, producing two
    competing reconnect loops on the same Alpaca WebSocket.
    """

    async def _on_data(_client_id: str, _data_type: str, _message: dict) -> None:
        return

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
    )

    start_call_count = 0
    start_release = asyncio.Event()

    class FakeConn:
        def __init__(self) -> None:
            self.is_connected = False
            self._running = False
            self._connected_event = asyncio.Event()
            self._start_lock = asyncio.Lock()

        async def start(self) -> None:
            nonlocal start_call_count
            start_call_count += 1
            self._running = True
            # Block until the test releases us so concurrent callers race
            # entirely on the "dormant restart" branch.
            await start_release.wait()
            # Simulate successful authentication.
            self.is_connected = True
            self._connected_event.set()

    fake = FakeConn()
    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, fake)

    # Two concurrent subscribers; create the tasks before awaiting so they
    # race in the same scheduler tick.
    t1 = asyncio.create_task(multiplexer._ensure_connected(AlpacaStreamType.STOCKS_SIP))
    t2 = asyncio.create_task(multiplexer._ensure_connected(AlpacaStreamType.STOCKS_SIP))

    # Give both tasks a chance to advance through the lock + dormant branch.
    # The first wins the lock and schedules start(); the second must see
    # _running == True under the lock and skip the create_task.
    for _ in range(50):
        await asyncio.sleep(0)
        if start_call_count >= 1:
            break

    # Release start() so both waiters can observe is_connected and return.
    start_release.set()
    results = await asyncio.wait_for(asyncio.gather(t1, t2), timeout=2.0)

    assert results == [True, True]
    assert start_call_count == 1, f"expected exactly one conn.start() under race, got {start_call_count}"


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


# ---------------------------------------------------------------------------
# Reconnect loop — regression test for the "double-connect" bug where a
# successful reconnect would immediately be torn down and re-established by
# the outer loop, doubling Alpaca connection churn and losing bars after
# every flap. Apr 15–17, 2026.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upstream_connect_and_run_does_not_double_connect_after_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single disconnect triggers exactly ONE reconnect, not two.

    Before the fix, `_reconnect_with_backoff` established a working WS and
    returned, then `_connect_and_run`'s outer loop called `connect()` again,
    tearing the fresh WS down to open another. This counted connect() calls
    across one disconnect event: we should see 2 (initial + reconnect), not 3.
    """
    import websockets

    from gateway.core.stream import AlpacaStreamType, UpstreamConnection

    async def _noop_on_message(_msg: dict) -> None:
        return

    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_noop_on_message,
        base_delay=0.0,  # no real backoff — speed up the test
        max_delay=0.0,
        max_retries=3,
    )

    connect_calls = 0
    auth_calls = 0

    async def _mock_connect() -> None:
        nonlocal connect_calls
        connect_calls += 1
        # Pretend the WS is live; the receive_loop below simulates its lifetime.
        conn._ws = cast(Any, SimpleNamespace(close=lambda *a, **kw: asyncio.sleep(0)))

    async def _mock_authenticate() -> None:
        nonlocal auth_calls
        auth_calls += 1
        conn._authenticated = True

    async def _mock_close_ws() -> None:
        conn._ws = None
        conn._authenticated = False
        conn._connected_event.clear()

    # First receive_loop raises ConnectionClosed (simulates a 1006 flap).
    # Second call blocks until we flip _running = False to end the test.
    receive_call = {"n": 0}
    stop_event = asyncio.Event()

    async def _mock_receive_loop() -> None:
        receive_call["n"] += 1
        if receive_call["n"] == 1:
            raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)
        # On the SECOND connect, hold the "connection" open until we stop.
        await stop_event.wait()

    monkeypatch.setattr(conn, "connect", _mock_connect)
    monkeypatch.setattr(conn, "authenticate", _mock_authenticate)
    monkeypatch.setattr(conn, "_close_ws", _mock_close_ws)
    monkeypatch.setattr(conn, "_receive_loop", _mock_receive_loop)

    # Drive the loop. It will: connect (1), receive_loop raises, backoff,
    # connect (2), receive_loop blocks on stop_event. We then stop it.
    conn._running = True
    task = asyncio.create_task(conn._connect_and_run())

    # Give the loop enough time to hit the second receive_loop.
    for _ in range(50):
        if receive_call["n"] >= 2:
            break
        await asyncio.sleep(0.01)

    assert receive_call["n"] == 2, "second receive_loop should have started"

    # Stop the loop cleanly.
    conn._running = False
    stop_event.set()
    await asyncio.wait_for(task, timeout=2.0)

    # One initial connect + one reconnect = 2 connect() calls. The old code
    # would produce 3 (initial + reconnect_with_backoff + outer loop's
    # redundant connect).
    assert connect_calls == 2, f"expected exactly 2 connect() calls, got {connect_calls}"
    assert auth_calls == 2, f"expected exactly 2 authenticate() calls, got {auth_calls}"


@pytest.mark.asyncio
async def test_upstream_connect_and_run_stops_on_non_recoverable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """'connection limit exceeded' halts the loop instead of retrying forever."""
    from gateway.core.stream import AlpacaStreamType, UpstreamConnection

    async def _noop_on_message(_msg: dict) -> None:
        return

    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_noop_on_message,
        base_delay=0.0,
        max_delay=0.0,
        max_retries=5,
    )

    async def _raising_connect() -> None:
        raise RuntimeError("connection limit exceeded")

    async def _noop_close() -> None:
        return

    monkeypatch.setattr(conn, "connect", _raising_connect)
    monkeypatch.setattr(conn, "_close_ws", _noop_close)

    conn._running = True
    await asyncio.wait_for(conn._connect_and_run(), timeout=2.0)

    assert conn._running is False


@pytest.mark.asyncio
async def test_upstream_connect_and_run_exhausts_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After `max_retries` transient failures, the loop gives up and goes dormant."""
    from gateway.core.stream import AlpacaStreamType, UpstreamConnection

    async def _noop_on_message(_msg: dict) -> None:
        return

    conn = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_noop_on_message,
        base_delay=0.0,
        max_delay=0.0,
        max_retries=3,
    )

    connect_calls = 0

    async def _raising_connect() -> None:
        nonlocal connect_calls
        connect_calls += 1
        raise RuntimeError("transient network error")

    async def _noop_close() -> None:
        return

    monkeypatch.setattr(conn, "connect", _raising_connect)
    monkeypatch.setattr(conn, "_close_ws", _noop_close)

    conn._running = True
    await asyncio.wait_for(conn._connect_and_run(), timeout=2.0)

    # Initial connect + max_retries reconnect attempts = max_retries + 1
    assert connect_calls == conn.max_retries + 1
    assert conn._running is False


# ---------------------------------------------------------------------------
# on_envelope: fires ONCE per envelope, regardless of fanout path.
#
# Before 2026-05-21, per-envelope side-effects (sink publish to Heber) lived
# inside on_data, which is only invoked by the FALLBACK fanout path. The
# production multiplexer is wired with on_broadcast=connections.broadcast_
# to_connection_ids which takes the fast-path and never calls on_data —
# silently bypassing the sink for ALL streaming events. These tests pin the
# fix: on_envelope must fire exactly once per envelope independent of which
# fanout path is active, and independent of how many clients are subscribed.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_envelope_fires_once_on_broadcast_fast_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When on_broadcast is set (production wiring), on_envelope must STILL
    fire exactly once per envelope. Regression for the streaming-sink
    bypass discovered 2026-05-21."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    envelope_calls: list[dict] = []
    broadcast_calls: list[tuple] = []
    on_data_calls: list[tuple] = []

    async def _on_envelope(envelope: dict) -> None:
        envelope_calls.append(envelope)

    async def _on_broadcast(payload, client_ids) -> int:
        broadcast_calls.append((payload, list(client_ids)))
        return len(client_ids)

    async def _on_data(client_id: str, data_type: str, envelope: dict) -> None:
        on_data_calls.append((client_id, data_type, envelope))

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=False,
        on_broadcast=_on_broadcast,
        on_envelope=_on_envelope,
    )

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return ["c-1", "c-2", "c-3"] if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-05-21T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    # Critical invariants:
    assert len(envelope_calls) == 1, (
        f"on_envelope must fire EXACTLY ONCE per envelope on the broadcast "
        f"fast-path, got {len(envelope_calls)}. This is the streaming-sink "
        f"bypass regression — production has on_broadcast set."
    )
    assert len(broadcast_calls) == 1, "broadcast should fire exactly once"
    # Multiplexer dedupes subscribers via set internally, so target list order is
    # implementation-defined — assert membership, not order.
    assert sorted(broadcast_calls[0][1]) == ["c-1", "c-2", "c-3"], "broadcast should target all subscribers"
    assert on_data_calls == [], "on_data must NOT be called on the broadcast fast-path"


@pytest.mark.asyncio
async def test_on_envelope_fires_once_on_fallback_fanout_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When on_broadcast is None (test wiring, no ConnectionManager
    optimization), on_envelope still fires exactly once per envelope —
    NOT once per client subscriber. Pins the invariant that on_envelope is
    per-envelope, not per-delivery."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    envelope_calls: list[dict] = []
    on_data_calls: list[tuple] = []

    async def _on_envelope(envelope: dict) -> None:
        envelope_calls.append(envelope)

    async def _on_data(client_id: str, data_type: str, envelope: dict) -> None:
        on_data_calls.append((client_id, data_type, envelope))

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=False,
        on_broadcast=None,  # fallback fanout path
        on_envelope=_on_envelope,
    )

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return ["c-1", "c-2", "c-3"] if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-05-21T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    assert len(envelope_calls) == 1, (
        f"on_envelope must fire exactly once per envelope, not per delivered "
        f"client (got {len(envelope_calls)} for 3 subscribers)."
    )
    assert len(on_data_calls) == 3, "on_data fires once per subscribed client in the fallback path"


@pytest.mark.asyncio
async def test_on_envelope_failure_does_not_block_fanout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sink publish failure inside on_envelope must NOT swallow the
    upstream message or prevent client fanout. Pins the invariant that
    on_envelope is a side-effect, not a gate."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    broadcast_calls: list[tuple] = []

    async def _failing_on_envelope(_envelope: dict) -> None:
        raise RuntimeError("simulated sink failure")

    async def _on_broadcast(payload, client_ids) -> int:
        broadcast_calls.append((payload, list(client_ids)))
        return len(client_ids)

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=lambda *_a, **_kw: asyncio.sleep(0),
        lazy_connect=False,
        on_broadcast=_on_broadcast,
        on_envelope=_failing_on_envelope,
    )

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return ["c-1"] if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    # Should not raise — on_envelope failure is logged and swallowed.
    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-05-21T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    assert len(broadcast_calls) == 1, "fanout must still happen even when on_envelope raises"


@pytest.mark.asyncio
async def test_on_envelope_fires_with_zero_subscribed_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression — codex caught: when no client is subscribed to a symbol
    (race window: Alpaca delivering an in-flight message as the last
    subscriber unsubscribes), the message still represents real upstream
    data and must reach Heber. The empty-clients fast-out only skips the
    FANOUT cost, not the on_envelope sink dispatch."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    envelope_calls: list[dict] = []
    broadcast_calls: list = []

    async def _on_envelope(envelope: dict) -> None:
        envelope_calls.append(envelope)

    async def _on_broadcast(payload, client_ids) -> int:
        broadcast_calls.append((payload, list(client_ids)))
        return len(client_ids)

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=lambda *_a, **_kw: asyncio.sleep(0),
        lazy_connect=False,
        on_broadcast=_on_broadcast,
        on_envelope=_on_envelope,
    )

    class _NoSubscribers:
        def get_clients_for_symbol(self, _symbol: str, _data_type: str) -> list[str]:
            return []

        def get_clients_for_symbol_view(self, _symbol: str, _data_type: str) -> list[str]:
            return []

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_NoSubscribers()))

    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-05-21T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    assert len(envelope_calls) == 1, (
        "on_envelope MUST fire even with zero subscribed clients — the message "
        "still represents real upstream data that Heber needs. Dropping it on "
        "empty-clients would re-introduce a streaming-sink bypass."
    )
    assert broadcast_calls == [], "fanout should be skipped when no clients are subscribed"


@pytest.mark.asyncio
async def test_handle_message_with_no_on_envelope_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When on_envelope is None (e.g. unit tests or legacy callers), the
    multiplexer must no-op gracefully on the per-envelope hook — neither
    raising nor failing fanout."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    broadcast_calls: list = []

    async def _on_broadcast(payload, client_ids) -> int:
        broadcast_calls.append((payload, list(client_ids)))
        return len(client_ids)

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=lambda *_a, **_kw: asyncio.sleep(0),
        lazy_connect=False,
        on_broadcast=_on_broadcast,
        on_envelope=None,  # explicit None — should no-op
    )

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return ["c-1"] if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    # Should not raise — None callback must short-circuit.
    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-05-21T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    assert len(broadcast_calls) == 1, "fanout should proceed normally when on_envelope is None"


@pytest.mark.asyncio
async def test_on_envelope_cancellation_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CancelledError raised inside on_envelope must propagate up so shutdown
    semantics work — not be swallowed by the catch-all exception handler."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    async def _cancelling_on_envelope(_envelope: dict) -> None:
        raise asyncio.CancelledError()

    async def _no_op_broadcast(_payload, _client_ids) -> int:
        return 0

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=lambda *_a, **_kw: asyncio.sleep(0),
        lazy_connect=False,
        on_broadcast=_no_op_broadcast,
        on_envelope=_cancelling_on_envelope,
    )

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return ["c-1"] if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    with pytest.raises(asyncio.CancelledError):
        await multiplexer._handle_message(
            AlpacaStreamType.STOCKS_SIP,
            {"T": "b", "S": "AAPL", "t": "2026-05-21T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
        )
