import sqlite3
from pathlib import Path

import pytest

from gateway.core.durable_outbox import (
    OutboxCapacityError,
    OutboxIntegrityError,
    SQLiteOutbox,
)


def _event(event_id: str, *, price: int = 100) -> dict[str, object]:
    return {"event_id": event_id, "feed": "trades", "price": price}


def test_outbox_uses_wal_and_full_synchronous_mode(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"

    with SQLiteOutbox(path), sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_admit_persists_events_in_sequence_order(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path) as outbox:
        assert outbox.admit("heber.live.trades", _event("evt-2")) is True
        assert outbox.admit("heber.live.trades", _event("evt-1")) is True

    with SQLiteOutbox(path) as reopened:
        pending = reopened.pending()

    assert [entry.event_id for entry in pending] == ["evt-2", "evt-1"]
    assert pending[0].sequence < pending[1].sequence


def test_exact_duplicate_is_a_no_op(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        event = _event("evt-1")
        assert outbox.admit("heber.live.trades", event) is True
        assert outbox.admit("heber.live.trades", event) is False

        assert [entry.event_id for entry in outbox.pending()] == ["evt-1"]


def test_same_topic_and_event_id_with_different_payload_fails_closed(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        outbox.admit("heber.live.trades", _event("evt-1", price=100))

        with pytest.raises(OutboxIntegrityError, match="evt-1"):
            outbox.admit("heber.live.trades", _event("evt-1", price=101))

        assert len(outbox.pending()) == 1


def test_same_event_id_may_be_admitted_to_a_different_topic(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        event = _event("evt-1")
        assert outbox.admit("heber.live.trades", event) is True
        assert outbox.admit("heber.backfill.trades", event) is True

        assert [entry.topic for entry in outbox.pending()] == [
            "heber.live.trades",
            "heber.backfill.trades",
        ]


def test_admit_many_commits_once_and_reports_exact_insertions(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        existing = _event("evt-existing")
        outbox.admit("heber.live.trades", existing)

        results = outbox.admit_many(
            [
                ("heber.live.trades", _event("evt-1")),
                ("heber.live.trades", existing),
                ("heber.live.trades", _event("evt-2")),
            ]
        )

        assert results == [True, False, True]
        assert [entry.event_id for entry in outbox.pending()] == [
            "evt-existing",
            "evt-1",
            "evt-2",
        ]


def test_admit_many_rolls_back_the_whole_batch_on_integrity_conflict(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        outbox.admit("heber.live.trades", _event("evt-existing"))

        with pytest.raises(OutboxIntegrityError, match="evt-existing"):
            outbox.admit_many(
                [
                    ("heber.live.trades", _event("evt-new")),
                    ("heber.live.trades", _event("evt-existing", price=101)),
                ]
            )

        assert [entry.event_id for entry in outbox.pending()] == ["evt-existing"]


def test_admit_requires_a_nonempty_event_id(tmp_path: Path) -> None:
    with (
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        pytest.raises(ValueError, match="event_id"),
    ):
        outbox.admit("heber.live.trades", {"feed": "trades"})


def test_acknowledge_deletes_only_the_selected_entry(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        outbox.admit("heber.live.trades", _event("evt-1"))
        outbox.admit("heber.live.trades", _event("evt-2"))
        first, second = outbox.pending()

        assert outbox.acknowledge(first.sequence) is True
        assert outbox.acknowledge(first.sequence) is False
        assert outbox.pending() == [second]


def test_record_failure_tracks_attempts_and_last_error(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        outbox.admit("heber.live.trades", _event("evt-1"))
        entry = outbox.pending()[0]

        assert outbox.record_failure(entry.sequence, "broker unavailable") is True
        assert outbox.record_failure(entry.sequence, "publish timed out") is True
        failed = outbox.pending()[0]

        assert failed.attempts == 2
        assert failed.last_error == "publish timed out"
        assert outbox.record_failure(999_999, "missing") is False


def test_pending_limit_preserves_oldest_first_order(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        for index in range(3):
            outbox.admit("heber.live.trades", _event(f"evt-{index}"))

        assert [entry.event_id for entry in outbox.pending(limit=2)] == ["evt-0", "evt-1"]
        with pytest.raises(ValueError, match="limit"):
            outbox.pending(limit=0)


def test_storage_size_reports_database_and_wal_files(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path) as outbox:
        outbox.admit("heber.live.trades", _event("evt-1"))
        size = outbox.storage_size()

        assert size.database_bytes == path.stat().st_size
        assert size.wal_bytes == Path(f"{path}-wal").stat().st_size
        assert size.total_bytes == size.database_bytes + size.wal_bytes


def test_capacity_limit_rejects_without_evicting_existing_rows(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    with SQLiteOutbox(path, max_bytes=1) as outbox:
        with pytest.raises(OutboxCapacityError):
            outbox.admit("heber:events", _event("evt-over-cap"))
        assert outbox.pending() == []


def test_acknowledging_a_row_releases_capacity_without_vacuum(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3", max_bytes=700) as outbox:
        outbox.admit("heber:events", _event("evt-1"))
        with pytest.raises(OutboxCapacityError):
            outbox.admit("heber:events", _event("evt-2"))

        assert outbox.acknowledge(outbox.pending()[0].sequence) is True
        assert outbox.admit("heber:events", _event("evt-2")) is True


def test_capacity_utilization_tracks_pending_backlog(tmp_path: Path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3", max_bytes=700) as outbox:
        assert outbox.capacity_utilization() == 0
        outbox.admit("heber:events", _event("evt-1"))
        assert 0.8 <= outbox.capacity_utilization() <= 1
