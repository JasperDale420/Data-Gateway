import json
import logging
from datetime import UTC, datetime, timedelta

from gateway.core.uw_eod_state import UwEodRunState, UwEodStateStore


def test_eod_state_store_persists_completed_date_across_instances(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    store = UwEodStateStore(path, stale_after_seconds=3600)

    assert store.claim("2026-06-12") is True
    store.mark_completed("2026-06-12", totals={"greek_exposure": {"published": 1, "errors": 0}})

    reloaded = UwEodStateStore(path, stale_after_seconds=3600)
    assert reloaded.should_skip("2026-06-12") is True


def test_eod_state_claim_blocks_same_day_after_restart(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    store = UwEodStateStore(path, stale_after_seconds=3600)

    assert store.claim("2026-06-12") is True
    reloaded = UwEodStateStore(path, stale_after_seconds=3600)

    assert reloaded.claim("2026-06-12") is False


def test_eod_state_claim_allows_retry_after_stale_running_marker(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    stale_started_at = datetime.now(UTC) - timedelta(hours=2)
    path.write_text(
        UwEodRunState(
            trading_date="2026-06-12",
            status="running",
            started_at=stale_started_at.isoformat(),
            completed_at=None,
            totals={},
        ).model_dump_json()
    )

    store = UwEodStateStore(path, stale_after_seconds=60)

    assert store.claim("2026-06-12") is True


def test_eod_state_claim_lock_is_exclusive_across_store_instances(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    first = UwEodStateStore(path, stale_after_seconds=3600)
    second = UwEodStateStore(path, stale_after_seconds=3600)

    with first._claim_lock(blocking=True) as first_acquired:
        assert first_acquired is True
        with second._claim_lock(blocking=False) as second_acquired:
            assert second_acquired is False


def test_eod_state_invalid_json_is_treated_as_missing_and_logged(tmp_path, caplog):
    path = tmp_path / "uw_eod_state.json"
    path.write_text("{not valid json")
    store = UwEodStateStore(path, stale_after_seconds=3600)

    with caplog.at_level(logging.WARNING, logger="data-gateway"):
        assert store.should_skip("2026-06-12") is False

    records = [json.loads(record.getMessage()) for record in caplog.records if record.getMessage().startswith("{")]
    warnings = [record for record in records if record.get("message") == "uw_eod_state_read_failed"]
    assert warnings

    assert store.claim("2026-06-12") is True
