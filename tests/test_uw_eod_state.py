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
