"""The backfill driver must accept explicit --days for targeted recovery.

The tier3 date window is derived from the PIT universe's ``asof`` date
(2026-06-09), so incident recovery for later dates (e.g. the 2026-07-20/21
EOD eviction) is impossible without an explicit date list.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("uw_backfill_driver", REPO / "scripts" / "uw_backfill_driver.py")
assert _spec is not None and _spec.loader is not None
driver = importlib.util.module_from_spec(_spec)
sys.modules["uw_backfill_driver"] = driver
_spec.loader.exec_module(driver)


def _args(tmp_path: Path, **overrides) -> argparse.Namespace:
    base = {
        "tier": 3,
        "feeds": ["oi_change"],
        "universe": str(REPO / "config" / "uw_pit_universe.json"),
        "state_file": str(tmp_path / "progress.json"),
        "daily_budget": 100,
        "concurrency": 2,
        "top_n": 3,
        "depth_days": 90,
        "symbols_file": None,
        "dry_run": True,
        "days": None,
        "stream": None,
        "max_stream_len": 1_000_000,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_explicit_days_override_universe_asof_window(tmp_path, capsys):
    """--days limits tier3 work to exactly the given dates."""
    args = _args(tmp_path, days=["2026-07-20", "2026-07-21"])
    rc = asyncio.run(driver.run(args))
    out = capsys.readouterr().out
    assert rc == 0
    # 1 feed x 3 symbols x 2 explicit days = 6 units, all pending.
    assert "[dry-run] 6 pending / 6 total" in out


def test_default_window_still_derived_from_universe_asof(tmp_path, capsys):
    """Without --days the asof-derived window is unchanged (not 6 units)."""
    args = _args(tmp_path)
    rc = asyncio.run(driver.run(args))
    out = capsys.readouterr().out
    assert rc == 0
    assert "[dry-run] 6 pending" not in out
