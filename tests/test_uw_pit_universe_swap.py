"""Tests for the PIT-universe EOD swap loader (gateway/core/uw_poller._load_pit_active).

A bad or missing universe file must never break gateway startup — it falls back to None.
"""

import json
from pathlib import Path

from gateway.core.uw_poller import _load_pit_active


def test_loads_active_symbols(tmp_path: Path) -> None:
    f = tmp_path / "u.json"
    f.write_text(json.dumps({"active": ["aapl", "MSFT", " nvda "], "backfill": []}))
    assert _load_pit_active(f) == ["AAPL", "MSFT", "NVDA"]


def test_missing_file_falls_back_to_none(tmp_path: Path) -> None:
    assert _load_pit_active(tmp_path / "nope.json") is None


def test_malformed_file_falls_back_to_none(tmp_path: Path) -> None:
    f = tmp_path / "bad.json"
    f.write_text("{not json")
    assert _load_pit_active(f) is None


def test_empty_active_falls_back_to_none(tmp_path: Path) -> None:
    f = tmp_path / "empty.json"
    f.write_text(json.dumps({"active": [], "backfill": []}))
    assert _load_pit_active(f) is None
