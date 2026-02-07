from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def test_release_readiness_dry_run_reports_diff_without_writing(tmp_path: Path) -> None:
    active_budgets = tmp_path / ".perf" / "perf_budgets.active.json"
    active_baseline = tmp_path / ".perf" / "perf_baseline.active.json"
    target_budgets = tmp_path / "config" / "perf_budgets.json"
    target_baseline = tmp_path / "config" / "perf_baseline.json"
    report_file = tmp_path / "perf-release-report.md"

    _write_json(target_budgets, {"suite_max_seconds": 6.0, "tests": {"t::a": 1.0}})
    _write_json(target_baseline, {"suite_baseline_seconds": 1.2, "tests": {"t::a": 0.2}})
    _write_json(active_budgets, {"suite_max_seconds": 5.0, "tests": {"t::a": 0.8}})
    _write_json(active_baseline, {"suite_baseline_seconds": 1.1, "tests": {"t::a": 0.18}})

    cmd = [
        sys.executable,
        "scripts/perf_release_readiness.py",
        "--active-budgets-file",
        str(active_budgets),
        "--active-baseline-file",
        str(active_baseline),
        "--target-budgets-file",
        str(target_budgets),
        "--target-baseline-file",
        str(target_baseline),
        "--report-file",
        str(report_file),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    assert _read_json(target_budgets)["suite_max_seconds"] == 6.0
    assert _read_json(target_baseline)["suite_baseline_seconds"] == 1.2
    assert report_file.exists()
    report = report_file.read_text()
    assert "Diff: Budgets" in report
    assert "Diff: Baseline" in report
    assert "Promotion performed: `no`" in report


def test_release_readiness_apply_promotes_active_files(tmp_path: Path) -> None:
    active_budgets = tmp_path / ".perf" / "perf_budgets.active.json"
    active_baseline = tmp_path / ".perf" / "perf_baseline.active.json"
    target_budgets = tmp_path / "config" / "perf_budgets.json"
    target_baseline = tmp_path / "config" / "perf_baseline.json"

    _write_json(target_budgets, {"suite_max_seconds": 6.0, "tests": {"t::a": 1.0}})
    _write_json(target_baseline, {"suite_baseline_seconds": 1.2, "tests": {"t::a": 0.2}})
    _write_json(active_budgets, {"suite_max_seconds": 4.0, "tests": {"t::a": 0.7}})
    _write_json(active_baseline, {"suite_baseline_seconds": 1.0, "tests": {"t::a": 0.17}})

    cmd = [
        sys.executable,
        "scripts/perf_release_readiness.py",
        "--active-budgets-file",
        str(active_budgets),
        "--active-baseline-file",
        str(active_baseline),
        "--target-budgets-file",
        str(target_budgets),
        "--target-baseline-file",
        str(target_baseline),
        "--apply",
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    assert _read_json(target_budgets)["suite_max_seconds"] == 4.0
    assert _read_json(target_baseline)["suite_baseline_seconds"] == 1.0
    assert "perf_release_readiness_promoted" in completed.stdout
