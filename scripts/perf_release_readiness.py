"""Release-readiness helper for perf config promotion with explicit diffs.

Usage:
    python scripts/perf_release_readiness.py
    python scripts/perf_release_readiness.py --apply
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-budgets-file", default=".perf/perf_budgets.active.json")
    parser.add_argument("--active-baseline-file", default=".perf/perf_baseline.active.json")
    parser.add_argument("--target-budgets-file", default="config/perf_budgets.json")
    parser.add_argument("--target-baseline-file", default="config/perf_baseline.json")
    parser.add_argument("--report-file", default=None)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json in '{path}': {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"invalid json in '{path}': expected top-level object")
    return data


def _render_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _diff_text(
    current_label: str,
    next_label: str,
    current_payload: dict[str, Any],
    next_payload: dict[str, Any],
) -> str:
    current_text = _render_json(current_payload).splitlines(keepends=True)
    next_text = _render_json(next_payload).splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            current_text,
            next_text,
            fromfile=current_label,
            tofile=next_label,
        )
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_render_json(payload))


def _build_report(
    apply_mode: bool,
    promoted: bool,
    budgets_diff: str,
    baseline_diff: str,
    target_budgets_path: Path,
    target_baseline_path: Path,
) -> str:
    budgets_changed = bool(budgets_diff)
    baseline_changed = bool(baseline_diff)
    lines = [
        "# Perf Release Readiness Report",
        "",
        f"- Apply mode: `{'yes' if apply_mode else 'no'}`",
        f"- Promotion performed: `{'yes' if promoted else 'no'}`",
        f"- Budgets changed: `{'yes' if budgets_changed else 'no'}`",
        f"- Baseline changed: `{'yes' if baseline_changed else 'no'}`",
        "",
        "## Diff: Budgets",
        "",
    ]
    if budgets_changed:
        lines.extend(["```diff", budgets_diff.rstrip(), "```"])
    else:
        lines.append("No changes.")

    lines.extend(["", "## Diff: Baseline", ""])
    if baseline_changed:
        lines.extend(["```diff", baseline_diff.rstrip(), "```"])
    else:
        lines.append("No changes.")

    lines.extend(
        [
            "",
            "## Next Command",
            "",
            f"- Run `git diff -- {target_budgets_path} {target_baseline_path}` to verify final tracked changes.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    active_budgets_path = Path(args.active_budgets_file)
    active_baseline_path = Path(args.active_baseline_file)
    target_budgets_path = Path(args.target_budgets_file)
    target_baseline_path = Path(args.target_baseline_file)

    try:
        active_budgets = _load_json(active_budgets_path)
        active_baseline = _load_json(active_baseline_path)
        current_budgets = _load_json(target_budgets_path)
        current_baseline = _load_json(target_baseline_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"perf_release_readiness_failed: {exc}", file=sys.stderr)
        return 1

    budgets_diff = _diff_text(
        current_label=str(target_budgets_path),
        next_label=str(active_budgets_path),
        current_payload=current_budgets,
        next_payload=active_budgets,
    )
    baseline_diff = _diff_text(
        current_label=str(target_baseline_path),
        next_label=str(active_baseline_path),
        current_payload=current_baseline,
        next_payload=active_baseline,
    )

    promoted = False
    if args.apply and (budgets_diff or baseline_diff):
        _write_json(target_budgets_path, active_budgets)
        _write_json(target_baseline_path, active_baseline)
        promoted = True

    if budgets_diff:
        print("=== Budgets Diff ===")
        print(budgets_diff.rstrip())
    else:
        print("=== Budgets Diff ===")
        print("No changes.")

    if baseline_diff:
        print("=== Baseline Diff ===")
        print(baseline_diff.rstrip())
    else:
        print("=== Baseline Diff ===")
        print("No changes.")

    if promoted:
        print("perf_release_readiness_promoted: updated target config files from active perf files")
    else:
        print("perf_release_readiness_checked: no promotion applied")

    if args.report_file:
        report = _build_report(
            apply_mode=bool(args.apply),
            promoted=promoted,
            budgets_diff=budgets_diff,
            baseline_diff=baseline_diff,
            target_budgets_path=target_budgets_path,
            target_baseline_path=target_baseline_path,
        )
        report_path = Path(args.report_file)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report)
        print(f"perf_release_readiness_report_written: {report_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
