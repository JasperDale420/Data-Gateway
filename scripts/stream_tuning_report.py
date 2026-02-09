"""Render stream tuning recommendations from a status snapshot JSON file.

Usage:
    python scripts/stream_tuning_report.py --status-file /path/to/status.json
    python scripts/stream_tuning_report.py --status-file /path/to/status.json --format json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status-file", required=True, help="Path to status response JSON")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format",
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json in '{path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid json in '{path}': expected top-level object")
    return payload


def _extract_stream_tuning_summary(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        summary = data.get("stream_tuning_summary")
        if isinstance(summary, dict):
            return summary
    summary = payload.get("stream_tuning_summary")
    if isinstance(summary, dict):
        return summary
    raise ValueError("stream_tuning_summary not found in status payload")


def _render_text(summary: dict[str, Any]) -> str:
    level = str(summary.get("overall_level", "unknown"))
    recommendations = summary.get("recommendations", [])
    suggested_env = summary.get("suggested_env", {})

    lines: list[str] = [f"overall_level={level}", ""]
    lines.append("recommendations:")
    if isinstance(recommendations, list) and recommendations:
        for item in recommendations:
            if isinstance(item, str) and item.strip():
                lines.append(f"- {item.strip()}")
    else:
        lines.append("- (none)")

    lines.extend(["", "suggested_env_exports:"])
    if isinstance(suggested_env, dict) and suggested_env:
        for key in sorted(suggested_env):
            value = suggested_env[key]
            lines.append(f"export {key}={value}")
    else:
        lines.append("# none")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    try:
        payload = _load_json(Path(args.status_file))
        summary = _extract_stream_tuning_summary(payload)
    except (FileNotFoundError, ValueError) as exc:
        print(f"stream_tuning_report_failed: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(json.dumps(summary, sort_keys=True))
    else:
        print(_render_text(summary), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
