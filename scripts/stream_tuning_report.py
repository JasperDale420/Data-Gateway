"""Render stream tuning recommendations from a status snapshot JSON file.

Usage:
    python scripts/stream_tuning_report.py --status-file /path/to/status.json
    python scripts/stream_tuning_report.py --status-file snap1.json --status-file snap2.json
    python scripts/stream_tuning_report.py --status-url http://localhost:8000/api/v1/status --api-key gw_...
    python scripts/stream_tuning_report.py --status-url http://localhost:8000/api/v1/status --samples 6 --interval-seconds 5
    python scripts/stream_tuning_report.py --status-file /path/to/status.json --format json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--status-file",
        action="append",
        help="Path to status response JSON (repeat for multiple snapshots)",
    )
    source_group.add_argument("--status-url", help="Status endpoint URL")
    parser.add_argument("--api-key", default=None, help="Optional gateway API key for status URL")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Network timeout in seconds for --status-url fetches",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=1,
        help="Number of status-url samples to fetch and aggregate",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.0,
        help="Sleep interval between --status-url samples",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env file path to update with suggested env values",
    )
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


def _load_json_url(url: str, *, api_key: str | None, timeout_seconds: float) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-Gateway-Key"] = api_key
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=max(0.1, timeout_seconds)) as response:
        raw = response.read().decode("utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json from '{url}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid json from '{url}': expected top-level object")
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


def _aggregate_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    if not summaries:
        raise ValueError("no stream_tuning_summary samples to aggregate")

    severity_rank = {"healthy": 0, "warning": 1, "critical": 2}

    overall_level = "healthy"
    backpressure_level = "healthy"
    fanout_level = "healthy"
    level_counts = {"healthy": 0, "warning": 0, "critical": 0}

    recommendations: list[str] = []
    rec_seen: set[str] = set()
    suggested_env: dict[str, int] = {}

    def _max_level(current: str, candidate: str) -> str:
        return max(current, candidate, key=lambda level: severity_rank.get(level, 0))

    for summary in summaries:
        overall = str(summary.get("overall_level", "healthy"))
        backpressure = str(summary.get("backpressure_level", "healthy"))
        fanout = str(summary.get("fanout_level", "healthy"))

        overall_level = _max_level(overall_level, overall)
        backpressure_level = _max_level(backpressure_level, backpressure)
        fanout_level = _max_level(fanout_level, fanout)
        if overall in level_counts:
            level_counts[overall] += 1

        for item in summary.get("recommendations", []):
            if not isinstance(item, str):
                continue
            normalized = item.strip()
            if not normalized or normalized in rec_seen:
                continue
            rec_seen.add(normalized)
            recommendations.append(normalized)

        env_map = summary.get("suggested_env", {})
        if isinstance(env_map, dict):
            for key, value in env_map.items():
                try:
                    numeric = int(value)
                except (TypeError, ValueError):
                    continue
                if key not in suggested_env:
                    suggested_env[key] = numeric
                else:
                    suggested_env[key] = max(suggested_env[key], numeric)

    return {
        "overall_level": overall_level,
        "backpressure_level": backpressure_level,
        "fanout_level": fanout_level,
        "has_recommendations": bool(recommendations),
        "recommendations": recommendations,
        "suggested_env": suggested_env,
        "sample_count": len(summaries),
        "sample_level_counts": level_counts,
    }


def _write_env_file(path: Path, suggested_env: dict[str, Any]) -> int:
    """Upsert suggested env keys in dotenv format and return number of changed keys."""
    if not suggested_env:
        return 0

    existing_lines = path.read_text().splitlines() if path.exists() else []
    output_lines: list[str] = []
    seen_keys: set[str] = set()
    changed = 0

    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output_lines.append(line)
            continue
        key, _, value = line.partition("=")
        env_key = key.strip()
        if env_key in suggested_env:
            next_value = str(suggested_env[env_key])
            if value != next_value:
                changed += 1
            output_lines.append(f"{env_key}={next_value}")
            seen_keys.add(env_key)
        else:
            output_lines.append(line)

    for key in sorted(suggested_env):
        if key in seen_keys:
            continue
        output_lines.append(f"{key}={suggested_env[key]}")
        changed += 1

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(output_lines) + "\n")
    return changed


def _render_text(summary: dict[str, Any]) -> str:
    level = str(summary.get("overall_level", "unknown"))
    recommendations = summary.get("recommendations", [])
    suggested_env = summary.get("suggested_env", {})

    lines: list[str] = [f"overall_level={level}"]
    if "sample_count" in summary:
        lines.append(f"sample_count={summary.get('sample_count', 1)}")
    lines.append("")
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
        summaries: list[dict[str, Any]] = []
        if args.status_file:
            for file_path in args.status_file:
                payload = _load_json(Path(file_path))
                summaries.append(_extract_stream_tuning_summary(payload))
        else:
            sample_count = max(1, int(args.samples))
            interval_seconds = max(0.0, float(args.interval_seconds))
            for i in range(sample_count):
                payload = _load_json_url(
                    args.status_url,
                    api_key=args.api_key,
                    timeout_seconds=float(args.timeout_seconds),
                )
                summaries.append(_extract_stream_tuning_summary(payload))
                if i + 1 < sample_count and interval_seconds > 0:
                    time.sleep(interval_seconds)
        summary = _aggregate_summaries(summaries)
    except (FileNotFoundError, ValueError) as exc:
        print(f"stream_tuning_report_failed: {exc}", file=sys.stderr)
        return 1

    suggested_env = summary.get("suggested_env", {})
    env_changed = 0
    if args.env_file:
        env_changed = _write_env_file(Path(args.env_file), suggested_env)

    if args.format == "json":
        rendered = dict(summary)
        rendered["env_file_updated"] = bool(args.env_file)
        rendered["env_keys_changed"] = env_changed
        print(json.dumps(rendered, sort_keys=True))
    else:
        output = _render_text(summary)
        if args.env_file:
            output += f"\nenv_file={args.env_file}\nenv_file_updated=yes\nenv_keys_changed={env_changed}\n"
        print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
