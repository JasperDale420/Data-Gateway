#!/usr/bin/env python3
"""Generate provider endpoint contract markdown from live FastAPI routes."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from fastapi.routing import APIRoute

from gateway.main import app

PROVIDER_PREFIXES = {
    "unusual_whales": "/api/v1/uw",
    "finnhub": "/api/v1/finnhub",
    "alphavantage": "/api/v1/alphavantage",
    "sec": "/api/v1/sec",
    "yfinance": "/api/v1/yf",
}


def _collect_routes() -> dict[str, list[tuple[str, str, str]]]:
    routes: dict[str, list[tuple[str, str, str]]] = defaultdict(list)

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue

        path = route.path
        for provider, prefix in PROVIDER_PREFIXES.items():
            if path.startswith(prefix):
                methods = sorted(m for m in route.methods if m not in {"HEAD", "OPTIONS"})
                for method in methods:
                    routes[provider].append((method, path, route.name))
                break

    for provider in routes:
        routes[provider].sort(key=lambda item: (item[1], item[0]))

    return routes


def _render_markdown(routes: dict[str, list[tuple[str, str, str]]]) -> str:
    lines = [
        "# Provider Endpoint Contract",
        "",
        "This file is generated from live FastAPI routes.",
        "",
        "## Scope",
        "",
        "- `/api/v1/uw/*`",
        "- `/api/v1/finnhub/*`",
        "- `/api/v1/alphavantage/*`",
        "- `/api/v1/sec/*`",
        "- `/api/v1/yf/*`",
        "",
        "## Summary",
        "",
        "| Provider | Route Count |",
        "|---|---:|",
    ]
    for provider in PROVIDER_PREFIXES:
        lines.append(f"| `{provider}` | {len(routes.get(provider, []))} |")

    for provider in PROVIDER_PREFIXES:
        provider_routes = routes.get(provider, [])
        lines.append("")
        lines.append(f"## `{provider}`")
        lines.append("")
        if not provider_routes:
            lines.append("_No routes found._")
            continue
        lines.append("| Method | Path | Handler |")
        lines.append("|---|---|---|")
        for method, path, handler in provider_routes:
            lines.append(f"| `{method}` | `{path}` | `{handler}` |")

    lines.append("")
    lines.append("## Regeneration")
    lines.append("")
    lines.append("```bash")
    lines.append("python scripts/generate_provider_contract.py")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="PROVIDER_ENDPOINT_CONTRACT.md",
        help="Output markdown path (default: PROVIDER_ENDPOINT_CONTRACT.md)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if output is out-of-date.",
    )
    args = parser.parse_args()

    routes = _collect_routes()
    rendered = _render_markdown(routes)
    output_path = Path(args.output)

    if args.check:
        existing = output_path.read_text() if output_path.exists() else ""
        if existing != rendered:
            print(
                f"{output_path} is out-of-date. Regenerate with: python scripts/generate_provider_contract.py"
            )
            return 1
        print(f"{output_path} is up-to-date.")
        return 0

    output_path.write_text(rendered)
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
