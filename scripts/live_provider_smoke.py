#!/usr/bin/env python3
"""Run live provider smoke checks and emit a markdown report.

This script is intentionally lightweight. It checks:
1) Provider health_check() status
2) One small live call per provider
3) Basic rate-limit signal detection from error text
"""

from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

from gateway.core.provider import DataProvider, HealthStatus
from gateway.core.registry import ProviderRegistry


@dataclass
class SmokeResult:
    provider: str
    credential_env: str | None
    credential_present: bool
    loaded: bool
    health_ok: bool
    health_error: str | None
    sample_ok: bool
    sample_status: str
    sample_detail: str


def _is_rate_limited(message: str) -> bool:
    text = message.lower()
    return "rate limit" in text or "too many request" in text or "http 429" in text


async def _run_sample_call(provider_name: str, provider: DataProvider) -> tuple[bool, str, str]:
    """Run one small call to verify non-stub behavior."""
    try:
        if provider_name == "alpaca":
            quotes = await provider.get_quotes(["AAPL"])
            if not quotes:
                return False, "empty", "alpaca quotes returned no data"
            return True, "ok", "alpaca quote returned data"

        if provider_name == "finnhub":
            quote = await provider.get_quote("AAPL")  # type: ignore[attr-defined]
            if quote is None:
                return False, "empty", "finnhub quote returned no data"
            return True, "ok", "finnhub quote returned data"

        if provider_name == "alphavantage":
            quote = await provider.get_quote("IBM")  # type: ignore[attr-defined]
            if quote is None:
                return False, "empty", "alphavantage quote returned no data"
            return True, "ok", "alphavantage quote returned data"

        if provider_name == "unusual_whales":
            tide = await provider.get_market_tide()  # type: ignore[attr-defined]
            if not tide:
                return False, "empty", "unusual_whales market tide returned no data"
            return True, "ok", "unusual_whales market tide returned data"

        if provider_name == "sec":
            company = await provider.get_company_by_ticker("AAPL")  # type: ignore[attr-defined]
            if not company:
                return False, "empty", "sec company lookup returned no data"
            return True, "ok", "sec company lookup returned data"

        return False, "skipped", "no sample call configured"
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if _is_rate_limited(message):
            return False, "rate_limited", message
        return False, "error", message


async def run_smoke() -> list[SmokeResult]:
    load_dotenv(override=False)

    registry = ProviderRegistry()
    await registry.load_from_config(Path("config/providers.yaml"))

    env_map = {
        "alpaca": "APCA_API_KEY_ID",
        "finnhub": "FINNHUB_API_KEY",
        "alphavantage": "ALPHAVANTAGE_API_KEY",
        "unusual_whales": "UNUSUAL_WHALES_API_KEY",
        "sec": "SEC_USER_AGENT",
    }
    providers = ["alpaca", "finnhub", "alphavantage", "unusual_whales", "sec"]
    results: list[SmokeResult] = []

    try:
        for name in providers:
            provider = registry.get(name)
            env_name = env_map.get(name)
            env_val = os.environ.get(env_name, "") if env_name else ""
            credential_present = bool(env_val)

            if not provider:
                results.append(
                    SmokeResult(
                        provider=name,
                        credential_env=env_name,
                        credential_present=credential_present,
                        loaded=False,
                        health_ok=False,
                        health_error="provider not loaded",
                        sample_ok=False,
                        sample_status="skipped",
                        sample_detail="provider not loaded",
                    )
                )
                continue

            health: HealthStatus = await provider.health_check()
            sample_ok, sample_status, sample_detail = await _run_sample_call(name, provider)

            results.append(
                SmokeResult(
                    provider=name,
                    credential_env=env_name,
                    credential_present=credential_present,
                    loaded=True,
                    health_ok=health.healthy,
                    health_error=health.error,
                    sample_ok=sample_ok,
                    sample_status=sample_status,
                    sample_detail=sample_detail,
                )
            )
    finally:
        await registry.shutdown()

    return results


def _render_report(results: list[SmokeResult]) -> str:
    timestamp = datetime.now(UTC).isoformat()
    lines = [
        "# Live Provider Smoke Report",
        "",
        f"Generated: {timestamp}",
        "",
        "## Scope",
        "",
        "- Providers checked: alpaca, finnhub, alphavantage, unusual_whales, sec",
        "- Checks: provider `health_check()` + one minimal sample live call",
        "",
        "## Results",
        "",
        "| Provider | Credential Env | Credential Present | Loaded | Health | Sample | Notes |",
        "|---|---|---:|---:|---|---|---|",
    ]

    for item in results:
        health = "ok" if item.health_ok else "fail"
        sample = "ok" if item.sample_ok else item.sample_status
        notes = item.sample_detail
        if item.health_error:
            notes = f"{notes}; health_error={item.health_error}"
        lines.append(
            f"| `{item.provider}` | `{item.credential_env or '-'}` | "
            f"{'yes' if item.credential_present else 'no'} | "
            f"{'yes' if item.loaded else 'no'} | `{health}` | `{sample}` | {notes} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `ok`: live check passed",
            "- `rate_limited`: credentials are valid but provider throttled request",
            "- `empty`: request succeeded but returned no payload",
            "- `error`/`fail`: connectivity, auth, or API contract issue",
        ]
    )
    return "\n".join(lines) + "\n"


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Run live provider smoke checks.")
    parser.add_argument(
        "--out",
        default="LIVE_PROVIDER_SMOKE_REPORT.md",
        help="Path for markdown report output.",
    )
    args = parser.parse_args()

    results = await run_smoke()
    report = _render_report(results)
    out_path = Path(args.out)
    out_path.write_text(report)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
