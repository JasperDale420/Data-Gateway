"""Resumable, budget-metered UnusualWhales historical backfill over the Atlas PIT universe.

Runs as its OWN process with its own UW provider + Redis sink, so backfill load never
touches the live capital-adjacent gateway event loop. Publishes EventEnvelopes to a
DEDICATED ``heber:events:backfill`` stream (not the live ``heber:events``), consumed by a
separate Heber ``heber-backfill-consumer`` — so a multi-year backfill can never fill the
live stream to its MAXLEN and evict un-read live events. Override with ``--stream`` /
``HEBER_BACKFILL_STREAM``. Heber ingests backfill records identically (same EventEnvelope).

ONLY backfills feeds that are already proven end-to-end (the EOD poller publishes them and
Heber has a Silver normalizer). Adding a NEW feed requires a gateway FEED_UNIQUE_FIELDS entry
+ a Heber normalizer + a parity test first — otherwise records silently drop at Bronze→Silver.

Tiers:
  tier1  cheap, ~1 call/ticker, full multi-year series. Run once over the whole universe.
  tier3  per-day snapshot feeds (~1 call/trading-day/ticker). Sampled: top-N symbols, N days.

OPERATIONAL SAFETY:
  - Backfill now publishes to the DEDICATED ``heber:events:backfill`` stream, so it can no
    longer evict live events from ``heber:events`` — the live stream and the backfill stream
    have independent MAXLEN caps and independent Heber consumers. --max-stream-len caps the
    backfill stream only; if a huge backfill outruns the ``heber-backfill-consumer`` it trims
    re-runnable BACKFILL data, never live capital-adjacent events.
    DEPLOY ORDERING: the Heber ``heber-backfill-consumer`` must be running before this driver
    publishes, else backfill records land in a stream nobody reads and eventually trim.
  - Shares the 40k/day UW quota with the live gateway (EOD ~8k + flow/darkpool/tide ~5k).
    Keep --daily-budget so EOD + live + backfill stay under 40k, and avoid the live EOD window.

Usage:
    set -a; source .env; set +a
    uv run python scripts/uw_backfill_driver.py --tier 1                    # cheap full-universe
    uv run python scripts/uw_backfill_driver.py --tier 1 --dry-run
    uv run python scripts/uw_backfill_driver.py --tier 3 --top-n 200 --depth-days 90
    uv run python scripts/uw_backfill_driver.py --self-check                # no network
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
DEFAULT_UNIVERSE = REPO / "config" / "uw_pit_universe.json"
DEFAULT_STATE = REPO / "state" / "uw_backfill_progress.json"
# Dedicated backfill stream — NOT the live "heber:events" — so bulk history can never fill the
# live stream to MAXLEN and evict un-read live events. Override via --stream / HEBER_BACKFILL_STREAM.
DEFAULT_BACKFILL_STREAM = "heber:events:backfill"


def load_dotenv(path: Path = REPO / ".env") -> None:
    """Populate os.environ from .env for keys not already set (UW key, sink URL)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        if k and k not in os.environ:
            os.environ[k] = v.strip().strip('"').strip("'")


# ── Feed registry (PROVEN feeds only — must exist in gateway EOD publish + Heber Silver) ──


@dataclass(frozen=True)
class Feed:
    name: str  # envelope feed string (must match the gateway/Heber contract exactly)
    tier: int
    fetch: Callable[[Any, str, str | None], Any]  # (provider, symbol, date_str|None) -> awaitable list
    etf_only: bool = False


def _feeds() -> dict[str, Feed]:
    # tier1: one call returns the whole series (date arg ignored). greek_exposure needs
    # timeframe=3Y — date alone returns empty (verified 2026-07-03).
    t1 = [
        Feed("greek_exposure", 1, lambda p, s, d: p.get_greek_exposure(s, timeframe="3Y")),
        Feed("ftds", 1, lambda p, s, d: p.get_ftds(s)),
        # short_volume carries the daily short-volume+ratio history. NOT short_interest —
        # that endpoint is a single latest snapshot, empty on this UW tier (per CHANGELOG).
        Feed("short_volume", 1, lambda p, s, d: p.get_short_volume(s)),
        Feed("earnings", 1, lambda p, s, d: p.get_earnings_ticker(s)),
        Feed("congress_trades", 1, lambda p, s, d: p.get_congress_trades(symbol=s, limit=200)),
        Feed("insider_trades", 1, lambda p, s, d: p.get_insiders(symbol=s, limit=200)),
        # Company financial statements: one call returns the full quarterly series
        # (~102 rows, 2005→now). ts_event is anchored to fiscal_date_ending by wrap_event.
        Feed("income_statement", 1, lambda p, s, d: p.get_income_statements(s)),
        Feed("balance_sheet", 1, lambda p, s, d: p.get_balance_sheets(s)),
        Feed("cash_flow", 1, lambda p, s, d: p.get_cash_flows(s)),
    ]
    # tier3: one call per trading day.
    # oi_change + historic_option_volume were previously excluded for event_id bugs; both are
    # now fixed (this branch cherry-picks the DG fix, and oi_change also needs empire-schemas'
    # NormalizedOIChange.option_symbol — see the PR's cross-repo dependency note).
    t3 = [
        Feed("oi_change", 3, lambda p, s, d: p.get_oi_change(s, date_str=d)),
        Feed("historic_option_volume", 3, lambda p, s, d: p.get_historic_option_volume(s, date_str=d)),
        Feed("darkpool", 3, lambda p, s, d: p.get_darkpool_ticker(s, date_str=d, limit=500)),
    ]
    return {f.name: f for f in t1 + t3}


FEEDS = _feeds()


# ── universe / state ──


def load_universe(path: Path) -> tuple[list[dict], list[str]]:
    d = json.loads(path.read_text())
    return d["backfill"], d["active"]


def trading_days(start: dt.date, end: dt.date) -> list[str]:
    # ponytail: weekday filter only, no holiday calendar. A holiday just yields an empty
    # response that gets marked done — a few wasted calls, not wrong data. Wire an NYSE
    # calendar here if the wasted-call rate ever matters.
    out, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out


class Progress:
    def __init__(self, path: Path):
        self.path = path
        self.done: set[str] = set()
        if path.exists():
            self.done = set(json.loads(path.read_text()).get("done", []))
        self._dirty = 0

    def has(self, key: str) -> bool:
        return key in self.done

    def mark(self, key: str) -> None:
        self.done.add(key)
        self._dirty += 1
        if self._dirty >= 50:
            self.flush()

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"done": sorted(self.done)}))
        tmp.replace(self.path)  # atomic
        self._dirty = 0


# ── publish ──


# Congress/insider payloads carry no event-time field, so wrap_event would fall back to
# now() and every re-fetch would mint a fresh event_id → duplicate Bronze rows, AND would
# differ from what the live EOD poller publishes for the same trade. Stamp `timestamp` from
# the filing date exactly as uw_poller._stamp_filing_ts_event does, so event_ids match.
_FILING_TS_KEYS = {
    "congress_trades": ("filed_at_date", "transaction_date"),
    "insider_trades": ("filing_date", "transaction_date"),
}


def _to_envelopes(records: Any, symbol: str, feed: str) -> list[dict]:
    from gateway.core.envelope import wrap_event

    if not records:
        return []
    if not isinstance(records, list):
        records = [records]
    symbol = symbol.upper()  # Heber's equity-key validator requires ^equity:[A-Z0-9...]
    ts_keys = _FILING_TS_KEYS.get(feed)
    envs = []
    for item in records:
        if item is None:
            continue
        payload = item.model_dump() if hasattr(item, "model_dump") else dict(item)
        if ts_keys and not payload.get("timestamp"):
            for k in ts_keys:
                if payload.get(k):
                    payload["timestamp"] = payload[k]
                    break
        # Force equity keys for these per-underlying feeds — several carry expiry/strike
        # fields that _infer_instrument_type would otherwise mis-key as option:{symbol}
        # (no OCC suffix) and Heber would drop 100% at Bronze→Silver.
        envs.append(
            wrap_event(
                event=payload,
                provider="unusual_whales",
                feed=feed,
                source="rest",
                symbol_override=symbol,
                instrument_type_override="equity",
                instrument_key_override=f"equity:{symbol}",
            )
        )
    return envs


async def run(args: argparse.Namespace) -> int:
    backfill, active = load_universe(Path(args.universe))
    selected = [FEEDS[n] for n in args.feeds] if args.feeds else [f for f in FEEDS.values() if f.tier == args.tier]
    prog = Progress(Path(args.state_file))

    # tier3 works a liquidity-sampled subset. The universe file is not liquidity-ranked, so
    # --top-n truncates the (alphabetical) active list unless --symbols-file is given. Logged,
    # never silent.
    if args.tier == 3:
        if args.symbols_file:
            syms = [s.strip().upper() for s in Path(args.symbols_file).read_text().split() if s.strip()]
        else:
            syms = active[: args.top_n]
            print(
                f"[warn] tier3 subset = first {len(syms)} of alphabetical active list, NOT "
                f"liquidity-ranked. Pass --symbols-file for a ranked subset.",
                file=sys.stderr,
            )
        end = dt.date.fromisoformat(active_asof(Path(args.universe)))
        days = trading_days(end - dt.timedelta(days=args.depth_days), end)
        work = [(s, f, day) for f in selected for s in syms for day in days]
    else:
        work = [(row["symbol"], f, None) for f in selected for row in backfill]

    total = len(work)
    print(f"tier{args.tier}: {len(selected)} feeds x work = {total} units; budget {args.daily_budget} calls")

    if args.dry_run:
        pending = sum(1 for s, f, d in work if not prog.has(_key(s, f, d)))
        print(f"[dry-run] {pending} pending / {total} total units (rest already done)")
        return 0

    from gateway.core.redis_sink import RedisStreamsSink
    from gateway.providers.uw import UnusualWhalesProvider

    load_dotenv()
    provider = UnusualWhalesProvider()
    await provider.initialize(
        {"api_key_env": "UNUSUAL_WHALES_API_KEY", "max_inflight_calls": args.concurrency}  # pragma: allowlist secret
    )
    if not getattr(provider, "_initialized", False):
        print("[fatal] UW provider failed to initialize (missing UNUSUAL_WHALES_API_KEY?)", file=sys.stderr)
        return 2
    redis_url = os.environ.get("GATEWAY_DATA_SINK_REDIS_URL", "")
    if not redis_url:
        print("[fatal] GATEWAY_DATA_SINK_REDIS_URL not set", file=sys.stderr)
        return 2
    # Publish to the dedicated backfill stream. Precedence: --stream > HEBER_BACKFILL_STREAM > default.
    topic = args.stream or os.environ.get("HEBER_BACKFILL_STREAM") or DEFAULT_BACKFILL_STREAM
    print(f"publishing to stream: {topic}")
    # max_len caps the dedicated backfill stream only; live "heber:events" is untouched. If a huge
    # backfill outruns heber-backfill-consumer this trims re-runnable backfill data, never live events.
    sink = RedisStreamsSink(redis_url=redis_url, max_len=args.max_stream_len)

    calls = 0
    published = 0
    sem = asyncio.Semaphore(args.concurrency)
    stop = asyncio.Event()

    async def one(sym: str, feed: Feed, day: str | None) -> None:
        nonlocal calls, published
        key = _key(sym, feed, day)
        if prog.has(key) or stop.is_set():
            return
        async with sem:
            if stop.is_set():
                return
            try:
                records = await feed.fetch(provider, sym, day)
                calls += 1
                envs = _to_envelopes(records, sym, feed.name)
                if not envs:
                    prog.mark(key)  # genuine empty response — nothing to publish, done
                else:
                    results = await sink.publish_batch_results([(topic, e) for e in envs])
                    ok = sum(results)
                    published += ok
                    # Only mark done when EVERY record landed. On a partial/failed publish
                    # (Redis blip, breaker open, pool exhausted) leave the unit pending so a
                    # rerun re-fetches and re-publishes; Heber dedups whatever already landed.
                    # Marking done here would be a permanent silent data gap on resume.
                    if ok == len(envs):
                        prog.mark(key)
                    else:
                        print(f"[partial] {key}: {ok}/{len(envs)} published, will retry", file=sys.stderr)
            except Exception as e:  # complete-but-log: skip one unit, keep the run alive
                print(f"[err] {key}: {type(e).__name__}: {e}", file=sys.stderr)
            if calls >= args.daily_budget:
                stop.set()

    # Chunk so we don't build 1M coroutines at once; stop early when budget hits.
    pending = [(s, f, d) for (s, f, d) in work if not prog.has(_key(s, f, d))]
    for i in range(0, len(pending), 500):
        if stop.is_set():
            break
        await asyncio.gather(*(one(s, f, d) for s, f, d in pending[i : i + 500]))
        prog.flush()
        print(f"  progress: {calls} calls, {published} records published, {len(prog.done)} units done")

    prog.flush()
    await sink.close()
    with contextlib.suppress(Exception):
        await provider.shutdown()
    print(
        f"DONE tier{args.tier}: {calls} calls, {published} records published"
        + (" (budget reached — rerun to continue)" if stop.is_set() else " (complete)")
    )
    return 0


def _key(sym: str, feed: Feed, day: str | None) -> str:
    return f"{sym}|{feed.name}" + (f"|{day}" if day else "")


def active_asof(universe_path: Path) -> str:
    return json.loads(universe_path.read_text())["asof"]


# ── self-check (no network) ──


def self_check() -> int:
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        sp = Path(td) / "s.json"
        p = Progress(sp)
        p.mark("AAPL|ftds")
        p.flush()
        assert Progress(sp).has("AAPL|ftds"), "progress did not persist"
        assert not Progress(sp).has("MSFT|ftds")
    days = trading_days(dt.date(2026, 1, 1), dt.date(2026, 1, 7))
    assert days == ["2026-01-01", "2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"], days
    # Every feed must be one the EOD poller publishes AND Heber has a Silver normalizer for.
    proven = {
        "greek_exposure",
        "ftds",
        "short_volume",
        "earnings",
        "congress_trades",
        "insider_trades",
        "oi_change",
        "historic_option_volume",
        "darkpool",
        "income_statement",
        "balance_sheet",
        "cash_flow",
    }
    assert set(FEEDS) == proven, f"feed set drifted from proven contract: {set(FEEDS) ^ proven}"
    # Guard the decoupling intent: the default publish target must stay the dedicated backfill
    # stream, never the live "heber:events" — a silent revert would re-introduce live eviction.
    assert DEFAULT_BACKFILL_STREAM == "heber:events:backfill", DEFAULT_BACKFILL_STREAM
    assert DEFAULT_BACKFILL_STREAM != "heber:events", "backfill must not default to the live stream"
    envs = _to_envelopes([{"symbol": "aapl", "date": "2025-01-02", "call_gamma": 1}], "aapl", "greek_exposure")
    assert envs and envs[0]["instrument_key"] == "equity:AAPL", envs  # symbol uppercased
    assert envs[0]["feed"] == "greek_exposure"
    # congress/insider get their ts_event stamped from the filing date (event_id parity with EOD)
    c = _to_envelopes(
        [{"ticker": "AAPL", "filed_at_date": "2025-03-04", "transaction_date": "2025-02-01"}], "AAPL", "congress_trades"
    )
    assert "2025-03-04" in c[0]["ts_event"], c[0]["ts_event"]
    i = _to_envelopes(
        [{"ticker": "AAPL", "filing_date": "2025-03-05", "transaction_date": "2025-02-02"}], "AAPL", "insider_trades"
    )
    assert "2025-03-05" in i[0]["ts_event"], i[0]["ts_event"]
    print("self-check OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tier", type=int, choices=[1, 3], default=1)
    ap.add_argument("--feeds", nargs="*", help="explicit feed names (overrides --tier default set)")
    ap.add_argument("--universe", default=str(DEFAULT_UNIVERSE))
    ap.add_argument("--state-file", default=str(DEFAULT_STATE))
    ap.add_argument("--daily-budget", type=int, default=15000, help="max UW calls this run")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument(
        "--stream",
        default=None,
        help=(
            "Redis stream to publish to. Default (heber:events:backfill via HEBER_BACKFILL_STREAM) is a "
            "DEDICATED backfill stream so bulk history cannot evict live events from heber:events. "
            "Only override to heber:events if you deliberately want the old shared-stream behavior."
        ),
    )
    ap.add_argument(
        "--max-stream-len",
        type=int,
        default=5_000_000,
        help="MAXLEN cap the driver's sink applies to the backfill stream (trims re-runnable backfill, never live events)",
    )
    ap.add_argument("--top-n", type=int, default=200, help="tier3: symbol subset size")
    ap.add_argument("--depth-days", type=int, default=90, help="tier3: trailing days to backfill")
    ap.add_argument("--symbols-file", help="tier3: newline/space list of symbols (liquidity-ranked)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()
    if args.self_check:
        return self_check()
    if args.feeds:
        bad = [f for f in args.feeds if f not in FEEDS]
        if bad:
            print(f"[fatal] unknown/unproven feeds: {bad}. Known: {sorted(FEEDS)}", file=sys.stderr)
            return 2
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
