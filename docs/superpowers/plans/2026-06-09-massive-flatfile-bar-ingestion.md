# Massive Flat-File Bar Ingestion (Plan 1 of 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest Massive (ex-Polygon) day- and minute-aggregate flat files into Heber's Silver `bars` dataset (raw OHLCV, delisted tickers included), reusing Heber's `BackfillCoordinator`, with an immutable vendor-raw archive and event-time `ts_available`.

**Architecture:** A new `heber/backfill/massive/` subpackage. A `MassiveFlatFileDownloader` mirrors S3 `.csv.gz` files into `_vendor_raw/` with an integrity manifest. A pure `parser` maps each CSV row to a Silver-`bars`-shaped record dict (ticker→`equity:{T}`, ns→UTC, deterministic `event_id`), routing regex-failing tickers to a rejects log. A schema-enforcing `parquet_writer` and an async `data_fetcher` are injected into Heber's existing `BackfillCoordinator`, which handles chunking, resume, Bronze+Silver writes, and catalog updates. A CLI runs `--full` (2003→now) or `--incremental` (gap-fill via `GapDetector`).

**Tech Stack:** Python 3.12, Heber (`heber.backfill`, `heber.schemas.silver`, `heber.models.envelope`, `heber.writer.compactor`), pyarrow, boto3, pytest.

**Implementation home:** All code/tests in the **Heber** repo (`/Users/jacobmcmillan/Empire/Heber`). The S3 credentials live in env vars `MASSIVE_S3_ACCESS_KEY_ID`, `MASSIVE_S3_SECRET_ACCESS_KEY`, `MASSIVE_S3_ENDPOINT`, `MASSIVE_S3_BUCKET` (already in Data-Gateway `.env`; copy to Heber's env for the run).

**Scope note:** This plan builds and unit/integration-tests the code **without** a paid subscription (download is exercised against a faked S3 client; parsing/writing against synthetic fixtures). The real bulk download is the operational runbook in the spec §12. Follow-on plans: **Plan 2** REST corporate-actions sweep (splits/dividends/delisted tickers), **Plan 3** launchd incremental scheduler.

**Reference:** spec `docs/superpowers/specs/2026-06-09-massive-historical-bar-ingestion-design.md` (in the Data-Gateway repo).

---

## File Structure (all under `/Users/jacobmcmillan/Empire/Heber`)

- Create `heber/backfill/massive/__init__.py` — package marker + public exports.
- Create `heber/backfill/massive/event_id.py` — deterministic bar `event_id`.
- Create `heber/backfill/massive/parser.py` — CSV row → Silver-`bars` record dict; ticker normalize/validate; rejects.
- Create `heber/backfill/massive/silver_writer.py` — schema-enforcing parquet writer for `bars`.
- Create `heber/backfill/massive/fetcher.py` — archive-backed async `data_fetcher`.
- Create `heber/backfill/massive/downloader.py` — S3 → `_vendor_raw/` + manifest + resume.
- Create `heber/backfill/massive/cli.py` — `--full` / `--incremental` entrypoint.
- Create tests under `tests/backfill/massive/` mirroring each module.

**Constants (define once in `parser.py`, import elsewhere):**
- `PROVIDER = "massive"`, `FEED = "bars"`, `INSTRUMENT_TYPE = "equity"`, `SOURCE = "backfill"`, `SCHEMA_VERSION = "v1"`.
- `TIMEFRAME_BY_DATASET = {"day_aggs_v1": "1d", "minute_aggs_v1": "1m"}`.

---

### Task 1: Deterministic bar event_id

**Files:**
- Create: `heber/backfill/massive/event_id.py`
- Test: `tests/backfill/massive/test_event_id.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/backfill/massive/test_event_id.py
from datetime import UTC, datetime

from heber.backfill.massive.event_id import compute_bar_event_id


def test_event_id_is_deterministic_and_hex16():
    ts = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    a = compute_bar_event_id("massive", "bars", "equity:AAPL", ts, "1d")
    b = compute_bar_event_id("massive", "bars", "equity:AAPL", ts, "1d")
    assert a == b
    assert len(a) == 32 and all(c in "0123456789abcdef" for c in a)


def test_event_id_changes_with_inputs():
    ts = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    base = compute_bar_event_id("massive", "bars", "equity:AAPL", ts, "1d")
    assert base != compute_bar_event_id("massive", "bars", "equity:MSFT", ts, "1d")
    assert base != compute_bar_event_id("massive", "bars", "equity:AAPL", ts, "1m")
    other_ts = datetime(2024, 1, 3, 0, 0, tzinfo=UTC)
    assert base != compute_bar_event_id("massive", "bars", "equity:AAPL", other_ts, "1d")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_event_id.py -v`
Expected: FAIL — `ModuleNotFoundError: heber.backfill.massive.event_id`

- [ ] **Step 3: Write minimal implementation**

```python
# heber/backfill/massive/event_id.py
"""Deterministic event_id for backfilled bars (idempotent re-ingest)."""

from datetime import datetime
from hashlib import blake2b


def compute_bar_event_id(
    provider: str,
    feed: str,
    instrument_key: str,
    ts_event: datetime,
    timeframe: str,
) -> str:
    """BLAKE2b(16) hex over the bar's identity fields.

    Mirrors the Data-Gateway envelope id scheme so the same logical bar always
    hashes to the same id, making re-ingestion idempotent.
    """
    parts = f"{provider}|{feed}|{instrument_key}|{ts_event.isoformat()}|{timeframe}"
    return blake2b(parts.encode("utf-8"), digest_size=16).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_event_id.py -v`
Expected: PASS (2 passed). Create `heber/backfill/massive/__init__.py` (empty) and `tests/backfill/massive/__init__.py` if pytest cannot import the package.

- [ ] **Step 5: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/backfill/massive/event_id.py heber/backfill/massive/__init__.py tests/backfill/massive/
git commit -m "feat(massive): deterministic bar event_id for backfill idempotency"
```

---

### Task 2: Flat-file row parser → Silver-`bars` record

**Files:**
- Create: `heber/backfill/massive/parser.py`
- Test: `tests/backfill/massive/test_parser.py`

Massive day/minute aggregate CSV header: `ticker,volume,open,close,high,low,window_start,transactions` where `window_start` is a Unix **nanosecond** timestamp. Output dicts carry the Silver `bars` columns (`heber/schemas/silver.py` `SILVER_SCHEMAS["bars"]`): `event_id, provider, feed, instrument_type, instrument_key, symbol, ts_event, source, schema_version, timeframe, bar_start_ts, open, high, low, close, volume, trade_count, vwap`. (`ts_ingest`, `ts_available`, `quality_flags`, `backfill_id` are added later by `BackfillWriter`.)

- [ ] **Step 1: Write the failing test**

```python
# tests/backfill/massive/test_parser.py
import gzip
from datetime import UTC, datetime
from decimal import Decimal

from heber.backfill.massive.parser import RejectCollector, parse_rows, row_to_record

HEADER = "ticker,volume,open,close,high,low,window_start,transactions"
# 2024-01-02T00:00:00Z == 1704153600 s == 1704153600000000000 ns
WS_NS = "1704153600000000000"


def test_row_to_record_maps_all_fields():
    row = {"ticker": "AAPL", "volume": "45846217", "open": "187.15", "close": "185.64",
           "high": "188.44", "low": "183.89", "window_start": WS_NS, "transactions": "1023456"}
    rec = row_to_record(row, timeframe="1d")
    assert rec["symbol"] == "AAPL"
    assert rec["instrument_key"] == "equity:AAPL"
    assert rec["instrument_type"] == "equity"
    assert rec["provider"] == "massive" and rec["feed"] == "bars" and rec["source"] == "backfill"
    assert rec["timeframe"] == "1d"
    assert rec["ts_event"] == datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    assert rec["bar_start_ts"] == datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    assert rec["open"] == 187.15 and rec["close"] == 185.64
    assert rec["high"] == 188.44 and rec["low"] == 183.89
    assert rec["volume"] == 45846217.0
    assert rec["trade_count"] == 1023456
    assert rec["vwap"] is None  # flat files omit vwap
    assert len(rec["event_id"]) == 32


def test_class_share_ticker_passes():
    row = {"ticker": "BRK.A", "volume": "10", "open": "1", "close": "1",
           "high": "1", "low": "1", "window_start": WS_NS, "transactions": "1"}
    rec = row_to_record(row, timeframe="1d")
    assert rec["instrument_key"] == "equity:BRK.A"


def test_parse_rows_routes_invalid_ticker_to_rejects():
    bad = {"ticker": "BAD$SYM", "volume": "1", "open": "1", "close": "1",
           "high": "1", "low": "1", "window_start": WS_NS, "transactions": "1"}
    good = {"ticker": "MSFT", "volume": "1", "open": "1", "close": "1",
            "high": "1", "low": "1", "window_start": WS_NS, "transactions": "1"}
    rejects = RejectCollector()
    out = list(parse_rows([bad, good], timeframe="1d", rejects=rejects))
    assert [r["symbol"] for r in out] == ["MSFT"]
    assert rejects.count == 1
    assert rejects.items[0]["ticker"] == "BAD$SYM"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: heber.backfill.massive.parser`

- [ ] **Step 3: Write minimal implementation**

```python
# heber/backfill/massive/parser.py
"""Parse Massive aggregate CSV rows into Silver-`bars` record dicts."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from heber.backfill.massive.event_id import compute_bar_event_id
from heber.models.envelope import INSTRUMENT_KEY_PATTERNS

PROVIDER = "massive"
FEED = "bars"
INSTRUMENT_TYPE = "equity"
SOURCE = "backfill"
SCHEMA_VERSION = "v1"
TIMEFRAME_BY_DATASET = {"day_aggs_v1": "1d", "minute_aggs_v1": "1m"}

_EQUITY_KEY = INSTRUMENT_KEY_PATTERNS["equity"]


class InvalidTickerError(ValueError):
    """Raised when a ticker cannot form a valid equity instrument_key."""


@dataclass
class RejectCollector:
    """Accumulates rows that failed normalization/validation (never silently dropped)."""

    items: list[dict[str, Any]] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.items)

    def add(self, ticker: str, reason: str) -> None:
        self.items.append({"ticker": ticker, "reason": reason})


def _normalize_ticker(raw: str) -> str:
    return raw.strip().upper()


def row_to_record(row: dict[str, str], timeframe: str) -> dict[str, Any]:
    """Map one CSV row to a Silver-`bars` record. Raises InvalidTickerError on bad ticker."""
    symbol = _normalize_ticker(row["ticker"])
    instrument_key = f"equity:{symbol}"
    if not _EQUITY_KEY.match(instrument_key):
        raise InvalidTickerError(symbol)

    # window_start is a Unix nanosecond timestamp.
    ts = datetime.fromtimestamp(int(row["window_start"]) / 1_000_000_000, tz=UTC)
    txns = row.get("transactions")

    return {
        "event_id": compute_bar_event_id(PROVIDER, FEED, instrument_key, ts, timeframe),
        "provider": PROVIDER,
        "feed": FEED,
        "instrument_type": INSTRUMENT_TYPE,
        "instrument_key": instrument_key,
        "symbol": symbol,
        "ts_event": ts,
        "source": SOURCE,
        "schema_version": SCHEMA_VERSION,
        "timeframe": timeframe,
        "bar_start_ts": ts,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "trade_count": int(txns) if txns not in (None, "") else None,
        "vwap": None,  # not present in aggregate flat files
    }


def parse_rows(
    rows: Iterable[dict[str, str]],
    timeframe: str,
    rejects: RejectCollector,
) -> Iterator[dict[str, Any]]:
    """Yield records for valid rows; route invalid tickers to `rejects`."""
    for row in rows:
        try:
            yield row_to_record(row, timeframe)
        except InvalidTickerError:
            rejects.add(row.get("ticker", ""), "instrument_key_regex_failed")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_parser.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/backfill/massive/parser.py tests/backfill/massive/test_parser.py
git commit -m "feat(massive): parse aggregate CSV rows into Silver bars records with reject routing"
```

---

### Task 3: Schema-enforcing Silver parquet writer

**Files:**
- Create: `heber/backfill/massive/silver_writer.py`
- Test: `tests/backfill/massive/test_silver_writer.py`

`BackfillWriter._write_parquet` calls `pa.Table.from_pylist(records)` with no schema, so an all-null `vwap` column would infer as `null` type and break compaction. We inject a `parquet_writer` that writes with the canonical `get_silver_schema("bars")`, which also drops extra keys (`backfill_id`).

- [ ] **Step 1: Write the failing test**

```python
# tests/backfill/massive/test_silver_writer.py
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from heber.backfill.massive.silver_writer import make_bars_parquet_writer
from heber.schemas.silver import get_silver_schema


def _record(ts):
    return {
        "event_id": "a" * 32, "provider": "massive", "feed": "bars",
        "instrument_type": "equity", "instrument_key": "equity:AAPL", "symbol": "AAPL",
        "ts_event": ts, "ts_ingest": ts, "ts_available": ts, "source": "backfill",
        "schema_version": "v1", "quality_flags": ["backfill"], "timeframe": "1d",
        "bar_start_ts": ts, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
        "volume": 100.0, "trade_count": 7, "vwap": None,
        "backfill_id": "job-123",  # extra key must be dropped by the schema
    }


def test_writer_emits_canonical_bars_schema(tmp_path: Path):
    ts = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    out = tmp_path / "part.parquet"
    make_bars_parquet_writer()([_record(ts)], out)

    table = pq.read_table(out)
    assert table.schema.equals(get_silver_schema("bars"))
    assert "backfill_id" not in table.schema.names
    assert table.num_rows == 1
    assert table.column("vwap").to_pylist() == [None]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_silver_writer.py -v`
Expected: FAIL — `ModuleNotFoundError: heber.backfill.massive.silver_writer`

- [ ] **Step 3: Write minimal implementation**

```python
# heber/backfill/massive/silver_writer.py
"""Schema-enforcing parquet writer injected into Heber's BackfillWriter."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from heber.schemas.silver import get_silver_schema


def make_bars_parquet_writer() -> Callable[[list[dict[str, Any]], Path], None]:
    """Return a parquet_writer(records, path) that writes the canonical `bars` schema.

    Using the explicit schema pins column types (e.g. all-null vwap stays float64)
    and ignores record keys not in the schema (e.g. backfill_id).
    """
    schema = get_silver_schema("bars")

    def _write(records: list[dict[str, Any]], path: Path) -> None:
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(table, str(path))

    return _write
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_silver_writer.py -v`
Expected: PASS. If `from_pylist` rejects extra keys on this pyarrow version, change `_write` to build column-wise: `pa.table({f.name: [r.get(f.name) for r in records] for f in schema}, schema=schema)`.

- [ ] **Step 5: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/backfill/massive/silver_writer.py tests/backfill/massive/test_silver_writer.py
git commit -m "feat(massive): schema-enforcing Silver bars parquet writer"
```

---

### Task 4: Archive-backed async data_fetcher

**Files:**
- Create: `heber/backfill/massive/fetcher.py`
- Test: `tests/backfill/massive/test_fetcher.py`

`BackfillCoordinator` calls `await data_fetcher(provider=, feed=, date=, symbols=)` per chunk date and passes the result to `write_batch`. Our fetcher reads the archived `.csv.gz` for that date (skips if absent, logging) and returns records via the parser. The archive layout is `{archive_root}/us_stocks_sip/{dataset}/YYYY/MM/YYYY-MM-DD.csv.gz`.

- [ ] **Step 1: Write the failing test**

```python
# tests/backfill/massive/test_fetcher.py
import gzip
from datetime import date
from pathlib import Path

import pytest

from heber.backfill.massive.fetcher import make_flatfile_fetcher
from heber.backfill.massive.parser import RejectCollector

HEADER = "ticker,volume,open,close,high,low,window_start,transactions"
WS_NS = "1704153600000000000"


def _write_archive(root: Path, dataset: str, d: date, body: str) -> None:
    p = root / "us_stocks_sip" / dataset / f"{d:%Y}" / f"{d:%m}" / f"{d:%Y-%m-%d}.csv.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        fh.write(body)


@pytest.mark.asyncio
async def test_fetcher_reads_archived_day(tmp_path: Path):
    _write_archive(tmp_path, "day_aggs_v1", date(2024, 1, 2),
                   f"{HEADER}\nAAPL,10,1,1.5,2,0.5,{WS_NS},7\n")
    rejects = RejectCollector()
    fetcher = make_flatfile_fetcher(str(tmp_path), dataset="day_aggs_v1", rejects=rejects)
    records = await fetcher(provider="massive", feed="bars", date=date(2024, 1, 2), symbols=None)
    assert len(records) == 1 and records[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_fetcher_missing_file_returns_empty(tmp_path: Path):
    fetcher = make_flatfile_fetcher(str(tmp_path), dataset="day_aggs_v1", rejects=RejectCollector())
    records = await fetcher(provider="massive", feed="bars", date=date(1999, 1, 4), symbols=None)
    assert records == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_fetcher.py -v`
Expected: FAIL — `ModuleNotFoundError: heber.backfill.massive.fetcher`

- [ ] **Step 3: Write minimal implementation**

```python
# heber/backfill/massive/fetcher.py
"""Archive-backed async data_fetcher for BackfillCoordinator."""

import csv
import gzip
from collections.abc import Awaitable, Callable
from datetime import date
from pathlib import Path
from typing import Any

import structlog

from heber.backfill.massive.parser import RejectCollector, parse_rows

logger = structlog.get_logger(__name__)

FetcherType = Callable[..., Awaitable[list[dict[str, Any]]]]


def archive_path(archive_root: str, dataset: str, d: date) -> Path:
    return (
        Path(archive_root)
        / "us_stocks_sip"
        / dataset
        / f"{d:%Y}"
        / f"{d:%m}"
        / f"{d:%Y-%m-%d}.csv.gz"
    )


def make_flatfile_fetcher(
    archive_root: str,
    dataset: str,
    rejects: RejectCollector,
) -> FetcherType:
    """Build an async fetcher that reads one archived day's file into records."""
    from heber.backfill.massive.parser import TIMEFRAME_BY_DATASET

    timeframe = TIMEFRAME_BY_DATASET[dataset]

    async def _fetch(*, provider: str, feed: str, date: date, symbols: list[str] | None) -> list[dict[str, Any]]:
        path = archive_path(archive_root, dataset, date)
        if not path.exists():
            logger.info("massive_archive_file_missing", dataset=dataset, date=date.isoformat(), path=str(path))
            return []
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            records = list(parse_rows(reader, timeframe=timeframe, rejects=rejects))
        logger.info("massive_archive_file_parsed", dataset=dataset, date=date.isoformat(), rows=len(records))
        return records

    return _fetch
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_fetcher.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/backfill/massive/fetcher.py tests/backfill/massive/test_fetcher.py
git commit -m "feat(massive): archive-backed async data_fetcher"
```

---

### Task 5: S3 downloader with manifest + resume

**Files:**
- Create: `heber/backfill/massive/downloader.py`
- Test: `tests/backfill/massive/test_downloader.py`

Mirrors `s3://{bucket}/us_stocks_sip/{dataset}/YYYY/MM/*.csv.gz` into `_vendor_raw/`, appending a manifest line `(s3_key, size, etag, sha256, downloaded_at)`. Resumable: a file already present whose recorded sha256 matches is skipped. boto3 client is injected for testability.

- [ ] **Step 1: Write the failing test**

```python
# tests/backfill/massive/test_downloader.py
import gzip
import json
from pathlib import Path

from heber.backfill.massive.downloader import MassiveFlatFileDownloader


class _FakeS3:
    """Minimal stand-in: one object under the dataset prefix."""
    def __init__(self, body: bytes):
        self._body = body
        self.get_calls = 0

    def get_paginator(self, _op):
        outer = self
        class _Pag:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": f"{Prefix}2024-01-02.csv.gz",
                                     "Size": len(outer._body), "ETag": '"etag123"'}]}
        return _Pag()

    def download_file(self, Bucket, Key, Filename):
        self.get_calls += 1
        Path(Filename).write_bytes(self._body)


def test_download_writes_file_and_manifest_then_skips_on_resume(tmp_path: Path):
    body = gzip.compress(b"ticker,volume,open,close,high,low,window_start,transactions\n")
    s3 = _FakeS3(body)
    dl = MassiveFlatFileDownloader(s3_client=s3, bucket="flatfiles", archive_root=str(tmp_path))

    n1 = dl.sync_month(dataset="day_aggs_v1", year=2024, month=1)
    assert n1 == 1 and s3.get_calls == 1
    f = tmp_path / "us_stocks_sip" / "day_aggs_v1" / "2024" / "01" / "2024-01-02.csv.gz"
    assert f.exists()
    manifest = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text().splitlines()]
    assert manifest[0]["s3_key"].endswith("2024-01-02.csv.gz")
    assert manifest[0]["sha256"]

    n2 = dl.sync_month(dataset="day_aggs_v1", year=2024, month=1)  # resume
    assert n2 == 0 and s3.get_calls == 1  # not re-downloaded
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_downloader.py -v`
Expected: FAIL — `ModuleNotFoundError: heber.backfill.massive.downloader`

- [ ] **Step 3: Write minimal implementation**

```python
# heber/backfill/massive/downloader.py
"""Download Massive flat files into an immutable vendor-raw archive with a manifest."""

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class MassiveFlatFileDownloader:
    """Mirror s3://{bucket}/us_stocks_sip/{dataset}/YYYY/MM/*.csv.gz to archive_root."""

    def __init__(self, s3_client: Any, bucket: str, archive_root: str):
        self._s3 = s3_client
        self._bucket = bucket
        self._archive_root = Path(archive_root)
        self._manifest_path = self._archive_root / "manifest.jsonl"
        self._seen = self._load_seen()

    def _load_seen(self) -> dict[str, str]:
        seen: dict[str, str] = {}
        if self._manifest_path.exists():
            for line in self._manifest_path.read_text(encoding="utf-8").splitlines():
                try:
                    row = json.loads(line)
                    seen[row["s3_key"]] = row["sha256"]
                except (ValueError, KeyError):
                    continue
        return seen

    def _local_path(self, key: str) -> Path:
        # key: us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz
        return self._archive_root / key

    def _append_manifest(self, row: dict[str, Any]) -> None:
        self._manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with self._manifest_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")

    def sync_month(self, dataset: str, year: int, month: int) -> int:
        """Download every object under the dataset/year/month prefix. Returns new files written."""
        prefix = f"us_stocks_sip/{dataset}/{year:04d}/{month:02d}/"
        written = 0
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                dest = self._local_path(key)
                if key in self._seen and dest.exists() and _sha256(dest) == self._seen[key]:
                    continue  # resume: already have a verified copy
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_suffix(dest.suffix + ".tmp")
                self._s3.download_file(self._bucket, key, str(tmp))
                os.replace(tmp, dest)
                digest = _sha256(dest)
                self._append_manifest({
                    "s3_key": key,
                    "size": obj.get("Size"),
                    "etag": (obj.get("ETag") or "").strip('"'),
                    "sha256": digest,
                    "downloaded_at": datetime.now(UTC).isoformat(),
                })
                self._seen[key] = digest
                written += 1
        logger.info("massive_sync_month", dataset=dataset, year=year, month=month, written=written)
        return written
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_downloader.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/backfill/massive/downloader.py tests/backfill/massive/test_downloader.py
git commit -m "feat(massive): S3 flat-file downloader with manifest and resume"
```

---

### Task 6: Coordinator wiring + CLI (`--full` / `--incremental`)

**Files:**
- Create: `heber/backfill/massive/cli.py`
- Test: `tests/backfill/massive/test_ingest_integration.py`

Wires our pieces into Heber's `BackfillCoordinator`: a `writer_factory` that injects the schema-enforcing `parquet_writer`, the archive-backed `data_fetcher`, and a `BackfillJobDefinition` with `ts_available_policy=CUSTOM, custom_delay_seconds=86400` (bar for day T becomes available T+1 — matching flat-file publish reality and preventing same-day lookahead). `--incremental` uses `GapDetector` to fill only missing dates.

- [ ] **Step 1: Write the failing integration test**

```python
# tests/backfill/massive/test_ingest_integration.py
import gzip
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from heber.backfill.massive.cli import ingest_range
from heber.backfill.massive.parser import RejectCollector

HEADER = "ticker,volume,open,close,high,low,window_start,transactions"
WS_NS = "1704153600000000000"  # 2024-01-02T00:00:00Z


def _seed(archive: Path, d: date, body: str) -> None:
    p = archive / "us_stocks_sip" / "day_aggs_v1" / f"{d:%Y}" / f"{d:%m}" / f"{d:%Y-%m-%d}.csv.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        fh.write(body)


@pytest.mark.asyncio
async def test_ingest_writes_silver_with_event_time_availability(tmp_path: Path):
    storage_root = tmp_path / "heber_data"
    archive_root = tmp_path / "vendor_raw"
    _seed(archive_root, date(2024, 1, 2), f"{HEADER}\nAAPL,10,1,1.5,2,0.5,{WS_NS},7\n")

    rejects = RejectCollector()
    job = await ingest_range(
        dataset="day_aggs_v1",
        start=date(2024, 1, 2), end=date(2024, 1, 2),
        storage_root=str(storage_root), archive_root=str(archive_root),
        rejects=rejects,
    )
    assert job.rows_written == 1

    temp_dir = storage_root / "silver" / "massive_bars" / "dt=2024-01-02" / f"_backfill_{job.backfill_id}"
    parts = list(temp_dir.glob("*.parquet"))
    assert len(parts) == 1
    table = pq.read_table(parts[0])
    row = table.to_pylist()[0]
    assert row["instrument_key"] == "equity:AAPL"
    assert "backfill" in row["quality_flags"]
    # event-time availability: bar for 2024-01-02 is available 2024-01-03 (+1 day), NOT the run date
    assert row["ts_available"] == datetime(2024, 1, 3, 0, 0, tzinfo=UTC)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_ingest_integration.py -v`
Expected: FAIL — `ModuleNotFoundError: heber.backfill.massive.cli`

- [ ] **Step 3: Write minimal implementation**

```python
# heber/backfill/massive/cli.py
"""CLI + programmatic entrypoint to ingest Massive flat files into Heber Silver."""

import argparse
import asyncio
from datetime import date

from heber.backfill import (
    BackfillCoordinator,
    BackfillJob,
    BackfillJobDefinition,
    BackfillWriter,
    GapDetector,
    TsAvailablePolicy,
)
from heber.backfill.massive.fetcher import make_flatfile_fetcher
from heber.backfill.massive.parser import FEED, PROVIDER, RejectCollector
from heber.backfill.massive.silver_writer import make_bars_parquet_writer

PUBLISH_LAG_SECONDS = 86_400  # bar for day T is available T+1 (matches flat-file publish)


def _writer_factory(**kwargs) -> BackfillWriter:
    return BackfillWriter(parquet_writer=make_bars_parquet_writer(), **kwargs)


async def ingest_range(
    *,
    dataset: str,
    start: date,
    end: date,
    storage_root: str,
    archive_root: str,
    rejects: RejectCollector,
) -> BackfillJob:
    """Ingest [start, end] for one dataset through Heber's BackfillCoordinator."""
    fetcher = make_flatfile_fetcher(archive_root, dataset=dataset, rejects=rejects)
    coordinator = BackfillCoordinator(
        storage_root=storage_root,
        data_fetcher=fetcher,
        writer_factory=_writer_factory,
    )
    definition = BackfillJobDefinition(
        provider=PROVIDER,
        feed=FEED,
        date_range_start=start,
        date_range_end=end,
        ts_available_policy=TsAvailablePolicy.CUSTOM,
        custom_delay_seconds=PUBLISH_LAG_SECONDS,
        rate_limit_per_second=1000.0,  # local file reads; not a network rate limit
    )
    job = coordinator.create_job(definition)
    return await coordinator.run_job(job.backfill_id, definition)


async def ingest_incremental(
    *, dataset: str, start: date, end: date, storage_root: str, archive_root: str, rejects: RejectCollector
) -> list[BackfillJob]:
    """Fill only the gaps in [start, end] using GapDetector."""
    gaps = GapDetector(storage_root=storage_root).detect_gaps(PROVIDER, FEED, start, end)
    jobs = []
    for gap_start, gap_end in gaps:
        jobs.append(await ingest_range(
            dataset=dataset, start=gap_start, end=gap_end,
            storage_root=storage_root, archive_root=archive_root, rejects=rejects,
        ))
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Massive flat files into Heber Silver bars")
    parser.add_argument("--dataset", choices=["day_aggs_v1", "minute_aggs_v1"], required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--storage-root", default="/Volumes/heber/data")
    parser.add_argument("--archive-root", default="/Volumes/heber/data/_vendor_raw/massive")
    parser.add_argument("--incremental", action="store_true")
    args = parser.parse_args()

    rejects = RejectCollector()
    runner = ingest_incremental if args.incremental else ingest_range
    if args.incremental:
        asyncio.run(ingest_incremental(
            dataset=args.dataset, start=args.start, end=args.end,
            storage_root=args.storage_root, archive_root=args.archive_root, rejects=rejects))
    else:
        asyncio.run(ingest_range(
            dataset=args.dataset, start=args.start, end=args.end,
            storage_root=args.storage_root, archive_root=args.archive_root, rejects=rejects))
    print(f"rejects: {rejects.count}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_ingest_integration.py -v`
Expected: PASS. If `BackfillCoordinator` cannot import (e.g. needs a real storage root for `_load_jobs`), pass the tmp `storage_root` (the test already does). Remove the unused `runner` line if the linter flags it.

- [ ] **Step 5: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/backfill/massive/cli.py tests/backfill/massive/test_ingest_integration.py
git commit -m "feat(massive): ingest CLI wiring flat files into Heber BackfillCoordinator"
```

---

### Task 7: Compaction + coverage verification

**Files:**
- Test: `tests/backfill/massive/test_compaction_and_coverage.py`
- Reference: `heber/writer/compactor.py` (`Compactor.compact_partition`), `heber/backfill/__init__.py` (`GapDetector.get_coverage_summary`)

After `write_batch` writes temp partitions (`silver/massive_bars/dt=.../_backfill_<id>/`), Heber's `Compactor` merges them into the canonical partition. This task verifies compaction runs on our output and that `GapDetector` reports full coverage afterward.

- [ ] **Step 1: Write the failing test**

```python
# tests/backfill/massive/test_compaction_and_coverage.py
import gzip
from datetime import date
from pathlib import Path

import pytest

from heber.backfill import GapDetector
from heber.backfill.massive.cli import ingest_range
from heber.backfill.massive.parser import RejectCollector
from heber.writer.compactor import Compactor

HEADER = "ticker,volume,open,close,high,low,window_start,transactions"
WS_NS = "1704153600000000000"


def _seed(archive: Path, d: date) -> None:
    p = archive / "us_stocks_sip" / "day_aggs_v1" / f"{d:%Y}" / f"{d:%m}" / f"{d:%Y-%m-%d}.csv.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        fh.write(f"{HEADER}\nAAPL,10,1,1.5,2,0.5,{WS_NS},7\n")


@pytest.mark.asyncio
async def test_coverage_after_ingest(tmp_path: Path):
    storage_root = tmp_path / "heber_data"
    archive_root = tmp_path / "vendor_raw"
    _seed(archive_root, date(2024, 1, 2))

    job = await ingest_range(
        dataset="day_aggs_v1", start=date(2024, 1, 2), end=date(2024, 1, 2),
        storage_root=str(storage_root), archive_root=str(archive_root), rejects=RejectCollector())

    # GapDetector sees the dt= partition produced under silver/massive_bars/
    summary = GapDetector(storage_root=str(storage_root)).get_coverage_summary(
        "massive", "bars", date(2024, 1, 2), date(2024, 1, 2))
    assert summary["covered_days"] == 1 and summary["gap_days"] == 0
```

- [ ] **Step 2: Run test to verify it fails / passes**

Run: `cd /Users/jacobmcmillan/Empire/Heber && uv run pytest tests/backfill/massive/test_compaction_and_coverage.py -v`
Expected: Initially may FAIL if `GapDetector._candidate_silver_roots` only matches `silver/massive_bars` via the `{provider}_{feed}` root (it does — line 198). Confirm the temp `dt=` dir is discovered by `glob("**/dt=*")`. If coverage is detected from the temp partition, PASS. If it requires compaction first, add before the assert:

```python
    Compactor(storage_root=str(storage_root)).compact_partition(
        storage_root / "silver" / "massive_bars" / "dt=2024-01-02")
```

(Read `Compactor.__init__` / `compact_partition` signature in `heber/writer/compactor.py` and adjust the call to match.)

- [ ] **Step 3: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add tests/backfill/massive/test_compaction_and_coverage.py
git commit -m "test(massive): coverage verification after ingest"
```

---

## Operational runbook (paid month — not code)

See spec §12. In short, once subscribed and creds are in Heber's env:
1. Download day-aggs full history: loop `MassiveFlatFileDownloader.sync_month("day_aggs_v1", y, m)` for `y,m` from 2003-09 → now.
2. Same for `minute_aggs_v1`.
3. Verify the manifest covers every trading day; **then** cancel the subscription.
4. Ingest from the archive (post-cancel OK): `python -m heber.backfill.massive.cli --dataset day_aggs_v1 --start 2003-09-10 --end <yesterday>` then `--dataset minute_aggs_v1 ...`.
5. Run the compactor (or rely on the running Heber compactor service) to merge temp partitions.
6. Spot-check a delisted name (e.g. SIVB in its 2022 era) resolves to `equity:SIVB` rows in Silver.

## Self-review notes

- **Spec coverage:** §4–§8 (flow, components, schema mapping, key normalization, storage) → Tasks 2–6. §6 `ts_available` leakage policy → Task 6 (CUSTOM, +1 day) with an explicit test. §9 retention/archive + §10 integrity manifest → Task 5. §10 reject routing → Task 2. §11 unknowns are now resolved (verified against real Heber code). Corporate-actions sweep (§2) and launchd scheduler (§5) are **Plan 2 / Plan 3** (out of this plan).
- **Verify-at-runtime (flagged inline, not placeholders):** `window_start` nanosecond unit (Task 2 — confirm against first real file); `from_pylist(schema=)` extra-key behavior on the installed pyarrow (Task 3 fallback given); `Compactor.compact_partition` exact signature (Task 7 — read before calling).
- **Type consistency:** `RejectCollector`, `make_flatfile_fetcher(archive_root, dataset, rejects)`, `make_bars_parquet_writer()`, `ingest_range(...)` signatures are used identically across tasks.

---

## Adversarial-review corrections (2026-06-10) — BINDING

Codex (`gpt-5.5`/`xhigh`) cross-checked this plan against the real Heber source. The wiring
(`data_fetcher`, `writer_factory`, `from_pylist(schema=)` dropping the extra `backfill_id` on
Heber's pinned pyarrow 23.0.0) is **confirmed correct**. The following corrections **supersede**
the tasks they name and must be implemented. See spec §15 for rationale.

### Correction A — Task 3: atomic parquet write
`make_bars_parquet_writer()` must write to a temp file and `os.replace` (Heber's default writer is
atomic; an injected writer must not regress that). Replace `_write`:

```python
import os, uuid
def _write(records: list[dict[str, Any]], path: Path) -> None:
    table = pa.Table.from_pylist(records, schema=schema)
    tmp = path.with_name(f"_{uuid.uuid4().hex}.parquet.tmp")
    pq.write_table(table, str(tmp))
    os.replace(tmp, path)
```

### Correction B — Task 6: fixed per-trading-date `ts_available` via a writer subclass
Heber's `CUSTOM` policy is per-row `ts_event + delay`, which **staggers minute bars**. Override
`set_ts_available` so every bar of trading date T gets the SAME `T+1 16:00 UTC`.
New file `heber/backfill/massive/writer.py`:

```python
"""BackfillWriter subclass: fixed per-trading-date publish-time ts_available + injected parquet writer."""
from datetime import UTC, datetime, time, timedelta
from typing import Any
from heber.backfill import BackfillWriter
from heber.backfill.massive.silver_writer import make_bars_parquet_writer

PUBLISH_HOUR_UTC = 16  # ~11:00 ET next morning, when Massive publishes day T's flat file

def next_publish_ts(bar_start_ts: datetime) -> datetime:
    pub_date = bar_start_ts.astimezone(UTC).date() + timedelta(days=1)
    return datetime.combine(pub_date, time(PUBLISH_HOUR_UTC, 0), tzinfo=UTC)

class MassiveBackfillWriter(BackfillWriter):
    def set_ts_available(self, record: dict[str, Any], ts_commit: datetime) -> dict[str, Any]:
        bar = record.get("bar_start_ts") or record.get("ts_event")
        record["ts_available"] = next_publish_ts(bar)  # same value for every bar of the date
        return record

def massive_writer_factory(**kwargs) -> MassiveBackfillWriter:
    return MassiveBackfillWriter(parquet_writer=make_bars_parquet_writer(), **kwargs)
```
Test (replaces Task 6's ts_available assertion): a 2024-01-02 day bar AND a 2024-01-02 14:31 minute
bar both get `ts_available == datetime(2024,1,3,16,0,tzinfo=UTC)` — proving no per-row stagger.

### Correction C — NEW Task 8: explicit promotion + dedup (temp → canonical)
Heber does NOT auto-promote `_backfill_<id>/` temp dirs and `Compactor` does not descend into them.
New file `heber/backfill/massive/promote.py`:

```python
"""Promote backfill temp partitions to canonical, idempotently (dedup by event_id)."""
import os, uuid
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from heber.schemas.silver import get_silver_schema

def promote_date(storage_root: str, dataset_dir: str, dt_iso: str) -> int:
    """Merge _backfill_*/ parquet for one dt= into a single canonical file; dedup by event_id."""
    part = Path(storage_root) / "silver" / dataset_dir / f"dt={dt_iso}"
    temp_files = list(part.glob("_backfill_*/*.parquet"))
    canonical = list(part.glob("part-*.parquet"))
    if not temp_files:
        return sum(pq.read_metadata(f).num_rows for f in canonical)

    # Fast path: first run (no canonical, single temp dir) — move files up, no read.
    temp_dirs = {f.parent for f in temp_files}
    if not canonical and len(temp_dirs) == 1:
        n = 0
        for i, f in enumerate(temp_files):
            dest = part / f"part-{i:05d}.parquet"
            os.replace(f, dest)
            n += pq.read_metadata(dest).num_rows
        next(iter(temp_dirs)).rmdir()
        return n

    # Merge path (re-run / partial): union + dedup by event_id, then atomic replace.
    schema = get_silver_schema("bars")
    rows = {}
    for f in [*canonical, *temp_files]:
        for r in pq.read_table(f).to_pylist():
            rows[r["event_id"]] = r  # last write wins; deterministic id ⇒ idempotent
    out = pa.Table.from_pylist(list(rows.values()), schema=schema)
    tmp = part / f"_promoting_{uuid.uuid4().hex}.parquet"
    pq.write_table(out, str(tmp))
    for f in [*canonical, *temp_files]:
        f.unlink(missing_ok=True)
    os.replace(tmp, part / "part-00000.parquet")
    for d in temp_dirs:
        try:
            d.rmdir()
        except OSError:
            pass
    return out.num_rows
```
Note: the merge path's `to_pylist()` is per-date; for a minute re-run that is one day's rows.
Re-runs are rare; the first-run fast path (the bulk load) never reads into memory.
Tests: (1) first run promotes temp→`part-00000.parquet`; (2) re-running the same date yields the
SAME row count (idempotent, no duplicates).

### Correction D — Task 6 rewrite: chunked, fail-loud, trading-calendar driver
Replace the `coordinator.run_job` path (it materializes a full per-date list and silently completes
missing days). Drive `write_batch` in bounded row-chunks over **trading days only**, fail loud on a
missing expected file, promote per date.

Add to `parser.py` a streaming chunker and broaden error catching:
```python
def iter_record_chunks(rows, timeframe, rejects, chunk_rows=200_000):
    chunk = []
    for row in rows:
        try:
            chunk.append(row_to_record(row, timeframe))
        except InvalidTickerError:
            rejects.add(row.get("ticker", ""), "instrument_key_regex_failed", row)
        except (KeyError, ValueError, TypeError) as e:   # malformed numeric/timestamp — never abort the chunk
            rejects.add(row.get("ticker", ""), f"parse_error:{e}", row)
        if len(chunk) >= chunk_rows:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
```
New file `heber/backfill/massive/driver.py`:
```python
"""Chunked, fail-loud ingestion driver over trading days."""
import csv, gzip
from datetime import date
from pathlib import Path
import pandas_market_calendars as mcal   # add to Heber deps
from heber.backfill import BackfillCoordinator, BackfillJobDefinition, TsAvailablePolicy
from heber.backfill.massive.fetcher import archive_path
from heber.backfill.massive.parser import FEED, PROVIDER, TIMEFRAME_BY_DATASET, RejectCollector, iter_record_chunks
from heber.backfill.massive.promote import promote_date
from heber.backfill.massive.writer import massive_writer_factory

class MissingArchiveFileError(RuntimeError): ...

def trading_days(start: date, end: date) -> list[date]:
    sched = mcal.get_calendar("XNYS").schedule(start_date=start, end_date=end)
    return [ts.date() for ts in sched.index]

def ingest(dataset: str, start: date, end: date, storage_root: str, archive_root: str,
           rejects: RejectCollector, chunk_rows: int = 200_000) -> int:
    timeframe = TIMEFRAME_BY_DATASET[dataset]
    coord = BackfillCoordinator(storage_root=storage_root, writer_factory=massive_writer_factory)
    job = coord.create_job(BackfillJobDefinition(
        provider=PROVIDER, feed=FEED, date_range_start=start, date_range_end=end,
        ts_available_policy=TsAvailablePolicy.CUSTOM))
    writer = massive_writer_factory(storage_root=storage_root,
                                    ts_available_policy=TsAvailablePolicy.CUSTOM, custom_delay_seconds=0)
    total = 0
    for d in trading_days(start, end):
        if d.isoformat() in job.progress_dates_completed:
            continue
        path = archive_path(archive_root, dataset, d)
        if not path.exists():
            raise MissingArchiveFileError(f"expected trading day not in archive: {dataset} {d}")
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            for chunk in iter_record_chunks(csv.DictReader(fh), timeframe, rejects, chunk_rows):
                total += writer.write_batch(job, chunk, d)
        promote_date(storage_root, f"{PROVIDER}_{FEED}", d.isoformat())
        job.progress_dates_completed.append(d.isoformat())
        coord._persist_job(job)
    rejects.flush(Path(archive_root) / "rejects" / f"{job.backfill_id}.jsonl")
    return total
```

### Correction E — durable rejects (reaffirms spec §7/§10)
`RejectCollector.add(ticker, reason, row)` stores the raw row; add `flush(path)` writing JSONL to
`_vendor_raw/massive/rejects/<run_id>.jsonl`. A run with a non-trivial reject count must surface it
(log + non-zero summary), not just print.

### Correction F — coverage verification (replaces GapDetector reliance)
Do not use `GapDetector` for completeness (calendar-day + dir-existence ⇒ false coverage). Verify by:
enumerate `trading_days(start, end)`, and for each assert a canonical `part-*.parquet` exists with
`num_rows > 0` (via `pq.read_metadata`). Weekends/holidays are excluded by construction.

### Still-open / out of this plan
- **Bronze retention**: pin `massive` Bronze in Heber retention config, or accept rebuild-only-from-`_vendor_raw` (spec §15.4). One-line config decision at deploy.
- **Survivorship gate**: Plan 1 = raw bars only; the REST splits/dividends/all-tickers sweep (**Plan 2**) is a pre-cancellation blocker and Atlas must not treat the corpus as survivorship-free until universe reconstruction + adjustment exist (spec §15.9).
- **New dependency**: `pandas_market_calendars` added to Heber.

---

## Round-2 adversarial-review corrections (2026-06-10) — BINDING

A second Codex (`gpt-5.5`/`xhigh`) pass on the corrected docs confirmed the round-1 corrections
land, but found production-safety gaps in the *corrective code above*. These supersede it.

1. **`promote_date` must be crash-safe (CRITICAL).** The merge path unlinks canonical+temp before
   the final `os.replace`, so a crash mid-promotion loses the visible partition. Required ordering:
   write the merged file to a **staging path**, fsync, `os.replace` it into the canonical
   `part-*.parquet` **first**, and only then delete the `_backfill_*` temp dirs. A crash leaves either
   the old canonical or the new one intact; re-running re-promotes idempotently (deterministic
   `event_id`). Add a per-`dt=` lock to prevent concurrent promotions.

2. **`promote_date` rerun dedup must not `to_pylist()` a whole minute day (HIGH).** Millions of dicts
   OOM. Use streaming dedup — DuckDB (`SELECT ... QUALIFY row_number() OVER (PARTITION BY event_id)`)
   or PyArrow `group_by`/`unique` over the concatenated dataset — never materialize to Python.

3. **The chunked driver must NOT call the private `_persist_job` or bypass `run_job` lifecycle
   (HIGH).** Doing so loses `RUNNING/COMPLETED/FAILED` status, `rows_written`, metrics, active-job
   tracking, and catalog coverage updates. Resolution: add a **public chunked-ingest path to Heber's
   `BackfillCoordinator`** (a `run_job` variant whose `data_fetcher` yields row-chunks, preserving all
   lifecycle/catalog/metrics) and call that — rather than reaching into Heber internals from the
   Massive adapter. This is a small Heber-side enhancement and should be its own task.

4. **`ts_available` must derive from the archive *trading date*, not the bar's UTC timestamp (HIGH).**
   Extended-hours minute bars cross UTC midnight, so `bar_start_ts.astimezone(UTC).date()` can be the
   wrong day. Carry the file's `trade_date` (the partition date) into each record and compute
   `ts_available = next-trading-session(trade_date) at publish hour`, using the exchange calendar so a
   Friday's data isn't marked available on Saturday. Test holiday and after-hours cases.

5. **Reader/Atlas path mismatch — the data we write would not be read (CRITICAL for the goal).**
   Backfill writes `silver/massive_bars/dt=...`, but `HeberReader.read_silver("bars")` reads
   `silver/feed=bars` and Atlas (`atlas/data/bin_cache.py`) expects
   `silver/feed=bars/instrument_type=equity`. **Resolution:** have the promotion step (we own it)
   write the deduped canonical output into the reader-canonical
   `silver/feed=bars/instrument_type=equity/dt=YYYY-MM-DD/` path. **Open design decision:** whether
   `massive` bars *replace* or *coexist with* the live `alpaca` bars already in that partition (the
   `provider` column distinguishes them, but Atlas must select one source to avoid duplicate
   (symbol, date) rows). Decide before implementing.

6. **Use Heber's existing `exchange-calendars` dependency (MEDIUM)** (`pyproject.toml`), not a new
   `pandas_market_calendars` — supersedes Correction D's import. (`import exchange_calendars as xcals;
   xcals.get_calendar("XNYS")`.)

7. **Reject hardening (HIGH):** validate the exact CSV header before iterating (a wrong
   delimiter/header otherwise makes *every* row a "reject"); capture file + line number per reject;
   and **fail loud if the reject rate exceeds a small threshold** (e.g. >0.5%) — a flood of rejects
   means a broken parser, not bad data.

8. **Preserve the raw vendor ticker (LOW):** Silver `symbol` is normalized `upper()`; keep the raw
   pre-normalization ticker in the Bronze JSONL for fidelity/debugging.

**Round-2 verdict (both reviewers): materially improved but NO-GO to implement as-written** until
items 1–5 are folded in. None change the architecture; they harden the new glue. These become
explicit tasks below.

**Read-path decision (resolved 2026-06-10):** Massive is the **source Atlas reads**, realized by
*physical separation*, not by writing into `feed=bars`. Massive bars stay in their own
`silver/massive_bars/` dataset; Atlas is repointed to read it; the live Alpaca `feed=bars` dataset
is untouched (trading systems keep producing/reading it). No comingling, no dedup-across-providers.

---

## Tasks 8–13 (round-2 corrections, executable)

### Task 8: Crash-safe, idempotent `promote_date` (supersedes Correction C)

**Files:** Modify `heber/backfill/massive/promote.py`; Test `tests/backfill/massive/test_promote.py`

- [ ] **Step 1 — failing tests:** (a) first run promotes `_backfill_*` → a single canonical
  `part-00000.parquet` and removes temp dirs; (b) **re-running the same date yields the SAME row
  count** (idempotent, deterministic `event_id`); (c) a simulated crash *after* staging but *before*
  cleanup leaves a readable canonical partition (no data-visible gap).
- [ ] **Step 2 — implementation (crash-safe ordering + streaming dedup):**

```python
import os, uuid
from pathlib import Path
import duckdb  # Heber already vendors duckdb; falls back to pyarrow group_by if unavailable
import pyarrow.parquet as pq

def promote_date(storage_root: str, dataset_dir: str, dt_iso: str) -> int:
    part = Path(storage_root) / "silver" / dataset_dir / f"dt={dt_iso}"
    temp_files = list(part.glob("_backfill_*/*.parquet"))
    canonical = part / "part-00000.parquet"
    if not temp_files:
        return pq.read_metadata(canonical).num_rows if canonical.exists() else 0

    lock = part / ".promote.lock"
    try:
        lock.touch(exist_ok=False)            # crude per-dt mutex; stale-lock cleanup on restart
    except FileExistsError as e:
        raise RuntimeError(f"promotion already in progress for {dt_iso}") from e
    try:
        staging = part / f".staging_{uuid.uuid4().hex}.parquet"
        sources = [str(f) for f in temp_files] + ([str(canonical)] if canonical.exists() else [])
        # Streaming dedup by event_id — never materializes to Python.
        con = duckdb.connect()
        con.execute(
            "COPY (SELECT * EXCLUDE(rn) FROM (SELECT *, row_number() OVER "
            "(PARTITION BY event_id ORDER BY ts_ingest DESC) rn FROM read_parquet($files)) WHERE rn=1) "
            "TO $out (FORMAT PARQUET)",
            {"files": sources, "out": str(staging)},
        )
        os.replace(staging, canonical)        # atomic commit FIRST
        for d in {f.parent for f in temp_files}:  # cleanup LAST
            for f in d.glob("*.parquet"):
                f.unlink(missing_ok=True)
            d.rmdir()
        return pq.read_metadata(canonical).num_rows
    finally:
        lock.unlink(missing_ok=True)
```
- [ ] **Step 3:** run tests; **Step 4:** commit `feat(massive): crash-safe streaming promotion`.

### Task 9: `ts_available` from archive trading date + next-session publish (supersedes Correction B)

**Files:** Modify `heber/backfill/massive/writer.py`, `driver.py`; Test `tests/backfill/massive/test_ts_available.py`

- [ ] The driver injects `record["trade_date"] = chunk_date.isoformat()` (the *file* date, authoritative
  — not the bar's UTC timestamp). `MassiveBackfillWriter.set_ts_available` computes from it:

```python
import exchange_calendars as xcals
from datetime import UTC, datetime, time
_XNYS = xcals.get_calendar("XNYS")
PUBLISH_HOUR_UTC = 16

def publish_ts_for(trade_date_iso: str) -> datetime:
    d = datetime.fromisoformat(trade_date_iso).date()
    nxt = _XNYS.next_session(d).date()  # next *trading* session (Fri→Mon, skips holidays)
    return datetime.combine(nxt, time(PUBLISH_HOUR_UTC, 0), tzinfo=UTC)
```
`set_ts_available` reads `record["trade_date"]` (every bar of the date → identical value; not in the
Silver schema, so `from_pylist(schema=)` drops it).
- [ ] **Tests:** all minutes of 2024-01-02 share `ts_available`; a **Friday** bar is available the
  following **Monday** (not Saturday); an after-hours minute that crosses UTC midnight still maps to
  its trading date. Commit.

### Task 10: Reject hardening — header validation, line numbers, rate threshold (supersedes Correction E)

**Files:** Modify `heber/backfill/massive/parser.py`; Test `tests/backfill/massive/test_rejects.py`

- [ ] Validate the exact CSV header (`ticker,volume,open,close,high,low,window_start,transactions`)
  before iterating — a wrong header/delimiter raises immediately (not "every row a reject").
- [ ] `RejectCollector.add(ticker, reason, row, *, line)` records file + line; `flush(path)` writes
  `_vendor_raw/massive/rejects/<run_id>.jsonl`.
- [ ] Driver raises if reject_rate > `0.005` (a flood ⇒ broken parser, not bad data).
- [ ] **Tests:** wrong header raises; a single bad row is captured with its line number; >0.5% rejects
  raises. Commit.

### Task 11: Public chunked-ingest path in Heber's coordinator (supersedes Correction D's private call)

**Files:** Modify `heber/backfill/__init__.py` (Heber enhancement); `heber/backfill/massive/driver.py`; Test `tests/backfill/test_coordinator_chunked.py`

- [ ] Add `BackfillCoordinator.run_job_chunked(backfill_id, definition, chunk_fetcher)` where
  `chunk_fetcher(provider, feed, date, symbols)` is an **async generator** yielding row-chunks. It must
  preserve the full `run_job` lifecycle: `RUNNING→COMPLETED/FAILED`, `rows_written`,
  `progress_dates_completed`, metrics, and `_update_catalog_metadata` — calling `write_batch` per
  chunk. (Read `run_job` at `heber/backfill/__init__.py:711` first and mirror its bookkeeping.)
- [ ] The Massive driver calls `run_job_chunked` (NOT private `_persist_job`), then `promote_date`
  per completed date.
- [ ] **Tests:** a 3-chunk date completes with correct `rows_written`, status COMPLETED, and one
  promoted canonical file. Commit.

### Task 12: Repoint Atlas to the Massive dataset; leave Alpaca `feed=bars` untouched

**Files:** Modify Atlas `atlas/data/bin_cache.py`; add a reader path in `heber/reader/core.py`; Test in each repo

- [ ] **Read first** `heber/reader/core.py` (around the `read_silver("bars")` path) and
  `atlas/data/bin_cache.py` (the `silver/feed=bars/instrument_type=equity` constant) — confirm exact
  current behavior before editing.
- [ ] Add `HeberReader.read_silver_dataset("massive_bars")` (or a `dataset=` arg) that reads
  `silver/massive_bars/dt=*/part-*.parquet`.
- [ ] Point Atlas's bar loader at the Massive dataset (config flag, default to Massive for research
  caches). **Do not** touch the live `feed=bars` write path — Alpaca bars keep flowing for trading.
- [ ] **Test/verify:** Atlas reads Massive rows (incl. a delisted ticker in its era); a count check
  confirms it is *not* reading Alpaca `feed=bars`. Commit.

### Task 13: Dependency + fidelity cleanups

- [ ] Use Heber's existing `exchange-calendars` (remove any `pandas_market_calendars` reference).
- [ ] Preserve the raw pre-normalization vendor ticker in the Bronze JSONL record (Silver `symbol`
  stays normalized). Commit.

### Bronze retention (deploy decision)
Pin `provider=massive` Bronze in Heber retention config (exempt from the 90-day delete), or accept
"rebuildable only from `_vendor_raw`." **Recommendation: pin it** — Bronze is the queryable raw-fidelity
layer and re-deriving from `_vendor_raw` is a manual re-ingest. One-line config change at deploy.
