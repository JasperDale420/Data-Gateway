# Massive Historical + Incremental Bar Ingestion into Heber — Design

- **Date:** 2026-06-09
- **Status:** Draft — pending user review
- **Author:** Jacob + Claude
- **Repos touched:** Heber (primary — new ingestion adapter), Data-Gateway (existing `massive` provider; source of S3 creds)

## 1. Goal

Build a survivorship-free, raw, self-adjustable US equity bar dataset (daily + minute,
2003-09-10 → present, **including delisted tickers**) in Heber long-term storage, sourced
from Massive (formerly Polygon.io) S3 flat files. Support a one-time historical bulk load
during a single paid subscription month, plus an ongoing daily incremental top-up that keeps
the corpus current using the same code path.

### Why this exists

The Atlas v2 research pipeline currently trains on yfinance OHLCV, which lists only *currently
active* tickers — baking survivorship bias into every backtest (the short side of a long-short
book is populated by companies already known to have survived). yfinance structurally cannot
fix this. Massive flat files are "the entire market that traded on day T," so delisted names
are present by construction. We store **raw** OHLCV and adjust ourselves (auditably) rather
than trusting a vendor's opaque `auto_adjust`.

## 2. Scope

### In scope
- Bulk download of Massive **day-aggregates** and **minute-aggregates** flat files (full history).
- Immutable local archive of the raw vendor `.csv.gz` files (`_vendor_raw/`).
- Parse + normalize → ingest into Heber Bronze + Silver via Heber's native `BackfillWriter`.
- A one-time REST sweep of corporate-actions metadata: **splits**, **dividends**, and the
  **all-tickers (incl. delisted)** universe — captured during the paid month, archived as JSON.
- Daily incremental updater (same adapter, gap-driven) on a launchd schedule.

### Out of scope (explicitly)
- **Trades / quotes flat files** (~8.5 TB / ~37 TB) — not needed for factor modeling; re-subscribe
  later if ever required.
- **Routing the bulk load through Data-Gateway's live Redis sink** — it caps streams at 100k
  entries and keeps a 24h dedup cache; it cannot hold tens of millions of historical bars.
- **Split/dividend adjustment modeling** (Silver→Gold). This spec *captures* the corporate-actions
  data; applying it to produce adjusted prices is a follow-on effort.
- **Intraday/real-time bars.** Flat files are EOD batch artifacts. Live trading data is already
  served by Data-Gateway from Alpaca and is a different consumer.

## 3. Two paths (and why this plan only builds one)

| Path | Mechanism | Use |
|---|---|---|
| **Bulk + incremental historical** (this plan) | Massive S3 flat files → parse → **Heber `BackfillWriter`** → Bronze + Silver. Bypasses Data-Gateway. | The research corpus: full history + daily top-up. |
| Live/real-time | Data-Gateway `massive` REST provider → Redis → Heber consumer | Real-time trading data. **Not used here**; the `massive.py` provider stays available but idle for this dataset. |

Keeping the historical corpus on a **single** pipeline (flat files only, for both backfill and
daily updates) is a hard requirement: stitching flat-file history to REST-fed recent data would
create a seam where `vwap` presence, timestamp conventions, adjustment handling, and delisted
coverage differ — exactly the kind of artifact that poisons a survivorship-sensitive model.

## 4. Data flow

```
            PAID-MONTH WINDOW (download only)              ANYTIME (can run post-cancel)
  ┌─────────────────────────────────────────────┐   ┌──────────────────────────────────────┐
  │ Massive S3 (files.massive.com, bucket        │   │ _vendor_raw/.csv.gz  (immutable)     │
  │  flatfiles)                                   │   │        │                              │
  │   us_stocks_sip/day_aggs_v1/YYYY/MM/*.csv.gz  │   │        ▼                              │
  │   us_stocks_sip/minute_aggs_v1/YYYY/MM/*.gz   │   │  MassiveFlatFileParser               │
  │        │  boto3 GetObject                     │   │   (CSV → record dicts, ticker         │
  │        ▼                                       │   │    normalize + instrument_key         │
  │  MassiveFlatFileDownloader ───────────────────┼──▶│    validate, ns→ts, field map)        │
  │   → _vendor_raw/ + manifest(size/ETag/sha256) │   │        │                              │
  └─────────────────────────────────────────────┘   │        ▼                              │
  ┌─────────────────────────────────────────────┐   │  Heber BackfillCoordinator/Writer     │
  │ Massive REST (api.massive.com, REST key)     │   │   write_batch(job, records, dt)       │
  │  /v3/reference/tickers?active=false  (universe)│  │        │                              │
  │  /v3/reference/splits, /v3/.../dividends      │   │        ├─▶ Bronze  JSONL.gz           │
  │        │                                       │   │        └─▶ Silver  Parquet            │
  │        ▼                                       │   │            (feed=bars,                │
  │  MassiveRestMetadataSweeper → _vendor_raw/    │   │             instrument_type=equity)   │
  │   massive_corp_actions/*.json                 │   └──────────────────────────────────────┘
  └─────────────────────────────────────────────┘
```

Critical scheduling insight: **only the download + REST sweep are gated by the paid
subscription.** Parsing and ingestion read from the local `_vendor_raw/` archive and can run
*after* the subscription is cancelled. So the paid month's critical path is maximizing raw
downloads, not finishing ingestion.

## 5. Components (each independently testable)

1. **`MassiveFlatFileDownloader`** — boto3 S3 client against `files.massive.com`/`flatfiles`.
   Enumerates date-partitioned keys for `day_aggs_v1` and `minute_aggs_v1`, downloads to
   `_vendor_raw/`, writes a manifest row per file `(s3_key, size, etag, sha256, downloaded_at)`.
   Idempotent/resumable: skips files already present whose sha256 matches the manifest.
2. **`MassiveRestMetadataSweeper`** — REST client (needs a valid REST key). Pulls all splits,
   all dividends, and the full ticker universe (`active=true` + `active=false`), paginating
   `next_url`. Archives raw JSON to `_vendor_raw/massive_corp_actions/`.
3. **`MassiveFlatFileParser`** — pure function: a `.csv.gz` path → iterator of Heber record dicts.
   Handles field mapping, `window_start` ns→timestamp, ticker normalization, and
   `instrument_key` validation. No I/O beyond reading the one file.
4. **Heber ingestion driver** — builds a `BackfillJobDefinition` and calls
   `BackfillWriter.write_batch(job, records, chunk_date)` per date partition, using
   `BackfillCoordinator` for chunking + resumable job state. Supports `--full` and
   `--incremental` (gap-driven via `GapDetector`) modes.
5. **Incremental scheduler** — a launchd plist invoking the driver in `--incremental` mode daily
   (~noon ET; Massive publishes day T's file ~11:00 ET on T+1).

## 6. Schema & field mapping

Heber's Silver bars schema is **already defined** — we map onto it, we do not invent it.

| Massive flat-file column | → Heber Silver / record field | Notes |
|---|---|---|
| `ticker` | `symbol`, and `instrument_key = equity:{TICKER}` | normalized; see §7 |
| `window_start` (Unix **ns**) | `bar_start_ts` and `ts_event` | convert ns→UTC datetime |
| `open` | `open` | raw |
| `high` | `high` | raw |
| `low` | `low` | raw |
| `close` | `close` | raw |
| `volume` | `volume` | raw |
| `transactions` | `trade_count` | optional |
| — | `vwap` | **null** — flat files omit vwap |
| — | `timeframe` | `"1d"` (day_aggs) / `"1m"` (minute_aggs) — exact token TBD, see §11 |
| — | `provider` | `"massive"` |
| — | `feed` | `"bars"` |
| — | `instrument_type` | `"equity"` |
| — | `quality_flags` | `["backfill"]` (added by `BackfillWriter`) |

### `ts_available` policy (leakage-critical)

`ts_available` is Heber's anti-leakage gate (a row is visible to an as-of-date `D` only if
`ts_available <= D`). The **default `COMMIT` policy would stamp every historical bar with the
backfill run time (June 2026)** — collapsing all history to "available now" and making any
point-in-time backtest as-of a past date see zero bars. That is wrong and must be avoided.

Use an **event-time policy** so `ts_available` tracks each bar's real-world availability:
- Daily bars: `ts_available = bar_date close + publish lag` (Massive publishes ~11:00 ET on T+1;
  T+1 11:00 ET is a safe, honest value).
- Minute bars: because we obtain them only via the EOD flat file, their honest pipeline
  availability is also T+1 (documented limitation — this corpus supports *daily-cadence*
  features over minute bars, **not** same-day intraday signals).

Concrete mechanism (`TsAvailablePolicy.CUSTOM` vs `EVENT` + delay) to be confirmed against
Heber's API in §11.

## 7. Ticker → `instrument_key` normalization

Heber validates equity keys against `^equity:[A-Z0-9]+(?:[.-][A-Z0-9]+)*$`.

- Massive tickers are uppercase; standard symbols and class shares (`BRK.A`, `BRK.B`) pass as-is.
- Build `equity:{TICKER}` and validate **before** calling `write_batch`.
- **Rows whose ticker fails the regex are never silently dropped and never written.** They are
  written to a `rejects/` sidecar log with the file, line, ticker, and reason, and counted.
  We review the reject report; if a real class of symbols is being rejected, we add an explicit
  normalization rule. (Per Empire policy: skip bad items loudly, never store malformed data.)

## 8. Storage layout (on `/Volumes/heber`)

```
/Volumes/heber/data/
  _vendor_raw/massive/                      # NEW — immutable, irreplaceable after cancel
    us_stocks_sip/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
    us_stocks_sip/minute_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
    massive_corp_actions/{splits,dividends,tickers}-<sweep_ts>.json
    manifest.jsonl                          # size + ETag + sha256 per file
    rejects/<run_id>.jsonl                  # rows that failed normalization/validation
  bronze/provider=massive/feed=bars/dt=YYYY-MM-DD/hour=HH/events-*.jsonl.gz   # Heber-managed
  silver/feed=bars/instrument_type=equity/dt=YYYY-MM-DD/part-*.parquet         # Heber-managed
  backfill/jobs/<backfill_id>.json          # Heber-managed resumable job state
```

## 9. Retention

- **Keep the vendor raw `.csv.gz` forever.** After the subscription is cancelled they are
  irreplaceable; ~107 GB total (day + minute) is trivial insurance against a future parsing/mapping
  fix or a decision to capture more fields.
- The archive is the **ground-truth** root of lineage (`vendor raw → Bronze → Silver → Gold`);
  Bronze is *derived* (parsed envelopes) and rebuildable from the archive.
- **To verify (§11):** whether Heber expires Bronze on a retention timer. If it does, the
  `_vendor_raw/` archive is the *only* durable raw copy — which makes keeping it mandatory.

## 10. Error handling

- **Download:** retry with backoff on transient S3 errors; resumable via manifest (skip verified
  files). Verify each file's sha256 against the recorded ETag/size; corrupt downloads are
  re-fetched, not ingested.
- **Parse:** malformed CSV rows → `rejects/` log, counted, never written.
- **Validation:** `instrument_key` regex failures → `rejects/` log (§7).
- **Write:** `BackfillWriter` exceptions fail the affected date chunk; the run is resumable from
  Heber's persisted job state. No partial-silently-dropped chunks.
- **Integrity gate:** post-ingest, `GapDetector` confirms there are no missing trading days in the
  Silver partitions for the ingested range.

## 11. Open items to verify during implementation

These were read from a code exploration of Heber and must be confirmed against the live API:
1. Exact `BackfillWriter` / `BackfillCoordinator` / `BackfillJobDefinition` signatures and the
   exact **record dict shape** `write_batch` expects (flat envelope fields vs nested `payload`).
2. Whether `BackfillWriter` itself runs `instrument_key` validation, or whether we must (we will
   validate regardless).
3. Heber's exact accepted **`timeframe` tokens** (`"1d"`/`"1m"` vs `"1Day"`/`"1Min"`).
4. The exact `TsAvailablePolicy` API for encoding event-time availability with a publish lag.
5. Whether Heber has a **Bronze retention/expiry** policy (affects §9).
6. Confirm `provider="massive"` is acceptable (Heber's provider field is an unconstrained string;
   existing data used `"alpaca"`/`"polygon"`).
7. A **valid REST API key** (the current `22O3…` key 401s) — required for the §2 corporate-actions
   sweep. The S3 flat-file creds cannot fetch REST endpoints.

## 12. Run plan for the paid month (ordered)

1. Subscribe to Massive **Advanced ($199/mo, personal)** — unlocks full-history downloads.
2. **Regenerate a valid REST API key** (for the corporate-actions sweep).
3. `MassiveFlatFileDownloader --dataset day_aggs --full` → archive (~1.1 GB, fast).
4. `MassiveFlatFileDownloader --dataset minute_aggs --full` → archive (~106 GB, bandwidth-bound;
   the long pole — start early).
5. `MassiveRestMetadataSweeper` → splits + dividends + all-tickers (active & inactive) JSON.
6. **Verify archive completeness** (manifest covers every trading day 2003-09-10 → yesterday for
   both datasets). The archive — not the ingestion — is the thing that must be complete before
   cancelling.
7. **Cancel the subscription** once the archive + REST sweep are verified complete.
8. *(Post-cancel, no time pressure)* Ingest archive → Heber: day_aggs then minute_aggs, via the
   Heber driver; verify with `GapDetector` + row-count + spot-checks (including a known delisted
   name, e.g. SIVB/FRC, appearing in its trading era).
9. Install the daily **incremental** launchd job — but note: it requires an active subscription to
   fetch new days. *(Decision point for later: keep a minimal/cheaper plan active for daily
   top-ups, or accept the corpus is frozen at cancellation and re-subscribe in batches.)*

## 13. Testing strategy

- **Unit:** `MassiveFlatFileParser` field mapping (ns→ts, all columns), ticker normalization
  (pass `BRK.A`; reject + log a synthetic bad ticker), vwap-null handling. Pure-function, no I/O.
- **Unit:** downloader manifest/resume logic (skip-if-verified) with a faked S3 client.
- **Integration:** parse one real archived `.csv.gz` → `write_batch` into a temp Heber storage
  root → read back the Silver parquet and assert schema + row count + a spot-checked bar.
- **Integration:** `--incremental` gap-fill — seed Silver with a gap, run, assert the gap closes.
- **Leakage check:** assert `ts_available` for a 2015 bar is ~2015 (not the run date).

## 14. Success criteria

- Silver `feed=bars` partitions exist continuously from 2003-09-10 → present for day_aggs, with
  delisted tickers present in their trading eras.
- Raw vendor `.csv.gz` archived with a verified integrity manifest; nothing deleted.
- A known delisted ticker resolves to valid `equity:{TICKER}` rows in Silver.
- `ts_available` reflects event-time availability (point-in-time backtests as-of past dates see
  the right rows).
- The daily incremental job closes gaps idempotently.
- Reject report is empty or contains only genuinely non-equity/malformed symbols, each explained.

---

## 15. Adversarial-review corrections (2026-06-10)

A two-session adversarial review (Codex `gpt-5.5`/`xhigh`) cross-checked this spec and Plan 1
against the real Heber source. The items below **supersede** the earlier sections they cite and
are the binding design.

### Confirmed against real Heber code (no change needed)
- `BackfillCoordinator.run_job` calls `await data_fetcher(provider=, feed=, date=, symbols=)`;
  the `writer_factory(storage_root=, ts_available_policy=, custom_delay_seconds=)` injection
  works; `BackfillWriter.__init__` accepts `parquet_writer`. (`heber/backfill/__init__.py`)
- Heber pins **pyarrow 23.0.0**, where `pa.Table.from_pylist(records, schema=...)` **drops**
  extra keys — so the writer's added `backfill_id` is safely ignored by the canonical schema.
- The equity key regex `^equity:[A-Z0-9]+(?:[.-][A-Z0-9]+)*$` **accepts** dot/hyphen class and
  preferred/warrant forms (`BRK.PRA`, `ABC.WS`, `ABC.U`, `ABC-WT`). Only slash/caret/space/
  double-punctuation forms (`ABC/WS`, `ABC^A`, `ABC WI`, `AAPL..WS`) fail — a much smaller
  reject class than §7 feared. **Still** preserve the raw vendor ticker and review reject classes.

### Binding corrections (supersede earlier sections)

1. **`ts_available` is a fixed per-trading-date publish timestamp — NOT per-row `ts_event + delay`** (supersedes §6).
   Heber's `CUSTOM` policy sets `ts_available = ts_event + custom_delay_seconds` **per row**, which
   would *stagger* minute bars (each minute available +1 day from its own time). Instead, a custom
   `BackfillWriter` subclass overrides `set_ts_available` to assign **every bar of trading date T the
   same value** `T+1 16:00 UTC` (≈ the next-morning ET flat-file publish). Day-granularity-correct,
   non-staggered, and never earlier than the bar date. (Plan 1 §"corrections" has the code.)

2. **Temp→canonical promotion + idempotency are NOT automatic — we must build them** (supersedes the
   reliance on the compactor in §4/§12.5). `BackfillWriter` writes nested
   `silver/massive_bars/dt=.../_backfill_<id>/*.parquet`; `Compactor.compact_partition` does **not**
   descend into `_backfill_*` dirs, and nothing promotes them. Re-running a date just makes sibling
   temp dirs (duplicate rows). Required: an explicit promotion step that, per `dt=`, reads all
   `_backfill_*` + any canonical parquet, **dedups by `event_id`**, writes canonical files
   atomically (temp + `os.replace`), then removes the temp dirs. Deterministic `event_id` makes this
   idempotent across re-runs.

3. **Storage path correction** (supersedes the §8 Silver diagram). The backfill writer lands Silver at
   `silver/massive_bars/dt=YYYY-MM-DD/` (the `{provider}_{feed}` layout), **not**
   `silver/feed=bars/instrument_type=equity/dt=...`. No provider-name collision with the live
   `alpaca`/`polygon` bars, but the Atlas reader must read `massive_bars`.

4. **Bronze retention is 90-day delete — confirmed** (resolves the §9/§11 open question). Heber's
   retention defaults delete Bronze after 90 days (`heber/retention/`). Therefore the `_vendor_raw/`
   archive is the **only durable raw copy** (keeping it is mandatory, not optional), and Massive
   Bronze must either be **pinned** in Heber retention config or explicitly documented as
   "rebuildable only from `_vendor_raw`."

5. **Minute ingestion must be streamed/chunked** (supersedes the implicit "one list per date" in §4).
   `run_job`/`write_batch` materialize full per-date lists and a full in-memory Arrow table; a single
   minute file is millions of rows. The minute path must read the CSV in bounded row-chunks and call
   `write_batch` per chunk. Daily aggregates (~10K rows/file) are fine as-is.

6. **Gap/coverage verification uses a trading calendar + row counts — not `GapDetector`'s dir scan**
   (supersedes §4/§12 reliance on `GapDetector`). `GapDetector` registers a `dt=` dir as "covered"
   (including un-promoted temp dirs) and iterates **calendar** days, so weekends/holidays are
   permanent false gaps. Use an exchange calendar (e.g. `pandas_market_calendars` XNYS) to enumerate
   expected trading days and verify **non-empty readable parquet row counts** for completeness.

7. **Missing expected files fail loud** (new). The archive-backed fetcher returns `[]` for a missing
   file and `run_job` would still mark the date complete at 0 rows. The driver must distinguish a
   known non-trading day (skip) from a **missing expected trading day** (raise) so a download gap can
   never masquerade as a completed ingest.

8. **Durable rejects** (reaffirms §7/§10, currently unimplemented in Plan 1). Rejects must be written
   to `_vendor_raw/massive/rejects/<run_id>.jsonl` with file, line, raw row, and reason — not held in
   memory. The parser must catch **parse/validation** errors broadly (numeric/timestamp), not only
   ticker-regex failures, so one malformed row never aborts a whole chunk.

9. **Survivorship is not delivered by Plan 1 alone** (gates the goal). Plan 1 produces raw bars only.
   The survivorship-free *universe* and the ability to *adjust* require the splits/dividends/
   all-tickers REST sweep (Plan 2). Therefore: the REST metadata sweep is a **pre-cancellation
   blocker**, and Atlas must not treat this dataset as survivorship-free until universe reconstruction
   + adjustment exist.

10. **Cancellation freezes the corpus** (clarifies §12.9). Parsing/ingesting from `_vendor_raw/` works
    post-cancel, but **daily top-ups require an active subscription**. After cancellation the dataset
    is frozen until either a (cheaper) tier is kept active or a batch re-subscription is scheduled.
    The spec must not imply "continuous updates" survive cancellation.
