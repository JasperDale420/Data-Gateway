# UnusualWhales PIT-Universe Backfill & Slow-Puller Plan

**Date:** 2026-07-03 · **Author:** gateway audit (Claude) · **Status:** awaiting operator go/no-go

New UW entitlement (per 2026-07-03 email): **40,000 calls/day**, **no per-minute cap**, **≥780 days** retrospective.
Goal: stop pulling the ~53 random EOD tickers, point per-ticker capture at the **Atlas V2 PIT liquidity universe**, and
metered-backfill all backfillable UW data for those names using **≤20,000 calls/day** (leaving ≥20k/day for live
flow-alerts / darkpool / tides / EOD).

All backfill-class and depth claims below are **live-API-probed on 2026-07-03 (AAPL/SPY)**, not doc-inferred.
Full per-endpoint classification (104 endpoints): `scratchpad/uw_classifications.json`.

---

## 0. The universe

Source: `Atlasv2/atlas/data/universes/pit_liquidity_top1000.parquet` — survivorship-free, top-1000 by 63-day median
dollar volume, rebuilt quarterly, membership intervals `(symbol, start_date, end_date)`.

| Set | Symbols | Use |
|---|---|---|
| Active as-of 2026-06-09 | **989** | forward EOD capture |
| Active any time in trailing 780d | **1,333** | survivorship-free backfill target |
| All-time | 3,157 | (not used) |

**Decision needed:** backfill the **1,333** trailing-780d union (survivorship-free — matches why the PIT universe
exists) vs. just the **989** live names. Recommendation: **1,333**, ideally honoring each symbol's membership interval
(don't pull a name's history outside the window it was liquid). Cost impact is only +33%.

---

## 1. Two critical corrections found during review (both verified)

1. **The wired backfill engine chunks by `DEFAULT_CHUNK_DAYS = 1`** (`backfill.py:27`). Every UW dispatch fn is called
   once per calendar day with the chunk-start date (`backfill.py:202-274`). So a "1-call returns full history" feed
   (e.g. `earnings`, which ignores dates entirely — `_uw_earnings` line 258) driven over a 780-day window costs
   **780 identical calls/ticker**, not 1. → **Any FULL_SERIES feed must be submitted as a 1-day window** (start==end →
   one chunk → one call → full history), or the engine needs a single-shot fetch mode.

2. **`_uw_greeks` passes `date_str` but not `timeframe`** (`backfill.py:230`), and probing shows
   `greek-exposure?date=<past>` **alone returns EMPTY**. The historical window is driven by `timeframe`, not `date`:
   - `greek-exposure?timeframe=3Y` → **752 rows, 2023→2026** in ONE call ✅
   - `volatility/realized?timeframe=3Y` → **744 rows, 2023→2026** in ONE call ✅
   → The current greek_exposure backfill is effectively **broken for history**; fix = forward `timeframe=3Y`.
   `iv-rank` ignores `timeframe`/`limit` (5 rows always) → not cheaply backfillable; **skip its deep backfill.**

---

## 2. What we can backfill ENTIRELY — cheap tier (do first)

Per-ticker, **1 call/ticker** each when fetched correctly (1-day window for FULL_SERIES; `timeframe=3Y` for series).

| Endpoint | Gives | Depth (probed) | Calls/tkr | Wired? |
|---|---|---|---|---|
| `earnings/{ticker}` | Earnings history (EPS act/est, reaction) | 1995→2026 | 1 | ✅ `earnings` |
| `stock/{ticker}/options-volume?limit=500` | Daily options volume + premium | ~2yr | 1 | provider only |
| `stock/{ticker}/greek-exposure?timeframe=3Y` | Daily GEX (Δ/Γ/vanna/charm) | 3yr | 1 | ✅ `greek_exposure` (needs `timeframe` fix) |
| `stock/{ticker}/volatility/realized?timeframe=3Y` | IV vs 30d realized vol | 3yr | 1 | provider only |
| `stock/{ticker}/insider-buy-sells` | Aggregated insider buy/sell series | 2003→2026 | 1 | provider only |
| `shorts/{ticker}/ftds` | Failures-to-deliver | 2021→2026 | 1 | ✅ `ftds` (EOD) |
| `shorts/{ticker}/interest-float` | Short interest %, float, days-to-cover | 2021+ | 1 | ⚠️ overlaps EOD `short_interest` |
| `shorts/{ticker}/volume-and-ratio` | Daily short volume + ratio | multi-yr | 1 | ✅ `short_volume` (EOD) |
| `shorts/{ticker}/data` | Borrow rate + shares available | multi-yr | 1 | provider only |
| `shorts/{ticker}/volumes-by-exchange` | Short volume by exchange | ~2yr | 1 | provider only |
| `institution/{ticker}/ownership` | 13F institutional ownership history | quarterly | ~2 | provider only |
| `seasonality/{ticker}/monthly` + `/year-month` | Seasonal aggregates | multi-yr | 2 | none |
| `etfs/{ticker}/in-outflow` (ETFs only) | ETF create/redeem daily flow | ~3yr | 1 | provider only |

**Skip:** `ohlc/1d` (use Alpaca bars — already wired); `iv-rank` (doesn't respect `timeframe`).

### Market-wide (ticker dimension collapses — cheap, take all)

`market/market-tide?date=` (~535 per-day), `market/{sector}/sector-tide` (11×535≈5,900), `earnings/afterhours`+`premarket`
(~1,070), `net-flow/expiry` (default combo ~535), `market/oi-change|spike|top-net-impact` (~535 each),
`congress/*` + `insider/transactions` + `institutions/latest_filings` (dozens each), `seasonality/market`+`{month}/performers` (13),
`market/total-options-volume?limit=500` (1). **Total ≈ 13k calls, <1 day.**

---

## 3. The budget sink — per-day snapshot feeds (Tier 3)

One call = one date → **~535 trading days/ticker**. These blow the budget; sample hard.

High-value candidates: `darkpool/{ticker}` (⚠️ 500-row/day cap → liquid names page, ~1.5-2× factor),
`net-prem-ticks` (1-min), `greek-flow` (needs date param), `spot-exposures` (depth only ~2025+ → ~365d),
`greek-exposure/strike`, `greek-exposure/expiry`, `interpolated-iv`, `oi-change` (⚠️ probed EMPTY at 700d — **depth
uncertain, investigate before committing**).

Cost of ONE such feed, full universe, full depth: **1,333 × 535 ≈ 713k calls ≈ 36 days at 20k/day.**
All ~7 chosen feeds unsampled ≈ **millions of calls / >1 year — NOT viable.**

**Sampling levers:** top-200 most-liquid subset (not 1,333) · 90 trading days for intraday microstructure (not 535) ·
only ~5-7 chosen feeds deep-backfilled, the rest **live-poll-forward only**.
Realistic sampled Phase-3 ≈ **500k-700k calls ≈ 4-7 weeks at ≤20k/day.**

---

## 4. Do-not-backfill (live-poll or skip)

- **≤14-day retention (impossible):** `flow-alerts` (both), `nope` (probed empty at 700d), `full-tape` (3-day wall + Advanced sub).
- **No date param → current snapshot only:** `greeks`, `stock-state`, `max-pain`, `oi-per-strike|expiry`, `option-chains`,
  `atm-chains`, `expiry-breakdown`, `flow-recent`, `screener/*`, `news/headlines`, `insider/{ticker}` roster,
  `etfs/{ticker}/holdings|weights|exposure|info`.
- **Reference/static dims (pull once):** `stock/{ticker}/info`, `institutions`, `politician-portfolios/people`.
- **Skip outright:** all `politician-portfolios/*` (enterprise/uncertain access), `greek-exposure/strike-expiry`,
  `option-contract/{id}/*` per-contract×per-day (intractable unless gated to a liquid contract set),
  `market/correlations` + `volatility/stats` (derivable).

---

## 5. Corrected cost model (1,333 tickers, 535 trading days)

| Bucket | Calls | Days @ 20k/day |
|---|---|---|
| Cheap per-ticker (Tier-1 + greek-exp/realized `timeframe=3Y`) ≈ 13/tkr | ~17,500 | <1 |
| Market-wide + sector (default combos) | ~13,000 | <1 |
| **Cheap grand total (Phase 1)** | **~30,000** | **~1.5-2** |
| One Tier-3 feed, full universe/depth | ~713,000 | ~36 |
| Tier-3 sampled (top-200, 90-535d, ~7 feeds, darkpool paging) | ~500-700k | ~4-7 weeks |
| **Forward EOD capture after universe swap** (989-1,333 × ~8 feeds/day) | **~8-10k/day ongoing** | — |

**Budget reconciliation:** live flow/darkpool/tides ≈ 5k/day today; forward EOD over the new universe ≈ 8-10k/day;
slow-puller backfill ≤ ~10k/day → **~23-25k/day of the 40k cap.** Comfortable. The ≤20k "slow-puller" ceiling must
cover **both** forward EOD capture **and** historical backfill — trimming the forward EOD feed set (below) matters.

---

## 6. Rollout

### Phase 0 — code fixes (½ day, before any bulk run)
1. Forward `timeframe=3Y` in `_uw_greeks`; add `volatility/realized` provider method + dispatch with `timeframe`.
2. Add provider+dispatch for the unwired FULL_SERIES feeds: `options-volume?limit=500`, `insider-buy-sells`,
   `shorts/{data,interest-float,volumes-by-exchange}`, `institution ownership`, `seasonality`, `etf in-outflow`.
   Apply the `instrument_type_override="equity"` gotcha (CLAUDE.md) for any feed carrying `strike`/`expiry`.
3. For FULL_SERIES feeds, submit backfills as **1-day windows** (or add a single-shot fetch mode) to dodge `chunk_days=1`.
4. Raise `_uw_darkpool_ticker` limit 200→500.
5. **Investigate `oi-change` historical depth** (probed empty at 700d) before sizing it.

### Phase 1 — cheap high-value bulk (~2 days, ~30k calls)
All Tier-1 + greek-exposure/realized-vol (`timeframe=3Y`) + all market-wide, over the 1,333-name universe. One-time.

### Phase 2 — universe swap for forward capture
Point EOD per-ticker capture at the PIT universe (export active list → `GATEWAY_UW_CORE_TICKERS`/universe file,
`dynamic_count=0`). **Trim the forward EOD feed set** to the cheap high-value ones; drop per-day feeds from daily
forward capture (backfill them sampled instead). Bump `uw_eod_concurrency` and verify the EOD window completes for ~1,000 tickers.

### Phase 3 — Tier-3 sampled backfill (4-7 weeks, metered ≤ remaining budget)
Top-200 liquid subset · 90-535d depth · ~5-7 chosen feeds · resumable progress state · never fan a per-day feed across
1,333 × 535.

---

## 7. Slow-puller design (Phase 2-3 mechanism)

A resumable background scheduler over the `(symbol × feed × date-range)` matrix:
- Reads the PIT universe (active membership).
- Walks a priority-ordered feed list; submits BackfillEngine jobs feed-by-feed.
- Enforces a **daily call budget** (config, default ≤10-15k for backfill so forward EOD + live fit under 40k).
- Persists per-(symbol,feed) completion cursor (like `UwEodStateStore`) so restarts resume, not restart.
- Stops for the day when budget hit; resumes next day. "Slowly pulls in all the data" = weeks to drain, by design.

Open question: build as a new gateway background service vs. a standalone driver script hitting `/api/v1/backfill`.
Recommendation: **standalone driver first** (simplest, observable, killable), promote to a gateway service only if it
needs to survive restarts unattended.
