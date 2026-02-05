# Provider Alignment Audit (PRD/API Contract)

Date: 2026-02-05
Scope: `uw`, `finnhub`, `alphavantage`, `sec`, `yf`

## Goal

Validate provider REST route alignment and error-shape consistency against the project contracts.

## Method

- Static route extraction from provider routers:
  - `gateway/api/uw/*.py`
  - `gateway/api/finnhub/*.py`
  - `gateway/api/alphavantage/*.py`
  - `gateway/api/sec.py`
  - `gateway/api/yf.py`
- Contract comparison against:
  - `API_REFERENCE.md` provider endpoint tables
  - `PRD.md` provider endpoint sections/examples
- Error-shape scan of `HTTPException(...)` usage in provider routers.

## Coverage Status

- Audited in this run:
  - Route inventory for all five providers.
  - Documentation parity against `API_REFERENCE.md`.
  - Error-code consistency scan.
- Not fully auditable in this run:
  - Strict PRD endpoint parity for all providers, because PRD does not define exhaustive endpoint lists for every provider.

## Route Inventory (Code)

| Provider | Implemented Routes |
|---|---:|
| UW | 125 |
| Finnhub | 45 |
| Alpha Vantage | 30 |
| SEC | 10 |
| yfinance | 16 |

## API_REFERENCE Drift Summary

`API_REFERENCE.md` currently provides simplified endpoint tables that diverge from implemented route paths.

| Provider | Documented in API_REFERENCE | Implemented in Code | Documented But Missing in Code | Implemented But Missing in Docs |
|---|---:|---:|---:|---:|
| UW | 19 | 125 | 16 | 121 |
| Finnhub | 16 | 45 | 16 | 45 |
| Alpha Vantage | 16 | 30 | 15 | 29 |
| SEC | 10 | 10 | 0 | 0 |
| yfinance | 11 | 16 | 0 | 5 |

### Representative path mismatches

- UW docs mention `/flow`, `/darkpool`, `/institution/{name}` while code exposes paths like `/flow/all`, `/darkpool/all`, `/institutions/{institution_id}/holdings`.
- Finnhub docs mention `/stock/profile/{symbol}`, `/stock/metric`, `/calendar/earnings` while code exposes `/profile/{symbol}`, `/metrics/{symbol}`, `/earnings`.
- Alpha Vantage docs mention `/company/{symbol}`, `/income/{symbol}`, `/economy/gdp` while code exposes `/overview/{symbol}`, `/income-statement/{symbol}`, `/economic/{indicator}`.
- yfinance code has additional routes not reflected in docs: `/ticker/{symbol}/actions`, `/ticker/{symbol}/splits`, `/ticker/{symbol}/news`, `/ticker/{symbol}/major-holders`, `/ticker/{symbol}/calendar`.

## Error Contract Consistency

Provider routes are using `response_model=SuccessResponse`, but provider errors are mostly raised as plain `HTTPException(detail=...)` without gateway error codes.

| Provider | `HTTPException` Raises Without `error.code` |
|---|---:|
| UW | 7 |
| Finnhub | 92 |
| Alpha Vantage | 42 |
| SEC | 21 |
| yfinance | 32 |

Impact:
- Error payloads are not reliably machine-parseable by a stable gateway code taxonomy (`GW-EXXXX`).
- Client-side retry/alert routing logic has to parse provider-specific strings.

## PRD Contract Findings

- `PRD.md` includes high-level provider coverage and examples.
- `PRD.md` does not currently contain a complete, versioned endpoint matrix for all five providers.
- Because of this, full automated PRD-vs-route parity checks are not possible for most provider paths.

## Actionable Remediation

1. Replace hand-maintained provider endpoint tables in `API_REFERENCE.md` with generated route listings from OpenAPI.
2. Add provider error helpers that always produce: `{"success": false, "error": {"code": "GW-...", "message": "..."}}`.
3. Add a PRD section that references a generated endpoint contract artifact per release.
4. Add CI check that detects route drift between code and docs.

## Audit Outcome

- New technical debt introduced in `AUDIT_TECHNICAL_DEBT.md`:
  - `TD-032` provider API-reference drift
  - `TD-033` provider error-shape inconsistency
  - `TD-034` PRD under-specification for provider endpoint contracts
