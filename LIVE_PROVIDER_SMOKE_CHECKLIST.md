# Live Provider Smoke Checklist

Use this checklist for runtime verification against real upstream providers.

## Preconditions

- `.env` has live credentials for provider keys used by this repo.
- Outbound network access is available.
- Provider account limits/permissions are known (especially free-tier throttles).

## Automated smoke pass

Run:

```bash
python scripts/live_provider_smoke.py --out LIVE_PROVIDER_SMOKE_REPORT.md
```

Review:
- `LIVE_PROVIDER_SMOKE_REPORT.md`
- Look for `health=ok` and `sample=ok` (or `sample=rate_limited` for known throttles).

## Manual API sanity checks (optional but recommended)

If gateway server is running locally:

```bash
curl -s -H "X-Gateway-Key: <your_key>" "http://localhost:8080/api/v1/alpaca/stocks/AAPL/quote"
curl -s -H "X-Gateway-Key: <your_key>" "http://localhost:8080/api/v1/finnhub/quote/AAPL"
curl -s -H "X-Gateway-Key: <your_key>" "http://localhost:8080/api/v1/uw/market/tide"
curl -s -H "X-Gateway-Key: <your_key>" "http://localhost:8080/api/v1/sec/company/ticker/AAPL"
```

Expected:
- Authenticated requests return non-401 responses.
- Payloads have data or explicit provider/rate-limit errors.
- No silent empty success for known liquid symbols (`AAPL`).

## Rate-limit behavior checks

- Re-run provider calls quickly in succession.
- Confirm provider throttles surface as explicit errors (`429`, `rate limit`, or documented provider note).
- Confirm gateway does not mislabel rate-limit responses as generic internal errors.
