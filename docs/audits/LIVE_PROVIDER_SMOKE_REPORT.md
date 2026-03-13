# Live Provider Smoke Report

Generated: 2026-02-05T21:43:28.838578+00:00

## Scope

- Providers checked: alpaca, finnhub, alphavantage, unusual_whales, sec
- Checks: provider `health_check()` + one minimal sample live call

## Results

| Provider | Credential Env | Credential Present | Loaded | Health | Sample | Notes |
|---|---|---:|---:|---|---|---|
| `alpaca` | `APCA_API_KEY_ID` | yes | yes | `ok` | `ok` | alpaca quote returned data |
| `finnhub` | `FINNHUB_API_KEY` | yes | yes | `ok` | `ok` | finnhub quote returned data |
| `alphavantage` | `ALPHAVANTAGE_API_KEY` | yes | yes | `ok` | `empty` | alphavantage quote returned no data |
| `unusual_whales` | `UNUSUAL_WHALES_API_KEY` | yes | yes | `ok` | `ok` | unusual_whales market tide returned data |
| `sec` | `SEC_USER_AGENT` | no | yes | `ok` | `ok` | sec company lookup returned data |

## Interpretation

- `ok`: live check passed
- `rate_limited`: credentials are valid but provider throttled request
- `empty`: request succeeded but returned no payload
- `error`/`fail`: connectivity, auth, or API contract issue
