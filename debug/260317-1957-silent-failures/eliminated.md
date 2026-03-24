# Eliminated Hypotheses — Data-Gateway

Disproven hypotheses narrow the search space and confirm correct areas.

---

| # | Hypothesis | Why Disproven |
|---|-----------|---------------|
| 2 | Finnhub crypto candles no pagination | Finnhub returns all candles in one response — their API doesn't paginate |
| 3 | UW `return []` is silent failure | Intentional guard clauses for optional provider, all logged with warnings |
| 6 | AlphaVantage `[:50]` truncation | Intentional design choice for economic indicators, commented in code |
| 7 | Finnhub `[:50]` truncation | Safety cap on news, Finnhub returns all data in one response |
| 9 | Normalizers silently produce bad values | Uses direct key access (`raw["t"]`), raises KeyError on missing fields — fail-fast |
| 10 | Stock endpoint missing default time range | 24-hour default correctly applied at lines 47-50 |
| 11 | Stream sink silently drops events | Backpressure drops are logged with event_id and tracked in metrics |
| 12 | `execute_alpaca_provider_call` swallows errors | All errors propagated as HTTPException with correct status codes |
| 13 | `_parse_timestamp` corrupts timestamps | Raises ValueError on bad input, simple and correct |
| 15 | Stock trades `[:limit]` post-pagination truncation | Performance inefficiency (fetches then trims) but data correctness is fine |
| 16 | yfinance returns empty on errors | Returns empty for "no data" (valid), exceptions propagate for actual errors |
