# Developer Notes

Practical notes for contributors working inside Data Gateway.

## High-Signal Gotchas

- Authentication header is `X-Gateway-Key` for protected REST endpoints.
- WebSocket clients must authenticate quickly after connection (`/ws` auth timeout is enforced).
- Provider permissions are controlled per client in `config/clients.yaml`.
- Provider registration and capability flags come from `config/providers.yaml`.
- The Unusual Whales SDK is a local submodule (`unusualwhales_sdk/`) and must be initialized after clone.

## Error-Log Severity & Double-Logging

- **4xx vs 5xx split.** Provider and API HTTP errors are logged by status class: `4xx → logger.warning`, `5xx → logger.error`. See `gateway/api/alpaca/common.py` (`_handle_alpaca_error`, and the `APIError` / `httpx.HTTPStatusError` branches in `execute_alpaca_provider_call`) and `gateway/providers/alpaca/market.py` (`get_bars`/`get_quotes`/`get_trades`, `log = logger.warning if status < 500 else logger.error`). A *flood of 4xx WARNINGs* is almost always a misconfigured client, not a gateway bug — e.g. requesting an index symbol like `SPX` from `/v2/stocks/bars` returns a `400`. Fix the caller; don't silence the log.
- **Provider + API double-logging.** The same upstream 4xx is logged twice for REST callers: once at the provider layer (`alpaca_bars_error` / `alpaca_quotes_error` in `gateway/providers/alpaca/market.py`) and once at the API layer (`provider_request_failed` in `gateway/api/alpaca/common.py`), both at WARNING. Non-REST callers — the pollers (`gateway/core/*_poller.py`), backfill engine, and option capture — go through the provider methods directly and so only hit the provider-layer log. When grepping error volume, expect roughly 2× the event count for REST-path 4xx versus poller-path 4xx.

## Retry Semantics

- `http_retry` (`gateway/core/http_client.py`) retries **only** transport/timeout errors and HTTP status `{429, 502, 503, 504}` (see `_should_retry_http_error`), 3 attempts with exponential backoff (1–10s). Every other status — including all `4xx` and `500` — is a **single attempt**: it raises on the first failure with no retry. So a `400` from a bad symbol or a `500` from upstream is surfaced immediately; a transient `503` is retried.

## Envelope Instrument-Type Inference

- `_infer_instrument_type` (`gateway/core/envelope.py`) flags any payload carrying `strike`/`expiry` as `instrument_type=option`. That is correct for options-flow / option-contract feeds, but **wrong for per-underlying analytics that happen to include an expiry** (e.g. `iv_term_structure`): it produces malformed `option:{symbol}` keys with no OCC suffix, which Heber's writer-side validator rejects (100% drop on Bronze→Silver). When adding a poller for a per-underlying feed that carries expiry fields, pass `instrument_type_override="equity"` and `instrument_key_override=f"equity:{ticker.upper()}"` to `wrap_event()`. Reference: `_poll_eod_iv_term_structure` in `gateway/core/uw_poller.py`. This is also documented in the CLAUDE.md "Gotcha" section — keep both in sync.

## Trading Write Idempotency

- Order writes auto-mint a `dg-<uuid>` `client_order_id` when the caller omits one (`_generate_client_order_id`, `gateway/api/alpaca/trading.py`). Alpaca natively dedupes `submit_order` by `client_order_id`, so this key is the idempotency token. A **`504` from a write does NOT mean the order failed** — the asyncio `wait_for` ceiling may have fired while the executor thread was still talking to Alpaca, so the order *may have landed*. The gateway returns the minted key in the 504 response detail (and in successful response meta); on a 504 the caller must either GET the order by that `client_order_id` to check whether it placed, or safely retry with the *same* key (Alpaca returns the existing order rather than double-placing). Callers must omit `client_order_id` or supply a real non-empty value — passing `""` is rejected with `400 GW-E4006` precisely because a fresh per-retry UUID would defeat dedup (`_validate_client_order_id`).

## Where Bugs Usually Hide

- `gateway/api/middleware.py`: cache/envelope interaction and response wrapping edge cases.
- `gateway/core/stream.py`: fanout, batching, and backpressure behavior under load.
- `gateway/core/registry.py`: provider lifecycle and health-check orchestration.
- `gateway/providers/*`: normalization differences between provider payload shapes.
- `gateway/api/*`: route-level rate-limit and permission checks.

## Fast Debug Loop

```bash
# Run gateway locally
uvicorn gateway.main:app --reload --port 8080

# Tail logs in Docker mode
docker-compose logs -f gateway

# Check health quickly
curl http://localhost:8080/health/ready

# Inspect generated route contract drift
python scripts/generate_provider_contract.py --check
```

## Documentation Map

- `README.md`: onboarding and quickstart.
- `docs/ARCHITECTURE.md`: system architecture and data flow.
- `docs/RUNBOOK.md`: operations and troubleshooting.
- `docs/API_REFERENCE.md`: endpoint and stream contract reference.
- `PROVIDER_ENDPOINT_CONTRACT.md`: generated live-route contract snapshot.
- `docs/audits/`: performance reports, audits, and smoke-check artifacts.
