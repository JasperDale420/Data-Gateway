# Orion and Kairos Remediation Handoff

Gateway evidence was pulled from existing July 5-11, 2026 logs. No runtime or production code was changed.

## Orion: July 7 Alpaca option rejects

Source log: `logs/data-gateway_errors_2026-07-07.log`.

Window: `2026-07-07T13:39:40.113446Z` through `2026-07-07T16:17:13.525154Z`.

Gateway saw 326 related reject log lines. For downstream remediation, the cleanest caller-owned count is the `provider_request_failed` records on `create_order.<locals>._call`, because those carry `client_id`, `symbol`, and Alpaca status.

| Reject type | Alpaca status/code | Caller `client_id` | Create-order rejects | Affected symbols |
| --- | --- | --- | ---: | --- |
| Uncovered option contract | `403` / `40310000` | `orion` | 75 | `AAPL260708C00315000` 37; `AAPL260708C00312500` 36; `INTC260710C00120000` 2 |
| Position intent mismatch | `422` / `42210000` | `orion` | 13 | `QCOM260710P00172500` 4; `NVDA260708C00190000` 4; `AAPL260708C00312500` 2; `INTC260710C00120000` 2; `AAPL260708C00315000` 1 |

Notes for Orion:

- The uncovered-option rejects also appear as 75 `alpaca_order_create_error`, 75 create-order `provider_request_failed`, 75 `alpaca_position_close_error`, and 75 close-position `provider_request_failed` lines. The close-position provider lines do not carry `client_id`; the nearby create-order lines identify `orion`.
- The intent mismatch rejects are order-create only and consistently say Alpaca inferred `sell_to_open` while the caller specified `sell_to_close`.

Representative evidence:

- `logs/data-gateway_errors_2026-07-07.log:742` - `orion` create-order reject for `AAPL260708C00315000`, `40310000`, uncovered option contract.
- `logs/data-gateway_errors_2026-07-07.log:747` - `orion` create-order reject for `AAPL260708C00312500`, `40310000`, uncovered option contract.
- `logs/data-gateway_errors_2026-07-07.log:1621` - `orion` create-order reject for `INTC260710C00120000`, `40310000`, uncovered option contract.
- `logs/data-gateway_errors_2026-07-07.log:859` - `orion` create-order reject for `AAPL260708C00312500`, `42210000`, position intent mismatch.
- `logs/data-gateway_errors_2026-07-07.log:3899` - `orion` create-order reject for `NVDA260708C00190000`, `42210000`, position intent mismatch.
- `logs/data-gateway_errors_2026-07-07.log:3931` - last sampled `NVDA260708C00190000` intent mismatch in the window.

## Kairos: July 5 rate-limit burst

Source log: `logs/data-gateway_errors_2026-07-05.log`.

Gateway logged 2,576 `rate_limit_exceeded` warnings for `client_id=kairos`, all at `limit=600`, from `2026-07-05T04:20:45.189664Z` through `2026-07-05T04:54:30.002751Z`.

Burst breakdown using a 60-second gap split:

| Window UTC | Count | Client | Limit |
| --- | ---: | --- | ---: |
| `04:20:45.189664` - `04:20:52.634081` | 161 | `kairos` | 600 |
| `04:23:28.410253` - `04:23:34.667299` | 384 | `kairos` | 600 |
| `04:27:18.854027` - `04:27:23.061548` | 383 | `kairos` | 600 |
| `04:48:50.857996` - `04:49:25.773901` | 706 | `kairos` | 600 |
| `04:50:26.651384` - `04:50:49.360986` | 192 | `kairos` | 600 |
| `04:52:24.185968` - `04:54:30.002751` | 750 | `kairos` | 600 |

Representative evidence:

- `logs/data-gateway_errors_2026-07-05.log:3` - first `kairos` `rate_limit_exceeded`, `limit=600`.
- `logs/data-gateway_errors_2026-07-05.log:164` - start of the second burst.
- `logs/data-gateway_errors_2026-07-05.log:933` - start of the 706-event burst.
- `logs/data-gateway_errors_2026-07-05.log:1832` - start of the final 750-event burst.
- `logs/data-gateway_errors_2026-07-05.log:2581` - final `kairos` `rate_limit_exceeded`, `limit=600`.
