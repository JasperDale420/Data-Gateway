# Data Gateway Redis Resilience

**Date**: 2026-03-24
**Status**: Approved

## Problem

`data-gateway-redis` has restarted 576 times. Each restart loads a 4.8 GB RDB dataset (~15 seconds), during which the UW poller cannot publish events. The circuit breaker opens after 20 failures and drops all subsequent events instead of buffering them. On 2026-03-24, the entire trading session's flow alerts were lost — zero bronze or silver data for the day.

Three root causes:
1. Redis persistence (AOF + RDB) creates a large dataset that takes 15s to reload, causing a restart loop
2. The circuit breaker drops events when OPEN instead of buffering them
3. Sink failures are logged at DEBUG, invisible in normal log review and health endpoints

## Design

### 1. Ephemeral Redis

Remove persistence to eliminate the 15-second startup penalty.

**docker-compose.yml changes:**
- Remove `--appendonly yes` from Redis command
- Add `--save "" --maxmemory 512mb --maxmemory-policy allkeys-lru`
- Remove `redis_data` volume mount

The dedup cache (2h TTL) and API response cache rebuild naturally. Heber's bronze-to-silver transformer already deduplicates, so duplicate events during cache warm-up are harmless.

### 2. Buffer Through Circuit Breaker OPEN State

When the circuit breaker is OPEN, route events to the sink's existing `_failed_buffer` (10K bounded deque) instead of dropping them.

**gateway/core/data_sink.py changes:**
- `publish_all()`: when circuit is OPEN, push event to the sink's `_failed_buffer` instead of returning early
- `publish_all_batch()`: same — route batch to buffer instead of dropping

No new drain logic needed. The existing `_drain_buffer()` in `RedisStreamsSink` already replays buffered events when the connection recovers. The 10K buffer supports ~50 minutes of flow polling at current rates.

### 3. Alerting: Log Level + Health Endpoint

**gateway/core/uw_poller.py:**
- Promote `uw_poller_no_sink` from DEBUG to WARNING
- Add `sink_available` boolean to `get_runtime_snapshot()`

**gateway/core/redis_sink.py:**
- Log WARNING `redis_sink_circuit_open` when circuit breaker opens
- Log INFO `redis_sink_circuit_recovered` when circuit closes

**Health endpoint (gateway/main.py or health route):**
- Include `data_sink` status in `/health` response
- Report `"degraded"` when Redis sink circuit breaker is OPEN or sink is disconnected
- Hippocrates and external monitors can detect outages automatically

## Scope

All changes are in the Data-Gateway repo. No changes to Heber, Kairos, or other consumers.

## Risks

- **Ephemeral Redis**: First few minutes after restart may produce duplicate events to Heber (dedup cache cold). Heber's transformer deduplicates, so this is cosmetic.
- **Buffer overflow**: If Redis is down for >50 minutes during market hours, the 10K buffer fills and oldest events are evicted. Acceptable given ephemeral Redis restarts in <1s.
- **maxmemory 512mb**: If cache pressure exceeds 512mb, LRU eviction kicks in. This is fine — the cache is a performance optimization, not a correctness requirement.
