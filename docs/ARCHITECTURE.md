# Architecture

## Overview

Data Gateway provides one authenticated REST and WebSocket surface for multiple market data providers. It centralizes auth, caching, rate limits, normalization, and streaming fan-out.

## System Components

- **API Routers (`gateway/api/`)**: HTTP endpoints by provider and feature.
- **Core Services (`gateway/core/`)**: Auth, cache, middleware, streaming, transport helpers.
- **Providers (`gateway/providers/`)**: External API adapters implementing provider contracts.
- **Schemas (`gateway/schemas.py`)**: Shared normalized response/data models.
- **Config (`config/*.yaml`, env vars)**: Provider enablement, client permissions, runtime settings.

## Data Flow

1. Client authenticates with `X-Gateway-Key` (REST) or WS auth message.
2. Request is authorized against `config/clients.yaml` permissions.
3. Router calls provider/service logic, optionally using cache.
4. Responses are normalized and wrapped in the standard envelope.
5. For stream data, multiplexer fans upstream messages to subscribed clients.
6. Optional sink publishing sends event envelopes to Redis topics for downstream ingestion.

## Key Design Decisions

- Use one gateway contract instead of provider-specific payloads.
- Keep provider clients pluggable through abstraction boundaries.
- Treat permission checks as mandatory boundaries.
- Prefer generated provider contract docs to avoid manual endpoint drift.

## External Integrations

- Alpaca
- Unusual Whales
- Finnhub
- Alpha Vantage
- Yahoo Finance
- SEC EDGAR
- NewsAPI.org
- Redis (cache + sink publishing)

## Related Docs

- [README.md](../README.md)
- [PRD.md](../PRD.md)
- [docs/API_REFERENCE.md](./API_REFERENCE.md)
- [docs/RUNBOOK.md](./RUNBOOK.md)
- [PROVIDER_ENDPOINT_CONTRACT.md](../PROVIDER_ENDPOINT_CONTRACT.md)
