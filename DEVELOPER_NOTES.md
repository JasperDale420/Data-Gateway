# Developer Notes

Practical notes for contributors working inside Data Gateway.

## High-Signal Gotchas

- Authentication header is `X-Gateway-Key` for protected REST endpoints.
- WebSocket clients must authenticate quickly after connection (`/ws` auth timeout is enforced).
- Provider permissions are controlled per client in `config/clients.yaml`.
- Provider registration and capability flags come from `config/providers.yaml`.
- The Unusual Whales SDK is a local submodule (`unusualwhales_sdk/`) and must be initialized after clone.

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
