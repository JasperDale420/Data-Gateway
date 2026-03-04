# API Reference

Canonical API docs live in these files:

- [API_REFERENCE.md](../API_REFERENCE.md) - gateway behavior, endpoint groups, WebSocket usage.
- [PROVIDER_ENDPOINT_CONTRACT.md](../PROVIDER_ENDPOINT_CONTRACT.md) - generated authoritative route inventory from live FastAPI routers.

## Regeneration

When routes change, regenerate the provider contract:

```bash
python scripts/generate_provider_contract.py
```

To validate no contract drift:

```bash
python scripts/generate_provider_contract.py --check
```
