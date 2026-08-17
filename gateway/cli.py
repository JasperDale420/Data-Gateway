"""Gateway CLI for key management and administration.

Usage:
    python -m gateway.cli generate-key
    python -m gateway.cli add-client <client_id> [--key KEY]
    python -m gateway.cli rotate-key <client_id>
    python -m gateway.cli list-clients
    python -m gateway.cli hash-key <key>
    python -m gateway.cli thaw-claim <SYMBOL> [--delete]
"""

import argparse
import asyncio
import hashlib
import json
import secrets
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import yaml

from gateway.core.order_ownership import (
    BrokerSymbolState,
    OrderOwnershipGuard,
    OwnershipConflict,
    OwnershipStoreUnavailable,
    canonical_broker_symbol,
)


def generate_key() -> str:
    """Generate a new API key (43 chars per PRD)."""
    return f"gw_{secrets.token_urlsafe(32)}"


def hash_key(key: str) -> str:
    """Hash a key with SHA-256."""
    digest = hashlib.sha256(key.encode()).hexdigest()
    return f"sha256:{digest}"


def load_clients(config_path: Path) -> dict:
    """Load clients config."""
    if not config_path.exists():
        return {"clients": []}
    with open(config_path) as f:
        return yaml.safe_load(f) or {"clients": []}


def save_clients(config_path: Path, config: dict) -> None:
    """Save clients config."""
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)


def cmd_generate_key(args):
    """Generate a new API key."""
    key = generate_key()
    print(f"Generated key: {key}")
    print(f"Hashed:       {hash_key(key)}")
    return 0


def cmd_add_client(args):
    """Add a new client."""
    config = load_clients(args.config)

    # Check if client exists
    for client in config.get("clients", []):
        if client.get("id") == args.client_id:
            print(f"Error: Client '{args.client_id}' already exists")
            return 1

    # Generate or use provided key
    key = args.key or generate_key()
    key_hash = hash_key(key)

    # Add client
    new_client = {
        "id": args.client_id,
        "key_hash": key_hash,
        "enabled": True,
        "permissions": {
            "providers": ["alpaca"],
            "feeds": ["bars", "quotes", "trades"],
            "max_symbols": 100,
            "rate_limit": 600,
        },
    }

    if "clients" not in config:
        config["clients"] = []
    config["clients"].append(new_client)

    save_clients(args.config, config)

    print(f"Added client: {args.client_id}")
    print(f"API Key:      {key}")
    print(f"Key Hash:     {key_hash}")
    print(f"Config saved: {args.config}")
    return 0


def cmd_rotate_key(args):
    """Rotate key for existing client."""
    config = load_clients(args.config)

    for client in config.get("clients", []):
        if client.get("id") == args.client_id:
            new_key = generate_key()

            client["key_hash"] = hash_key(new_key)

            # ponytail: rotation is a hard cutover (the old key dies immediately,
            # fail-closed). The previous `old_key_hashes` grace-period write was
            # dead — auth.py never read it — and could not be safely honored
            # without an expiry timestamp (an indefinitely-valid old key is a
            # security hole). A real grace period needs a deliberate
            # expiring-key design; until then, don't accumulate dead hashes.
            client.pop("old_key_hashes", None)

            save_clients(args.config, config)

            print(f"Rotated key for: {args.client_id}")
            print(f"New API Key:     {new_key}")
            print(f"Key Hash:        {client['key_hash']}")
            return 0

    print(f"Error: Client '{args.client_id}' not found")
    return 1


def cmd_list_clients(args):
    """List all clients."""
    config = load_clients(args.config)

    clients = config.get("clients", [])
    if not clients:
        print("No clients configured.")
        return 0

    print(f"{'ID':<20} {'Enabled':<10} {'Rate Limit':<12} {'Providers'}")
    print("-" * 60)

    for client in clients:
        client_id = client.get("id", "unknown")
        enabled = "Yes" if client.get("enabled", True) else "No"
        perms = client.get("permissions", {})
        rate_limit = perms.get("rate_limit", 60)
        providers = ", ".join(perms.get("providers", []))

        print(f"{client_id:<20} {enabled:<10} {rate_limit:<12} {providers}")

    return 0


def cmd_hash_key(args):
    """Hash a provided key."""
    hashed = hash_key(args.key)
    print(f"Input: {args.key[:4]}...{args.key[-4:]}")
    print(f"Hash:  {hashed}")
    return 0


def cmd_revoke_client(args):
    """Revoke/disable a client."""
    config = load_clients(args.config)

    for client in config.get("clients", []):
        if client.get("id") == args.client_id:
            if args.delete:
                config["clients"].remove(client)
                save_clients(args.config, config)
                print(f"Deleted client: {args.client_id}")
            else:
                client["enabled"] = False
                save_clients(args.config, config)
                print(f"Disabled client: {args.client_id}")
            return 0

    print(f"Error: Client '{args.client_id}' not found")
    return 1


def _describe_claim(label: str, claim: dict | None) -> None:
    print(f"{label}: {json.dumps(claim, sort_keys=True) if claim is not None else 'none'}")


def _thaw_refusal(symbol: str, reason: str) -> int:
    print(f"REFUSED: {symbol} is not safe to thaw ({reason}).")
    print("Resolve the broker state first, then re-run.")
    return 1


async def thaw_claim(
    *,
    guard: OrderOwnershipGuard,
    reconcile: Callable[[str], Awaitable[BrokerSymbolState]],
    symbol: str,
    delete: bool = False,
) -> int:
    """Clear a frozen ownership claim after proving the broker state is unambiguous.

    A freeze is deliberately one-way inside the request path: it marks a symbol
    whose broker outcome the Gateway could not determine, and only a human who
    has checked the broker may lift it. This applies that check mechanically —
    a fresh reconciliation must show no open order from anyone but the claim
    owner, and ``--delete`` additionally requires the symbol to be flat.

    The symbol's fence is held for the whole command, which is what every
    Gateway mutation takes before it writes, so no broker write can start
    against the symbol while it is being inspected and no second thaw can run
    beside this one. The fence is revalidated immediately before the write, and
    the write itself only applies to the exact claim that was reviewed.
    """
    try:
        canonical = canonical_broker_symbol(symbol)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    try:
        fence_token = await guard.acquire_fence(canonical)
    except (OwnershipConflict, OwnershipStoreUnavailable) as exc:
        return _thaw_refusal(canonical, f"the symbol's fence could not be taken ({exc})")

    try:
        stored = await guard.read_claim(canonical)
        _describe_claim(f"{canonical} before", stored.claim if stored is not None else None)
        if stored is None:
            print(f"ERROR: no ownership claim exists for {canonical}.")
            return 1
        if not (stored.claim.get("frozen_reason") or stored.claim.get("mutation_pending")):
            return _thaw_refusal(canonical, "the claim is neither frozen nor mid-mutation, so there is nothing to lift")

        state = await reconcile(canonical)
        print(
            f"broker state: has_position={state.has_position} "
            f"order_owners={sorted(owner or '<manual>' for owner in state.order_owners)} "
            f"complete={state.complete}"
        )
        if not state.complete:
            return _thaw_refusal(canonical, "broker reconciliation was incomplete")
        if state.order_owners - {stored.claim["owner"]}:
            return _thaw_refusal(canonical, "open orders belong to another owner")
        if delete and (state.has_position or state.order_owners):
            return _thaw_refusal(canonical, "the broker still holds a position or open order")

        await guard.renew_fence(canonical, fence_token)
        await guard.thaw(canonical, expected=stored, delete=delete)
        after = await guard.read_claim(canonical)
        _describe_claim(f"{canonical} after", after.claim if after is not None else None)
        return 0
    except (OwnershipConflict, OwnershipStoreUnavailable) as exc:
        return _thaw_refusal(canonical, str(exc))
    finally:
        await guard.release_fence(canonical, fence_token)


def cmd_thaw_claim(args):
    """Thaw one frozen ownership claim after a fresh broker reconciliation."""
    from gateway.api.alpaca.trading import _reconcile_broker_symbol_state, get_order_ownership_guard
    from gateway.config import get_settings
    from gateway.core.registry import ProviderRegistry

    async def _run() -> int:
        registry = ProviderRegistry()
        await registry.load_from_config(get_settings().providers_config_path)
        provider = registry.get("alpaca")
        if provider is None:
            print("ERROR: the Alpaca provider is unavailable; broker state cannot be verified.")
            return 1

        async def _reconcile(symbol: str) -> BrokerSymbolState:
            return await _reconcile_broker_symbol_state(provider, symbol)

        try:
            return await thaw_claim(
                guard=get_order_ownership_guard(),
                reconcile=_reconcile,
                symbol=args.symbol,
                delete=args.delete,
            )
        finally:
            await registry.shutdown()

    return asyncio.run(_run())


def main():
    parser = argparse.ArgumentParser(
        description="Gateway CLI for key management",
        prog="python -m gateway.cli",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/clients.yaml"),
        help="Path to clients config file",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate-key
    subparsers.add_parser("generate-key", help="Generate a new API key")

    # add-client
    add_parser = subparsers.add_parser("add-client", help="Add a new client")
    add_parser.add_argument("client_id", help="Client ID")
    add_parser.add_argument("--key", help="Use specific key (default: generate new)")

    # rotate-key
    rotate_parser = subparsers.add_parser("rotate-key", help="Rotate client key")
    rotate_parser.add_argument("client_id", help="Client ID")

    # list-clients
    subparsers.add_parser("list-clients", help="List all clients")

    # hash-key
    hash_parser = subparsers.add_parser("hash-key", help="Hash a key")
    hash_parser.add_argument("key", help="Key to hash")

    # revoke-client
    revoke_parser = subparsers.add_parser("revoke-client", help="Revoke/disable a client")
    revoke_parser.add_argument("client_id", help="Client ID")
    revoke_parser.add_argument("--delete", action="store_true", help="Delete instead of disable")

    # thaw-claim
    thaw_parser = subparsers.add_parser(
        "thaw-claim",
        help="Clear a frozen order-ownership claim after verifying broker state",
    )
    thaw_parser.add_argument("symbol", help="Ticker or full OCC option contract")
    thaw_parser.add_argument(
        "--delete",
        action="store_true",
        help="Drop the claim entirely (requires no broker position and no open orders)",
    )

    args = parser.parse_args()

    commands = {
        "generate-key": cmd_generate_key,
        "add-client": cmd_add_client,
        "rotate-key": cmd_rotate_key,
        "list-clients": cmd_list_clients,
        "hash-key": cmd_hash_key,
        "revoke-client": cmd_revoke_client,
        "thaw-claim": cmd_thaw_claim,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
