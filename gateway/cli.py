"""Gateway CLI for key management and administration.

Usage:
    python -m gateway.cli generate-key
    python -m gateway.cli add-client <client_id> [--key KEY]
    python -m gateway.cli rotate-key <client_id>
    python -m gateway.cli list-clients
    python -m gateway.cli hash-key <key>
"""

import argparse
import hashlib
import secrets
import sys
from pathlib import Path

import yaml


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
            old_hash = client.get("key_hash")

            client["key_hash"] = hash_key(new_key)

            # Keep old key hash for deprecation period
            if "old_key_hashes" not in client:
                client["old_key_hashes"] = []
            if old_hash:
                client["old_key_hashes"].append(old_hash)

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

    args = parser.parse_args()

    commands = {
        "generate-key": cmd_generate_key,
        "add-client": cmd_add_client,
        "rotate-key": cmd_rotate_key,
        "list-clients": cmd_list_clients,
        "hash-key": cmd_hash_key,
        "revoke-client": cmd_revoke_client,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
