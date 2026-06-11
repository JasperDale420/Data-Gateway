"""Client authentication and authorization."""

import hashlib
import hmac
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from gateway.core.audit import get_audit_logger
from gateway.core.logger import logger


@dataclass
class ClientPermissions:
    """Client permission model."""

    providers: list[str] = field(default_factory=list)
    feeds: list[str] = field(default_factory=list)
    max_symbols: int = 100
    rate_limit: int = 60  # requests per minute
    ws_subscriptions_max: int = 500  # max WebSocket subscriptions
    trading: bool = False  # capability to place/mutate orders and positions


@dataclass
class Client:
    """Authenticated client."""

    id: str
    permissions: ClientPermissions
    role: str = "client"
    enabled: bool = True


class ClientAuthenticator:
    """Handles client authentication from API keys.

    Supports both plaintext keys (dev) and hashed keys (production).
    - Plaintext: {"key": "gw_abc123..."}
    - Hashed: {"key_hash": "sha256:abc123..."}
    """

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self._clients: dict[str, Client] = {}
        self._plaintext_keys: dict[str, str] = {}  # key -> client_id
        self._hashed_keys: dict[str, str] = {}  # hash -> client_id
        self._load_clients()

    def _load_clients(self) -> None:
        """Load clients from YAML configuration."""
        if not self.config_path.exists():
            logger.warning("clients_config_not_found", path=str(self.config_path))
            return

        with open(self.config_path) as f:
            config = yaml.safe_load(f)

        clients = config.get("clients", [])
        for client_data in clients:
            client_id = client_data["id"]

            permissions = ClientPermissions(
                providers=client_data.get("permissions", {}).get("providers", []),
                feeds=client_data.get("permissions", {}).get("feeds", []),
                max_symbols=client_data.get("permissions", {}).get("max_symbols", 100),
                rate_limit=client_data.get("permissions", {}).get("rate_limit", 60),
                trading=bool(client_data.get("permissions", {}).get("trading", False)),
            )

            client = Client(
                id=client_id,
                permissions=permissions,
                role=str(client_data.get("role", "client")).lower(),
                enabled=client_data.get("enabled", True),
            )

            self._clients[client_id] = client

            # Support both plaintext and hashed keys
            if "key" in client_data:
                self._plaintext_keys[client_data["key"]] = client_id
            if "key_hash" in client_data:
                # Store just the hash portion (after "sha256:")
                key_hash = client_data["key_hash"]
                if key_hash.startswith("sha256:"):
                    key_hash = key_hash[7:]
                self._hashed_keys[key_hash] = client_id

        logger.info(
            "clients_loaded",
            count=len(self._clients),
            plaintext_keys=len(self._plaintext_keys),
            hashed_keys=len(self._hashed_keys),
        )

        # F-13: plaintext keys in clients.yaml are one mistake from leaking via
        # any clone, branch, or CI artifact. Warn loudly listing affected client
        # IDs so operators can migrate to key_hash.
        if self._plaintext_keys:
            plaintext_client_ids = sorted(set(self._plaintext_keys.values()))
            logger.warning(
                "clients_plaintext_keys_in_use",
                count=len(plaintext_client_ids),
                client_ids=plaintext_client_ids,
                migration="rotate to key_hash via 'gateway rotate-key <client_id>'",
            )

    def authenticate(
        self,
        api_key: str,
        ip: str | None = None,
        user_agent: str | None = None,
    ) -> Client | None:
        """Authenticate a client by API key.

        Returns Client if valid, None if invalid.
        Checks plaintext keys first, then hashed keys.
        """
        # SHA256 fingerprint instead of raw prefix — even DEBUG logs can ship to
        # remote aggregators where any plaintext leakage matters.
        key_fingerprint_dbg = hashlib.sha256(api_key.encode()).hexdigest()[:8] if api_key else "empty"
        logger.debug("auth_check_start", key_fingerprint=key_fingerprint_dbg)
        audit = get_audit_logger()

        # Check plaintext keys (dev mode)
        client_id = self._plaintext_keys.get(api_key)

        # Check hashed keys (production mode). Compare the computed digest
        # against each stored hash with hmac.compare_digest so a timing
        # side-channel can't be used to recover a valid key byte-by-byte.
        if not client_id:
            key_hash = hashlib.sha256(api_key.encode()).hexdigest()
            for stored_hash, stored_client_id in self._hashed_keys.items():
                if hmac.compare_digest(key_hash, stored_hash):
                    client_id = stored_client_id
                    break

        if not client_id:
            # Never log raw key material. SHA256-prefix gives correlation ability
            # without leaking plaintext into log files or compliance audit trails.
            key_fingerprint = hashlib.sha256(api_key.encode()).hexdigest()[:12] if api_key else "empty"
            logger.warning("auth_failed_invalid_key", key_fingerprint=key_fingerprint, key_length=len(api_key))
            audit.auth_failure(
                ip=ip or "unknown",
                user_agent=user_agent,
                metadata={"reason": "invalid_key", "key_fingerprint": key_fingerprint, "key_length": len(api_key)},
            )
            return None

        client = self._clients.get(client_id)
        if not client:
            logger.warning("auth_failed_client_not_found", client_id=client_id)
            audit.auth_failure(
                ip=ip or "unknown",
                client_id=client_id,
                metadata={"reason": "client_not_found"},
            )
            return None

        if not client.enabled:
            logger.warning("auth_failed_client_disabled", client_id=client_id)
            audit.auth_failure(
                ip=ip or "unknown",
                client_id=client_id,
                metadata={"reason": "client_disabled"},
            )
            return None

        logger.debug("auth_success", client_id=client_id)
        audit.auth_success(
            client_id=client_id,
            ip=ip or "unknown",
            user_agent=user_agent,
        )
        return client

    def get_client(self, client_id: str) -> Client | None:
        """Get client by ID."""
        return self._clients.get(client_id)

    def reload(self) -> None:
        """Reload clients from configuration."""
        self._clients.clear()
        self._plaintext_keys.clear()
        self._hashed_keys.clear()
        self._load_clients()
        logger.info("clients_reloaded")

    @staticmethod
    def hash_key(key: str) -> str:
        """Hash an API key for secure storage."""
        digest = hashlib.sha256(key.encode()).hexdigest()
        return f"sha256:{digest}"
