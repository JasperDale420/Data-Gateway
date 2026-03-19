"""Client authentication and authorization."""

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger()


@dataclass
class ClientPermissions:
    """Client permission model."""

    providers: list[str] = field(default_factory=list)
    feeds: list[str] = field(default_factory=list)
    max_symbols: int = 100
    rate_limit: int = 60  # requests per minute
    ws_subscriptions_max: int = 500  # max WebSocket subscriptions


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

        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.error("auth_receive_error", error="yaml_parse_error", details=str(e))
            return
        except OSError as e:
            logger.error("auth_receive_error", error="file_read_error", details=str(e))
            return

        if config is None:
            logger.warning("clients_config_empty", path=str(self.config_path))
            return

        clients = config.get("clients", [])
        if not isinstance(clients, list):
            logger.error(
                "auth_receive_error",
                error="invalid_config_structure",
                details="'clients' must be a list",
            )
            return

        for idx, client_data in enumerate(clients):
            if not isinstance(client_data, dict):
                logger.warning(
                    "auth_receive_error",
                    error="invalid_client_entry",
                    index=idx,
                    details="client entry must be a dictionary",
                )
                continue

            try:
                client_id = client_data["id"]
            except KeyError:
                logger.warning(
                    "auth_receive_error",
                    error="missing_client_id",
                    index=idx,
                    details="client entry missing required 'id' field",
                )
                continue

            permissions = ClientPermissions(
                providers=client_data.get("permissions", {}).get("providers", []),
                feeds=client_data.get("permissions", {}).get("feeds", []),
                max_symbols=client_data.get("permissions", {}).get("max_symbols", 100),
                rate_limit=client_data.get("permissions", {}).get("rate_limit", 60),
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

    def authenticate(self, api_key: str) -> Client | None:
        """Authenticate a client by API key.

        Returns Client if valid, None if invalid.
        Checks plaintext keys first, then hashed keys.
        """
        # Check plaintext keys (dev mode)
        client_id = self._plaintext_keys.get(api_key)

        # Check hashed keys (production mode)
        if not client_id:
            key_hash = hashlib.sha256(api_key.encode()).hexdigest()
            client_id = self._hashed_keys.get(key_hash)

        if not client_id:
            key_preview = api_key[:10] + "..." if len(api_key) > 10 else api_key
            logger.warning("auth_failed_invalid_key", key_prefix=key_preview)
            return None

        client = self._clients.get(client_id)
        if not client:
            logger.warning("auth_failed_client_not_found", client_id=client_id)
            return None

        if not client.enabled:
            logger.warning("auth_failed_client_disabled", client_id=client_id)
            return None

        logger.info("auth_success", client_id=client_id)
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
