"""Data Gateway client for trading bot integration.

Provides a lightweight HTTP session wrapper that authenticates
with the Data Gateway using the ``X-Gateway-Key`` header.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from requests import Session


class DataGatewayClient:
    """HTTP client pre-configured for Data Gateway authentication.

    Args:
        api_key: Gateway API key.  When *None*, the key is resolved from
            ``clients_config_path`` (first enabled client) or the
            ``GATEWAY_API_KEY`` environment variable.
        base_url: Gateway base URL (default ``http://localhost:8080``).
        clients_config_path: Path to ``clients.yaml`` for key look-up.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "http://localhost:8080",
        clients_config_path: Path | None = None,
    ) -> None:
        resolved_key = api_key

        if resolved_key is None and clients_config_path is not None:
            resolved_key = self._load_key_from_yaml(clients_config_path)

        if resolved_key is None:
            resolved_key = os.environ.get("GATEWAY_API_KEY")

        if not resolved_key:
            raise ValueError(
                "Gateway API key is required. Provide via api_key param, clients.yaml, or GATEWAY_API_KEY env var."
            )

        self.base_url = base_url.rstrip("/")
        self.session = Session()
        self.session.headers["X-Gateway-Key"] = resolved_key

    @staticmethod
    def _load_key_from_yaml(path: Path) -> str | None:
        """Return the first enabled client key from a clients.yaml file."""
        if not path.exists():
            return None
        try:
            config = yaml.safe_load(path.read_text()) or {}
        except Exception:
            return None

        for entry in config.get("clients", []):
            if entry.get("enabled", False):
                key = entry.get("key")
                if isinstance(key, str) and key:
                    return key
        return None
