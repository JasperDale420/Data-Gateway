"""
Data-Gateway client for market data access.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import requests
import yaml

logger = logging.getLogger(__name__)


class DataGatewayClient:
    """Client for interacting with the Data-Gateway."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        api_key: str | None = None,
        clients_config_path: str | Path | None = None,
    ):
        """
        Initialize Data-Gateway client.

        Args:
            base_url: Data-Gateway base URL
            api_key: Client API key.
            clients_config_path: Optional path to clients.yaml for key resolution.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = self._resolve_api_key(api_key=api_key, clients_config_path=clients_config_path)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Gateway-Key": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _resolve_api_key(self, *, api_key: str | None, clients_config_path: str | Path | None) -> str:
        """Resolve API key from explicit input, env, or clients config."""
        if api_key and api_key.strip():
            return api_key.strip()

        env_key = os.getenv("GATEWAY_API_KEY", "").strip()
        if env_key:
            return env_key

        config_path = Path(clients_config_path) if clients_config_path else self._default_clients_config_path()
        if config_path.exists():
            key = self._read_key_from_clients_yaml(config_path)
            if key:
                return key

        raise ValueError(
            "Gateway API key is required. Set GATEWAY_API_KEY, pass api_key, "
            "or provide a clients.yaml with an enabled client key."
        )

    def _default_clients_config_path(self) -> Path:
        """Return repo-default clients.yaml path."""
        return Path(__file__).resolve().parents[3] / "config" / "clients.yaml"

    def _read_key_from_clients_yaml(self, path: Path) -> str | None:
        """Read a client key from clients.yaml, preferring the test client if present."""
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            logger.warning("Failed to parse clients config %s: %s", path, exc)
            return None

        clients = raw.get("clients")
        if not isinstance(clients, list):
            return None

        test_candidate: str | None = None
        first_enabled: str | None = None

        for client in clients:
            if not isinstance(client, dict):
                continue

            enabled = client.get("enabled", True)
            if not enabled:
                continue

            key_raw = client.get("key") or client.get("api_key")
            if not isinstance(key_raw, str) or not key_raw.strip():
                continue
            key = key_raw.strip()

            client_id = str(client.get("id") or client.get("name") or "").strip().lower()
            if client_id == "test":
                test_candidate = key
                break

            if first_enabled is None:
                first_enabled = key

        return test_candidate or first_enabled

    def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        """Make authenticated request to Data-Gateway."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Gateway request failed for %s %s: %s", method, url, e)
            if hasattr(e.response, "text"):
                logger.error("Gateway response body: %s", e.response.text)
            raise

    def get_health(self) -> dict[str, Any]:
        """Get gateway health status."""
        return self._request("GET", "/health")

    def get_providers(self) -> list[str]:
        """Get list of available data providers."""
        response = self._request("GET", "/api/v1/providers")
        return response.get("providers", [])

    def get_stock_bars(self, symbol: str, timeframe: str = "1Day", limit: int = 100) -> dict[str, Any]:
        """
        Get stock bars from Alpaca.

        Args:
            symbol: Stock symbol (e.g., 'SPY')
            timeframe: Bar timeframe (1Min, 5Min, 15Min, 1Day, etc.)
            limit: Number of bars to return
        """
        endpoint = f"/api/v1/alpaca/stocks/{symbol}/bars"
        params = {"timeframe": timeframe, "limit": limit}
        return self._request("GET", endpoint, params=params)

    def get_account(self) -> dict[str, Any]:
        """Get Alpaca account information."""
        return self._request("GET", "/api/v1/alpaca/account")

    def get_unusual_whales_flow(self, symbol: str, limit: int = 50) -> dict[str, Any]:
        """
        Get unusual options flow from Unusual Whales.

        Args:
            symbol: Stock symbol
            limit: Number of flow events to return
        """
        endpoint = f"/api/v1/uw/flow/{symbol}"
        params = {"limit": limit}
        return self._request("GET", endpoint, params=params)

    def get_quotes(self, symbol: str) -> dict[str, Any]:
        """Get latest quotes for a symbol."""
        endpoint = f"/api/v1/alpaca/stocks/{symbol}/quotes"
        return self._request("GET", endpoint)

    def get_trades(self, symbol: str, limit: int = 100) -> dict[str, Any]:
        """Get recent trades for a symbol."""
        endpoint = f"/api/v1/alpaca/stocks/{symbol}/trades"
        params = {"limit": limit}
        return self._request("GET", endpoint, params=params)


# Example usage
if __name__ == "__main__":
    # Test the client
    client = DataGatewayClient()

    print("Testing Data-Gateway client...")

    # Test health
    health = client.get_health()
    print(f"Health: {health}")

    # Test providers
    providers = client.get_providers()
    print(f"Available providers: {providers}")

    # Test account (if authenticated)
    try:
        account = client.get_account()
        print(f"Account: {account.keys()}")
    except requests.exceptions.RequestException:
        print("Account endpoint requires proper authentication")

    # Test stock bars
    try:
        bars = client.get_stock_bars("SPY", "1Day", 5)
        print(f"SPY bars: {len(bars.get('bars', []))} bars retrieved")
    except Exception as e:
        print(f"Failed to get bars: {e}")
