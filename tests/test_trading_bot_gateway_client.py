from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_gateway_client_module():
    module_path = Path(__file__).resolve().parents[1] / "trading-bot" / "src" / "core" / "gateway_client.py"
    spec = importlib.util.spec_from_file_location("trading_bot_gateway_client", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load trading-bot gateway_client module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_gateway_client_uses_x_gateway_key_header() -> None:
    module = _load_gateway_client_module()
    client = module.DataGatewayClient(api_key="gw_test_key")

    assert client.session.headers.get("X-Gateway-Key") == "gw_test_key"
    assert "X-API-Key" not in client.session.headers


def test_gateway_client_loads_key_from_yaml_when_not_provided(tmp_path: Path) -> None:
    module = _load_gateway_client_module()
    clients_yaml = tmp_path / "clients.yaml"
    clients_yaml.write_text(
        """
clients:
  - id: test
    key: gw_from_yaml_123
    enabled: true
        """.strip(),
        encoding="utf-8",
    )

    client = module.DataGatewayClient(api_key=None, clients_config_path=clients_yaml)
    assert client.session.headers.get("X-Gateway-Key") == "gw_from_yaml_123"


def test_gateway_client_raises_when_key_missing_everywhere(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_gateway_client_module()
    clients_yaml = tmp_path / "clients.yaml"
    clients_yaml.write_text("clients: []\n", encoding="utf-8")
    monkeypatch.delenv("GATEWAY_API_KEY", raising=False)

    with pytest.raises(ValueError, match="Gateway API key is required"):
        module.DataGatewayClient(api_key=None, clients_config_path=clients_yaml)
