"""Tests for provider registry ordering logic."""

from gateway.core.provider import DataProvider
from gateway.core.registry import ProviderRegistry


class MockProvider(DataProvider):
    def __init__(self, name: str, capabilities: list[str]):
        self._name = name
        self._capabilities = capabilities

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> list[str]:
        return self._capabilities

    @property
    def supported_feeds(self) -> list[str]:
        return ["sip"]

    async def initialize(self, config: dict):
        pass

    async def shutdown(self):
        pass

    async def health_check(self):
        pass


def test_get_ordered_providers():
    registry = ProviderRegistry()

    # Mock config
    registry._config = {
        "providers": {
            "p1": {"enabled": True, "priority": 10},
            "p2": {"enabled": True, "priority": 100},
            "p3": {"enabled": False, "priority": 50},
            "p4": {"enabled": True, "priority": 50},
        }
    }

    # Mock providers
    p1 = MockProvider("p1", ["stock_bars"])
    p2 = MockProvider("p2", ["stock_bars"])
    p3 = MockProvider("p3", ["stock_bars"])
    p4 = MockProvider("p4", ["stock_bars"])
    p5 = MockProvider("p5", ["other_cap"])  # Wrong capability

    registry._providers = {
        "p1": p1,
        "p2": p2,
        "p3": p3,
        "p4": p4,
        "p5": p5,
    }

    # Execute
    ordered = registry.get_ordered_providers("stock_bars")

    # Assertions
    # p2 (100) -> p4 (50) -> p1 (10)
    # p3 is disabled
    # p5 has wrong capability
    assert len(ordered) == 3
    assert ordered[0].name == "p2"
    assert ordered[1].name == "p4"
    assert ordered[2].name == "p1"


def test_get_ordered_providers_default_priority():
    registry = ProviderRegistry()

    # No priority in config, defaults to 50
    registry._config = {
        "providers": {
            "p1": {"enabled": True},
        }
    }

    p1 = MockProvider("p1", ["cap"])
    registry._providers = {"p1": p1}

    ordered = registry.get_ordered_providers("cap")
    assert len(ordered) == 1
    assert ordered[0].name == "p1"
