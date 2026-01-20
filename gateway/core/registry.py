"""Provider registry with dynamic loading."""

import importlib
from pathlib import Path
from typing import Any

import structlog
import yaml

from gateway.core.provider import DataProvider, HealthStatus

logger = structlog.get_logger()


class ProviderRegistry:
    """Manages provider lifecycle and routing."""

    def __init__(self):
        self._providers: dict[str, DataProvider] = {}
        self._config: dict[str, Any] = {}
        self._routes: dict[str, list[str]] = {}

    async def load_from_config(self, config_path: Path) -> None:
        """Load and initialize all enabled providers from config."""
        if not config_path.exists():
            logger.warning("providers_config_not_found", path=str(config_path))
            return

        with open(config_path) as f:
            self._config = yaml.safe_load(f)

        providers_config = self._config.get("providers", {})

        for name, provider_config in providers_config.items():
            if not provider_config.get("enabled", True):
                logger.info("provider_disabled", provider=name)
                continue

            try:
                await self._load_provider(name, provider_config)
            except Exception as e:
                logger.error(
                    "provider_load_failed",
                    provider=name,
                    error=str(e),
                    exc_info=True,
                )

        # Load routes
        self._routes = self._config.get("routes", {})

        logger.info(
            "providers_loaded",
            count=len(self._providers),
            names=list(self._providers.keys()),
        )

    async def _load_provider(self, name: str, config: dict[str, Any]) -> None:
        """Load a single provider."""
        module_path = config.get("module")
        class_name = config.get("class")

        if not module_path or not class_name:
            logger.warning(
                "provider_config_incomplete",
                provider=name,
                module=module_path,
                class_name=class_name,
            )
            return

        # Dynamic import
        try:
            module = importlib.import_module(module_path)
            provider_class = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            logger.warning(
                "provider_import_failed",
                provider=name,
                module=module_path,
                error=str(e),
            )
            return

        # Instantiate and initialize
        provider: DataProvider = provider_class()
        await provider.initialize(config.get("config", {}))

        self._providers[name] = provider
        logger.info(
            "provider_registered",
            provider=name,
            capabilities=provider.capabilities,
        )

    async def shutdown(self) -> None:
        """Shutdown all providers."""
        for name, provider in self._providers.items():
            try:
                await provider.shutdown()
                logger.info("provider_shutdown", provider=name)
            except Exception as e:
                logger.error(
                    "provider_shutdown_failed",
                    provider=name,
                    error=str(e),
                )

    def get(self, name: str) -> DataProvider | None:
        """Get provider by name."""
        return self._providers.get(name)

    def list_providers(self) -> list[str]:
        """List registered provider names."""
        return list(self._providers.keys())

    def get_for_route(self, route: str) -> list[DataProvider]:
        """Get providers for a route in priority order."""
        provider_names = self._routes.get(route, {}).get("providers", [])
        providers = []
        for name in provider_names:
            if provider := self._providers.get(name):
                providers.append(provider)
        return providers

    async def health_check_all(self) -> dict[str, HealthStatus]:
        """Health check all providers."""
        results = {}
        for name, provider in self._providers.items():
            try:
                results[name] = await provider.health_check()
            except Exception as e:
                results[name] = HealthStatus(healthy=False, error=str(e))
        return results

    def get_stats(self) -> dict[str, Any]:
        """Get registry statistics."""
        return {
            "total_providers": len(self._providers),
            "providers": list(self._providers.keys()),
            "routes": list(self._routes.keys()),
        }

    def has_provider(self, name: str) -> bool:
        """Check if provider exists."""
        return name in self._providers

    def get_provider_config(self, name: str) -> Any:
        """Get provider config by name."""
        providers_config = self._config.get("providers", {})
        config = providers_config.get(name)
        if config:
            # Return a simple object with enabled/priority
            class ProviderConfig:
                def __init__(self, enabled: bool, priority: int):
                    self.enabled = enabled
                    self.priority = priority

            return ProviderConfig(
                enabled=config.get("enabled", True),
                priority=config.get("priority", 50),
            )
        return None

    def get_capabilities(self, name: str) -> list[str]:
        """Get provider capabilities."""
        provider = self._providers.get(name)
        if provider and hasattr(provider, "capabilities"):
            return provider.capabilities
        return []

    def set_provider_enabled(self, name: str, enabled: bool) -> None:
        """Enable/disable a provider."""
        providers_config = self._config.get("providers", {})
        if name in providers_config:
            providers_config[name]["enabled"] = enabled
            # Note: This is in-memory only; persisting requires writing to YAML
            logger.info("provider_enabled_changed", provider=name, enabled=enabled)
