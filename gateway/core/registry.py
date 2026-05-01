"""Provider registry with dynamic loading."""

import asyncio
import importlib
from pathlib import Path
from typing import Any

import yaml

from gateway.core.logger import logger
from gateway.core.provider import DataProvider, HealthStatus


class RequiredProviderInitError(RuntimeError):
    """Raised when a provider marked `required: true` fails to initialize.

    In production mode (`strict_required=True`) this aborts gateway startup
    rather than booting in a degraded state where /health is 200 but routes
    return "Provider access denied" for missing providers.
    """


class ProviderRegistry:
    """Manages provider lifecycle and routing."""

    def __init__(self):
        self._providers: dict[str, DataProvider] = {}
        self._config: dict[str, Any] = {}
        self._routes: dict[str, list[str]] = {}
        self._failed_required: list[tuple[str, str]] = []  # (name, error)

    async def load_from_config(self, config_path: Path, strict_required: bool = False) -> None:
        """Load and initialize all enabled providers from config.

        Args:
            config_path: Path to providers.yaml
            strict_required: When True, raise RequiredProviderInitError if any
                provider marked `required: true` in YAML fails to import or
                initialize. When False (default for local dev), failures are
                logged but startup continues. Production deployments should
                pass True so degraded boots fail loudly.
        """
        if not config_path.exists():
            logger.warning("providers_config_not_found", path=str(config_path))
            return

        with open(config_path) as f:
            self._config = yaml.safe_load(f)

        providers_config = self._config.get("providers", {})
        self._failed_required = []

        for name, provider_config in providers_config.items():
            if not provider_config.get("enabled", True):
                logger.info("provider_disabled", provider=name)
                continue

            is_required = bool(provider_config.get("required", False))
            try:
                await self._load_provider(name, provider_config)
            except Exception as e:
                if is_required:
                    self._failed_required.append((name, str(e)))
                logger.error(
                    "provider_load_failed",
                    provider=name,
                    required=is_required,
                    error=str(e),
                    exc_info=True,
                )

            # _load_provider returns early on import/missing-module without raising —
            # check whether the provider was actually registered for required providers.
            if is_required and name not in self._providers and not any(n == name for n, _ in self._failed_required):
                self._failed_required.append((name, "provider failed to register (import or config error)"))

        # Load routes
        self._routes = self._config.get("routes", {})

        logger.info(
            "providers_loaded",
            count=len(self._providers),
            names=list(self._providers.keys()),
            failed_required=[n for n, _ in self._failed_required],
        )

        if strict_required and self._failed_required:
            failures = "; ".join(f"{n}: {err}" for n, err in self._failed_required)
            raise RequiredProviderInitError(
                f"{len(self._failed_required)} required provider(s) failed to initialize: {failures}. "
                "Set GATEWAY_DEBUG=true for local development tolerance, or fix the configuration."
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

        async def _check_provider(name: str, provider: DataProvider) -> tuple[str, HealthStatus]:
            try:
                return name, await provider.health_check()
            except Exception as e:
                return name, HealthStatus(healthy=False, error=str(e))

        results: dict[str, HealthStatus] = {}
        checks = await asyncio.gather(*(_check_provider(name, provider) for name, provider in self._providers.items()))
        for name, status in checks:
            results[name] = status
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
            capabilities = provider.capabilities
            if isinstance(capabilities, list):
                return capabilities
            if isinstance(capabilities, tuple | set):
                return list(capabilities)
        return []

    def set_provider_enabled(self, name: str, enabled: bool) -> None:
        """Enable/disable a provider."""
        providers_config = self._config.get("providers", {})
        if name in providers_config:
            providers_config[name]["enabled"] = enabled
            # Note: This is in-memory only; persisting requires writing to YAML
            logger.info("provider_enabled_changed", provider=name, enabled=enabled)

    def get_ordered_providers(self, capability: str) -> list[DataProvider]:
        """Get providers with a capability, ordered by priority (descending)."""
        cap_map = {
            "stock_bars": "supports_bars",
            "stock_quotes": "supports_quotes",
            "stock_trades": "supports_trades",
            "news": "supports_news",
        }
        attr_name = cap_map.get(capability, capability)

        def _supports_capability(provider_obj: DataProvider) -> bool:
            capabilities = provider_obj.capabilities
            if isinstance(capabilities, list | tuple | set):
                return capability in capabilities or attr_name in capabilities
            return bool(getattr(capabilities, attr_name, False))

        candidates = []
        for name, provider in self._providers.items():
            if not _supports_capability(provider):
                continue

            # Check enabled/priority schema
            # We use the internal _config dict which holds the loaded yaml
            # Structure: providers -> {name} -> {enabled: bool, priority: int}
            p_config = self._config.get("providers", {}).get(name, {})

            if not p_config.get("enabled", True):
                continue

            priority = p_config.get("priority", 50)
            candidates.append((priority, provider))

        # Sort by priority descending (higher number = higher priority)
        candidates.sort(key=lambda x: x[0], reverse=True)
        return [p for _, p in candidates]
