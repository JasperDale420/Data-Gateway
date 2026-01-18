"""Core gateway components."""

from gateway.core.auth import Client, ClientAuthenticator
from gateway.core.balancer import KeyLoadBalancer, alpaca_key_balancer
from gateway.core.cache import InMemoryCache
from gateway.core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitOpenError,
    CircuitState,
    get_circuit_breaker,
    get_circuit_registry,
)
from gateway.core.connections import ConnectionManager
from gateway.core.dedup import RequestDeduplicator, get_deduplicator
from gateway.core.multiplexer import SubscriptionManager
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.core.redis_cache import HybridCache, RedisCache
from gateway.core.registry import ProviderRegistry
from gateway.core.replay import (
    ReplayConfig,
    ReplayMessage,
    ReplaySession,
    ReplaySessionManager,
    ReplayState,
    get_replay_manager,
)
from gateway.core.ring_buffer import MessageRingBuffer, get_ring_buffer
from gateway.core.validator import (
    DataValidator,
    ValidationError,
    ValidationErrorCodes,
    ValidationResult,
    get_validator,
)

__all__ = [
    "ClientAuthenticator",
    "Client",
    "InMemoryCache",
    "RedisCache",
    "HybridCache",
    "ConnectionManager",
    "DataProvider",
    "ProviderCapabilities",
    "HealthStatus",
    "ProviderRegistry",
    "SubscriptionManager",
    "KeyLoadBalancer",
    "alpaca_key_balancer",
    "MessageRingBuffer",
    "get_ring_buffer",
    "RequestDeduplicator",
    "get_deduplicator",
    "DataValidator",
    "ValidationResult",
    "ValidationError",
    "ValidationErrorCodes",
    "get_validator",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "CircuitState",
    "CircuitOpenError",
    "get_circuit_breaker",
    "get_circuit_registry",
    "ReplaySession",
    "ReplaySessionManager",
    "ReplayConfig",
    "ReplayState",
    "ReplayMessage",
    "get_replay_manager",
]

# Security module exports
from gateway.core.security import (
    APIKey,
    APIKeyManager,
    InputValidationError,
    InputValidator,
    Permission,
    Role,
    SecretsManager,
    ValidationErrorCode,
    check_permission,
    get_api_key_manager,
    get_input_validator,
    get_secrets_manager,
)

__all__ += [
    "InputValidator",
    "InputValidationError",
    "ValidationErrorCode",
    "APIKeyManager",
    "APIKey",
    "Role",
    "Permission",
    "check_permission",
    "SecretsManager",
    "get_input_validator",
    "get_api_key_manager",
    "get_secrets_manager",
]
