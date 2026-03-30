"""WebSocket/API infrastructure models."""

from typing import Literal

from pydantic import BaseModel, Field

__all__ = [
    "AuthMessage",
    "SubscribeMessage",
    "UnsubscribeMessage",
    "AuthResult",
    "SubscriptionAck",
    "HealthResponse",
    "ErrorResponse",
    "SuccessResponse",
    "ResponseMeta",
]


class AuthMessage(BaseModel):
    """Client authentication message."""

    action: Literal["auth"]
    key: str
    request_id: str | None = None


class SubscribeMessage(BaseModel):
    """Subscribe to symbols."""

    action: Literal["subscribe"]
    symbols: list[str]
    feeds: list[str] = Field(default_factory=lambda: ["bars"])
    feed: str | None = Field(default=None, description="Legacy: use feeds instead")
    request_id: str | None = None


class UnsubscribeMessage(BaseModel):
    """Unsubscribe from symbols."""

    action: Literal["unsubscribe"]
    symbols: list[str]
    feeds: list[str] | None = None
    feed: str | None = Field(default=None, description="Legacy: use feeds instead")
    request_id: str | None = None


class AuthResult(BaseModel):
    """Authentication result."""

    type: Literal["auth_result"]
    status: Literal["ok", "error"]
    client_id: str | None = None
    code: str | None = None
    message: str | None = None


class SubscriptionAck(BaseModel):
    """Subscription acknowledgement."""

    type: Literal["subscription_ack"]
    subscribed: list[str]
    failed: list[str] = Field(default_factory=list)
    feeds: list[str] | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str


class ErrorResponse(BaseModel):
    """Error response."""

    success: bool = False
    error: dict


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool = True
    data: dict | list | None = None  # Can be dict, list, or None depending on endpoint
    meta: dict | None = None
    pagination: dict | None = None  # For paginated endpoints


class ResponseMeta(BaseModel):
    """Common metadata for API responses."""

    provider: str = "alpaca"
    symbol: str | None = None
    timeframe: str | None = None
    count: int | None = None
