"""News models."""

from datetime import datetime

from pydantic import BaseModel, Field

__all__ = [
    "NormalizedNewsImage",
    "NormalizedNewsArticle",
]


class NormalizedNewsImage(BaseModel):
    """Image associated with a news article."""

    url: str
    size: str | None = None  # "thumb", "small", "large"


class NormalizedNewsArticle(BaseModel):
    """Normalized news article."""

    article_id: str
    headline: str
    summary: str | None = None
    content: str | None = None
    url: str | None = None
    source: str
    author: str | None = None
    published_at: datetime
    updated_at: datetime | None = None  # Alpaca: updated_at (RFC-3339)
    symbols: list[str] = Field(default_factory=list)
    images: list[NormalizedNewsImage] = Field(default_factory=list)  # Alpaca: images (thumb/small/large)
    provider: str
