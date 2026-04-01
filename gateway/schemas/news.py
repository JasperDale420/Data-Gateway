"""News models — re-exported from empire_schemas, plus gateway-specific models."""

from empire_schemas.responses import NormalizedNewsArticle
from pydantic import BaseModel

__all__ = [
    "NormalizedNewsImage",
    "NormalizedNewsArticle",
]


# Gateway-specific model (not in empire_schemas)
class NormalizedNewsImage(BaseModel):
    """Image associated with a news article."""

    url: str
    size: str | None = None  # "thumb", "small", "large"
