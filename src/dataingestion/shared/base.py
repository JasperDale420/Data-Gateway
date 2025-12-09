from abc import ABC, abstractmethod
from typing import Any


class BaseIngestor(ABC):
    """Abstract base class for all data ingestors."""

    @abstractmethod
    async def ingest(self, *args, **kwargs) -> Any:
        """Core method to fetch data."""
        pass

    @abstractmethod
    async def validate(self, data: Any) -> bool:
        """Validate fetched data."""
        pass
