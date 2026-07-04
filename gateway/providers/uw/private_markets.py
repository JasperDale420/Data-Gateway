"""UW private-markets mixin.

Raw-HTTP endpoints for UnusualWhales Nasdaq Private Markets (pre-IPO) data not
covered by the vendored SDK (v5.1): company listing / profile / funding rounds /
investors / management / pricing history, plus top investors, investor profiles,
and cross-dataset search. Premium endpoints.
"""

from typing import Any
from urllib.parse import quote


class UWPrivateMarketsMixin:
    """Mixin providing UW private-markets REST endpoints."""

    async def get_private_markets_companies(
        self,
        sector: str | None = None,
        name: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """List Nasdaq Private Markets companies, optionally filtered by sector or name."""
        return await self._raw_get(
            "/api/private-markets/companies",
            {"sector": sector, "name": name, "limit": limit, "offset": offset},
        )

    async def get_private_markets_company(self, npm_ticker: str) -> Any:
        """Profile for a single private-markets company."""
        return await self._raw_get(f"/api/private-markets/companies/{quote(npm_ticker)}", {})

    async def get_private_markets_funding(
        self,
        npm_ticker: str,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Funding round history for a single private-markets company."""
        return await self._raw_get(
            f"/api/private-markets/companies/{quote(npm_ticker)}/funding",
            {"limit": limit, "offset": offset},
        )

    async def get_private_markets_company_investors(
        self,
        npm_ticker: str,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Disclosed investors for a single private-markets company."""
        return await self._raw_get(
            f"/api/private-markets/companies/{quote(npm_ticker)}/investors",
            {"limit": limit, "offset": offset},
        )

    async def get_private_markets_management(
        self,
        npm_ticker: str,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Disclosed management/leadership for a single private-markets company."""
        return await self._raw_get(
            f"/api/private-markets/companies/{quote(npm_ticker)}/management",
            {"limit": limit, "offset": offset},
        )

    async def get_private_markets_pricing(
        self,
        npm_ticker: str,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Historical implied per-share pricing for a single private-markets company."""
        return await self._raw_get(
            f"/api/private-markets/companies/{quote(npm_ticker)}/pricing",
            {"start_date": start_date, "end_date": end_date, "limit": limit, "offset": offset},
        )

    async def get_top_private_markets_investors(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Most prolific investors across the private-markets dataset."""
        return await self._raw_get(
            "/api/private-markets/investors",
            {"limit": limit, "offset": offset},
        )

    async def get_private_markets_investor(self, name: str) -> Any:
        """Portfolio of companies for a specific investor (by name)."""
        return await self._raw_get(f"/api/private-markets/investors/{quote(name)}", {})

    async def get_private_markets_search(
        self,
        query: str,
        limit: int | None = None,
    ) -> Any:
        """Substring-search across private-markets companies and investors."""
        return await self._raw_get(
            "/api/private-markets/search",
            {"query": query, "limit": limit},
        )
