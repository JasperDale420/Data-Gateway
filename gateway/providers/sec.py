"""SEC EDGAR data provider for filings and company data."""

import os
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from gateway.core.http_client import create_async_http_client
from gateway.core.metrics import httpx_event_hooks
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities

logger = structlog.get_logger()

# SEC EDGAR API endpoints (free, no API key required)
SEC_BASE_URL = "https://data.sec.gov"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# SEC requires User-Agent header
DEFAULT_SEC_USER_AGENT = "Empire Trading Framework contact@empire-trading.com"

# Cache TTL for SEC data (filings update infrequently)
SEC_CACHE_TTL = 3600


class SECProvider(DataProvider):
    """SEC EDGAR data provider for filings, company data, and institutional holdings.

    Uses the free data.sec.gov API. No API key required.
    Rate limit: 10 requests/second (SEC fair use policy).
    """

    def __init__(self):
        self._client: httpx.AsyncClient | None = None
        self._ticker_to_cik: dict[str, str] = {}

    @property
    def name(self) -> str:
        return "sec"

    @property
    def supported_feeds(self) -> list[str]:
        return ["filings", "company", "13f", "insiders"]

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=False,
            supports_quotes=False,
            supports_trades=False,
            supports_options=False,
            supports_streaming=False,
            supports_historical=True,
            max_symbols_per_request=1,
            max_historical_range_days=3650,
            rate_limit_requests_per_minute=600,  # 10/sec
            supports_adjusted_prices=False,
            supports_extended_hours=False,
            supported_timeframes=[],
        )

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize SEC provider with HTTP client."""
        user_agent_env = config.get("user_agent_env", "SEC_USER_AGENT")
        user_agent = os.environ.get(user_agent_env, DEFAULT_SEC_USER_AGENT)

        self._client = create_async_http_client(
            headers={"User-Agent": user_agent},
            timeout=30.0,
            event_hooks=httpx_event_hooks("sec"),
        )

        # Load ticker to CIK mapping
        await self._load_ticker_mapping()

        logger.info("sec_provider_initialized")

    async def shutdown(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("sec_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Check SEC API connectivity."""
        if not self._client:
            return HealthStatus(healthy=False, error="Client not initialized")

        try:
            start = datetime.now(UTC)
            # Check a known company (Apple)
            response = await self._client.get(f"{SEC_BASE_URL}/submissions/CIK0000320193.json")
            latency = (datetime.now(UTC) - start).total_seconds() * 1000

            if response.status_code == 200:
                return HealthStatus(
                    healthy=True,
                    latency_ms=latency,
                    last_check=datetime.now(UTC),
                )
            else:
                return HealthStatus(
                    healthy=False,
                    error=f"HTTP {response.status_code}",
                    last_check=datetime.now(UTC),
                )
        except Exception as e:
            return HealthStatus(
                healthy=False,
                error=str(e),
                last_check=datetime.now(UTC),
            )

    async def _load_ticker_mapping(self) -> None:
        """Load ticker to CIK mapping from SEC."""
        if not self._client:
            return

        try:
            response = await self._client.get(SEC_TICKERS_URL)
            if response.status_code == 200:
                data = response.json()
                for _, company in data.items():
                    ticker = company.get("ticker", "").upper()
                    cik = str(company.get("cik_str", "")).zfill(10)
                    if ticker:
                        self._ticker_to_cik[ticker] = cik
                logger.info("sec_ticker_mapping_loaded", count=len(self._ticker_to_cik))
        except Exception as e:
            logger.warning("sec_ticker_mapping_failed", error=str(e))

    def _format_cik(self, cik: str) -> str:
        """Format CIK to 10-digit with leading zeros."""
        return cik.zfill(10)

    # ─────────────────────────────────────────────────────────────────
    # Company Lookup
    # ─────────────────────────────────────────────────────────────────

    async def get_cik_by_ticker(self, ticker: str) -> str | None:
        """Get CIK for a ticker symbol."""
        return self._ticker_to_cik.get(ticker.upper())

    async def get_company_info(self, cik: str) -> dict[str, Any]:
        """Get company info and filing history by CIK."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        cik_formatted = self._format_cik(cik)

        response = await self._client.get(f"{SEC_BASE_URL}/submissions/CIK{cik_formatted}.json")
        response.raise_for_status()
        data = response.json()

        return {
            "cik": cik_formatted,
            "name": data.get("name"),
            "sic": data.get("sic"),
            "sic_description": data.get("sicDescription"),
            "tickers": data.get("tickers", []),
            "exchanges": data.get("exchanges", []),
            "ein": data.get("ein"),
            "state": data.get("stateOfIncorporation"),
            "fiscal_year_end": data.get("fiscalYearEnd"),
            "website": data.get("website"),
            "investor_website": data.get("investorWebsite"),
            "category": data.get("category"),
            "entity_type": data.get("entityType"),
        }

    async def get_company_by_ticker(self, ticker: str) -> dict[str, Any]:
        """Get company info by ticker symbol."""
        cik = await self.get_cik_by_ticker(ticker)
        if not cik:
            raise ValueError(f"Ticker {ticker} not found in SEC database")
        return await self.get_company_info(cik)

    # ─────────────────────────────────────────────────────────────────
    # Filings
    # ─────────────────────────────────────────────────────────────────

    async def get_filings(
        self,
        cik: str,
        form_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get company filings by CIK, optionally filtered by form type."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        cik_formatted = self._format_cik(cik)

        response = await self._client.get(f"{SEC_BASE_URL}/submissions/CIK{cik_formatted}.json")
        response.raise_for_status()
        data = response.json()

        recent = data.get("filings", {}).get("recent", {})

        # Build list of filings
        filings = []
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_documents = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])

        for i in range(min(len(forms), limit)):
            form = forms[i] if i < len(forms) else None

            # Filter by form type if specified
            if form_type and form != form_type.upper():
                continue

            filings.append(
                {
                    "form": form,
                    "filing_date": filing_dates[i] if i < len(filing_dates) else None,
                    "accession_number": (accession_numbers[i] if i < len(accession_numbers) else None),
                    "primary_document": (primary_documents[i] if i < len(primary_documents) else None),
                    "description": descriptions[i] if i < len(descriptions) else None,
                }
            )

            if len(filings) >= limit:
                break

        return filings

    # ─────────────────────────────────────────────────────────────────
    # 13F Institutional Holdings
    # ─────────────────────────────────────────────────────────────────

    async def get_13f_holdings(self, cik: str) -> list[dict[str, Any]]:
        """Get 13F institutional holdings for an investment manager."""
        # 13F filings are form type "13F-HR"
        filings = await self.get_filings(cik, form_type="13F-HR", limit=10)
        return filings

    # ─────────────────────────────────────────────────────────────────
    # Insider Trades (Form 3/4/5)
    # ─────────────────────────────────────────────────────────────────

    async def get_insider_trades(self, cik: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get insider trading filings (Form 3, 4, 5)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        cik_formatted = self._format_cik(cik)

        response = await self._client.get(f"{SEC_BASE_URL}/submissions/CIK{cik_formatted}.json")
        response.raise_for_status()
        data = response.json()

        recent = data.get("filings", {}).get("recent", {})

        # Filter for insider trading forms
        insider_forms = {"3", "4", "5"}
        filings = []
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])

        for i in range(len(forms)):
            if forms[i] in insider_forms:
                filings.append(
                    {
                        "form": forms[i],
                        "filing_date": filing_dates[i] if i < len(filing_dates) else None,
                        "accession_number": (accession_numbers[i] if i < len(accession_numbers) else None),
                    }
                )

                if len(filings) >= limit:
                    break

        return filings

    # ─────────────────────────────────────────────────────────────────
    # XBRL Company Facts
    # ─────────────────────────────────────────────────────────────────

    async def get_company_facts(self, cik: str) -> dict[str, Any]:
        """Get XBRL company facts (financial data from filings)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        cik_formatted = self._format_cik(cik)

        response = await self._client.get(f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik_formatted}.json")
        response.raise_for_status()
        data = response.json()

        return {
            "cik": cik_formatted,
            "entity_name": data.get("entityName"),
            "facts": data.get("facts", {}),
        }

    # ─────────────────────────────────────────────────────────────────
    # Phase 4: Advanced XBRL & Search
    # ─────────────────────────────────────────────────────────────────

    async def get_company_concept(
        self,
        cik: str,
        taxonomy: str = "us-gaap",
        concept: str = "Revenues",
    ) -> dict[str, Any]:
        """Get historical values for a single XBRL concept (e.g., Revenue, EPS)."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        cik_formatted = self._format_cik(cik)

        response = await self._client.get(
            f"{SEC_BASE_URL}/api/xbrl/companyconcept/CIK{cik_formatted}/{taxonomy}/{concept}.json"
        )
        response.raise_for_status()
        data = response.json()

        return {
            "cik": cik_formatted,
            "taxonomy": taxonomy,
            "concept": concept,
            "entity_name": data.get("entityName"),
            "label": data.get("label"),
            "description": data.get("description"),
            "units": data.get("units", {}),
        }

    async def get_xbrl_frames(
        self,
        taxonomy: str = "us-gaap",
        concept: str = "Revenues",
        unit: str = "USD",
        period: str = "CY2023",
    ) -> dict[str, Any]:
        """Get aggregated XBRL data across all companies for a period."""
        if not self._client:
            raise RuntimeError("Provider not initialized")

        response = await self._client.get(f"{SEC_BASE_URL}/api/xbrl/frames/{taxonomy}/{concept}/{unit}/{period}.json")
        response.raise_for_status()
        data = response.json()

        return {
            "taxonomy": taxonomy,
            "concept": concept,
            "unit": unit,
            "period": period,
            "label": data.get("label"),
            "description": data.get("description"),
            "data": data.get("data", []),
        }

    async def search_filings(
        self,
        query: str,
        form_type: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Search filings by keywords using EDGAR full-text search.

        Args:
            query: Search keywords
            form_type: Optional form type filter (e.g., "10-K", "8-K")
            start_date: Start date in YYYY-MM-DD format (default: 2020-01-01)
            end_date: End date in YYYY-MM-DD format (default: today)
            limit: Max results to return
        """
        if not self._client:
            raise RuntimeError("Provider not initialized")

        # Default date range if not specified
        if not start_date:
            start_date = "2020-01-01"
        if not end_date:
            end_date = datetime.now(UTC).strftime("%Y-%m-%d")

        # Note: SEC full-text search is at efts.sec.gov
        params = {
            "q": query,
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
        }
        if form_type:
            params["forms"] = form_type

        try:
            response = await self._client.get(
                "https://efts.sec.gov/LATEST/search-index",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            hits = data.get("hits", {}).get("hits", [])
            results = []
            for hit in hits[:limit]:
                source = hit.get("_source", {})
                results.append(
                    {
                        "cik": source.get("ciks", [None])[0] if source.get("ciks") else None,
                        "company_name": (
                            source.get("display_names", [None])[0] if source.get("display_names") else None
                        ),
                        "form": source.get("form"),
                        "filing_date": source.get("file_date"),
                        "description": source.get("file_description"),
                    }
                )

            return results

        except Exception as e:
            logger.error("sec_search_failed", query=query, error=str(e))
            return []
