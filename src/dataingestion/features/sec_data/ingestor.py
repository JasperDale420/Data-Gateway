import httpx
import asyncio
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataingestion.shared.base import BaseIngestor
from dataingestion.shared.config import get_settings
from dataingestion.features.sec_data.schemas import SecQuery, FilingResponse, Filing
from dataingestion.shared.storage import data_lake

settings = get_settings()


class AsyncRateLimiter:
    def __init__(self, max_calls: int, period: float = 1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            self.calls = [t for t in self.calls if now - t < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.calls[0] + self.period - now
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
            self.calls.append(time.time())


class SecIngestor(BaseIngestor):
    BASE_URL = "https://data.sec.gov"
    TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

    def __init__(self):
        self.headers = {"User-Agent": settings.SEC_USER_AGENT}
        self.ticker_map: Optional[Dict[str, str]] = None
        self.rate_limiter = AsyncRateLimiter(max_calls=10, period=1.0)

    async def _get_cik(self, ticker: str) -> str:
        if not self.ticker_map:
            await self.rate_limiter.acquire()
            async with httpx.AsyncClient() as client:
                response = await client.get(self.TICKERS_URL, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                # data is { "0": { "cik_str": 123, "ticker": "AAA", "title": "..." }, ... }
                self.ticker_map = {
                    entry["ticker"].upper(): str(entry["cik_str"]).zfill(10)
                    for entry in data.values()
                }

        return self.ticker_map.get(ticker.upper())

    async def ingest(self, query: SecQuery) -> FilingResponse:
        cik = await self._get_cik(query.ticker)
        if not cik:
            raise ValueError(f"Ticker {query.ticker} not found")

        url = f"{self.BASE_URL}/submissions/CIK{cik}.json"

        await self.rate_limiter.acquire()
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

        filing_response = await self.validate(data)

        # Write to Data Lake (Bronze)
        for filing in filing_response.filings:
            # We save the individual filing metadata/pointer, not the full document content here?
            # Actually ingest() seems to fetch the submission JSON which contains a list of filings.
            # The loop below saves each filing object.
            # Let's save the *entire* submission JSON as one Bronze file, which is more "Raw".
            pass

        # Save the full submission JSON to Bronze
        data_lake.save_bronze(
            data=data,
            provider="sec",
            slice_name="submissions",
            asset_class="equity",
            symbol=query.ticker,
            date=datetime.now(),
            filename=f"submission_{cik}",
        )

        return filing_response

    async def get_company_facts(self, ticker: str) -> Dict[str, Any]:
        """Fetch XBRL company facts (PIT data)."""
        cik = await self._get_cik(ticker)
        if not cik:
            raise ValueError(f"Ticker {ticker} not found")

        # Note: companyfacts URL is slightly different
        url = f"{self.BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"

        await self.rate_limiter.acquire()
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

        # Save raw JSON to Bronze
        data_lake.save_bronze(
            data=data,
            provider="sec",
            slice_name="company_facts",
            asset_class="equity",
            symbol=ticker,
            date=datetime.now(),
            filename=f"facts_{cik}",
        )

        return data

    async def save_filings(self, response: FilingResponse) -> str:
        """
        Save filings to the data lake.
        Path: raw/sec/filings/{cik}/date={date}/filings.parquet
        """
        if not response.filings:
            return ""

        # Convert filings to DataFrame for Parquet storage
        # Note: save_bronze supports DataFrame -> Parquet
        import pandas as pd

        df = pd.DataFrame([f.model_dump() for f in response.filings])

        # Use today's date for ingestion partition
        today = datetime.now()

        return data_lake.save_bronze(
            data=df,
            provider="sec",
            slice_name="filings",
            asset_class="equity",
            symbol=response.cik,
            date=today,
            filename="filings_summary",
        )

    async def validate(self, data: Dict[str, Any]) -> FilingResponse:
        # Transform the 'filings' -> 'recent' structure into a list of objects
        recent = data.get("filings", {}).get("recent", {})
        filings_list = self._parse_recent(recent)

        # Check for additional historical files
        files = data.get("filings", {}).get("files", [])
        for file_info in files:
            file_name = file_info.get("name")
            if file_name:
                url = f"{self.BASE_URL}/submissions/{file_name}"
                await self.rate_limiter.acquire()
                async with httpx.AsyncClient() as client:
                    try:
                        response = await client.get(url, headers=self.headers)
                        response.raise_for_status()
                        historical_data = response.json()
                        # historical_recent = historical_data.get("recent", {})
                        # Actually, submission-X.json usually has the same structure as 'recent' block directly at root,
                        # OR it mimics the main structure. Let's assume it's the 'recent' dict directly.
                        # Verification needed: usually it is a dict with accessionNumber, etc.
                        # Let's try parsing it as 'recent'
                        filings_list.extend(self._parse_recent(historical_data))
                    except Exception as e:
                        print(f"Failed to fetch historical file {file_name}: {e}")

        return FilingResponse(
            cik=data["cik"],
            entityType=data["entityType"],
            sic=data["sic"],
            sicDescription=data["sicDescription"],
            name=data["name"],
            tickers=data["tickers"],
            exchanges=data["exchanges"],
            ein=data["ein"],
            description=data["description"],
            website=data["website"],
            investorWebsite=data["investorWebsite"],
            category=data["category"],
            fiscalYearEnd=data["fiscalYearEnd"],
            stateOfIncorporation=data["stateOfIncorporation"],
            stateOfIncorporationDescription=data["stateOfIncorporationDescription"],
            filings=filings_list,
        )

    def _parse_recent(self, recent: Dict[str, Any]) -> List[Filing]:
        filings_list = []
        if recent:
            # All lists in 'recent' should be the same length
            # Check if accessionNumber exists
            if "accessionNumber" not in recent:
                return []

            count = len(recent["accessionNumber"])
            for i in range(count):
                filing_data = {
                    "accessionNumber": recent["accessionNumber"][i],
                    "filingDate": recent["filingDate"][i],
                    "reportDate": recent["reportDate"][i],
                    "acceptanceDateTime": recent["acceptanceDateTime"][i],
                    "act": recent["act"][i],
                    "form": recent["form"][i],
                    "fileNumber": recent["fileNumber"][i],
                    "filmNumber": recent["filmNumber"][i],
                    "items": recent["items"][i],
                    "size": recent["size"][i],
                    "isXBRL": recent["isXBRL"][i],
                    "isInlineXBRL": recent["isInlineXBRL"][i],
                    "primaryDocument": recent["primaryDocument"][i],
                    "primaryDocDescription": recent["primaryDocDescription"][i],
                }
                filings_list.append(Filing(**filing_data))
        return filings_list


sec_ingestor = SecIngestor()
