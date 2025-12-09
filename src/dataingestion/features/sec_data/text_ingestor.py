import httpx
import logging
from typing import Optional
from dataingestion.shared.config import get_settings
from dataingestion.features.sec_data.ingestor import sec_ingestor

logger = logging.getLogger(__name__)
settings = get_settings()


class TextIngestor:
    """
    Fetches full text (HTML) of SEC filings.
    """

    BASE_URL = "https://www.sec.gov/Archives/edgar/data"

    async def get_filing_text(
        self, cik: str, accession_number: str, primary_document: str
    ) -> Optional[str]:
        """
        Download the primary document text.
        URL Format: https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number_no_dashes}/{primary_document}
        """
        # Remove dashes from accession number for the URL
        acc_no_dashes = accession_number.replace("-", "")

        # Strip leading zeros from CIK for the URL (SEC Archives uses int-like CIK)
        cik_int = str(int(cik))

        url = f"{self.BASE_URL}/{cik_int}/{acc_no_dashes}/{primary_document}"

        logger.info(f"Fetching filing text from: {url}")

        await sec_ingestor.rate_limiter.acquire()
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url, headers=sec_ingestor.headers)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(f"Failed to fetch filing text: {e}")
                return None


text_ingestor = TextIngestor()
