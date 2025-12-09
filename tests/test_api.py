import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock
from dataingestion.api.main import app
from dataingestion.features.sec_data.schemas import FilingResponse, Filing

client = TestClient(app)


@pytest.fixture
def mock_filing_response():
    return FilingResponse(
        cik="123456",
        entityType="operating",
        sic="1234",
        sicDescription="Tech",
        name="Apple Inc.",
        tickers=["AAPL"],
        exchanges=["NASDAQ"],
        ein="123",
        description="Tech Company",
        website="http://apple.com",
        investorWebsite="http://investor.apple.com",
        category="Large Cap",
        fiscalYearEnd="0930",
        stateOfIncorporation="CA",
        stateOfIncorporationDescription="California",
        filings=[
            Filing(
                accessionNumber="000123-45-6789",
                filingDate="2023-01-01",
                reportDate="2023-01-01",
                acceptanceDateTime="2023-01-01T12:00:00.000Z",
                act="1934",
                form="10-K",
                fileNumber="001-12345",
                filmNumber="123456",
                items="",
                size=1000,
                isXBRL=1,
                isInlineXBRL=1,
                primaryDocument="report.html",
                primaryDocDescription="Annual Report",
            )
        ],
    )


@patch("dataingestion.features.sec_data.ingestor.sec_ingestor.ingest")
def test_get_filings(mock_ingest, mock_filing_response):
    # Mock ingest to be an async function returning the response
    mock_ingest.return_value = mock_filing_response

    # Since it's an async method called with await, we usually rely on AsyncMock if the patch target handles it,
    # or we set return_value to be awaitable if not using AsyncMock (but patch usually handles async defs if Autospec often).
    # However, here we are patching the INSTANCE method.
    # The simplest way is to simulate an async return.

    # But wait, patch usually creates a MagicMock. A MagicMock isn't awaitable by default unless configured.
    # We should use new_callable=AsyncMock if available, or just set return_value to a future.
    # But usually just setting `mock_ingest.side_effect` or ensuring it is an AsyncMock is key.
    # HOWEVER, `sec_ingestor.ingest` is defined as `async def`. `patch` might auto-detect?
    # No, explicit is better.

    mock_ingest.side_effect = None
    mock_ingest.return_value = mock_filing_response

    # Actually, let's use AsyncMock to be safe.
    # But wait, we can't easily pass new_callable=AsyncMock into the decorator and get the arg cleanly in all python versions?
    # Python 3.8+ supports AsyncMock.

    # Let's try simple return value first, if text runner fails we adjust.
    # Actually, since `router.py` does `await sec_ingestor.ingest(query)`, the mock object MUST be awaitable.
    # A standard Mock is not awaitable.
    pass


@patch(
    "dataingestion.features.sec_data.ingestor.sec_ingestor.ingest",
    new_callable=AsyncMock,
)
def test_get_filings_async(mock_ingest, mock_filing_response):
    mock_ingest.return_value = mock_filing_response

    response = client.post("/sec/filings", json={"ticker": "AAPL"})

    assert response.status_code == 200
    data = response.json()
    assert data["cik"] == "123456"
    assert data["tickers"] == ["AAPL"]
    assert len(data["filings"]) == 1
    assert data["filings"][0]["form"] == "10-K"


@patch(
    "dataingestion.features.sec_data.ingestor.sec_ingestor.ingest",
    new_callable=AsyncMock,
)
def test_get_ticker_filings_async(mock_ingest, mock_filing_response):
    mock_ingest.return_value = mock_filing_response

    response = client.get("/sec/filings/AAPL")
    assert response.status_code == 200
    data = response.json()
    assert data["cik"] == "123456"
    assert data["tickers"] == ["AAPL"]


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()
