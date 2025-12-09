from fastapi import APIRouter, HTTPException
from dataingestion.features.sec_data.schemas import SecQuery, FilingResponse
from dataingestion.features.sec_data.ingestor import sec_ingestor
from dataingestion.shared.cache import cached

router = APIRouter(prefix="/sec", tags=["SEC"])


@router.post("/filings", response_model=FilingResponse)
@cached(ttl=300, prefix="sec_filings")  # Cache for 5 minutes
async def get_filings(query: SecQuery):
    """
    Search SEC filings using Lucene query syntax.
    Example query: "ticker:AAPL AND formType:10-K"
    """
    try:
        return await sec_ingestor.ingest(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filings/{ticker}", response_model=FilingResponse)
@cached(ttl=300, prefix="sec_ticker")
async def get_ticker_filings(ticker: str, form_type: str = "10-K"):
    """Helper endpoint for simple ticker searches."""
    # query_string = f"ticker:{ticker} AND formType:{form_type}"
    query = SecQuery(ticker=ticker)
    try:
        return await sec_ingestor.ingest(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
