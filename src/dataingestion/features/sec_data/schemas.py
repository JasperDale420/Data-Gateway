from pydantic import BaseModel, Field
from typing import List


class Filing(BaseModel):
    accessionNumber: str
    filingDate: str
    reportDate: str
    acceptanceDateTime: str
    act: str
    form: str
    fileNumber: str
    filmNumber: str
    items: str
    size: int
    isXBRL: int
    isInlineXBRL: int
    primaryDocument: str
    primaryDocDescription: str


class FilingResponse(BaseModel):
    cik: str
    entityType: str
    sic: str
    sicDescription: str
    name: str
    tickers: List[str]
    exchanges: List[str]
    ein: str
    description: str
    website: str
    investorWebsite: str
    category: str
    fiscalYearEnd: str
    stateOfIncorporation: str
    stateOfIncorporationDescription: str
    filings: List[Filing]


class SecQuery(BaseModel):
    ticker: str = Field(..., description="Ticker symbol to look up")
