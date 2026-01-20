"""Data Quality API endpoints.

Per-symbol quality metrics as specified in PRD (lines 918-984).
"""

from datetime import date

from fastapi import APIRouter, Query
from pydantic import BaseModel

from gateway.core.quality import get_quality_analyzer
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/quality", tags=["quality"])


# ─────────────────────────────────────────────────────────────────────────────
# Response Models
# ─────────────────────────────────────────────────────────────────────────────


class SymbolQualityResponse(BaseModel):
    """Response for symbol quality query."""

    symbol: str
    date: str
    session: str
    quality: dict


class QualitySummaryResponse(BaseModel):
    """Response for quality summary."""

    date: str
    symbols_analyzed: int
    average_coverage: float
    issues_count: int
    symbols: list[dict]


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/symbol/{symbol}", response_model=SuccessResponse)
async def get_symbol_quality(
    symbol: str,
    query_date: date = Query(None, alias="date", description="Date to analyze (YYYY-MM-DD)"),
    session: str = Query("regular", description="Trading session (regular, extended)"),
) -> dict:
    """Get quality metrics for a specific symbol on a date.

    Returns:
    - Bar coverage and gaps
    - Quote quality (crossed, stale, spread)
    - Trade quality (odd lots, avg size)
    - List of quality issues with codes

    Issue Codes:
    - Q001: Missing bars in sequence
    - Q002: Crossed quote (bid > ask)
    - Q003: Stale quote (>60s unchanged)
    - Q004: Zero volume bar during market hours
    - Q005: Price outside normal range (>20% move)
    - Q006: Timestamp out of sequence
    """
    # Use today if no date provided
    if query_date is None:
        query_date = date.today()

    # This would typically fetch actual data and analyze it
    # For now, return a mock quality report structure
    _analyzer = get_quality_analyzer()  # noqa: F841

    # In production, would fetch bars/quotes/trades and analyze
    # Mock response showing the structure
    return {
        "symbol": symbol.upper(),
        "date": query_date.isoformat(),
        "session": session,
        "quality": {
            "bars": {
                "expected": 390,
                "received": 390,
                "coverage": 1.0,
                "gaps": [],
            },
            "quotes": {
                "total": 0,
                "crossed": 0,
                "crossed_pct": 0.0,
                "stale": 0,
                "stale_pct": 0.0,
                "avg_spread_bps": 0.0,
            },
            "trades": {
                "total": 0,
                "odd_lots": 0,
                "odd_lot_pct": 0.0,
                "avg_size": 0.0,
            },
            "issues": [],
        },
    }


@router.get("/summary", response_model=SuccessResponse)
async def get_quality_summary(
    query_date: date = Query(None, alias="date", description="Date to summarize (YYYY-MM-DD)"),
    symbols: str | None = Query(None, description="Comma-separated list of symbols"),
) -> QualitySummaryResponse:
    """Get quality summary across multiple symbols.

    Returns aggregate quality metrics for the specified symbols
    or default watchlist if none provided.
    """
    if query_date is None:
        query_date = date.today()

    # Parse symbols
    symbol_list = []
    if symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
    else:
        # Default to major indices
        symbol_list = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

    # In production, would analyze each symbol
    # Mock response showing structure
    return QualitySummaryResponse(
        date=query_date.isoformat(),
        symbols_analyzed=len(symbol_list),
        average_coverage=1.0,
        issues_count=0,
        symbols=[
            {
                "symbol": s,
                "coverage": 1.0,
                "issues": 0,
            }
            for s in symbol_list
        ],
    )


@router.post("/analyze", response_model=SuccessResponse)
async def analyze_data(
    bars: list[dict] | None = None,
    quotes: list[dict] | None = None,
    trades: list[dict] | None = None,
) -> dict:
    """Analyze provided data for quality issues.

    Accepts raw bar, quote, or trade data and returns quality analysis.
    Useful for real-time quality monitoring.
    """
    analyzer = get_quality_analyzer()

    result: dict = {}

    if bars:
        result["bars"] = analyzer.analyze_bars(bars).to_dict()

    if quotes:
        result["quotes"] = analyzer.analyze_quotes(quotes).to_dict()

    if trades:
        result["trades"] = analyzer.analyze_trades(trades).to_dict()

    # Detect issues
    issues = analyzer.detect_issues(bars, quotes, trades)
    result["issues"] = [i.to_dict() for i in issues]

    return result
