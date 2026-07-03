"""UW fundamentals endpoints — financials, company profiles, IPO calendar, ticker directory.

Raw-HTTP passthrough endpoints not covered by the vendored SDK (v5.1).
"""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_LIMIT,
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    count_of,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/calendar/ipo", response_model=SuccessResponse)
async def get_ipo_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Upcoming IPOs in the next 3 months."""
    return await execute_uw_cached(
        cache=cache,
        cache_key="uw:ipo-calendar",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ipo_calendar(),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/companies/listings", response_model=SuccessResponse)
async def get_company_listings(
    status: str | None = Query(default=None, description="`active` or `delisted`"),
    date: str | None = Query(default=None, description="Only used when status=delisted. ISO date."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """All US-traded securities, optionally filtered to delisted as of a date."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:company-listings:{status or 'all'}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_company_listings(status=status, date_str=date),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/companies/{symbol}/dividends", response_model=SuccessResponse)
async def get_company_dividends(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Historical dividend events for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:company-dividends:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_company_dividends(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/companies/{symbol}/earnings-estimates", response_model=SuccessResponse)
async def get_earnings_estimates(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Analyst-driven forward earnings estimates by quarter/year."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:earnings-estimates:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_earnings_estimates(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/companies/{symbol}/profile", response_model=SuccessResponse)
async def get_company_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Normalized company profile (sector, industry, market cap, P/E)."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:company-profile:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_company_profile(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/companies/{symbol}/splits", response_model=SuccessResponse)
async def get_company_splits(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Historical stock split events for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:company-splits:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_company_splits(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/companies/{symbol}/transcripts/{quarter}", response_model=SuccessResponse)
async def get_earnings_transcript(
    symbol: str,
    quarter: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Full earnings-call transcript for a ticker and quarter (e.g. 2024Q1)."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:earnings-transcript:{symbol}:{quarter}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_earnings_transcript(symbol=symbol, quarter=quarter),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock-directory/ticker-exchanges", response_model=SuccessResponse)
async def get_ticker_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Mapping of all tickers to their exchanges."""
    return await execute_uw_cached(
        cache=cache,
        cache_key="uw:ticker-exchanges",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_exchanges(),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/stock/{symbol}/balance-sheets", response_model=SuccessResponse)
async def get_balance_sheets(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Balance sheet data for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:balance-sheets:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_balance_sheets(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/cash-flows", response_model=SuccessResponse)
async def get_cash_flows(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Cash flow statement data for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:cash-flows:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_cash_flows(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/earnings", response_model=SuccessResponse)
async def get_stock_earnings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Earnings history for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:stock-earnings:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_stock_earnings(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/financials", response_model=SuccessResponse)
async def get_stock_financials(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Full financial data (income, balance sheets, cash flows, earnings)."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:stock-financials:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_stock_financials(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/fundamental-breakdown", response_model=SuccessResponse)
async def get_fundamental_breakdown(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Fundamental financial data (EPS, revenue, dividends, share counts)."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:fundamental-breakdown:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_fundamental_breakdown(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/income-statements", response_model=SuccessResponse)
async def get_income_statements(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Income statement data for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:income-statements:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_income_statements(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/ownership", response_model=SuccessResponse)
async def get_stock_ownership(
    symbol: str,
    limit: int | None = Query(default=None, ge=1, description=DESC_LIMIT),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Institutions, insider trades, and politicians with the most shares."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:stock-ownership:{symbol}:{limit or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_stock_ownership(symbol=symbol, limit=limit),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/technical-indicator/{function}", response_model=SuccessResponse)
async def get_technical_indicator(
    symbol: str,
    function: str,
    interval: str | None = Query(default=None, description="`1min`, `5min`, `15min`, `30min`, `60min`, `daily`"),
    time_period: int | None = Query(default=None, ge=1, description="Number of periods"),
    series_type: str | None = Query(default=None, description="`close`, `open`, `high`, `low`"),
    month: str | None = Query(default=None, description="Month filter (YYYY-MM)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Technical indicator time series for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:technical-indicator:{symbol}:{function}:"
            f"{interval or 'def'}:{time_period or 'def'}:{series_type or 'def'}:{month or 'all'}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_technical_indicator(
            symbol=symbol,
            function=function,
            interval=interval,
            time_period=time_period,
            series_type=series_type,
            month=month,
        ),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )
