"""UW congress-extension endpoints — window analytics, unusual congressional trades,
politician portfolios, v2 institutional activity, and v2 short interest.

Raw-HTTP passthrough endpoints not covered by the vendored SDK (v5.1).
"""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
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


@router.get("/analytics/sliding", response_model=SuccessResponse)
async def get_sliding_window_analytics(
    symbols: str = Query(description="Comma-separated tickers"),
    range: str = Query(description="ISO date start or relative range"),
    calculations: str = Query(description="Comma-separated calculations"),
    range_end: str | None = Query(default=None, description="Optional end date for ISO range"),
    interval: str | None = Query(default=None, description="Sampling interval"),
    ohlc: str | None = Query(default=None, description="OHLC field"),
    window_size: int | None = Query(default=None, description="Sliding window size"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Sliding-window statistical analytics across one or more tickers."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:analytics-sliding:{symbols}:{range}:{range_end or 'none'}:"
            f"{interval or 'def'}:{ohlc or 'def'}:{window_size or 'def'}:{calculations}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_sliding_window_analytics(
            symbols=symbols,
            range=range,
            calculations=calculations,
            range_end=range_end,
            interval=interval,
            ohlc=ohlc,
            window_size=window_size,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/analytics/window", response_model=SuccessResponse)
async def get_fixed_window_analytics(
    symbols: str = Query(description="Comma-separated tickers"),
    range: str = Query(description="ISO date start or relative range"),
    calculations: str = Query(description="Comma-separated calculations"),
    range_end: str | None = Query(default=None, description="Optional end date for ISO range"),
    interval: str | None = Query(default=None, description="`1min`, `5min`, `15min`, `30min`, `60min`, or `DAILY`"),
    ohlc: str | None = Query(default=None, description="`open`, `high`, `low`, or `close`"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Fixed-window statistical analytics across one or more tickers."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:analytics-window:{symbols}:{range}:{range_end or 'none'}:"
            f"{interval or 'def'}:{ohlc or 'def'}:{calculations}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_fixed_window_analytics(
            symbols=symbols,
            range=range,
            calculations=calculations,
            range_end=range_end,
            interval=interval,
            ohlc=ohlc,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/congress/congress-trader", response_model=SuccessResponse)
async def get_congress_trader_recent_reports(
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    date: str | None = Query(default=None, description=DESC_DATE),
    ticker: str | None = Query(default=None, description="Filter by ticker"),
    name: str | None = Query(default=None, description="Filter by congress member name"),
    page: int | None = Query(default=None, description="Page number (1-indexed)"),
    date_from: str | None = Query(default=None, description="Inclusive lower bound date (ISO 8601)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Recent reports by the given congress member."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:congress-trader:{limit or 'def'}:{date or 'none'}:{ticker or 'all'}:"
            f"{name or 'all'}:{page or 1}:{date_from or 'none'}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_congress_trader_recent_reports(
            limit=limit,
            date_str=date,
            ticker=ticker,
            name=name,
            page=page,
            date_from=date_from,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/congress/politicians", response_model=SuccessResponse)
async def get_congress_politicians(
    last_traded_within_months: int | None = Query(
        default=None, description="Filter to politicians active within N months"
    ),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Distinct list of politicians for which trade data exists."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:congress-politicians:{last_traded_within_months or 'all'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_congress_politicians(last_traded_within_months=last_traded_within_months),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/congress/unusual-trades", response_model=SuccessResponse)
async def get_unusual_congress_trades(
    types: str | None = Query(default=None, description="Comma-separated unusual-activity tags"),
    limit: int | None = Query(default=None, ge=1, le=500, description="Results per page (max 500)"),
    page: int | None = Query(default=None, description="Page number (1-indexed)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Congressional trades flagged as unusual, optionally filtered by reason tags."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:unusual-congress-trades:{types or 'all'}:{limit or 'def'}:{page or 1}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_unusual_congress_trades(types=types, limit=limit, page=page),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/congress/unusual-trades/by-tickers", response_model=SuccessResponse)
async def get_unusual_congress_trades_by_tickers(
    tickers: str | None = Query(default=None, description="Comma-separated tickers to filter by"),
    transaction_type: str | None = Query(default=None, description="`buy` or `sell`"),
    date_from: str | None = Query(default=None, description="Inclusive lower bound on transaction_date (ISO 8601)"),
    date_to: str | None = Query(default=None, description="Inclusive upper bound on transaction_date (ISO 8601)"),
    politician: str | None = Query(default=None, description="Case-insensitive substring match on politician name"),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    page: int | None = Query(default=None, description="Page number (1-indexed)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Unusual congressional trades filtered by one or more tickers."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:unusual-congress-by-tickers:{tickers or 'all'}:{transaction_type or 'all'}:"
            f"{date_from or 'none'}:{date_to or 'none'}:{politician or 'all'}:{limit or 'def'}:{page or 1}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_unusual_congress_trades_by_tickers(
            tickers=tickers,
            transaction_type=transaction_type,
            date_from=date_from,
            date_to=date_to,
            politician=politician,
            limit=limit,
            page=page,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/congress/unusual-trades/chart-data", response_model=SuccessResponse)
async def get_unusual_congress_trades_chart_data(
    date_from: str | None = Query(default=None, description="Inclusive lower bound on transaction_date (ISO 8601)"),
    date_to: str | None = Query(default=None, description="Inclusive upper bound on transaction_date (ISO 8601)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Trade points and SPY daily closes over the requested date range."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:unusual-congress-chart:{date_from or 'none'}:{date_to or 'none'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_unusual_congress_trades_chart_data(date_from=date_from, date_to=date_to),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/congress/unusual-trades/stats", response_model=SuccessResponse)
async def get_unusual_congress_trades_stats(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Most recent cached overview statistics for unusual congressional trades."""
    return await execute_uw_cached(
        cache=cache,
        cache_key="uw:unusual-congress-stats",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_unusual_congress_trades_stats(),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/institution/{name}/activity/v2", response_model=SuccessResponse)
async def get_institution_activity_v2(
    name: str,
    ticker_symbol: str | None = Query(default=None, description="Filter by ticker symbol"),
    start_date: str | None = Query(default=None, description="Inclusive start date (ISO 8601)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (ISO 8601)"),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    page: int | None = Query(default=None, description="Page number (1-indexed)"),
    order: str | None = Query(default=None, description="Field to order by"),
    order_direction: str | None = Query(default=None, description="`asc` or `desc`"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Trading activities for a given institution (v2)."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:institution-activity-v2:{name}:{ticker_symbol or 'all'}:{start_date or 'none'}:"
            f"{end_date or 'none'}:{limit or 'def'}:{page or 1}:{order or 'def'}:{order_direction or 'def'}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_institution_activity_v2(
            name=name,
            ticker_symbol=ticker_symbol,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            page=page,
            order=order,
            order_direction=order_direction,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/politician-portfolios/disclosures", response_model=SuccessResponse)
async def get_politician_disclosures(
    politician_id: str | None = Query(default=None, description="Filter by politician ID"),
    latest_only: bool | None = Query(default=None, description="Return only the most recent disclosure per politician"),
    year: int | None = Query(default=None, description="Filter by disclosure year"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Annual disclosure file records for politicians."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:politician-disclosures:{politician_id or 'all'}:{latest_only}:{year or 'all'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_politician_disclosures(
            politician_id=politician_id, latest_only=latest_only, year=year
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/short_screener", response_model=SuccessResponse)
async def get_short_screener(
    tickers: str | None = Query(default=None, description="Comma-separated tickers to filter by"),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset"),
    min_short_interest: float | None = Query(default=None, description="Minimum short interest"),
    max_short_interest: float | None = Query(default=None, description="Maximum short interest"),
    min_days_to_cover: float | None = Query(default=None, description="Minimum days to cover"),
    max_days_to_cover: float | None = Query(default=None, description="Maximum days to cover"),
    min_si_float: float | None = Query(default=None, description="Minimum short-interest float"),
    max_si_float: float | None = Query(default=None, description="Maximum short-interest float"),
    min_si_float_with_synth_long_pct_of_total_shares: float | None = Query(
        default=None, description="Minimum SI float with synthetic long pct of total shares"
    ),
    max_si_float_with_synth_long_pct_of_total_shares: float | None = Query(
        default=None, description="Maximum SI float with synthetic long pct of total shares"
    ),
    min_total_float: float | None = Query(default=None, description="Minimum total float"),
    max_total_float: float | None = Query(default=None, description="Maximum total float"),
    order_by: str | None = Query(default=None, description="Field to order by"),
    order_direction: str | None = Query(default=None, description="`asc` or `desc`"),
    min_market_date: str | None = Query(default=None, description="Minimum market date (ISO 8601)"),
    max_market_date: str | None = Query(default=None, description="Maximum market date (ISO 8601)"),
    min_fee_rate: float | None = Query(default=None, description="Minimum fee rate"),
    max_fee_rate: float | None = Query(default=None, description="Maximum fee rate"),
    min_rebate_rate: float | None = Query(default=None, description="Minimum rebate rate"),
    max_rebate_rate: float | None = Query(default=None, description="Maximum rebate rate"),
    min_short_shares_available: float | None = Query(default=None, description="Minimum short shares available"),
    max_short_shares_available: float | None = Query(default=None, description="Maximum short shares available"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Short interest and float data for percentage calculations based off search params."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:short-screener:{tickers or 'all'}:{limit or 'def'}:{offset or 0}:"
            f"{min_short_interest}:{max_short_interest}:{min_days_to_cover}:{max_days_to_cover}:"
            f"{min_si_float}:{max_si_float}:"
            f"{min_si_float_with_synth_long_pct_of_total_shares}:"
            f"{max_si_float_with_synth_long_pct_of_total_shares}:"
            f"{min_total_float}:{max_total_float}:{order_by or 'def'}:{order_direction or 'def'}:"
            f"{min_market_date or 'none'}:{max_market_date or 'none'}:"
            f"{min_fee_rate}:{max_fee_rate}:{min_rebate_rate}:{max_rebate_rate}:"
            f"{min_short_shares_available}:{max_short_shares_available}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_short_screener(
            tickers=tickers,
            limit=limit,
            offset=offset,
            min_short_interest=min_short_interest,
            max_short_interest=max_short_interest,
            min_days_to_cover=min_days_to_cover,
            max_days_to_cover=max_days_to_cover,
            min_si_float=min_si_float,
            max_si_float=max_si_float,
            min_si_float_with_synth_long_pct_of_total_shares=min_si_float_with_synth_long_pct_of_total_shares,
            max_si_float_with_synth_long_pct_of_total_shares=max_si_float_with_synth_long_pct_of_total_shares,
            min_total_float=min_total_float,
            max_total_float=max_total_float,
            order_by=order_by,
            order_direction=order_direction,
            min_market_date=min_market_date,
            max_market_date=max_market_date,
            min_fee_rate=min_fee_rate,
            max_fee_rate=max_fee_rate,
            min_rebate_rate=min_rebate_rate,
            max_rebate_rate=max_rebate_rate,
            min_short_shares_available=min_short_shares_available,
            max_short_shares_available=max_short_shares_available,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/shorts/{symbol}/interest-float/v2", response_model=SuccessResponse)
async def get_short_interest_float_v2(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """V2 short interest, float size, and days-to-cover for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:short-interest-float-v2:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_short_interest_float_v2(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )
