"""UW options-flow / greeks / GEX / options-pulse endpoints.

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


@router.get("/group-flow/{flow_group}/greek-flow/{expiry}", response_model=SuccessResponse)
async def get_group_flow_greek_flow_by_expiry(
    flow_group: str,
    expiry: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Group flow greek flow (delta & vega) per minute for a given expiry."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:group-greek-flow-expiry:{flow_group}:{expiry}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_group_flow_greek_flow_by_expiry(
            flow_group=flow_group, expiry=expiry, date_str=date
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/lit-flow/recent", response_model=SuccessResponse)
async def get_lit_flow_recent(
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    date: str | None = Query(default=None, description=DESC_DATE),
    min_premium: int | None = Query(default=None, description="Minimum premium"),
    max_premium: int | None = Query(default=None, description="Maximum premium"),
    min_size: int | None = Query(default=None, description="Minimum size"),
    max_size: int | None = Query(default=None, description="Maximum size"),
    min_volume: int | None = Query(default=None, description="Minimum volume"),
    max_volume: int | None = Query(default=None, description="Maximum volume"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Latest lit exchange trades market-wide."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:lit-flow-recent:{limit}:{date or 'latest'}:{min_premium}:{max_premium}:"
            f"{min_size}:{max_size}:{min_volume}:{max_volume}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_lit_flow_recent(
            limit=limit,
            date_str=date,
            min_premium=min_premium,
            max_premium=max_premium,
            min_size=min_size,
            max_size=max_size,
            min_volume=min_volume,
            max_volume=max_volume,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/lit-flow/{symbol}", response_model=SuccessResponse)
async def get_ticker_lit_flow(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    newer_than: str | None = Query(default=None, description="Only trades newer than this timestamp"),
    older_than: str | None = Query(default=None, description="Only trades older than this timestamp"),
    min_premium: int | None = Query(default=None, description="Minimum premium"),
    max_premium: int | None = Query(default=None, description="Maximum premium"),
    min_size: int | None = Query(default=None, description="Minimum size"),
    max_size: int | None = Query(default=None, description="Maximum size"),
    min_volume: int | None = Query(default=None, description="Minimum volume"),
    max_volume: int | None = Query(default=None, description="Maximum volume"),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Lit exchange trades for a ticker on a given day."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:lit-flow:{symbol}:{date or 'latest'}:{newer_than}:{older_than}:{min_premium}:"
            f"{max_premium}:{min_size}:{max_size}:{min_volume}:{max_volume}:{limit}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_lit_flow(
            symbol=symbol,
            date_str=date,
            newer_than=newer_than,
            older_than=older_than,
            min_premium=min_premium,
            max_premium=max_premium,
            min_size=min_size,
            max_size=max_size,
            min_volume=min_volume,
            max_volume=max_volume,
            limit=limit,
        ),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/market/movers", response_model=SuccessResponse)
async def get_market_movers(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Top gainers, losers, and most actively traded US tickers for the latest session."""
    return await execute_uw_cached(
        cache=cache,
        cache_key="uw:market-movers",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_movers(),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/market/oi-change", response_model=SuccessResponse)
async def get_market_oi_change(
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    order: str | None = Query(default=None, description="Sort order (default descending)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Contracts with the highest OI (open interest) change market-wide."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:market-oi-change:{date or 'latest'}:{limit}:{order}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_oi_change(date_str=date, limit=limit, order=order),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/option-trades/exchange-breakdown/{date}", response_model=SuccessResponse)
async def get_option_trades_exchange_breakdown(
    date: str,
    tickers: list[str] | None = Query(default=None, alias="ticker[]", description="One or more underlying symbols"),
    by_trade_code: bool | None = Query(default=None, description="Additionally group each row by trade condition code"),
    min_premium: str | None = Query(default=None, description="Minimum print premium (price x size x 100)"),
    limit: int = Query(
        default=100, ge=1, le=500, description="Whole-universe mode: tickers per page (default 100, max 500)"
    ),
    page: int = Query(default=1, ge=1, description="Whole-universe mode: 1-based page of tickers (default 1)"),
    order: str | None = Query(default=None, description="Whole-universe mode: rank tickers by `volume` or `premium`"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Option tape aggregated by options exchange for a trading date."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:option-exchange-breakdown:{date}:{tickers}:{by_trade_code}:{min_premium}:{limit}:{page}:{order}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_option_trades_exchange_breakdown(
            date_str=date,
            tickers=tickers,
            by_trade_code=by_trade_code,
            min_premium=min_premium,
            limit=limit,
            page=page,
            order=order,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/option-trades/flow-alerts/{id}", response_model=SuccessResponse)
async def get_flow_alert_by_id(
    id: str,
    older_than: str | None = Query(default=None, description="Only trades older than this timestamp"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Trades that made up a specific flow alert."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:flow-alert-by-id:{id}:{older_than}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_flow_alert_by_id(id=id, older_than=older_than),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/option-trades/optionable-tickers", response_model=SuccessResponse)
async def get_optionable_tickers(
    ticker: str | None = Query(
        default=None, description="Optional: check a single symbol instead of the whole universe"
    ),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Current universe of underlying symbols that have listed options."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:optionable-tickers:{ticker or 'all'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_optionable_tickers(ticker=ticker),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/options-pulse/sectors", response_model=SuccessResponse)
async def get_options_pulse_sectors(
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Latest Options Pulse sentiment per sector and industry on a date."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:options-pulse-sectors:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_options_pulse_sectors(date_str=date),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/options-pulse/top", response_model=SuccessResponse)
async def get_options_pulse_top(
    direction: str | None = Query(default=None, description="`bullish` (default) or `bearish`"),
    date: str | None = Query(default=None, description=DESC_DATE),
    ticker: str | None = Query(default=None, description="Restrict to tickers with this prefix"),
    min_score: float | None = Query(default=None, description="Minimum sentiment score"),
    max_score: float | None = Query(default=None, description="Maximum sentiment score"),
    min_txn: int | None = Query(default=None, description="Minimum total opening-buy transactions (put + call)"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows (default 50, max 500)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Cross-symbol Options Pulse scanner ranked by sentiment."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:options-pulse-top:{direction}:{date or 'latest'}:{ticker}:{min_score}:{max_score}:{min_txn}:{limit}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_options_pulse_top(
            direction=direction,
            date_str=date,
            ticker=ticker,
            min_score=min_score,
            max_score=max_score,
            min_txn=min_txn,
            limit=limit,
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/options-pulse/total", response_model=SuccessResponse)
async def get_options_pulse_total(
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Market-wide Options Pulse gauge snapshot + intraday series for a date."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:options-pulse-total:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_options_pulse_total(date_str=date),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/stock/{symbol}/flow-alerts", response_model=SuccessResponse)
async def get_ticker_flow_alerts(
    symbol: str,
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    is_ask_side: bool | None = Query(default=None, description="Filter to ask-side trades"),
    is_bid_side: bool | None = Query(default=None, description="Filter to bid-side trades"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Flow alerts for a ticker (deprecated upstream endpoint)."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:ticker-flow-alerts:{symbol}:{limit}:{is_ask_side}:{is_bid_side}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_flow_alerts(
            symbol=symbol, limit=limit, is_ask_side=is_ask_side, is_bid_side=is_bid_side
        ),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/flow-per-expiry", response_model=SuccessResponse)
async def get_ticker_flow_per_expiry(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Option flow per expiry for the last trading day."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:ticker-flow-per-expiry:{symbol}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_flow_per_expiry(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/flow-per-strike", response_model=SuccessResponse)
async def get_ticker_flow_per_strike(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Option flow per strike for a given trading day."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:ticker-flow-per-strike:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_flow_per_strike(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/gex-levels", response_model=SuccessResponse)
async def get_gex_levels(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Key gamma-exposure (GEX) price levels for a ticker on a market date."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:gex-levels:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_gex_levels(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/options-pulse", response_model=SuccessResponse)
async def get_ticker_options_pulse(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Options Pulse sentiment for a single ticker (snapshot + intraday series)."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:ticker-options-pulse:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_options_pulse(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/spot-exposures/expiry-strike", response_model=SuccessResponse)
async def get_ticker_spot_exposures_expiry_strike(
    symbol: str,
    expirations: list[str] = Query(alias="expirations[]", description="One or more expirations (YYYY-MM-DD)"),
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    page: int | None = Query(default=None, description="1-based page"),
    min_strike: float | None = Query(default=None, description="Minimum strike"),
    max_strike: float | None = Query(default=None, description="Maximum strike"),
    min_dte: int | None = Query(default=None, description="Minimum days to expiry"),
    max_dte: int | None = Query(default=None, description="Maximum days to expiry"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Most recent spot GEX exposures across strikes for a ticker & expiration."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=(
            f"uw:spot-exposures-expiry-strike:{symbol}:{expirations}:{date or 'latest'}:{limit}:"
            f"{page}:{min_strike}:{max_strike}:{min_dte}:{max_dte}"
        ),
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_spot_exposures_expiry_strike(
            symbol=symbol,
            expirations=expirations,
            date_str=date,
            limit=limit,
            page=page,
            min_strike=min_strike,
            max_strike=max_strike,
            min_dte=min_dte,
            max_dte=max_dte,
        ),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )
