"""UW cross-asset endpoints — commodities, crypto, digital currencies, FX, economy.

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


@router.get("/commodities/{name}", response_model=SuccessResponse)
async def get_commodity_series(
    name: str,
    interval: str | None = Query(default=None, description="Series cadence. Defaults to monthly."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Long-running price series for a commodity."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:commodity:{name}:{interval or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_commodity_series(name=name, interval=interval),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/crypto/whale-transactions", response_model=SuccessResponse)
async def get_crypto_whale_transactions(
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Recent crypto whale transactions."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:crypto-whale-tx:{limit or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_crypto_whale_transactions(limit=limit),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/crypto/whales/recent", response_model=SuccessResponse)
async def get_recent_crypto_whale_trades(
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Recent large crypto trades (whale trades) across all pairs."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:crypto-whales-recent:{limit or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_recent_crypto_whale_trades(limit=limit),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/crypto/{pair}/ohlc/{candle_size}", response_model=SuccessResponse)
async def get_crypto_ohlc_candles(
    pair: str,
    candle_size: str,
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """OHLC candle data for a crypto pair at a given candle size."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:crypto-ohlc:{pair}:{candle_size}:{limit or 'def'}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_crypto_ohlc_candles(
            pair=pair, candle_size=candle_size, limit=limit, date_str=date
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/crypto/{pair}/state", response_model=SuccessResponse)
async def get_crypto_pair_state(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Current state for a crypto pair including 24h OHLCV data."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:crypto-state:{pair}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_crypto_pair_state(pair=pair),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/digital-currencies/history", response_model=SuccessResponse)
async def get_digital_currency_history(
    symbol: str = Query(description="Digital asset symbol."),
    market: str = Query(description="Fiat market code."),
    interval: str | None = Query(default=None, description="`daily` (default), `weekly`, or `monthly`."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Daily, weekly, or monthly OHLC bars for a digital asset."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:digital-history:{symbol}:{market}:{interval or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_digital_currency_history(symbol=symbol, market=market, interval=interval),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/digital-currencies/intraday", response_model=SuccessResponse)
async def get_digital_currency_intraday(
    symbol: str = Query(description="Digital asset symbol."),
    market: str = Query(description="Fiat market code."),
    interval: str | None = Query(default=None, description="`1min`, `5min`, `15min`, `30min`, or `60min`."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Intraday OHLC bars for a digital asset against a fiat market."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:digital-intraday:{symbol}:{market}:{interval or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_digital_currency_intraday(
            symbol=symbol, market=market, interval=interval
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/economy/{indicator}", response_model=SuccessResponse)
async def get_economic_indicator(
    indicator: str,
    interval: str | None = Query(default=None, description="Series cadence."),
    maturity: str | None = Query(default=None, description="Only for treasury-yield."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Long-running US economic indicator series."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:economy:{indicator}:{interval or 'def'}:{maturity or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_economic_indicator(
            indicator=indicator, interval=interval, maturity=maturity
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/forex/history", response_model=SuccessResponse)
async def get_forex_history(
    from_currency: str = Query(alias="from", description="From currency code (ISO 4217)."),
    to_currency: str = Query(alias="to", description="To currency code (ISO 4217)."),
    interval: str | None = Query(default=None, description="`daily` (default), `weekly`, or `monthly`."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Daily, weekly, or monthly OHLC bars for a currency pair."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:fx-history:{from_currency}:{to_currency}:{interval or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_forex_history(
            from_currency=from_currency, to_currency=to_currency, interval=interval
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/forex/intraday", response_model=SuccessResponse)
async def get_forex_intraday(
    from_currency: str = Query(alias="from", description="From currency code (ISO 4217)."),
    to_currency: str = Query(alias="to", description="To currency code (ISO 4217)."),
    interval: str | None = Query(default=None, description="`1min`, `5min`, `15min`, `30min`, or `60min`."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Intraday OHLC bars for a currency pair."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:fx-intraday:{from_currency}:{to_currency}:{interval or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_forex_intraday(
            from_currency=from_currency, to_currency=to_currency, interval=interval
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/forex/rate", response_model=SuccessResponse)
async def get_forex_rate(
    from_currency: str = Query(alias="from", description="From currency code (ISO 4217)."),
    to_currency: str = Query(alias="to", description="To currency code (ISO 4217)."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Realtime spot exchange rate between two currencies."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:fx-rate:{from_currency}:{to_currency}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_forex_rate(from_currency=from_currency, to_currency=to_currency),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )
