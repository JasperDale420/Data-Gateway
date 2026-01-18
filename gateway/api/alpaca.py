"""Alpaca REST API endpoints."""

import asyncio
from datetime import UTC, date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.registry import ProviderRegistry

router = APIRouter(prefix="/api/v1/alpaca", tags=["alpaca"])


@router.get("/stocks/{symbol}/bars")
async def get_stock_bars(
    symbol: str,
    timeframe: str = Query(default="1Day", description="Bar timeframe"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    limit: int = Query(default=1000, le=10000, description="Max bars to return"),
    feed: str = Query(default="sip", description="Data feed: sip or iex"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for a stock."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    # Default time range: last 24 hours
    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=24)

    try:
        await require_provider_rate_limit("alpaca")
        bars = await provider.get_bars(
            symbols=[symbol.upper()],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "timeframe": timeframe,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            "meta": {
                "count": len(bars),
                "provider": "alpaca",
                "feed": feed,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/{symbol}/quotes")
async def get_stock_quotes(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a stock."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        quotes = await provider.get_quotes(symbols=[symbol.upper()])

        if not quotes:
            raise HTTPException(status_code=404, detail=f"No quote found for {symbol}")

        return {
            "success": True,
            "data": quotes[0].model_dump(mode="json"),
            "meta": {"provider": "alpaca"},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/{symbol}/trades")
async def get_stock_trades(
    symbol: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for a stock."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(hours=1)

    try:
        await require_provider_rate_limit("alpaca")
        trades = await provider.get_trades(
            symbols=[symbol.upper()],
            start=start,
            end=end,
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "trades": [t.model_dump(mode="json") for t in trades[:limit]],
            },
            "meta": {
                "count": len(trades),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/{symbol}/snapshot")
async def get_stock_snapshot(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a stock (latest bar + quote)."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        quotes = await provider.get_quotes(symbols=[symbol.upper()])
        end = datetime.now(UTC)
        start = end - timedelta(minutes=5)
        bars = await provider.get_bars(
            symbols=[symbol.upper()],
            timeframe="1Min",
            start=start,
            end=end,
            limit=1,
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "quote": quotes[0].model_dump(mode="json") if quotes else None,
                "latest_bar": bars[0].model_dump(mode="json") if bars else None,
            },
            "meta": {"provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/bars/latest")
async def get_latest_bars(
    symbols: str = Query(..., description="Comma-separated symbols"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bar for each symbol."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        bars = await provider.get_latest_bars(symbols_list)
        return {
            "success": True,
            "data": [b.model_dump(mode="json") for b in bars],
            "meta": {"count": len(bars), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/trades/latest")
async def get_latest_trades(
    symbols: str = Query(..., description="Comma-separated symbols"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trade for each symbol."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        trades = await provider.get_latest_trades(symbols_list)
        return {
            "success": True,
            "data": [t.model_dump(mode="json") for t in trades],
            "meta": {"count": len(trades), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/quotes")
async def get_historical_quotes(
    symbols: str = Query(..., description="Comma-separated symbols"),
    start: datetime = Query(..., description="Start time (ISO 8601)"),
    end: datetime = Query(..., description="End time (ISO 8601)"),
    limit: int = Query(default=10000, le=10000, description="Max quotes"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical quotes for symbols."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        quotes = await provider.get_historical_quotes(symbols_list, start, end, limit)
        return {
            "success": True,
            "data": [q.model_dump(mode="json") for q in quotes],
            "meta": {"count": len(quotes), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/snapshots")
async def get_snapshots(
    symbols: str = Query(..., description="Comma-separated symbols"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshots for symbols."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        snapshots = await provider.get_snapshots(symbols_list)
        return {
            "success": True,
            "data": snapshots,
            "meta": {"count": len(snapshots), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/stocks/auctions")
async def get_auctions(
    symbols: str = Query(..., description="Comma-separated symbols"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    limit: int = Query(default=1000, le=10000, description="Max auctions per symbol"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get auction data for symbols."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = [s.strip().upper() for s in symbols.split(",")]
        auctions = await provider.get_auctions(symbols_list, start, end, limit)
        return {
            "success": True,
            "data": auctions,
            "meta": {"count": len(auctions), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Options Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/options/chain/{underlying}")
async def get_option_chain(
    underlying: str,
    expiration_date: str | None = Query(default=None, description="Exact expiration (YYYY-MM-DD)"),
    expiration_date_gte: str | None = Query(default=None, description="Expiration on or after"),
    expiration_date_lte: str | None = Query(default=None, description="Expiration on or before"),
    strike_price_gte: float | None = Query(default=None, description="Strike at or above"),
    strike_price_lte: float | None = Query(default=None, description="Strike at or below"),
    option_type: str | None = Query(default=None, description="call or put"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get full option chain with greeks for an underlying."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        contracts = await provider.get_option_chain(
            underlying=underlying.upper(),
            expiration_date=expiration_date,
            expiration_gte=expiration_date_gte,
            expiration_lte=expiration_date_lte,
            strike_gte=strike_price_gte,
            strike_lte=strike_price_lte,
            option_type=option_type,
        )

        return {
            "success": True,
            "data": {
                "underlying": underlying.upper(),
                "contracts": [c.model_dump(mode="json") for c in contracts],
            },
            "meta": {
                "count": len(contracts),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/options/chain/{underlying}/snapshot")
async def get_option_chain_snapshot(
    underlying: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current option chain snapshot for an underlying."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        # Get near-term expirations only for snapshot
        from datetime import date

        today = date.today()
        contracts = await provider.get_option_chain(
            underlying=underlying.upper(),
            expiration_gte=today.isoformat(),
        )

        return {
            "success": True,
            "data": {
                "underlying": underlying.upper(),
                "contracts": [c.model_dump(mode="json") for c in contracts[:100]],
            },
            "meta": {
                "count": min(len(contracts), 100),
                "total_available": len(contracts),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/options/{contract}/bars")
async def get_option_bars(
    contract: str,
    timeframe: str = Query(default="1Day", description="Bar timeframe"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    limit: int = Query(default=1000, le=10000, description="Max bars to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for an option contract."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    if not end:
        end = datetime.now(UTC)
    if not start:
        start = end - timedelta(days=30)

    try:
        await require_provider_rate_limit("alpaca")
        bars = await provider.get_option_bars(
            contracts=[contract.upper()],
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "contract": contract.upper(),
                "timeframe": timeframe,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            "meta": {
                "count": len(bars),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/options/{contract}/quotes")
async def get_option_quotes(
    contract: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for an option contract."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        quotes = await provider.get_option_quotes(contracts=[contract.upper()])

        if not quotes:
            raise HTTPException(status_code=404, detail=f"No quote found for {contract}")

        return {
            "success": True,
            "data": quotes[0].model_dump(mode="json"),
            "meta": {"provider": "alpaca"},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/options/trades")
async def get_options_trades(
    contracts: str = Query(..., description="Comma-separated option contracts"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    limit: int = Query(default=1000, le=10000, description="Max trades"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for option contracts."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        contracts_list = [c.strip().upper() for c in contracts.split(",")]
        trades = await provider.get_option_trades(contracts_list, start, end, limit)
        return {
            "success": True,
            "data": [t.model_dump(mode="json") for t in trades],
            "meta": {"count": len(trades), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/options/trades/latest")
async def get_options_latest_trades(
    contracts: str = Query(..., description="Comma-separated option contracts"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trades for option contracts."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        contracts_list = [c.strip().upper() for c in contracts.split(",")]
        trades = await provider.get_option_latest_trades(contracts_list)
        return {
            "success": True,
            "data": [t.model_dump(mode="json") for t in trades],
            "meta": {"count": len(trades), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/options/snapshots/{underlying}")
async def get_options_snapshots(
    underlying: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get snapshots for all options on an underlying."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        snapshots = await provider.get_option_snapshots(underlying)
        return {
            "success": True,
            "data": snapshots,
            "meta": {
                "count": len(snapshots),
                "underlying": underlying.upper(),
                "provider": "alpaca",
            },
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Crypto Endpoints
# ─────────────────────────────────────────────────────────────────


@router.get("/crypto/{pair}/bars")
async def get_crypto_bars(
    pair: str,
    timeframe: str = Query(default="1Hour", description="Bar timeframe"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    limit: int = Query(default=1000, le=10000, description="Max bars to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical bars for a crypto pair (e.g., BTC/USD)."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        bars = await provider.get_crypto_bars(
            pair=pair.upper(),
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "pair": pair.upper(),
                "timeframe": timeframe,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            "meta": {"count": len(bars), "provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/{pair}/trades")
async def get_crypto_trades(
    pair: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical trades for a crypto pair."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        trades = await provider.get_crypto_trades(
            pair=pair.upper(),
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": {
                "pair": pair.upper(),
                "trades": [trade.model_dump(mode="json") for trade in trades],
            },
            "meta": {"count": len(trades), "provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/{pair}/quotes")
async def get_crypto_quotes(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest quote for a crypto pair."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        quote = await provider.get_crypto_quotes(pair=pair.upper())

        if not quote:
            raise HTTPException(status_code=404, detail=f"No quote found for {pair}")

        return {
            "success": True,
            "data": quote.model_dump(mode="json"),
            "meta": {"provider": "alpaca"},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/{pair}/snapshot")
async def get_crypto_snapshot(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get current snapshot for a crypto pair."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        snapshot = await provider.get_crypto_snapshot(pair=pair.upper())

        return {
            "success": True,
            "data": snapshot,
            "meta": {"provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/bars/latest")
async def get_crypto_latest_bars(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest bars for crypto pairs."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = [p.strip().upper() for p in pairs.split(",")]
        data = await provider.get_crypto_latest_bars(pairs_list)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/trades/latest")
async def get_crypto_latest_trades(
    pairs: str = Query(..., description="Comma-separated crypto pairs"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest trades for crypto pairs."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = [p.strip().upper() for p in pairs.split(",")]
        data = await provider.get_crypto_latest_trades(pairs_list)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Forex Endpoints (REST only - no WebSocket per PRD)
# ─────────────────────────────────────────────────────────────────


@router.get("/forex/rates")
async def get_forex_rates(
    pairs: str = Query(..., description="Comma-separated pairs: EUR/USD,GBP/USD"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest forex rates."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = [p.strip().upper() for p in pairs.split(",")]
        data = await provider.get_forex_rates(pairs=pairs_list)

        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data.get("rates", {})), "provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/rates/historical")
async def get_forex_rates_historical(
    pairs: str = Query(..., description="Comma-separated pairs: EUR/USD,GBP/USD"),
    timeframe: str = Query(default="1Day", description="1Min, 1Hour, 1Day"),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical forex rates."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = [p.strip().upper() for p in pairs.split(",")]
        data = await provider.get_forex_rates_historical(
            pairs=pairs_list,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )

        # Serialize bars
        serialized = {
            pair: [bar.model_dump(mode="json") for bar in bars] for pair, bars in data.items()
        }

        return {
            "success": True,
            "data": {"pairs": serialized, "timeframe": timeframe},
            "meta": {
                "total_bars": sum(len(b) for b in data.values()),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# News Endpoints (Phase 1)
# ─────────────────────────────────────────────────────────────────


@router.get("/news")
async def get_news(
    symbols: str | None = Query(default=None, description="Comma-separated symbols: AAPL,MSFT"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    limit: int = Query(default=10, le=50, description="Max articles to return"),
    include_content: bool = Query(default=False, description="Include full article content"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get news articles with optional symbol filtering."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = None
        if symbols:
            symbols_list = [s.strip().upper() for s in symbols.split(",")]

        articles = await provider.get_news(
            symbols=symbols_list,
            start=start,
            end=end,
            limit=limit,
            include_content=include_content,
        )

        return {
            "success": True,
            "data": {
                "articles": [a.model_dump(mode="json") for a in articles],
            },
            "meta": {
                "count": len(articles),
                "provider": "alpaca",
                "symbols_filter": symbols_list,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Screener Endpoints (Phase 1)
# ─────────────────────────────────────────────────────────────────


@router.get("/screener/most-actives")
async def get_most_actives(
    by: str = Query(default="volume", description="Sort by: volume or trades"),
    top: int = Query(default=10, le=100, description="Number of results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get most active stocks by volume or trade count."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    if by not in ("volume", "trades"):
        raise HTTPException(status_code=400, detail="'by' must be 'volume' or 'trades'")

    try:
        await require_provider_rate_limit("alpaca")
        actives = await provider.get_most_actives(by=by, top=top)

        return {
            "success": True,
            "data": {
                "most_actives": [a.model_dump(mode="json") for a in actives],
            },
            "meta": {
                "count": len(actives),
                "by": by,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/screener/movers")
async def get_movers(
    market_type: str = Query(default="stocks", description="Market type: stocks"),
    top: int = Query(default=10, le=50, description="Number of results per side"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get top movers - gainers and losers."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        movers = await provider.get_movers(market_type=market_type, top=top)

        return {
            "success": True,
            "data": {
                "gainers": [g.model_dump(mode="json") for g in movers.get("gainers", [])],
                "losers": [loser.model_dump(mode="json") for loser in movers.get("losers", [])],
            },
            "meta": {
                "gainers_count": len(movers.get("gainers", [])),
                "losers_count": len(movers.get("losers", [])),
                "market_type": market_type,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Phase 2: Corporate Actions
# ─────────────────────────────────────────────────────────────────


@router.get("/corporate-actions/{symbol}")
async def get_corporate_actions(
    symbol: str,
    types: str | None = Query(
        default=None, description="Action types: dividend,split,merger,spinoff"
    ),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get corporate actions for a symbol."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        types_list = None
        if types:
            types_list = [t.strip() for t in types.split(",")]

        actions = await provider.get_corporate_actions(
            symbols=[symbol.upper()],
            types=types_list,
            start=start,
            end=end,
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "actions": [a.model_dump(mode="json") for a in actions],
            },
            "meta": {
                "count": len(actions),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Phase 3: Metadata & Orderbook
# ─────────────────────────────────────────────────────────────────


@router.get("/meta/conditions")
async def get_condition_codes(
    asset_class: str = Query(default="stocks", description="Asset class: stocks, options, crypto"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get condition codes for trades/quotes."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await provider.get_condition_codes(asset_class=asset_class)

        return {
            "success": True,
            "data": data,
            "meta": {
                "asset_class": asset_class,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/meta/exchanges")
async def get_exchange_codes(
    asset_class: str = Query(default="stocks", description="Asset class: stocks, options, crypto"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get exchange codes."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await provider.get_exchange_codes(asset_class=asset_class)

        return {
            "success": True,
            "data": data,
            "meta": {
                "asset_class": asset_class,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/{pair}/orderbook")
async def get_crypto_orderbook(
    pair: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get crypto orderbook for a trading pair."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await provider.get_crypto_orderbook(pair=pair.upper())

        # Handle both dict and NormalizedOrderbook
        if hasattr(data, "model_dump"):
            data_dict = data.model_dump(mode="json")
        else:
            data_dict = data

        return {
            "success": True,
            "data": data_dict,
            "meta": {
                "pair": pair.upper(),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Trading API Endpoints (SDK-based, sync → async via to_thread)
# ─────────────────────────────────────────────────────────────────


@router.get("/account")
async def get_account(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account information."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_account)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.post("/orders")
async def create_order(
    symbol: str,
    side: str,
    qty: float | None = None,
    notional: float | None = None,
    order_type: str = "market",
    time_in_force: str = "day",
    limit_price: float | None = None,
    stop_price: float | None = None,
    client_order_id: str | None = None,
    extended_hours: bool = False,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new order."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(
            provider.create_order,
            symbol=symbol,
            qty=qty,
            notional=notional,
            side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            limit_price=limit_price,
            stop_price=stop_price,
            client_order_id=client_order_id,
            extended_hours=extended_hours,
        )
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/orders")
async def get_orders(
    status: str = Query(default="open", description="Order status: open, closed, all"),
    limit: int = Query(default=50, le=500, description="Max orders to return"),
    direction: str = Query(default="desc", description="Sort direction: asc, desc"),
    symbols: str | None = Query(default=None, description="Comma-separated symbols"),
    nested: bool = Query(default=True, description="Include nested multi-leg orders"),
    side: str | None = Query(default=None, description="Filter by side: buy, sell"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all orders with optional filters."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = symbols.split(",") if symbols else None
        data = await asyncio.to_thread(
            provider.get_orders,
            status=status,
            limit=limit,
            direction=direction,
            symbols=symbols_list,
            nested=nested,
            side=side,
        )
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/orders/{order_id}")
async def get_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific order by ID."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_order, order_id)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/orders:by_client_order_id")
async def get_order_by_client_id(
    client_order_id: str = Query(..., description="Client order ID"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get order by client order ID."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_order_by_client_id, client_order_id)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/orders/{order_id}")
async def cancel_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Cancel an order by ID."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        success = await asyncio.to_thread(provider.cancel_order, order_id)
        return {
            "success": success,
            "data": {"order_id": order_id, "cancelled": success},
            "meta": {"provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/orders")
async def cancel_all_orders(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Cancel all open orders."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.cancel_all_orders)
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/positions")
async def get_positions(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all open positions."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_positions)
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/positions/{symbol}")
async def get_position(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get position for a specific symbol."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_position, symbol)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/positions/{symbol}")
async def close_position(
    symbol: str,
    qty: float | None = Query(default=None, description="Quantity to close"),
    percentage: float | None = Query(default=None, description="Percentage to close (0-100)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close a position (fully or partially)."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.close_position, symbol, qty, percentage)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/positions")
async def close_all_positions(
    cancel_orders: bool = Query(default=True, description="Cancel open orders first"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close all open positions."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.close_all_positions, cancel_orders)
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/portfolio/history")
async def get_portfolio_history(
    period: str | None = Query(default="1M", description="Period: 1D, 1W, 1M, 3M, 6M, 1A, all"),
    timeframe: str | None = Query(default="1D", description="Timeframe: 1Min, 5Min, 15Min, 1H, 1D"),
    extended_hours: bool = Query(default=False, description="Include extended hours"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account portfolio history."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(
            provider.get_portfolio_history,
            period=period,
            timeframe=timeframe,
            extended_hours=extended_hours,
        )
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/assets")
async def get_assets(
    status: str | None = Query(default="active", description="Asset status: active, inactive"),
    asset_class: str | None = Query(
        default="us_equity", description="Asset class: us_equity, crypto"
    ),
    exchange: str | None = Query(default=None, description="Exchange filter"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get available assets."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(
            provider.get_assets, status=status, asset_class=asset_class, exchange=exchange
        )
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/assets/{symbol}")
async def get_asset(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific asset by symbol."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_asset, symbol)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/clock")
async def get_clock(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get market clock."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_clock)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/calendar")
async def get_calendar(
    start: date | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: date | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get trading calendar."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_calendar, start, end)
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Account Configuration & Activities
# ─────────────────────────────────────────────────────────────────


@router.get("/account/configurations")
async def get_account_configurations(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account configuration settings."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_account_configurations)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.patch("/account/configurations")
async def set_account_configurations(
    dtbp_check: str | None = None,
    trade_confirm_email: str | None = None,
    suspend_trade: bool | None = None,
    no_shorting: bool | None = None,
    fractional_trading: bool | None = None,
    max_margin_multiplier: str | None = None,
    pdt_check: str | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Update account configuration settings."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(
            provider.set_account_configurations,
            dtbp_check=dtbp_check,
            trade_confirm_email=trade_confirm_email,
            suspend_trade=suspend_trade,
            no_shorting=no_shorting,
            fractional_trading=fractional_trading,
            max_margin_multiplier=max_margin_multiplier,
            pdt_check=pdt_check,
        )
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/account/activities")
async def get_account_activities(
    activity_types: str | None = Query(default=None, description="Comma-separated activity types"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account activities."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        types_list = activity_types.split(",") if activity_types else None
        data = await asyncio.to_thread(provider.get_account_activities, types_list)
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Watchlists
# ─────────────────────────────────────────────────────────────────


@router.get("/watchlists")
async def get_watchlists(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all watchlists."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_watchlists)
        return {"success": True, "data": data, "meta": {"count": len(data), "provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.post("/watchlists")
async def create_watchlist(
    name: str,
    symbols: str | None = Query(default=None, description="Comma-separated symbols"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = symbols.split(",") if symbols else None
        data = await asyncio.to_thread(provider.create_watchlist, name, symbols_list)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/watchlists/{watchlist_id}")
async def get_watchlist(
    watchlist_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific watchlist by ID."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_watchlist, watchlist_id)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.put("/watchlists/{watchlist_id}")
async def update_watchlist(
    watchlist_id: str,
    name: str | None = None,
    symbols: str | None = Query(default=None, description="Comma-separated symbols"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Update a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = symbols.split(",") if symbols else None
        data = await asyncio.to_thread(provider.update_watchlist, watchlist_id, name, symbols_list)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/watchlists/{watchlist_id}")
async def delete_watchlist(
    watchlist_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Delete a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        success = await asyncio.to_thread(provider.delete_watchlist, watchlist_id)
        return {
            "success": success,
            "data": {"watchlist_id": watchlist_id, "deleted": success},
            "meta": {"provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.post("/watchlists/{watchlist_id}/assets")
async def add_asset_to_watchlist(
    watchlist_id: str,
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Add an asset to a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.add_asset_to_watchlist, watchlist_id, symbol)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/watchlists/{watchlist_id}/assets/{symbol}")
async def remove_asset_from_watchlist(
    watchlist_id: str,
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Remove an asset from a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.remove_asset_from_watchlist, watchlist_id, symbol)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Order Modifications
# ─────────────────────────────────────────────────────────────────


@router.patch("/orders/{order_id}")
async def replace_order(
    order_id: str,
    qty: float | None = None,
    limit_price: float | None = None,
    stop_price: float | None = None,
    time_in_force: str | None = None,
    client_order_id: str | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Replace/modify an existing order."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(
            provider.replace_order,
            order_id,
            qty=qty,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
        )
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
