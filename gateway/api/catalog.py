"""API Catalog endpoints for stream and endpoint discovery."""

from typing import Any

import structlog
from fastapi import APIRouter, Depends

from gateway.api.deps import require_api_key

logger = structlog.get_logger()

router = APIRouter(
    prefix="/catalog",
    tags=["catalog"],
    dependencies=[Depends(require_api_key)],
)


# REST API Provider Catalog
PROVIDER_CATALOG = {
    "alpaca": {
        "name": "Alpaca Markets",
        "description": "Stock, options, and crypto market data + trading execution",
        "base_path": "/alpaca",
        "documentation": "https://docs.alpaca.markets/",
        "categories": {
            "stocks": {
                "description": "Stock market data",
                "endpoints": [
                    "/stocks/{symbol}/bars",
                    "/stocks/{symbol}/quotes",
                    "/stocks/{symbol}/trades",
                    "/stocks/{symbol}/snapshot",
                    "/stocks/bars",
                    "/stocks/quotes",
                    "/stocks/trades",
                    "/stocks/snapshots",
                ],
            },
            "options": {
                "description": "Options market data",
                "endpoints": [
                    "/options/{symbol}/bars",
                    "/options/{symbol}/quotes",
                    "/options/{symbol}/trades",
                    "/options/{symbol}/snapshot",
                    "/options/bars",
                    "/options/snapshots",
                    "/options/chain/{underlying}",
                ],
            },
            "crypto": {
                "description": "Cryptocurrency market data",
                "endpoints": [
                    "/crypto/{symbol}/bars",
                    "/crypto/{symbol}/quotes",
                    "/crypto/{symbol}/trades",
                    "/crypto/{symbol}/snapshot",
                    "/crypto/bars",
                    "/crypto/snapshots",
                ],
            },
            "trading": {
                "description": "Order management and account",
                "endpoints": [
                    "/account",
                    "/orders",
                    "/orders/{order_id}",
                    "/positions",
                    "/positions/{symbol}",
                    "/watchlists",
                    "/calendar",
                    "/clock",
                    "/assets",
                    "/assets/{symbol}",
                ],
            },
        },
    },
    "unusual_whales": {
        "name": "Unusual Whales",
        "description": "Options flow, dark pool, institutional, and alternative data",
        "base_path": "/uw",
        "documentation": "https://docs.unusualwhales.com/",
        "categories": {
            "options_flow": {
                "description": "Real-time options flow and analytics",
                "endpoints": [
                    "/flow",
                    "/flow/alerts",
                    "/flow/historical",
                    "/flow/ticker/{ticker}",
                ],
            },
            "dark_pool": {
                "description": "Dark pool and off-exchange data",
                "endpoints": [
                    "/darkpool",
                    "/darkpool/ticker/{ticker}",
                ],
            },
            "institutional": {
                "description": "Institutional holdings and 13F data",
                "endpoints": [
                    "/institution",
                    "/institution/{name}",
                    "/institution/{name}/holdings",
                ],
            },
            "insider": {
                "description": "Insider trading activity",
                "endpoints": [
                    "/insider/transactions",
                    "/insider/ticker/{ticker}",
                ],
            },
            "analytics": {
                "description": "Stock and options analytics",
                "endpoints": [
                    "/stock/{ticker}/overview",
                    "/stock/{ticker}/options/volume",
                    "/stock/{ticker}/options/greeks",
                    "/stock/{ticker}/volume/analysis",
                ],
            },
            "etf": {
                "description": "ETF holdings and flow",
                "endpoints": [
                    "/etf",
                    "/etf/{ticker}",
                    "/etf/{ticker}/holdings",
                ],
            },
            "screeners": {
                "description": "Stock and options screeners",
                "endpoints": [
                    "/screener/options",
                    "/screener/stocks",
                ],
            },
        },
    },
    "sec_edgar": {
        "name": "SEC EDGAR",
        "description": "SEC filings, company facts, and insider data",
        "base_path": "/sec",
        "documentation": "https://www.sec.gov/developer",
        "categories": {
            "company": {
                "description": "Company information",
                "endpoints": [
                    "/company/{cik}",
                    "/company/ticker/{ticker}",
                ],
            },
            "filings": {
                "description": "SEC filing data",
                "endpoints": [
                    "/filings/{cik}",
                    "/filings/{cik}/{form_type}",
                    "/search",
                ],
            },
            "institutional": {
                "description": "13F and insider data",
                "endpoints": [
                    "/13f/{cik}",
                    "/insiders/{cik}",
                ],
            },
            "xbrl": {
                "description": "XBRL structured data",
                "endpoints": [
                    "/facts/{cik}",
                    "/concept/{cik}/{concept}",
                    "/frames/{concept}/{period}",
                ],
            },
        },
    },
    "finnhub": {
        "name": "Finnhub",
        "description": "Fundamentals, earnings, news, and alternative data",
        "base_path": "/finnhub",
        "documentation": "https://finnhub.io/docs/api",
        "categories": {
            "company": {
                "description": "Company profiles and metrics",
                "endpoints": [
                    "/stock/profile/{symbol}",
                    "/stock/metric",
                    "/stock/peers",
                    "/stock/executive",
                ],
            },
            "financials": {
                "description": "Financial statements",
                "endpoints": [
                    "/stock/financials",
                    "/stock/financials-reported",
                    "/stock/revenue-estimate",
                ],
            },
            "earnings": {
                "description": "Earnings data and estimates",
                "endpoints": [
                    "/stock/earnings",
                    "/stock/eps-estimate",
                    "/stock/earnings-quality-score",
                    "/calendar/earnings",
                ],
            },
            "news": {
                "description": "Company and market news",
                "endpoints": [
                    "/news/company/{symbol}",
                    "/news/market",
                    "/news/sentiment",
                ],
            },
            "alternative": {
                "description": "Alternative data",
                "endpoints": [
                    "/stock/social-sentiment",
                    "/stock/insider-transactions",
                    "/stock/congressional-trading",
                ],
            },
        },
    },
    "alpha_vantage": {
        "name": "Alpha Vantage",
        "description": "Technical indicators, forex, and economic data",
        "base_path": "/alphavantage",
        "documentation": "https://www.alphavantage.co/documentation/",
        "categories": {
            "overview": {
                "description": "Company fundamentals",
                "endpoints": [
                    "/company/{symbol}",
                    "/income/{symbol}",
                    "/balance/{symbol}",
                    "/cashflow/{symbol}",
                    "/earnings/{symbol}",
                ],
            },
            "technical": {
                "description": "Technical indicators",
                "endpoints": [
                    "/indicator/{indicator}/{symbol}",
                    "/sma/{symbol}",
                    "/ema/{symbol}",
                    "/rsi/{symbol}",
                    "/macd/{symbol}",
                ],
            },
            "forex": {
                "description": "Forex data",
                "endpoints": [
                    "/forex/rate",
                    "/forex/intraday",
                    "/forex/daily",
                ],
            },
            "economy": {
                "description": "Economic indicators",
                "endpoints": [
                    "/economy/gdp",
                    "/economy/inflation",
                    "/economy/interest-rate",
                    "/economy/unemployment",
                ],
            },
        },
    },
    "yahoo_finance": {
        "name": "Yahoo Finance",
        "description": "Free stock quotes, financials, and analysis",
        "base_path": "/yf",
        "documentation": "https://pypi.org/project/yfinance/",
        "categories": {
            "quotes": {
                "description": "Stock quotes and history",
                "endpoints": [
                    "/ticker/{symbol}",
                    "/ticker/{symbol}/info",
                    "/ticker/{symbol}/history",
                ],
            },
            "financials": {
                "description": "Financial data",
                "endpoints": [
                    "/ticker/{symbol}/financials",
                    "/ticker/{symbol}/earnings",
                    "/ticker/{symbol}/dividends",
                    "/ticker/{symbol}/splits",
                ],
            },
            "options": {
                "description": "Options chains",
                "endpoints": [
                    "/ticker/{symbol}/options",
                    "/ticker/{symbol}/options/{expiration}",
                ],
            },
            "analysis": {
                "description": "Analyst data",
                "endpoints": [
                    "/ticker/{symbol}/recommendations",
                    "/ticker/{symbol}/holders",
                    "/ticker/{symbol}/sustainability",
                ],
            },
        },
    },
    "news": {
        "name": "News Aggregator",
        "description": "Consolidated news from multiple sources",
        "base_path": "/news",
        "categories": {
            "articles": {
                "description": "News articles",
                "endpoints": [
                    "/articles",
                    "/articles/{article_id}",
                ],
            },
            "sentiment": {
                "description": "News sentiment",
                "endpoints": [
                    "/sentiment/{symbol}",
                ],
            },
        },
    },
}


router = APIRouter(prefix="/catalog", tags=["catalog"])


# WebSocket Stream Metadata
STREAM_CATALOG = {
    "stocks_sip": {
        "name": "Stocks (SIP)",
        "description": "Real-time stock data from all US exchanges via Securities Information Processor",
        "provider": "alpaca",
        "endpoint": "wss://stream.data.alpaca.markets/v2/sip",
        "gateway_feed": "stock_bars",
        "channels": {
            "trades": {
                "description": "Real-time trade executions",
                "subscribe_key": "trades",
                "message_type": "t",
            },
            "quotes": {
                "description": "Real-time NBBO quotes",
                "subscribe_key": "quotes",
                "message_type": "q",
            },
            "bars": {
                "description": "Minute bars emitted after each minute",
                "subscribe_key": "bars",
                "message_type": "b",
            },
            "dailyBars": {
                "description": "Daily bars updated each minute during market hours",
                "subscribe_key": "dailyBars",
                "message_type": "d",
            },
            "updatedBars": {
                "description": "Updated bars for late trades (emitted at half-minute marks)",
                "subscribe_key": "updatedBars",
                "message_type": "u",
            },
            "lulds": {
                "description": "Limit Up/Limit Down price bands",
                "subscribe_key": "lulds",
                "message_type": "l",
            },
            "statuses": {
                "description": "Trading halt/resume status updates",
                "subscribe_key": "statuses",
                "message_type": "s",
            },
            "imbalances": {
                "description": "Auction imbalance data (opening/closing)",
                "subscribe_key": "imbalances",
                "message_type": "i",
            },
        },
        "symbol_format": "Ticker symbol (e.g., AAPL, MSFT, GOOGL)",
        "wildcard_support": True,
        "wildcard_example": '{"action":"subscribe","bars":["*"]}',
        "subscribe_example": {
            "action": "subscribe",
            "trades": ["AAPL"],
            "quotes": ["AAPL", "MSFT"],
            "bars": ["SPY"],
        },
    },
    "stocks_iex": {
        "name": "Stocks (IEX)",
        "description": "Real-time stock data from IEX exchange (free tier)",
        "provider": "alpaca",
        "endpoint": "wss://stream.data.alpaca.markets/v2/iex",
        "gateway_feed": "stock_bars",
        "channels": {
            "trades": {
                "description": "Real-time trade executions",
                "subscribe_key": "trades",
                "message_type": "t",
            },
            "quotes": {
                "description": "Real-time quotes",
                "subscribe_key": "quotes",
                "message_type": "q",
            },
            "bars": {
                "description": "Minute bars",
                "subscribe_key": "bars",
                "message_type": "b",
            },
            "dailyBars": {
                "description": "Daily bars",
                "subscribe_key": "dailyBars",
                "message_type": "d",
            },
            "updatedBars": {
                "description": "Updated bars for late trades",
                "subscribe_key": "updatedBars",
                "message_type": "u",
            },
        },
        "symbol_format": "Ticker symbol (e.g., AAPL, MSFT)",
        "wildcard_support": True,
        "subscribe_example": {
            "action": "subscribe",
            "bars": ["AAPL", "MSFT"],
        },
    },
    "options_opra": {
        "name": "Options (OPRA)",
        "description": "Real-time options data from OPRA (requires Algo Trader Plus)",
        "provider": "alpaca",
        "endpoint": "wss://stream.data.alpaca.markets/v1beta1/opra",
        "gateway_feed": "option_bars",
        "encoding": "msgpack",
        "encoding_note": "OPRA stream uses MessagePack binary format, not JSON",
        "channels": {
            "trades": {
                "description": "Options trade executions",
                "subscribe_key": "trades",
                "message_type": "t",
            },
            "quotes": {
                "description": "Options quotes (bid/ask)",
                "subscribe_key": "quotes",
                "message_type": "q",
            },
            "bars": {
                "description": "Options minute bars",
                "subscribe_key": "bars",
                "message_type": "b",
            },
        },
        "symbol_format": "OCC format: ROOT + YYMMDD + C/P + 8-digit strike (e.g., AAPL240119C00190000)",
        "wildcard_support": False,
        "wildcard_note": "Wildcard (*) not supported for options quotes due to data volume",
        "subscribe_example": {
            "action": "subscribe",
            "quotes": ["AAPL240119C00190000"],
            "trades": ["SPY240119P00450000"],
        },
    },
    "crypto": {
        "name": "Crypto",
        "description": "Real-time cryptocurrency data",
        "provider": "alpaca",
        "endpoint": "wss://stream.data.alpaca.markets/v1beta3/crypto/us",
        "gateway_feed": "crypto_bars",
        "channels": {
            "trades": {
                "description": "Crypto trade executions",
                "subscribe_key": "trades",
                "message_type": "t",
            },
            "quotes": {
                "description": "Crypto quotes",
                "subscribe_key": "quotes",
                "message_type": "q",
            },
            "bars": {
                "description": "Minute bars (includes quote midpoints if no trades)",
                "subscribe_key": "bars",
                "message_type": "b",
            },
            "dailyBars": {
                "description": "Daily bars",
                "subscribe_key": "dailyBars",
                "message_type": "d",
            },
            "updatedBars": {
                "description": "Updated bars",
                "subscribe_key": "updatedBars",
                "message_type": "u",
            },
            "orderbooks": {
                "description": "Level 2 orderbook updates",
                "subscribe_key": "orderbooks",
                "message_type": "o",
            },
        },
        "symbol_format": "Pair format: BASE/QUOTE (e.g., BTC/USD, ETH/USD)",
        "wildcard_support": True,
        "subscribe_example": {
            "action": "subscribe",
            "bars": ["BTC/USD", "ETH/USD"],
            "orderbooks": ["BTC/USD"],
        },
    },
    "news": {
        "name": "News",
        "description": "Real-time news articles from Benzinga and other sources",
        "provider": "alpaca",
        "endpoint": "wss://stream.data.alpaca.markets/v1beta1/news",
        "gateway_feed": "news",
        "channels": {
            "news": {
                "description": "News articles with headline, summary, content, and related symbols",
                "subscribe_key": "news",
                "message_type": "n",
            },
        },
        "symbol_format": "Ticker symbol or * for all news",
        "wildcard_support": True,
        "subscribe_example": {
            "action": "subscribe",
            "news": ["*"],
        },
        "symbol_filtered_example": {
            "action": "subscribe",
            "news": ["AAPL", "TSLA", "NVDA"],
        },
    },
}


@router.get("/streams")
async def get_streams() -> dict[str, Any]:
    """Get all available WebSocket streams and their capabilities.

    Returns metadata about each stream including:
    - Available channels (trades, quotes, bars, etc.)
    - Symbol format requirements
    - Subscribe message examples
    - Wildcard support
    """
    return {
        "gateway_websocket": "ws://localhost:8080/ws",
        "protocol": {
            "auth": {
                "action": "auth",
                "key": "<your-gateway-api-key>",
            },
            "subscribe": {
                "action": "subscribe",
                "feed": "<gateway_feed>",
                "symbols": ["<symbol1>", "<symbol2>"],
            },
            "unsubscribe": {
                "action": "unsubscribe",
                "feed": "<gateway_feed>",
                "symbols": ["<symbol1>"],
            },
        },
        "streams": STREAM_CATALOG,
    }


@router.get("/streams/{stream_id}")
async def get_stream(stream_id: str) -> dict[str, Any]:
    """Get details for a specific stream.

    Args:
        stream_id: Stream identifier (stocks_sip, options_opra, crypto, news)
    """
    if stream_id not in STREAM_CATALOG:
        return {
            "error": f"Unknown stream: {stream_id}",
            "available_streams": list(STREAM_CATALOG.keys()),
        }
    return STREAM_CATALOG[stream_id]


@router.get("/feeds")
async def get_feeds() -> dict[str, Any]:
    """Get mapping of Gateway feed names to upstream streams.

    Use these feed names in the 'feed' field when subscribing via the Gateway WebSocket.
    """
    feed_mapping = {
        # Stock feeds
        "stock_bars": {
            "stream": "stocks_sip",
            "channels": ["bars"],
            "description": "Stock minute bars",
        },
        "stock_quotes": {
            "stream": "stocks_sip",
            "channels": ["quotes"],
            "description": "Stock NBBO quotes",
        },
        "stock_trades": {
            "stream": "stocks_sip",
            "channels": ["trades"],
            "description": "Stock trade executions",
        },
        "stock_dailyBars": {
            "stream": "stocks_sip",
            "channels": ["dailyBars"],
            "description": "Stock daily bars (updated each minute)",
        },
        "stock_updatedBars": {
            "stream": "stocks_sip",
            "channels": ["updatedBars"],
            "description": "Stock updated bars for late trades",
        },
        "stock_lulds": {
            "stream": "stocks_sip",
            "channels": ["lulds"],
            "description": "Limit Up/Limit Down price bands",
        },
        "stock_statuses": {
            "stream": "stocks_sip",
            "channels": ["statuses"],
            "description": "Trading halt/resume status updates",
        },
        "stock_imbalances": {
            "stream": "stocks_sip",
            "channels": ["imbalances"],
            "description": "Auction imbalance data (opening/closing)",
        },
        # Options feeds
        "option_bars": {
            "stream": "options_opra",
            "channels": ["bars"],
            "description": "Options minute bars",
        },
        "option_quotes": {
            "stream": "options_opra",
            "channels": ["quotes"],
            "description": "Options quotes",
        },
        "option_trades": {
            "stream": "options_opra",
            "channels": ["trades"],
            "description": "Options trade executions",
        },
        # Crypto feeds
        "crypto_bars": {
            "stream": "crypto",
            "channels": ["bars"],
            "description": "Crypto minute bars",
        },
        "crypto_quotes": {
            "stream": "crypto",
            "channels": ["quotes"],
            "description": "Crypto quotes",
        },
        "crypto_trades": {
            "stream": "crypto",
            "channels": ["trades"],
            "description": "Crypto trade executions",
        },
        "crypto_dailyBars": {
            "stream": "crypto",
            "channels": ["dailyBars"],
            "description": "Crypto daily bars",
        },
        "crypto_updatedBars": {
            "stream": "crypto",
            "channels": ["updatedBars"],
            "description": "Crypto updated bars",
        },
        "crypto_orderbooks": {
            "stream": "crypto",
            "channels": ["orderbooks"],
            "description": "Crypto Level 2 orderbooks",
        },
        # News
        "news": {
            "stream": "news",
            "channels": ["news"],
            "description": "Real-time news articles",
        },
    }

    return {
        "feeds": feed_mapping,
        "subscribe_example": {
            "action": "subscribe",
            "feed": "stock_bars",
            "symbols": ["AAPL", "MSFT"],
        },
    }


@router.get("/providers")
async def get_providers() -> dict[str, Any]:
    """Get all available REST API providers and their endpoints.

    Returns a catalog of all data providers with:
    - Base path for each provider
    - Endpoint categories
    - Individual endpoint paths
    """
    return {
        "total_providers": len(PROVIDER_CATALOG),
        "providers": PROVIDER_CATALOG,
        "openapi_docs": "/docs",
    }


@router.get("/providers/{provider_id}")
async def get_provider(provider_id: str) -> dict[str, Any]:
    """Get details for a specific provider.

    Args:
        provider_id: Provider identifier (alpaca, unusual_whales, sec_edgar, etc.)
    """
    if provider_id not in PROVIDER_CATALOG:
        return {
            "error": f"Unknown provider: {provider_id}",
            "available_providers": list(PROVIDER_CATALOG.keys()),
        }
    return PROVIDER_CATALOG[provider_id]


@router.get("/")
async def get_catalog_summary() -> dict[str, Any]:
    """Get a summary of all available APIs.

    This is the main entry point for API discovery.
    """
    return {
        "data_gateway": "Unified financial data gateway",
        "discovery_endpoints": {
            "/catalog/streams": "WebSocket stream discovery",
            "/catalog/feeds": "Gateway feed name mappings",
            "/catalog/providers": "REST API provider discovery",
        },
        "websocket": {
            "endpoint": "ws://localhost:8080/ws",
            "streams": list(STREAM_CATALOG.keys()),
        },
        "rest_api": {
            "providers": list(PROVIDER_CATALOG.keys()),
            "openapi_docs": "/docs",
        },
    }
