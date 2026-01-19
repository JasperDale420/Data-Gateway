"""API Catalog endpoints for stream and endpoint discovery."""

from typing import Any

import structlog
from fastapi import APIRouter

logger = structlog.get_logger()

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
        "crypto_orderbooks": {
            "stream": "crypto",
            "channels": ["orderbooks"],
            "description": "Crypto Level 2 orderbooks",
        },
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
