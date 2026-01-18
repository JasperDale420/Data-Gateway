Alpaca Market Data - Complete Catalog
📈 Stocks (US Equities)
Data Type	Granularity	Historical?	Notes
Bars (OHLCV)	1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M	✅ Yes	Aggregated price data
Trades	Tick-level	✅ Yes	Every individual trade
Quotes	Tick-level	✅ Yes	Best bid/ask at every update
Auctions	Daily	✅ Yes	Opening/closing auction data
Snapshots	Latest	❌ Real-time only	Current state (trade, quote, bars)
📊 Options
Data Type	Granularity	Historical?	Notes
Bars (OHLCV)	1m, 1h, 1D	✅ Yes	Contract-level aggregates
Trades	Tick-level	✅ Yes	Every option trade
Quotes	Tick-level	✅ Yes	Bid/ask per contract
Greeks	Latest	❌ Real-time only	IV, delta, gamma, theta, vega
Option Chain	Latest	❌ Real-time only	All contracts for an underlying
🪙 Crypto
Data Type	Granularity	Historical?	Notes
Bars (OHLCV)	1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M	✅ Yes	BTC, ETH, etc.
Trades	Tick-level	✅ Yes	Individual trades
Quotes	Tick-level	✅ Yes	Best bid/ask
Orderbook	Latest	❌ Real-time only	L2 orderbook depth
💱 Forex
Data Type	Granularity	Historical?	Notes
Rates	1m, 5m, 15m, 30m, 1h, 1D	✅ Yes	Currency pair exchange rates
Latest Rates	Current	❌ Real-time only	Most recent FX rate
📰 Other Data
Data Type	Description
News	Headlines + content for stocks/crypto with sentiment
Corporate Actions	Dividends, splits, spin-offs, mergers
Company Logos	PNG images for UI display
Screener - Most Active	Top stocks by volume/trade count
Screener - Movers	Top gainers/losers by % change
Fixed Income	Bond pricing (latest only)
Crypto Perpetual Futures	Perp contract pricing/funding rates
⏱️ Timeframe Options (for bars)
1Min / 1T - 1 minute
5Min / 5T - 5 minutes
15Min / 15T - 15 minutes
30Min / 30T - 30 minutes
1Hour / 1H - 1 hour
4Hour / 4H - 4 hours
1Day / 1D - Daily
1Week / 1W - Weekly
1Month / 1M - Monthly
📅 Historical Data Availability
Stocks: ~6+ years of historical data
Options: ~2 years
Crypto: ~3-4 years
Forex: ~2 years
What should we prioritize ingesting? The current implementation handles stock bars. Would you like me to add:

Options bars/trades
Crypto bars
News articles
Corporate actions
Tick-level trades/quotes (warning: massive storage requirements)
