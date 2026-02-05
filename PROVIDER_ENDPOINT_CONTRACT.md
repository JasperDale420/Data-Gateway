# Provider Endpoint Contract

This file is generated from provider route declarations in `gateway/api/*`.

## Scope

- `/api/v1/uw/*`
- `/api/v1/finnhub/*`
- `/api/v1/alphavantage/*`
- `/api/v1/sec/*`
- `/api/v1/yf/*`

## Summary

| Provider | Route Count |
|---|---:|
| `unusual_whales` | 125 |
| `finnhub` | 45 |
| `alphavantage` | 30 |
| `sec` | 10 |
| `yfinance` | 16 |

## `unusual_whales`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/uw/alerts` | `gateway/api/uw/market_data.py:get_custom_alerts` |
| `GET` | `/api/v1/uw/alerts/all` | `gateway/api/uw/alerts.py:get_all_alerts` |
| `GET` | `/api/v1/uw/alerts/configuration` | `gateway/api/uw/alerts.py:get_alerts_configuration` |
| `GET` | `/api/v1/uw/congress/late-reports` | `gateway/api/uw/extended.py:get_congress_late_reports` |
| `GET` | `/api/v1/uw/congress/recent` | `gateway/api/uw/intelligence.py:get_recent_congress_trades` |
| `GET` | `/api/v1/uw/congress/reports` | `gateway/api/uw/extended.py:get_congress_reports` |
| `GET` | `/api/v1/uw/congress/{symbol}` | `gateway/api/uw/market.py:get_congress` |
| `GET` | `/api/v1/uw/contract/{contract_id}/intraday` | `gateway/api/uw/options_data.py:get_intraday_option_data` |
| `GET` | `/api/v1/uw/contract/{contract_id}/volume-profile` | `gateway/api/uw/misc.py:get_volume_profile` |
| `GET` | `/api/v1/uw/contract/{option_symbol}/price-history` | `gateway/api/uw/contracts.py:get_contract_price_history` |
| `GET` | `/api/v1/uw/darkpool/all` | `gateway/api/uw/flow.py:get_darkpool_all` |
| `GET` | `/api/v1/uw/darkpool/{symbol}` | `gateway/api/uw/flow.py:get_darkpool_symbol` |
| `GET` | `/api/v1/uw/darkpool/{symbol}/levels` | `gateway/api/uw/intelligence.py:get_off_lit_levels` |
| `GET` | `/api/v1/uw/earnings/afterhours` | `gateway/api/uw/earnings.py:get_earnings_afterhours` |
| `GET` | `/api/v1/uw/earnings/premarket` | `gateway/api/uw/earnings.py:get_earnings_premarket` |
| `GET` | `/api/v1/uw/earnings/{symbol}` | `gateway/api/uw/earnings.py:get_earnings_ticker` |
| `GET` | `/api/v1/uw/etf/{symbol}/country-weights` | `gateway/api/uw/etf_extended.py:get_etf_country_weights` |
| `GET` | `/api/v1/uw/etf/{symbol}/exposure` | `gateway/api/uw/etf.py:get_etf_exposure` |
| `GET` | `/api/v1/uw/etf/{symbol}/flows` | `gateway/api/uw/etf.py:get_etf_flows` |
| `GET` | `/api/v1/uw/etf/{symbol}/holdings` | `gateway/api/uw/etf.py:get_etf_holdings` |
| `GET` | `/api/v1/uw/etf/{symbol}/inflow-outflow` | `gateway/api/uw/etf_extended.py:get_etf_inflow_outflow` |
| `GET` | `/api/v1/uw/etf/{symbol}/info` | `gateway/api/uw/etf_extended.py:get_etf_info` |
| `GET` | `/api/v1/uw/etf/{symbol}/ticker-exposure` | `gateway/api/uw/etf_extended.py:get_etf_ticker_exposure` |
| `GET` | `/api/v1/uw/etf/{symbol}/tide` | `gateway/api/uw/misc.py:get_etf_tide` |
| `GET` | `/api/v1/uw/flow/all` | `gateway/api/uw/flow.py:get_flow_all` |
| `GET` | `/api/v1/uw/flow/contract/{option_symbol}` | `gateway/api/uw/contracts.py:get_contract_flow` |
| `GET` | `/api/v1/uw/flow/full-tape` | `gateway/api/uw/contracts.py:get_full_tape_flow` |
| `GET` | `/api/v1/uw/flow/{symbol}` | `gateway/api/uw/flow.py:get_flow_symbol` |
| `GET` | `/api/v1/uw/gex/{symbol}` | `gateway/api/uw/greeks.py:get_gex` |
| `GET` | `/api/v1/uw/gex/{symbol}/expiry` | `gateway/api/uw/greeks.py:get_gex_by_expiry` |
| `GET` | `/api/v1/uw/gex/{symbol}/strike` | `gateway/api/uw/greeks.py:get_gex_by_strike` |
| `GET` | `/api/v1/uw/insider/sector-flow` | `gateway/api/uw/insiders.py:get_insider_sector_flow` |
| `GET` | `/api/v1/uw/insider/ticker-flow` | `gateway/api/uw/insiders.py:get_insider_ticker_flow` |
| `GET` | `/api/v1/uw/insider/transactions` | `gateway/api/uw/insiders.py:get_insider_transactions` |
| `GET` | `/api/v1/uw/insider/{symbol}/insiders` | `gateway/api/uw/insiders.py:get_ticker_insiders` |
| `GET` | `/api/v1/uw/insiders/{symbol}` | `gateway/api/uw/market.py:get_insiders` |
| `GET` | `/api/v1/uw/institutions` | `gateway/api/uw/institutions.py:get_all_institutions` |
| `GET` | `/api/v1/uw/institutions/latest-filings` | `gateway/api/uw/institutions.py:get_latest_institutional_filings` |
| `GET` | `/api/v1/uw/institutions/{institution_id}/activity` | `gateway/api/uw/institutions.py:get_institution_activity` |
| `GET` | `/api/v1/uw/institutions/{institution_id}/holdings` | `gateway/api/uw/institutions.py:get_institution_holdings` |
| `GET` | `/api/v1/uw/institutions/{institution_id}/sectors` | `gateway/api/uw/institutions.py:get_institution_sector_exposure` |
| `GET` | `/api/v1/uw/institutions/{symbol}` | `gateway/api/uw/market.py:get_institutions` |
| `GET` | `/api/v1/uw/institutions/{symbol}/ownership` | `gateway/api/uw/institutions.py:get_institutional_ownership` |
| `GET` | `/api/v1/uw/market/calendar` | `gateway/api/uw/market_data.py:get_economic_calendar` |
| `GET` | `/api/v1/uw/market/correlations` | `gateway/api/uw/market_data.py:get_market_correlations` |
| `GET` | `/api/v1/uw/market/economic-calendar` | `gateway/api/uw/calendar.py:get_economic_calendar_market` |
| `GET` | `/api/v1/uw/market/fda-calendar` | `gateway/api/uw/calendar.py:get_fda_calendar` |
| `GET` | `/api/v1/uw/market/holidays` | `gateway/api/uw/calendar.py:get_market_holidays` |
| `GET` | `/api/v1/uw/market/imbalances` | `gateway/api/uw/calendar.py:get_market_imbalances` |
| `GET` | `/api/v1/uw/market/insider-trades` | `gateway/api/uw/calendar.py:get_market_insider_trades` |
| `GET` | `/api/v1/uw/market/net-flow-expiry` | `gateway/api/uw/flow_analytics.py:get_net_flow_expiry` |
| `GET` | `/api/v1/uw/market/options-volume` | `gateway/api/uw/calendar.py:get_market_options_volume` |
| `GET` | `/api/v1/uw/market/sector-stats` | `gateway/api/uw/calendar.py:get_sector_stats` |
| `GET` | `/api/v1/uw/market/sector/{sector}/tide` | `gateway/api/uw/market_data.py:get_sector_tide` |
| `GET` | `/api/v1/uw/market/spike` | `gateway/api/uw/extended.py:get_market_spike` |
| `GET` | `/api/v1/uw/market/tide` | `gateway/api/uw/market.py:get_market_tide` |
| `GET` | `/api/v1/uw/market/top-impact` | `gateway/api/uw/market_data.py:get_top_net_impact` |
| `GET` | `/api/v1/uw/market/{etf}/etf-tide` | `gateway/api/uw/calendar.py:get_market_tide_by_etf` |
| `GET` | `/api/v1/uw/news/headlines` | `gateway/api/uw/misc.py:get_news_headlines` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/flow` | `gateway/api/uw/contracts.py:get_option_contract_flow` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/historic` | `gateway/api/uw/contracts.py:get_option_contract_historic` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/intraday` | `gateway/api/uw/contracts.py:get_option_contract_intraday` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/volume-profile` | `gateway/api/uw/contracts.py:get_option_contract_volume_profile` |
| `GET` | `/api/v1/uw/politicians/people` | `gateway/api/uw/politicians.py:get_politician_people` |
| `GET` | `/api/v1/uw/politicians/recent-trades` | `gateway/api/uw/politicians.py:get_politician_recent_trades` |
| `GET` | `/api/v1/uw/politicians/{politician_id}/portfolios` | `gateway/api/uw/politicians.py:get_politician_portfolios` |
| `GET` | `/api/v1/uw/politicians/{symbol}/holders` | `gateway/api/uw/politicians.py:get_politician_holders` |
| `GET` | `/api/v1/uw/screener/analysts` | `gateway/api/uw/alerts.py:get_analyst_ratings` |
| `GET` | `/api/v1/uw/screener/contracts` | `gateway/api/uw/options_data.py:get_options_screener` |
| `GET` | `/api/v1/uw/screener/option-contracts` | `gateway/api/uw/contracts.py:get_screener_option_contracts` |
| `GET` | `/api/v1/uw/screener/options` | `gateway/api/uw/screener.py:get_screener_options` |
| `GET` | `/api/v1/uw/screener/stocks` | `gateway/api/uw/screener.py:get_screener_stocks` |
| `GET` | `/api/v1/uw/screener/stocks` | `gateway/api/uw/contracts.py:get_screener_stocks_extended` |
| `GET` | `/api/v1/uw/seasonality/market` | `gateway/api/uw/seasonality.py:get_market_seasonality` |
| `GET` | `/api/v1/uw/seasonality/monthly-top-performers/{month}` | `gateway/api/uw/extended.py:get_monthly_top_performers` |
| `GET` | `/api/v1/uw/seasonality/{symbol}` | `gateway/api/uw/seasonality.py:get_ticker_seasonality` |
| `GET` | `/api/v1/uw/seasonality/{symbol}/price-changes-by-month` | `gateway/api/uw/extended.py:get_price_changes_by_month_year` |
| `GET` | `/api/v1/uw/sectors/{sector}/tickers` | `gateway/api/uw/stock.py:get_sector_tickers` |
| `GET` | `/api/v1/uw/shorts/{symbol}/data` | `gateway/api/uw/extended.py:get_shorts_data` |
| `GET` | `/api/v1/uw/shorts/{symbol}/interest-float` | `gateway/api/uw/extended.py:get_short_interest_float` |
| `GET` | `/api/v1/uw/shorts/{symbol}/volumes-by-exchange` | `gateway/api/uw/extended.py:get_short_volumes_by_exchange` |
| `GET` | `/api/v1/uw/stock/{symbol}/atm-options` | `gateway/api/uw/stock.py:get_atm_option_contracts` |
| `GET` | `/api/v1/uw/stock/{symbol}/candles` | `gateway/api/uw/stock.py:get_stock_candles` |
| `GET` | `/api/v1/uw/stock/{symbol}/daily-expiry-breakdown` | `gateway/api/uw/stock.py:get_daily_expiry_breakdown` |
| `GET` | `/api/v1/uw/stock/{symbol}/flow-per-strike-intraday` | `gateway/api/uw/stock.py:get_flow_per_strike_intraday` |
| `GET` | `/api/v1/uw/stock/{symbol}/flow-recent` | `gateway/api/uw/stock.py:get_flow_recent` |
| `GET` | `/api/v1/uw/stock/{symbol}/greek-exposure-by-strike-expiry/{expiry}` | `gateway/api/uw/stock.py:get_greek_exposure_by_strike_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/greek-flow-by-expiry/{expiry}` | `gateway/api/uw/stock.py:get_greek_flow_by_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/greeks-by-strike/{expiry}` | `gateway/api/uw/stock.py:get_greeks_by_strike_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/info` | `gateway/api/uw/stock.py:get_stock_info` |
| `GET` | `/api/v1/uw/stock/{symbol}/insider-trades` | `gateway/api/uw/stock.py:get_stock_insider_trades` |
| `GET` | `/api/v1/uw/stock/{symbol}/oi-per-expiry` | `gateway/api/uw/stock.py:get_oi_per_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/oi-per-strike` | `gateway/api/uw/stock.py:get_oi_per_strike` |
| `GET` | `/api/v1/uw/stock/{symbol}/option-chains` | `gateway/api/uw/stock.py:get_stock_option_chains` |
| `GET` | `/api/v1/uw/stock/{symbol}/option-contracts` | `gateway/api/uw/stock.py:get_stock_option_contracts` |
| `GET` | `/api/v1/uw/stock/{symbol}/option-volume-by-price` | `gateway/api/uw/stock.py:get_option_volume_by_price_level` |
| `GET` | `/api/v1/uw/stock/{symbol}/options-volume` | `gateway/api/uw/stock.py:get_options_volume` |
| `GET` | `/api/v1/uw/stock/{symbol}/risk-reversal-skew/{expiry}` | `gateway/api/uw/stock.py:get_risk_reversal_skew` |
| `GET` | `/api/v1/uw/stock/{symbol}/spot-exposures` | `gateway/api/uw/stock.py:get_spot_exposures` |
| `GET` | `/api/v1/uw/stock/{symbol}/spot-exposures-by-expiry-strike/{expiry}` | `gateway/api/uw/stock.py:get_spot_exposures_by_expiry_strike` |
| `GET` | `/api/v1/uw/stock/{symbol}/state` | `gateway/api/uw/stock.py:get_stock_state` |
| `GET` | `/api/v1/uw/stock/{symbol}/volume-oi-by-expiry` | `gateway/api/uw/stock.py:get_volume_oi_by_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/volume-price-levels` | `gateway/api/uw/stock.py:get_stock_volume_price_levels` |
| `GET` | `/api/v1/uw/trades/full-tape/{date}` | `gateway/api/uw/misc.py:get_full_tape` |
| `GET` | `/api/v1/uw/{symbol}/flow-expiry` | `gateway/api/uw/flow_analytics.py:get_flow_per_expiry` |
| `GET` | `/api/v1/uw/{symbol}/flow-strike` | `gateway/api/uw/flow_analytics.py:get_flow_per_strike` |
| `GET` | `/api/v1/uw/{symbol}/ftds` | `gateway/api/uw/shorts.py:get_ftds` |
| `GET` | `/api/v1/uw/{symbol}/greek-flow` | `gateway/api/uw/flow_analytics.py:get_greek_flow` |
| `GET` | `/api/v1/uw/{symbol}/greek-flow-expiry` | `gateway/api/uw/misc.py:get_greek_flow_expiry` |
| `GET` | `/api/v1/uw/{symbol}/interpolated-iv` | `gateway/api/uw/flow_analytics.py:get_interpolated_iv` |
| `GET` | `/api/v1/uw/{symbol}/iv-rank` | `gateway/api/uw/options.py:get_iv_rank` |
| `GET` | `/api/v1/uw/{symbol}/iv-surface` | `gateway/api/uw/volatility.py:get_iv_surface` |
| `GET` | `/api/v1/uw/{symbol}/iv-term-structure` | `gateway/api/uw/volatility.py:get_iv_term_structure` |
| `GET` | `/api/v1/uw/{symbol}/max-pain` | `gateway/api/uw/options.py:get_max_pain` |
| `GET` | `/api/v1/uw/{symbol}/net-premium` | `gateway/api/uw/options.py:get_net_premium` |
| `GET` | `/api/v1/uw/{symbol}/nope` | `gateway/api/uw/intelligence.py:get_nope` |
| `GET` | `/api/v1/uw/{symbol}/oi-change` | `gateway/api/uw/options.py:get_oi_change` |
| `GET` | `/api/v1/uw/{symbol}/option-volume` | `gateway/api/uw/options_data.py:get_historic_option_volume` |
| `GET` | `/api/v1/uw/{symbol}/pc-ratio` | `gateway/api/uw/intelligence.py:get_put_call_ratio` |
| `GET` | `/api/v1/uw/{symbol}/realized-vol` | `gateway/api/uw/volatility.py:get_realized_vol` |
| `GET` | `/api/v1/uw/{symbol}/short-interest` | `gateway/api/uw/shorts.py:get_short_interest` |
| `GET` | `/api/v1/uw/{symbol}/short-volume` | `gateway/api/uw/shorts.py:get_short_volume` |
| `GET` | `/api/v1/uw/{symbol}/spot-exposures` | `gateway/api/uw/flow_analytics.py:get_spot_exposures_by_strike` |
| `GET` | `/api/v1/uw/{symbol}/vol-stats` | `gateway/api/uw/volatility.py:get_vol_stats` |
| `GET` | `/api/v1/uw/{symbol}/volume-levels` | `gateway/api/uw/misc.py:get_option_volume_levels` |

## `finnhub`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/finnhub/bars/{symbol}` | `gateway/api/finnhub/quotes.py:get_bars` |
| `GET` | `/api/v1/finnhub/congress-trading` | `gateway/api/finnhub/alternative.py:get_congress_trading` |
| `GET` | `/api/v1/finnhub/crypto/candles/{symbol}` | `gateway/api/finnhub/crypto.py:get_crypto_candles` |
| `GET` | `/api/v1/finnhub/crypto/exchanges` | `gateway/api/finnhub/crypto.py:get_crypto_exchanges` |
| `GET` | `/api/v1/finnhub/crypto/symbols` | `gateway/api/finnhub/crypto.py:get_crypto_symbols` |
| `GET` | `/api/v1/finnhub/crypto/{symbol}/profile` | `gateway/api/finnhub/crypto.py:get_crypto_profile` |
| `GET` | `/api/v1/finnhub/earnings` | `gateway/api/finnhub/earnings.py:get_earnings_calendar` |
| `GET` | `/api/v1/finnhub/estimates/ebit/{symbol}` | `gateway/api/finnhub/earnings.py:get_ebit_estimates` |
| `GET` | `/api/v1/finnhub/estimates/ebitda/{symbol}` | `gateway/api/finnhub/earnings.py:get_ebitda_estimates` |
| `GET` | `/api/v1/finnhub/estimates/eps/{symbol}` | `gateway/api/finnhub/earnings.py:get_eps_estimates` |
| `GET` | `/api/v1/finnhub/estimates/revenue/{symbol}` | `gateway/api/finnhub/earnings.py:get_revenue_estimates` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/country` | `gateway/api/finnhub/etf.py:get_etf_country` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/holdings` | `gateway/api/finnhub/etf.py:get_etf_holdings` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/profile` | `gateway/api/finnhub/etf.py:get_etf_profile` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/sector` | `gateway/api/finnhub/etf.py:get_etf_sector` |
| `GET` | `/api/v1/finnhub/executives/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_executives` |
| `GET` | `/api/v1/finnhub/fda-calendar` | `gateway/api/finnhub/alternative.py:get_fda_calendar` |
| `GET` | `/api/v1/finnhub/financials/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_financials` |
| `GET` | `/api/v1/finnhub/forex/candles/{symbol}` | `gateway/api/finnhub/forex.py:get_forex_candles` |
| `GET` | `/api/v1/finnhub/forex/exchanges` | `gateway/api/finnhub/forex.py:get_forex_exchanges` |
| `GET` | `/api/v1/finnhub/forex/rates` | `gateway/api/finnhub/forex.py:get_forex_rates` |
| `GET` | `/api/v1/finnhub/forex/symbols` | `gateway/api/finnhub/forex.py:get_forex_symbols` |
| `GET` | `/api/v1/finnhub/fund-ownership/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_fund_ownership` |
| `GET` | `/api/v1/finnhub/index/{symbol}/constituents` | `gateway/api/finnhub/etf.py:get_index_constituents` |
| `GET` | `/api/v1/finnhub/index/{symbol}/historical` | `gateway/api/finnhub/etf.py:get_index_historical` |
| `GET` | `/api/v1/finnhub/insider-sentiment/{symbol}` | `gateway/api/finnhub/analysis.py:get_insider_sentiment` |
| `GET` | `/api/v1/finnhub/insider-transactions/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_insider_transactions` |
| `GET` | `/api/v1/finnhub/lobbying/{symbol}` | `gateway/api/finnhub/alternative.py:get_lobbying` |
| `GET` | `/api/v1/finnhub/metrics/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_metrics` |
| `GET` | `/api/v1/finnhub/mutual-fund/{symbol}/holdings` | `gateway/api/finnhub/funds.py:get_mutual_fund_holdings` |
| `GET` | `/api/v1/finnhub/mutual-fund/{symbol}/profile` | `gateway/api/finnhub/funds.py:get_mutual_fund_profile` |
| `GET` | `/api/v1/finnhub/mutual-fund/{symbol}/sector` | `gateway/api/finnhub/funds.py:get_mutual_fund_sector` |
| `GET` | `/api/v1/finnhub/news/market/{category}` | `gateway/api/finnhub/news.py:get_market_news` |
| `GET` | `/api/v1/finnhub/news/{symbol}` | `gateway/api/finnhub/news.py:get_company_news` |
| `GET` | `/api/v1/finnhub/ownership/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_ownership` |
| `GET` | `/api/v1/finnhub/patterns/{symbol}` | `gateway/api/finnhub/analysis.py:get_pattern_recognition` |
| `GET` | `/api/v1/finnhub/peers/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_peers` |
| `GET` | `/api/v1/finnhub/price-target/{symbol}` | `gateway/api/finnhub/earnings.py:get_price_target` |
| `GET` | `/api/v1/finnhub/profile/{symbol}` | `gateway/api/finnhub/fundamentals.py:get_company_profile` |
| `GET` | `/api/v1/finnhub/quote/{symbol}` | `gateway/api/finnhub/quotes.py:get_quote` |
| `GET` | `/api/v1/finnhub/recommendations/{symbol}` | `gateway/api/finnhub/earnings.py:get_recommendations` |
| `GET` | `/api/v1/finnhub/social-sentiment/{symbol}` | `gateway/api/finnhub/analysis.py:get_social_sentiment` |
| `GET` | `/api/v1/finnhub/support-resistance/{symbol}` | `gateway/api/finnhub/analysis.py:get_support_resistance` |
| `GET` | `/api/v1/finnhub/upgrade-downgrade/{symbol}` | `gateway/api/finnhub/analysis.py:get_upgrade_downgrade` |
| `GET` | `/api/v1/finnhub/usa-spending/{symbol}` | `gateway/api/finnhub/alternative.py:get_usa_spending` |

## `alphavantage`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/alphavantage/balance-sheet/{symbol}` | `gateway/api/alphavantage/fundamentals.py:get_balance_sheet` |
| `GET` | `/api/v1/alphavantage/calendar/earnings` | `gateway/api/alphavantage/calendars.py:get_earnings_calendar` |
| `GET` | `/api/v1/alphavantage/calendar/ipo` | `gateway/api/alphavantage/calendars.py:get_ipo_calendar` |
| `GET` | `/api/v1/alphavantage/cash-flow/{symbol}` | `gateway/api/alphavantage/fundamentals.py:get_cash_flow` |
| `GET` | `/api/v1/alphavantage/crypto/daily/{symbol}` | `gateway/api/alphavantage/crypto.py:get_crypto_daily` |
| `GET` | `/api/v1/alphavantage/crypto/rating/{symbol}` | `gateway/api/alphavantage/crypto.py:get_crypto_rating` |
| `GET` | `/api/v1/alphavantage/daily/{symbol}` | `gateway/api/alphavantage/timeseries.py:get_daily` |
| `GET` | `/api/v1/alphavantage/earnings/{symbol}` | `gateway/api/alphavantage/fundamentals.py:get_earnings` |
| `GET` | `/api/v1/alphavantage/economic/{indicator}` | `gateway/api/alphavantage/economic.py:get_economic_indicator` |
| `GET` | `/api/v1/alphavantage/forex/daily/{from_symbol}/{to_symbol}` | `gateway/api/alphavantage/forex.py:get_forex_daily` |
| `GET` | `/api/v1/alphavantage/forex/rate/{from_currency}/{to_currency}` | `gateway/api/alphavantage/forex.py:get_forex_rate` |
| `GET` | `/api/v1/alphavantage/income-statement/{symbol}` | `gateway/api/alphavantage/fundamentals.py:get_income_statement` |
| `GET` | `/api/v1/alphavantage/indicator/adx/{symbol}` | `gateway/api/alphavantage/indicators.py:get_adx` |
| `GET` | `/api/v1/alphavantage/indicator/atr/{symbol}` | `gateway/api/alphavantage/indicators.py:get_atr` |
| `GET` | `/api/v1/alphavantage/indicator/bbands/{symbol}` | `gateway/api/alphavantage/indicators.py:get_bbands` |
| `GET` | `/api/v1/alphavantage/indicator/cci/{symbol}` | `gateway/api/alphavantage/indicators.py:get_cci` |
| `GET` | `/api/v1/alphavantage/indicator/ema/{symbol}` | `gateway/api/alphavantage/indicators.py:get_ema` |
| `GET` | `/api/v1/alphavantage/indicator/macd/{symbol}` | `gateway/api/alphavantage/indicators.py:get_macd` |
| `GET` | `/api/v1/alphavantage/indicator/obv/{symbol}` | `gateway/api/alphavantage/indicators.py:get_obv` |
| `GET` | `/api/v1/alphavantage/indicator/rsi/{symbol}` | `gateway/api/alphavantage/indicators.py:get_rsi` |
| `GET` | `/api/v1/alphavantage/indicator/sma/{symbol}` | `gateway/api/alphavantage/indicators.py:get_sma` |
| `GET` | `/api/v1/alphavantage/indicator/stoch/{symbol}` | `gateway/api/alphavantage/indicators.py:get_stoch` |
| `GET` | `/api/v1/alphavantage/indicator/{indicator}/{symbol}` | `gateway/api/alphavantage/indicators.py:get_technical_indicator` |
| `GET` | `/api/v1/alphavantage/intraday/{symbol}` | `gateway/api/alphavantage/timeseries.py:get_intraday` |
| `GET` | `/api/v1/alphavantage/listing-status` | `gateway/api/alphavantage/calendars.py:get_listing_status` |
| `GET` | `/api/v1/alphavantage/monthly/{symbol}` | `gateway/api/alphavantage/timeseries.py:get_monthly` |
| `GET` | `/api/v1/alphavantage/overview/{symbol}` | `gateway/api/alphavantage/fundamentals.py:get_company_overview` |
| `GET` | `/api/v1/alphavantage/quote/{symbol}` | `gateway/api/alphavantage/timeseries.py:get_quote` |
| `GET` | `/api/v1/alphavantage/search` | `gateway/api/alphavantage/timeseries.py:search_symbols` |
| `GET` | `/api/v1/alphavantage/weekly/{symbol}` | `gateway/api/alphavantage/timeseries.py:get_weekly` |

## `sec`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/sec/13f/{cik}` | `gateway/api/sec.py:get_13f_holdings` |
| `GET` | `/api/v1/sec/company/ticker/{ticker}` | `gateway/api/sec.py:get_company_by_ticker` |
| `GET` | `/api/v1/sec/company/{cik}` | `gateway/api/sec.py:get_company_info` |
| `GET` | `/api/v1/sec/concept/{cik}/{concept}` | `gateway/api/sec.py:get_company_concept` |
| `GET` | `/api/v1/sec/facts/{cik}` | `gateway/api/sec.py:get_company_facts` |
| `GET` | `/api/v1/sec/filings/{cik}` | `gateway/api/sec.py:get_filings` |
| `GET` | `/api/v1/sec/filings/{cik}/{form_type}` | `gateway/api/sec.py:get_filings_by_type` |
| `GET` | `/api/v1/sec/frames/{concept}/{period}` | `gateway/api/sec.py:get_xbrl_frames` |
| `GET` | `/api/v1/sec/insiders/{cik}` | `gateway/api/sec.py:get_insider_trades` |
| `GET` | `/api/v1/sec/search` | `gateway/api/sec.py:search_filings` |

## `yfinance`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/yf/ticker/{symbol}` | `gateway/api/yf.py:get_ticker_info` |
| `GET` | `/api/v1/yf/ticker/{symbol}/actions` | `gateway/api/yf.py:get_actions` |
| `GET` | `/api/v1/yf/ticker/{symbol}/calendar` | `gateway/api/yf.py:get_calendar` |
| `GET` | `/api/v1/yf/ticker/{symbol}/dividends` | `gateway/api/yf.py:get_dividends` |
| `GET` | `/api/v1/yf/ticker/{symbol}/earnings` | `gateway/api/yf.py:get_earnings` |
| `GET` | `/api/v1/yf/ticker/{symbol}/financials` | `gateway/api/yf.py:get_financials` |
| `GET` | `/api/v1/yf/ticker/{symbol}/history` | `gateway/api/yf.py:get_history` |
| `GET` | `/api/v1/yf/ticker/{symbol}/holders` | `gateway/api/yf.py:get_holders` |
| `GET` | `/api/v1/yf/ticker/{symbol}/info` | `gateway/api/yf.py:get_company_info` |
| `GET` | `/api/v1/yf/ticker/{symbol}/major-holders` | `gateway/api/yf.py:get_major_holders` |
| `GET` | `/api/v1/yf/ticker/{symbol}/news` | `gateway/api/yf.py:get_news` |
| `GET` | `/api/v1/yf/ticker/{symbol}/options` | `gateway/api/yf.py:get_options` |
| `GET` | `/api/v1/yf/ticker/{symbol}/options/{expiration}` | `gateway/api/yf.py:get_options_chain` |
| `GET` | `/api/v1/yf/ticker/{symbol}/recommendations` | `gateway/api/yf.py:get_recommendations` |
| `GET` | `/api/v1/yf/ticker/{symbol}/splits` | `gateway/api/yf.py:get_splits` |
| `GET` | `/api/v1/yf/ticker/{symbol}/sustainability` | `gateway/api/yf.py:get_sustainability` |

## Regeneration

```bash
python scripts/generate_provider_contract.py
```
