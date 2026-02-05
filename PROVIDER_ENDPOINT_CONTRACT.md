# Provider Endpoint Contract

This file is generated from live FastAPI routes.

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
| `GET` | `/api/v1/uw/alerts` | `get_custom_alerts` |
| `GET` | `/api/v1/uw/alerts/all` | `get_all_alerts` |
| `GET` | `/api/v1/uw/alerts/configuration` | `get_alerts_configuration` |
| `GET` | `/api/v1/uw/congress/late-reports` | `get_congress_late_reports` |
| `GET` | `/api/v1/uw/congress/recent` | `get_recent_congress_trades` |
| `GET` | `/api/v1/uw/congress/reports` | `get_congress_reports` |
| `GET` | `/api/v1/uw/congress/{symbol}` | `get_congress` |
| `GET` | `/api/v1/uw/contract/{contract_id}/intraday` | `get_intraday_option_data` |
| `GET` | `/api/v1/uw/contract/{contract_id}/volume-profile` | `get_volume_profile` |
| `GET` | `/api/v1/uw/contract/{option_symbol}/price-history` | `get_contract_price_history` |
| `GET` | `/api/v1/uw/darkpool/all` | `get_darkpool_all` |
| `GET` | `/api/v1/uw/darkpool/{symbol}` | `get_darkpool_symbol` |
| `GET` | `/api/v1/uw/darkpool/{symbol}/levels` | `get_off_lit_levels` |
| `GET` | `/api/v1/uw/earnings/afterhours` | `get_earnings_afterhours` |
| `GET` | `/api/v1/uw/earnings/premarket` | `get_earnings_premarket` |
| `GET` | `/api/v1/uw/earnings/{symbol}` | `get_earnings_ticker` |
| `GET` | `/api/v1/uw/etf/{symbol}/country-weights` | `get_etf_country_weights` |
| `GET` | `/api/v1/uw/etf/{symbol}/exposure` | `get_etf_exposure` |
| `GET` | `/api/v1/uw/etf/{symbol}/flows` | `get_etf_flows` |
| `GET` | `/api/v1/uw/etf/{symbol}/holdings` | `get_etf_holdings` |
| `GET` | `/api/v1/uw/etf/{symbol}/inflow-outflow` | `get_etf_inflow_outflow` |
| `GET` | `/api/v1/uw/etf/{symbol}/info` | `get_etf_info` |
| `GET` | `/api/v1/uw/etf/{symbol}/ticker-exposure` | `get_etf_ticker_exposure` |
| `GET` | `/api/v1/uw/etf/{symbol}/tide` | `get_etf_tide` |
| `GET` | `/api/v1/uw/flow/all` | `get_flow_all` |
| `GET` | `/api/v1/uw/flow/contract/{option_symbol}` | `get_contract_flow` |
| `GET` | `/api/v1/uw/flow/full-tape` | `get_full_tape_flow` |
| `GET` | `/api/v1/uw/flow/{symbol}` | `get_flow_symbol` |
| `GET` | `/api/v1/uw/gex/{symbol}` | `get_gex` |
| `GET` | `/api/v1/uw/gex/{symbol}/expiry` | `get_gex_by_expiry` |
| `GET` | `/api/v1/uw/gex/{symbol}/strike` | `get_gex_by_strike` |
| `GET` | `/api/v1/uw/insider/sector-flow` | `get_insider_sector_flow` |
| `GET` | `/api/v1/uw/insider/ticker-flow` | `get_insider_ticker_flow` |
| `GET` | `/api/v1/uw/insider/transactions` | `get_insider_transactions` |
| `GET` | `/api/v1/uw/insider/{symbol}/insiders` | `get_ticker_insiders` |
| `GET` | `/api/v1/uw/insiders/{symbol}` | `get_insiders` |
| `GET` | `/api/v1/uw/institutions` | `get_all_institutions` |
| `GET` | `/api/v1/uw/institutions/latest-filings` | `get_latest_institutional_filings` |
| `GET` | `/api/v1/uw/institutions/{institution_id}/activity` | `get_institution_activity` |
| `GET` | `/api/v1/uw/institutions/{institution_id}/holdings` | `get_institution_holdings` |
| `GET` | `/api/v1/uw/institutions/{institution_id}/sectors` | `get_institution_sector_exposure` |
| `GET` | `/api/v1/uw/institutions/{symbol}` | `get_institutions` |
| `GET` | `/api/v1/uw/institutions/{symbol}/ownership` | `get_institutional_ownership` |
| `GET` | `/api/v1/uw/market/calendar` | `get_economic_calendar` |
| `GET` | `/api/v1/uw/market/correlations` | `get_market_correlations` |
| `GET` | `/api/v1/uw/market/economic-calendar` | `get_economic_calendar_market` |
| `GET` | `/api/v1/uw/market/fda-calendar` | `get_fda_calendar` |
| `GET` | `/api/v1/uw/market/holidays` | `get_market_holidays` |
| `GET` | `/api/v1/uw/market/imbalances` | `get_market_imbalances` |
| `GET` | `/api/v1/uw/market/insider-trades` | `get_market_insider_trades` |
| `GET` | `/api/v1/uw/market/net-flow-expiry` | `get_net_flow_expiry` |
| `GET` | `/api/v1/uw/market/options-volume` | `get_market_options_volume` |
| `GET` | `/api/v1/uw/market/sector-stats` | `get_sector_stats` |
| `GET` | `/api/v1/uw/market/sector/{sector}/tide` | `get_sector_tide` |
| `GET` | `/api/v1/uw/market/spike` | `get_market_spike` |
| `GET` | `/api/v1/uw/market/tide` | `get_market_tide` |
| `GET` | `/api/v1/uw/market/top-impact` | `get_top_net_impact` |
| `GET` | `/api/v1/uw/market/{etf}/etf-tide` | `get_market_tide_by_etf` |
| `GET` | `/api/v1/uw/news/headlines` | `get_news_headlines` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/flow` | `get_option_contract_flow` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/historic` | `get_option_contract_historic` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/intraday` | `get_option_contract_intraday` |
| `GET` | `/api/v1/uw/option-contract/{option_symbol}/volume-profile` | `get_option_contract_volume_profile` |
| `GET` | `/api/v1/uw/politicians/people` | `get_politician_people` |
| `GET` | `/api/v1/uw/politicians/recent-trades` | `get_politician_recent_trades` |
| `GET` | `/api/v1/uw/politicians/{politician_id}/portfolios` | `get_politician_portfolios` |
| `GET` | `/api/v1/uw/politicians/{symbol}/holders` | `get_politician_holders` |
| `GET` | `/api/v1/uw/screener/analysts` | `get_analyst_ratings` |
| `GET` | `/api/v1/uw/screener/contracts` | `get_options_screener` |
| `GET` | `/api/v1/uw/screener/option-contracts` | `get_screener_option_contracts` |
| `GET` | `/api/v1/uw/screener/options` | `get_screener_options` |
| `GET` | `/api/v1/uw/screener/stocks` | `get_screener_stocks` |
| `GET` | `/api/v1/uw/screener/stocks` | `get_screener_stocks_extended` |
| `GET` | `/api/v1/uw/seasonality/market` | `get_market_seasonality` |
| `GET` | `/api/v1/uw/seasonality/monthly-top-performers/{month}` | `get_monthly_top_performers` |
| `GET` | `/api/v1/uw/seasonality/{symbol}` | `get_ticker_seasonality` |
| `GET` | `/api/v1/uw/seasonality/{symbol}/price-changes-by-month` | `get_price_changes_by_month_year` |
| `GET` | `/api/v1/uw/sectors/{sector}/tickers` | `get_sector_tickers` |
| `GET` | `/api/v1/uw/shorts/{symbol}/data` | `get_shorts_data` |
| `GET` | `/api/v1/uw/shorts/{symbol}/interest-float` | `get_short_interest_float` |
| `GET` | `/api/v1/uw/shorts/{symbol}/volumes-by-exchange` | `get_short_volumes_by_exchange` |
| `GET` | `/api/v1/uw/stock/{symbol}/atm-options` | `get_atm_option_contracts` |
| `GET` | `/api/v1/uw/stock/{symbol}/candles` | `get_stock_candles` |
| `GET` | `/api/v1/uw/stock/{symbol}/daily-expiry-breakdown` | `get_daily_expiry_breakdown` |
| `GET` | `/api/v1/uw/stock/{symbol}/flow-per-strike-intraday` | `get_flow_per_strike_intraday` |
| `GET` | `/api/v1/uw/stock/{symbol}/flow-recent` | `get_flow_recent` |
| `GET` | `/api/v1/uw/stock/{symbol}/greek-exposure-by-strike-expiry/{expiry}` | `get_greek_exposure_by_strike_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/greek-flow-by-expiry/{expiry}` | `get_greek_flow_by_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/greeks-by-strike/{expiry}` | `get_greeks_by_strike_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/info` | `get_stock_info` |
| `GET` | `/api/v1/uw/stock/{symbol}/insider-trades` | `get_stock_insider_trades` |
| `GET` | `/api/v1/uw/stock/{symbol}/oi-per-expiry` | `get_oi_per_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/oi-per-strike` | `get_oi_per_strike` |
| `GET` | `/api/v1/uw/stock/{symbol}/option-chains` | `get_stock_option_chains` |
| `GET` | `/api/v1/uw/stock/{symbol}/option-contracts` | `get_stock_option_contracts` |
| `GET` | `/api/v1/uw/stock/{symbol}/option-volume-by-price` | `get_option_volume_by_price_level` |
| `GET` | `/api/v1/uw/stock/{symbol}/options-volume` | `get_options_volume` |
| `GET` | `/api/v1/uw/stock/{symbol}/risk-reversal-skew/{expiry}` | `get_risk_reversal_skew` |
| `GET` | `/api/v1/uw/stock/{symbol}/spot-exposures` | `get_spot_exposures` |
| `GET` | `/api/v1/uw/stock/{symbol}/spot-exposures-by-expiry-strike/{expiry}` | `get_spot_exposures_by_expiry_strike` |
| `GET` | `/api/v1/uw/stock/{symbol}/state` | `get_stock_state` |
| `GET` | `/api/v1/uw/stock/{symbol}/volume-oi-by-expiry` | `get_volume_oi_by_expiry` |
| `GET` | `/api/v1/uw/stock/{symbol}/volume-price-levels` | `get_stock_volume_price_levels` |
| `GET` | `/api/v1/uw/trades/full-tape/{date}` | `get_full_tape` |
| `GET` | `/api/v1/uw/{symbol}/flow-expiry` | `get_flow_per_expiry` |
| `GET` | `/api/v1/uw/{symbol}/flow-strike` | `get_flow_per_strike` |
| `GET` | `/api/v1/uw/{symbol}/ftds` | `get_ftds` |
| `GET` | `/api/v1/uw/{symbol}/greek-flow` | `get_greek_flow` |
| `GET` | `/api/v1/uw/{symbol}/greek-flow-expiry` | `get_greek_flow_expiry` |
| `GET` | `/api/v1/uw/{symbol}/interpolated-iv` | `get_interpolated_iv` |
| `GET` | `/api/v1/uw/{symbol}/iv-rank` | `get_iv_rank` |
| `GET` | `/api/v1/uw/{symbol}/iv-surface` | `get_iv_surface` |
| `GET` | `/api/v1/uw/{symbol}/iv-term-structure` | `get_iv_term_structure` |
| `GET` | `/api/v1/uw/{symbol}/max-pain` | `get_max_pain` |
| `GET` | `/api/v1/uw/{symbol}/net-premium` | `get_net_premium` |
| `GET` | `/api/v1/uw/{symbol}/nope` | `get_nope` |
| `GET` | `/api/v1/uw/{symbol}/oi-change` | `get_oi_change` |
| `GET` | `/api/v1/uw/{symbol}/option-volume` | `get_historic_option_volume` |
| `GET` | `/api/v1/uw/{symbol}/pc-ratio` | `get_put_call_ratio` |
| `GET` | `/api/v1/uw/{symbol}/realized-vol` | `get_realized_vol` |
| `GET` | `/api/v1/uw/{symbol}/short-interest` | `get_short_interest` |
| `GET` | `/api/v1/uw/{symbol}/short-volume` | `get_short_volume` |
| `GET` | `/api/v1/uw/{symbol}/spot-exposures` | `get_spot_exposures_by_strike` |
| `GET` | `/api/v1/uw/{symbol}/vol-stats` | `get_vol_stats` |
| `GET` | `/api/v1/uw/{symbol}/volume-levels` | `get_option_volume_levels` |

## `finnhub`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/finnhub/bars/{symbol}` | `get_bars` |
| `GET` | `/api/v1/finnhub/congress-trading` | `get_congress_trading` |
| `GET` | `/api/v1/finnhub/crypto/candles/{symbol}` | `get_crypto_candles` |
| `GET` | `/api/v1/finnhub/crypto/exchanges` | `get_crypto_exchanges` |
| `GET` | `/api/v1/finnhub/crypto/symbols` | `get_crypto_symbols` |
| `GET` | `/api/v1/finnhub/crypto/{symbol}/profile` | `get_crypto_profile` |
| `GET` | `/api/v1/finnhub/earnings` | `get_earnings_calendar` |
| `GET` | `/api/v1/finnhub/estimates/ebit/{symbol}` | `get_ebit_estimates` |
| `GET` | `/api/v1/finnhub/estimates/ebitda/{symbol}` | `get_ebitda_estimates` |
| `GET` | `/api/v1/finnhub/estimates/eps/{symbol}` | `get_eps_estimates` |
| `GET` | `/api/v1/finnhub/estimates/revenue/{symbol}` | `get_revenue_estimates` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/country` | `get_etf_country` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/holdings` | `get_etf_holdings` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/profile` | `get_etf_profile` |
| `GET` | `/api/v1/finnhub/etf/{symbol}/sector` | `get_etf_sector` |
| `GET` | `/api/v1/finnhub/executives/{symbol}` | `get_executives` |
| `GET` | `/api/v1/finnhub/fda-calendar` | `get_fda_calendar` |
| `GET` | `/api/v1/finnhub/financials/{symbol}` | `get_financials` |
| `GET` | `/api/v1/finnhub/forex/candles/{symbol}` | `get_forex_candles` |
| `GET` | `/api/v1/finnhub/forex/exchanges` | `get_forex_exchanges` |
| `GET` | `/api/v1/finnhub/forex/rates` | `get_forex_rates` |
| `GET` | `/api/v1/finnhub/forex/symbols` | `get_forex_symbols` |
| `GET` | `/api/v1/finnhub/fund-ownership/{symbol}` | `get_fund_ownership` |
| `GET` | `/api/v1/finnhub/index/{symbol}/constituents` | `get_index_constituents` |
| `GET` | `/api/v1/finnhub/index/{symbol}/historical` | `get_index_historical` |
| `GET` | `/api/v1/finnhub/insider-sentiment/{symbol}` | `get_insider_sentiment` |
| `GET` | `/api/v1/finnhub/insider-transactions/{symbol}` | `get_insider_transactions` |
| `GET` | `/api/v1/finnhub/lobbying/{symbol}` | `get_lobbying` |
| `GET` | `/api/v1/finnhub/metrics/{symbol}` | `get_metrics` |
| `GET` | `/api/v1/finnhub/mutual-fund/{symbol}/holdings` | `get_mutual_fund_holdings` |
| `GET` | `/api/v1/finnhub/mutual-fund/{symbol}/profile` | `get_mutual_fund_profile` |
| `GET` | `/api/v1/finnhub/mutual-fund/{symbol}/sector` | `get_mutual_fund_sector` |
| `GET` | `/api/v1/finnhub/news/market/{category}` | `get_market_news` |
| `GET` | `/api/v1/finnhub/news/{symbol}` | `get_company_news` |
| `GET` | `/api/v1/finnhub/ownership/{symbol}` | `get_ownership` |
| `GET` | `/api/v1/finnhub/patterns/{symbol}` | `get_pattern_recognition` |
| `GET` | `/api/v1/finnhub/peers/{symbol}` | `get_peers` |
| `GET` | `/api/v1/finnhub/price-target/{symbol}` | `get_price_target` |
| `GET` | `/api/v1/finnhub/profile/{symbol}` | `get_company_profile` |
| `GET` | `/api/v1/finnhub/quote/{symbol}` | `get_quote` |
| `GET` | `/api/v1/finnhub/recommendations/{symbol}` | `get_recommendations` |
| `GET` | `/api/v1/finnhub/social-sentiment/{symbol}` | `get_social_sentiment` |
| `GET` | `/api/v1/finnhub/support-resistance/{symbol}` | `get_support_resistance` |
| `GET` | `/api/v1/finnhub/upgrade-downgrade/{symbol}` | `get_upgrade_downgrade` |
| `GET` | `/api/v1/finnhub/usa-spending/{symbol}` | `get_usa_spending` |

## `alphavantage`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/alphavantage/balance-sheet/{symbol}` | `get_balance_sheet` |
| `GET` | `/api/v1/alphavantage/calendar/earnings` | `get_earnings_calendar` |
| `GET` | `/api/v1/alphavantage/calendar/ipo` | `get_ipo_calendar` |
| `GET` | `/api/v1/alphavantage/cash-flow/{symbol}` | `get_cash_flow` |
| `GET` | `/api/v1/alphavantage/crypto/daily/{symbol}` | `get_crypto_daily` |
| `GET` | `/api/v1/alphavantage/crypto/rating/{symbol}` | `get_crypto_rating` |
| `GET` | `/api/v1/alphavantage/daily/{symbol}` | `get_daily` |
| `GET` | `/api/v1/alphavantage/earnings/{symbol}` | `get_earnings` |
| `GET` | `/api/v1/alphavantage/economic/{indicator}` | `get_economic_indicator` |
| `GET` | `/api/v1/alphavantage/forex/daily/{from_symbol}/{to_symbol}` | `get_forex_daily` |
| `GET` | `/api/v1/alphavantage/forex/rate/{from_currency}/{to_currency}` | `get_forex_rate` |
| `GET` | `/api/v1/alphavantage/income-statement/{symbol}` | `get_income_statement` |
| `GET` | `/api/v1/alphavantage/indicator/adx/{symbol}` | `get_adx` |
| `GET` | `/api/v1/alphavantage/indicator/atr/{symbol}` | `get_atr` |
| `GET` | `/api/v1/alphavantage/indicator/bbands/{symbol}` | `get_bbands` |
| `GET` | `/api/v1/alphavantage/indicator/cci/{symbol}` | `get_cci` |
| `GET` | `/api/v1/alphavantage/indicator/ema/{symbol}` | `get_ema` |
| `GET` | `/api/v1/alphavantage/indicator/macd/{symbol}` | `get_macd` |
| `GET` | `/api/v1/alphavantage/indicator/obv/{symbol}` | `get_obv` |
| `GET` | `/api/v1/alphavantage/indicator/rsi/{symbol}` | `get_rsi` |
| `GET` | `/api/v1/alphavantage/indicator/sma/{symbol}` | `get_sma` |
| `GET` | `/api/v1/alphavantage/indicator/stoch/{symbol}` | `get_stoch` |
| `GET` | `/api/v1/alphavantage/indicator/{indicator}/{symbol}` | `get_technical_indicator` |
| `GET` | `/api/v1/alphavantage/intraday/{symbol}` | `get_intraday` |
| `GET` | `/api/v1/alphavantage/listing-status` | `get_listing_status` |
| `GET` | `/api/v1/alphavantage/monthly/{symbol}` | `get_monthly` |
| `GET` | `/api/v1/alphavantage/overview/{symbol}` | `get_company_overview` |
| `GET` | `/api/v1/alphavantage/quote/{symbol}` | `get_quote` |
| `GET` | `/api/v1/alphavantage/search` | `search_symbols` |
| `GET` | `/api/v1/alphavantage/weekly/{symbol}` | `get_weekly` |

## `sec`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/sec/13f/{cik}` | `get_13f_holdings` |
| `GET` | `/api/v1/sec/company/ticker/{ticker}` | `get_company_by_ticker` |
| `GET` | `/api/v1/sec/company/{cik}` | `get_company_info` |
| `GET` | `/api/v1/sec/concept/{cik}/{concept}` | `get_company_concept` |
| `GET` | `/api/v1/sec/facts/{cik}` | `get_company_facts` |
| `GET` | `/api/v1/sec/filings/{cik}` | `get_filings` |
| `GET` | `/api/v1/sec/filings/{cik}/{form_type}` | `get_filings_by_type` |
| `GET` | `/api/v1/sec/frames/{concept}/{period}` | `get_xbrl_frames` |
| `GET` | `/api/v1/sec/insiders/{cik}` | `get_insider_trades` |
| `GET` | `/api/v1/sec/search` | `search_filings` |

## `yfinance`

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/v1/yf/ticker/{symbol}` | `get_ticker_info` |
| `GET` | `/api/v1/yf/ticker/{symbol}/actions` | `get_actions` |
| `GET` | `/api/v1/yf/ticker/{symbol}/calendar` | `get_calendar` |
| `GET` | `/api/v1/yf/ticker/{symbol}/dividends` | `get_dividends` |
| `GET` | `/api/v1/yf/ticker/{symbol}/earnings` | `get_earnings` |
| `GET` | `/api/v1/yf/ticker/{symbol}/financials` | `get_financials` |
| `GET` | `/api/v1/yf/ticker/{symbol}/history` | `get_history` |
| `GET` | `/api/v1/yf/ticker/{symbol}/holders` | `get_holders` |
| `GET` | `/api/v1/yf/ticker/{symbol}/info` | `get_company_info` |
| `GET` | `/api/v1/yf/ticker/{symbol}/major-holders` | `get_major_holders` |
| `GET` | `/api/v1/yf/ticker/{symbol}/news` | `get_news` |
| `GET` | `/api/v1/yf/ticker/{symbol}/options` | `get_options` |
| `GET` | `/api/v1/yf/ticker/{symbol}/options/{expiration}` | `get_options_chain` |
| `GET` | `/api/v1/yf/ticker/{symbol}/recommendations` | `get_recommendations` |
| `GET` | `/api/v1/yf/ticker/{symbol}/splits` | `get_splits` |
| `GET` | `/api/v1/yf/ticker/{symbol}/sustainability` | `get_sustainability` |

## Regeneration

```bash
python scripts/generate_provider_contract.py
```
