"""UW Institutional mixin — institutions, insiders, congress, ETF, politician methods."""

from decimal import Decimal
from typing import Any

import structlog

from ._base import ERR_NOT_INITIALIZED, _safe_int

logger = structlog.get_logger()


class UWInstitutionalMixin:
    """Institutional, insider, congress, ETF holdings, and politician endpoints."""

    # ─────────────────────────────────────────────────────────────────
    # Phase 1: Core Institutional / Congress / Insiders
    # ─────────────────────────────────────────────────────────────────

    async def get_institutions(
        self,
        symbol: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict]:
        """Get 13F institutional holdings for a ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import institution

            kwargs: dict[str, Any] = {"client": self._client}
            try:
                response, used_local_offset = await self._call_sync_with_optional_offset(
                    institution.get_ownership.sync,
                    call_args=(symbol.upper(),),
                    kwargs=kwargs,
                    limit=limit,
                    offset=offset,
                )
            except TypeError:
                response = await self._call_sync(
                    institution.get_ownership.sync,
                    symbol.upper(),
                    client=self._client,
                )
                used_local_offset = True

            holdings = []
            data_items = self._extract_data(response)
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            for item in data_items:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                holdings.append(
                    {
                        "institution": get("institution") or get("name"),
                        "shares": get("shares") or get("current_shares"),
                        "value": get("value") or get("market_value"),
                        "change": get("change") or get("shares_change"),
                        "percent_outstanding": get("percent_outstanding"),
                        "report_date": get("report_date") or get("filing_date"),
                    }
                )

            logger.info("uw_institutions_fetched", symbol=symbol, count=len(holdings))
            return holdings

        except Exception as e:
            logger.error("uw_institutions_failed", symbol=symbol, error=str(e))
            raise

    async def get_congress_trades(self, symbol: str | None = None, limit: int = 100) -> list[dict]:
        """Get congressional trades, optionally filtered by ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import congress

            response = await self._call_sync(congress.get_trades.sync, client=self._client, limit=limit)

            trades = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                ticker = get("ticker") or get("symbol") or ""

                # Filter by symbol if provided
                if symbol and ticker.upper() != symbol.upper():
                    continue

                trades.append(
                    {
                        # Match UW Senate Stock schema exactly
                        "ticker": ticker,
                        "name": get("name"),
                        "txn_type": get("txn_type") or get("transaction_type"),
                        "amounts": get("amounts") or get("amount"),
                        "transaction_date": get("transaction_date"),
                        "filed_at_date": get("filed_at_date") or get("disclosure_date"),
                        "member_type": get("member_type") or get("chamber"),
                        "politician_id": get("politician_id"),
                        "reporter": get("reporter"),
                        "notes": get("notes"),
                        "issuer": get("issuer"),
                        "is_active": get("is_active"),
                    }
                )

            logger.info("uw_congress_fetched", symbol=symbol, count=len(trades))
            return trades

        except Exception as e:
            logger.error("uw_congress_failed", symbol=symbol, error=str(e))
            raise

    async def get_insiders(self, symbol: str | None = None, limit: int = 100) -> list[dict]:
        """Get insider transactions, optionally filtered by ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(
                market.get_insider_trades.sync,
                client=self._client,
                limit=limit,
            )

            transactions = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)

                ticker = get("ticker") or ""

                # Filter by symbol if provided
                if symbol and ticker.upper() != symbol.upper():
                    continue

                # Match UW Insider Trade Agg schema exactly
                transactions.append(
                    {
                        "ticker": ticker,
                        "owner_name": get("owner_name"),
                        "officer_title": get("officer_title"),
                        "transaction_code": get("transaction_code"),
                        "amount": get("amount"),
                        "price": get("price"),
                        "transaction_date": get("transaction_date"),
                        "filing_date": get("filing_date"),
                        "formtype": get("formtype"),
                        "id": get("id"),
                        "is_10b5_1": get("is_10b5_1"),
                        "is_director": get("is_director"),
                        "is_officer": get("is_officer"),
                        "is_ten_percent_owner": get("is_ten_percent_owner"),
                        "sector": get("sector"),
                        "marketcap": get("marketcap"),
                        "is_s_p_500": get("is_s_p_500"),
                        "next_earnings_date": get("next_earnings_date"),
                        "shares_owned_before": get("shares_owned_before"),
                        "shares_owned_after": get("shares_owned_after"),
                        "security_title": get("security_title"),
                        "natureofownership": get("natureofownership"),
                        "transactions": get("transactions"),
                    }
                )

            logger.info("uw_insiders_fetched", symbol=symbol, count=len(transactions))
            return transactions

        except Exception as e:
            logger.error("uw_insiders_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # ETF Holdings / Exposure / Flows (normalized)
    # ─────────────────────────────────────────────────────────────────

    async def get_etf_holdings(self, symbol: str) -> list:
        """Get ETF holdings."""
        from gateway.schemas import NormalizedETFHolding

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import etfs as etf

            response = await self._call_sync(
                etf.get_holdings.sync,
                symbol.upper(),
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedETFHolding(
                        etf_symbol=symbol.upper(),
                        holding_symbol=get("ticker") or get("symbol") or "",
                        weight=Decimal(str(get("weight") or get("percentage") or 0)),
                        shares=_safe_int(get("shares")) if get("shares") else None,
                        market_value=(Decimal(str(get("market_value") or 0)) if get("market_value") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_etf_holdings_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_etf_holdings_failed", symbol=symbol, error=str(e))
            raise

    async def get_etf_exposure(self, symbol: str) -> list:
        """Get ETF exposure for a stock (which ETFs hold it)."""
        from gateway.schemas import NormalizedETFHolding

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import etfs as etf

            response = await self._call_sync(
                etf.get_ticker_exposure.sync,
                symbol.upper(),
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedETFHolding(
                        etf_symbol=get("etf") or get("etf_ticker") or "",
                        holding_symbol=symbol.upper(),
                        weight=Decimal(str(get("weight") or get("percentage") or 0)),
                        shares=_safe_int(get("shares")) if get("shares") else None,
                        market_value=(Decimal(str(get("market_value") or 0)) if get("market_value") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_etf_exposure_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_etf_exposure_failed", symbol=symbol, error=str(e))
            raise

    async def get_etf_flows(self, symbol: str) -> list:
        """Get ETF inflow/outflow data."""
        from gateway.schemas import NormalizedETFFlow

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import etfs as etf

            response = await self._call_sync(
                etf.get_in_outflow.sync,
                client=self._client,
                ticker=symbol.upper(),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                inflow = Decimal(str(get("inflow") or 0))
                outflow = Decimal(str(get("outflow") or 0))
                results.append(
                    NormalizedETFFlow(
                        symbol=symbol.upper(),
                        date=str(get("date") or ""),
                        inflow=inflow,
                        outflow=outflow,
                        net_flow=inflow - outflow,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_etf_flows_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_etf_flows_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Recent Congress Trades (raw dict)
    # ─────────────────────────────────────────────────────────────────

    async def get_recent_congress_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent congressional trades across all tickers."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import congress

            response = await self._call_sync(congress.get_trades.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data[:limit]:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": get("ticker") or get("symbol"),
                        "politician": get("politician") or get("representative"),
                        "party": get("party"),
                        "chamber": get("chamber"),
                        "transaction_type": get("transaction_type") or get("type"),
                        "amount_range": get("amount") or get("amount_range"),
                        "date": str(get("transaction_date") or get("date") or ""),
                        "disclosure_date": str(get("disclosure_date") or ""),
                    }
                )

            logger.info("uw_recent_congress_trades_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_recent_congress_trades_failed", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Politician Portfolios
    # ─────────────────────────────────────────────────────────────────

    async def get_politician_people(self) -> list[dict]:
        """Get all politician names and IDs."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.politician_portfolios import get_people

        try:
            response = await self._call_sync(get_people.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_politician_people_failed", error=str(e))
            raise

    async def get_politician_recent_trades(self, limit: int = 50) -> list[dict]:
        """Get latest transacted trades by congress members."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.politician_portfolios import get_recent_trades

        try:
            response = await self._call_sync(get_recent_trades.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_politician_trades_failed", error=str(e))
            raise

    async def get_politician_portfolios(self, politician_id: str) -> list[dict]:
        """Get all portfolios and holdings for a politician."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.politician_portfolios import get_portfolios

        try:
            response = await self._call_sync(get_portfolios.sync, politician_id, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_politician_portfolios_failed", error=str(e), politician_id=politician_id)
            raise

    async def get_politician_holders(self, symbol: str) -> list[dict]:
        """Get politician portfolio holders for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.politician_portfolios import get_holders

        try:
            response = await self._call_sync(get_holders.sync, client=self._client, ticker=symbol.upper())
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_politician_holders_failed", error=str(e), symbol=symbol)
            raise

    # ─────────────────────────────────────────────────────────────────
    # Market-wide Insider Trades
    # ─────────────────────────────────────────────────────────────────

    async def get_market_insider_trades(self) -> list[dict]:
        """Get market-wide insider trades."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_insider_trades

        try:
            response = await self._call_sync(get_insider_trades.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_market_insider_trades_failed", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Institution Endpoints
    # ─────────────────────────────────────────────────────────────────

    async def get_all_institutions(self, limit: int = 100) -> list[dict]:
        """Get list of all institutions."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.institution import get_institutions

        try:
            response = await self._call_sync(get_institutions.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_institutions_list_failed", error=str(e))
            raise

    async def get_institution_activity(self, institution_id: str) -> list[dict]:
        """Get latest activity by an institution."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.institution import get_activity

        try:
            response = await self._call_sync(get_activity.sync, institution_id, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_institution_activity_failed", error=str(e), institution_id=institution_id)
            raise

    async def get_institution_holdings(self, institution_id: str) -> list[dict]:
        """Get current holdings of an institution."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.institution import get_holdings

        try:
            response = await self._call_sync(get_holdings.sync, institution_id, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_institution_holdings_failed", error=str(e), institution_id=institution_id)
            raise

    async def get_institution_sector_exposure(self, institution_id: str) -> list[dict]:
        """Get sector exposure of an institution."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.institution import get_sector_exposure

        try:
            response = await self._call_sync(get_sector_exposure.sync, institution_id, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_institution_sector_failed", error=str(e), institution_id=institution_id)
            raise

    async def get_institutional_ownership(self, symbol: str) -> list[dict]:
        """Get institutional ownership for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.institution import get_ownership

        try:
            response = await self._call_sync(get_ownership.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_institutional_ownership_failed", error=str(e), symbol=symbol)
            raise

    async def get_latest_institutional_filings(self, limit: int = 50) -> list[dict]:
        """Get latest institutional filings."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.institution import get_latest_filings

        try:
            response = await self._call_sync(get_latest_filings.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_latest_filings_failed", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Insider Endpoints
    # ─────────────────────────────────────────────────────────────────

    async def get_insider_transactions(self, limit: int = 100) -> list[dict]:
        """Get all recent insider transactions."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.insider import get_insider_transactions

        try:
            response = await self._call_sync(get_insider_transactions.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_insider_transactions_failed", error=str(e))
            raise

    async def get_insider_sector_flow(self) -> list[dict]:
        """Get insider trading flow by sector."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.insider import get_insider_sector_flow

        try:
            response = await self._call_sync(get_insider_sector_flow.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_insider_sector_flow_failed", error=str(e))
            raise

    async def get_insider_ticker_flow(self) -> list[dict]:
        """Get insider trading flow by ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.insider import get_insider_ticker_flow

        try:
            response = await self._call_sync(get_insider_ticker_flow.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_insider_ticker_flow_failed", error=str(e))
            raise

    async def get_ticker_insiders(self, symbol: str) -> list[dict]:
        """Get insiders for a specific ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.insider import get_ticker_insiders

        try:
            response = await self._call_sync(get_ticker_insiders.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_ticker_insiders_failed", error=str(e), symbol=symbol)
            raise

    # ─────────────────────────────────────────────────────────────────
    # Phase 5: ETF Endpoints
    # ─────────────────────────────────────────────────────────────────

    async def get_etf_info(self, symbol: str) -> dict | None:
        """Get ETF information."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.etfs import get_info

        try:
            response = await self._call_sync(get_info.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        except Exception as e:
            logger.error("uw_etf_info_failed", error=str(e), symbol=symbol)
            raise

    async def get_etf_inflow_outflow(self, symbol: str) -> list[dict]:
        """Get ETF inflow/outflow data."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.etfs import get_in_outflow

        try:
            response = await self._call_sync(get_in_outflow.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_etf_inflow_outflow_failed", error=str(e), symbol=symbol)
            raise

    async def get_etf_ticker_exposure(self, symbol: str) -> list[dict]:
        """Get ticker exposure within an ETF."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.etfs import get_ticker_exposure

        try:
            response = await self._call_sync(get_ticker_exposure.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_etf_ticker_exposure_failed", error=str(e), symbol=symbol)
            raise

    async def get_etf_country_weights(self, symbol: str) -> list[dict]:
        """Get ETF country weights."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.etfs import get_sector_country_weights

        try:
            response = await self._call_sync(get_sector_country_weights.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_etf_country_weights_failed", error=str(e), symbol=symbol)
            raise

    # ─────────────────────────────────────────────────────────────────
    # Stock Insider Trades
    # ─────────────────────────────────────────────────────────────────

    async def get_stock_insider_trades(self, symbol: str) -> list[dict]:
        """Get insider trades for a specific stock."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_insider_trades

        try:
            response = await self._call_sync(get_insider_trades.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_stock_insider_trades_failed", error=str(e), symbol=symbol)
            raise

    # ─────────────────────────────────────────────────────────────────
    # Congress Module (Final Batch)
    # ─────────────────────────────────────────────────────────────────

    async def get_congress_trader_reports(
        self,
        name: str,
        ticker: str | None = None,
        date_str: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get recent reports by a specific congress member.

        Args:
            name: The politician's name (e.g., 'Nancy Pelosi')
            ticker: Optional, filter by stock symbol
            date_str: Optional, filter by date (YYYY-MM-DD)
            limit: Maximum results (default 100, max 200)

        Returns:
            List of dicts matching UW Senate Stock schema.
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.congress import get_trader

        try:
            response = await self._call_sync(
                get_trader.sync,
                client=self._client,
                name=name,
                ticker=ticker,
                date=date_str,
                limit=limit,
            )
            data = self._extract_data(response)
            trades = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                trades.append(
                    {
                        "ticker": get("ticker") or get("symbol") or "",
                        "name": get("name"),
                        "txn_type": get("txn_type") or get("transaction_type"),
                        "amounts": get("amounts") or get("amount"),
                        "transaction_date": get("transaction_date"),
                        "filed_at_date": get("filed_at_date") or get("disclosure_date"),
                        "member_type": get("member_type") or get("chamber"),
                        "politician_id": get("politician_id"),
                        "reporter": get("reporter"),
                        "notes": get("notes"),
                        "issuer": get("issuer"),
                        "is_active": get("is_active"),
                    }
                )
            logger.info("uw_congress_trader_fetched", name=name, count=len(trades))
            return trades
        except Exception as e:
            logger.error("uw_congress_trader_failed", name=name, error=str(e))
            raise

    async def get_congress_late_reports(self, limit: int = 50) -> list[dict]:
        """Get late congressional trading reports."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.congress import get_late_reports

        try:
            response = await self._call_sync(get_late_reports.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_congress_late_reports_failed", error=str(e))
            raise

    async def get_congress_reports(self, limit: int = 50) -> list[dict]:
        """Get congressional trading reports."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.congress import get_reports

        try:
            response = await self._call_sync(get_reports.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_congress_reports_failed", error=str(e))
            raise
