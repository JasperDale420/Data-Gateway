"""UW Flow mixin — flow alerts, darkpool, tide, greek flow, tape, net flow."""

from datetime import datetime
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from gateway.schemas import (
    NormalizedDarkpoolTrade,
    NormalizedFlowAlert,
    NormalizedMarketTide,
)

from ._base import ERR_NOT_INITIALIZED, _or_unset, _safe_int
from .transient import _uw_error_context, is_transient_upstream_error

_ET = ZoneInfo("America/New_York")

from gateway.core.logger import logger


class UWFlowMixin:
    """Mixin providing options flow, darkpool, tide, and greek flow endpoints."""

    async def _get_darkpool_recent_raw(
        self,
        *,
        limit: int,
        offset: int,
    ) -> list[NormalizedDarkpoolTrade]:
        """Fetch darkpool recent trades via raw HTTP when SDK parsing fails."""
        if not self._client:
            return []

        try:
            http_client = self._client.get_httpx_client()
            path = "/api/darkpool/recent"
            params: dict[str, str] = {"limit": str(limit)}
            if offset > 0:
                params["offset"] = str(offset)

            response = await self._call_sync(
                http_client.get,
                path,
                params=params,
            )
            response.raise_for_status()
            payload = self._json_payload(response)
        except Exception as e:
            status_code = self._extract_http_status_code(e)
            context = _uw_error_context(e, provider_endpoint="darkpool_recent_raw", path=path)
            if status_code is not None and status_code >= 500:
                logger.warning("uw_darkpool_recent_upstream_unavailable", error=str(e), **context)
            else:
                logger.error("uw_darkpool_recent_raw_failed", error=str(e), **context)
            raise

        data_items = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(data_items, list):
            data_items = [data_items] if isinstance(data_items, dict) else []

        trades: list[NormalizedDarkpoolTrade] = []
        for item in data_items:
            trade = self._normalize_darkpool_trade(item)
            if trade:
                trades.append(trade)
        return trades

    async def get_flow_alerts(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> list[NormalizedFlowAlert]:
        """Get latest options flow alerts."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import flow

            response, used_local_offset = await self._call_sync_with_optional_offset(
                flow.get_ticker_order_flow.sync,
                kwargs={"client": self._client},
                limit=limit,
                offset=offset,
            )

            alerts = []
            data_items = []
            if response and hasattr(response, "data") and response.data:
                data_items = response.data
            elif response and hasattr(response, "additional_properties") and response.additional_properties:
                data_items = response.additional_properties.get("data", [])
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            for item in data_items:
                alert = self._normalize_flow_alert(item)
                if alert:
                    alerts.append(alert)

            logger.info("uw_flow_alerts_fetched", count=len(alerts))
            return alerts

        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning(
                    "uw_flow_alerts_failed",
                    error=str(e),
                    **_uw_error_context(e, provider_endpoint="flow_alerts"),
                )
            else:
                logger.error(
                    "uw_flow_alerts_failed",
                    error=str(e),
                    exc_info=True,
                    **_uw_error_context(e, provider_endpoint="flow_alerts"),
                )
            raise

    async def get_ticker_flow(
        self,
        symbol: str,
        date_str: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[NormalizedFlowAlert]:
        """Get flow data for a specific ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import flow

            # SDK uses sync method with ticker_symbol parameter
            kwargs: dict[str, Any] = {
                "client": self._client,
                "ticker_symbol": symbol.upper(),
            }
            if date_str:
                kwargs["date"] = date_str

            try:
                response, used_local_offset = await self._call_sync_with_optional_offset(
                    flow.get_ticker_order_flow.sync,
                    kwargs=kwargs,
                    limit=limit,
                    offset=offset,
                )
            except TypeError:
                kwargs.pop("date", None)
                response, used_local_offset = await self._call_sync_with_optional_offset(
                    flow.get_ticker_order_flow.sync,
                    kwargs=kwargs,
                    limit=limit,
                    offset=offset,
                )

            alerts = []
            # Data is in additional_properties['data'], not response.data
            data_items = []
            if response is not None and hasattr(response, "additional_properties") and response.additional_properties:
                data_items = response.additional_properties.get("data", [])
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            for item in data_items:
                alert = self._normalize_flow_alert(item)
                if alert:
                    alerts.append(alert)

            logger.info("uw_ticker_flow_fetched", symbol=symbol, count=len(alerts))
            return alerts

        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning(
                    "uw_ticker_flow_failed",
                    error=str(e),
                    **_uw_error_context(e, provider_endpoint="ticker_flow", symbol=symbol),
                )
            else:
                logger.error(
                    "uw_ticker_flow_failed",
                    error=str(e),
                    exc_info=True,
                    **_uw_error_context(e, provider_endpoint="ticker_flow", symbol=symbol),
                )
            raise

    async def get_darkpool_recent(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> list[NormalizedDarkpoolTrade]:
        """Get recent darkpool trades."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import darkpool

            response, used_local_offset = await self._call_sync_with_optional_offset(
                darkpool.get_trades_by_date.sync,
                kwargs={"client": self._client},
                limit=limit,
                offset=offset,
            )

            trades = []
            # The SDK puts parsed trades on DarkpoolTradeResponse.data; _extract_data
            # reads both .data and additional_properties['data']. Reading only
            # additional_properties here returned [] on every call, so trades were
            # captured solely by the raw-HTTP fallback (which fires on SDK exceptions).
            data_items = self._extract_data(response)
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            for item in data_items:
                trade = self._normalize_darkpool_trade(item)
                if trade:
                    trades.append(trade)

            logger.info("uw_darkpool_recent_fetched", count=len(trades))
            return trades

        except Exception as e:
            logger.debug("uw_darkpool_recent_sdk_failed", error=str(e))
            fallback_trades = await self._get_darkpool_recent_raw(limit=limit, offset=offset)
            if fallback_trades:
                logger.info("uw_darkpool_recent_fetched", count=len(fallback_trades), source="raw_http")
            return fallback_trades

    async def get_darkpool_ticker(
        self,
        symbol: str,
        date_str: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[NormalizedDarkpoolTrade]:
        """Get darkpool trades for a specific ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import darkpool

            kwargs: dict[str, Any] = {"client": self._client, "ticker": symbol.upper()}
            if date_str:
                kwargs["date"] = date_str

            try:
                response, used_local_offset = await self._call_sync_with_optional_offset(
                    darkpool.get_trades_by_ticker.sync,
                    kwargs=kwargs,
                    limit=limit,
                    offset=offset,
                )
            except TypeError:
                kwargs.pop("date", None)
                response, used_local_offset = await self._call_sync_with_optional_offset(
                    darkpool.get_trades_by_ticker.sync,
                    kwargs=kwargs,
                    limit=limit,
                    offset=offset,
                )

            # _extract_data handles both response shapes: legacy additional_properties["data"]
            # and the SDK's typed DarkpoolTradeResponse carrying rows in .data (where
            # additional_properties is empty — the old manual parse returned 0 rows forever).
            data_items = self._extract_data(response)
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            trades = []
            for item in data_items:
                trade = self._normalize_darkpool_trade(item)
                if trade:
                    trades.append(trade)

            logger.info("uw_darkpool_ticker_fetched", symbol=symbol, count=len(trades))
            return trades

        except Exception as e:
            logger.error("uw_darkpool_ticker_failed", symbol=symbol, error=str(e))
            raise

    async def get_market_tide(
        self,
        date_str: str | None = None,
    ) -> list[NormalizedMarketTide]:
        """Get market tide/sentiment data."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import market

            kwargs = {}
            if date_str:
                kwargs["date"] = date_str
            response = await self._call_sync(
                market.get_market_tide.sync,
                client=self._client,
                **kwargs,
            )

            tides = []
            for item in self._extract_data(response):
                tide = self._normalize_market_tide(item)
                if tide:
                    tides.append(tide)

            logger.info("uw_market_tide_fetched", count=len(tides))
            return tides

        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning(
                    "uw_market_tide_failed",
                    error=str(e),
                    **_uw_error_context(e, provider_endpoint="market_tide"),
                )
            else:
                logger.error(
                    "uw_market_tide_failed",
                    error=str(e),
                    exc_info=True,
                    **_uw_error_context(e, provider_endpoint="market_tide"),
                )
            raise

    async def get_group_greek_flow(self, flow_group: str, date_str: str | None = None) -> list[dict]:
        """Get raw Group Flow Greek flow data for Heber ingestion.

        Returns per-minute delta/vega flow for a flow group (e.g., 'airline', 'tech').
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api import group_flow

        try:
            response = await self._call_sync(
                group_flow.get_greek_flow.sync,
                flow_group,
                client=self._client,
                date=_or_unset(date_str),
            )
            data = self._extract_data(response)
            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    {
                        "flow_group": get("flow_group"),
                        "timestamp": get("timestamp"),
                        "dir_delta_flow": get("dir_delta_flow"),
                        "dir_vega_flow": get("dir_vega_flow"),
                        "total_delta_flow": get("total_delta_flow"),
                        "total_vega_flow": get("total_vega_flow"),
                        "otm_dir_delta_flow": get("otm_dir_delta_flow"),
                        "otm_dir_vega_flow": get("otm_dir_vega_flow"),
                        "otm_total_delta_flow": get("otm_total_delta_flow"),
                        "otm_total_vega_flow": get("otm_total_vega_flow"),
                        "net_call_premium": get("net_call_premium"),
                        "net_put_premium": get("net_put_premium"),
                        "net_call_volume": get("net_call_volume"),
                        "net_put_volume": get("net_put_volume"),
                        "volume": get("volume"),
                        "transactions": get("transactions"),
                    }
                )
            logger.info("uw_group_greek_flow_fetched", flow_group=flow_group, count=len(results))
            return results
        except Exception as e:
            logger.error("uw_group_greek_flow_failed", flow_group=flow_group, error=str(e))
            raise

    async def get_group_greek_flow_by_expiry(
        self, flow_group: str, expiry: str, date_str: str | None = None
    ) -> list[dict]:
        """Get raw Group Flow Greek flow by expiry for Heber ingestion.

        Returns per-minute-per-expiry delta/vega flow for a flow group.
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api import group_flow

        try:
            response = await self._call_sync(
                group_flow.get_greek_flow_expiry.sync,
                flow_group,
                expiry,
                client=self._client,
                date=_or_unset(date_str),
            )
            data = self._extract_data(response)
            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    {
                        "flow_group": get("flow_group"),
                        "timestamp": get("timestamp"),
                        "expiry": get("expiry"),
                        "dir_delta_flow": get("dir_delta_flow"),
                        "dir_vega_flow": get("dir_vega_flow"),
                        "total_delta_flow": get("total_delta_flow"),
                        "total_vega_flow": get("total_vega_flow"),
                        "otm_dir_delta_flow": get("otm_dir_delta_flow"),
                        "otm_dir_vega_flow": get("otm_dir_vega_flow"),
                        "otm_total_delta_flow": get("otm_total_delta_flow"),
                        "otm_total_vega_flow": get("otm_total_vega_flow"),
                        "net_call_premium": get("net_call_premium"),
                        "net_put_premium": get("net_put_premium"),
                        "net_call_volume": get("net_call_volume"),
                        "net_put_volume": get("net_put_volume"),
                        "volume": get("volume"),
                        "transactions": get("transactions"),
                    }
                )
            logger.info(
                "uw_group_greek_flow_by_expiry_fetched",
                flow_group=flow_group,
                expiry=expiry,
                count=len(results),
            )
            return results
        except Exception as e:
            logger.error(
                "uw_group_greek_flow_by_expiry_failed",
                flow_group=flow_group,
                expiry=expiry,
                error=str(e),
            )
            raise

    async def get_off_lit_levels(self, symbol: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get dark pool volume per price level.

        Args:
            symbol: Stock ticker symbol
            date_str: Date in YYYY-MM-DD format (defaults to today)
        """
        from datetime import datetime

        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            # Default date to today (ET) if not provided
            if not date_str:
                date_str = datetime.now(_ET).strftime("%Y-%m-%d")

            response = await self._call_sync(
                stock.get_stock_volume_price_levels.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=date_str,
            )
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "price": float(get("price") or 0) if get("price") else None,
                        "volume": _safe_int(get("volume")) if get("volume") else None,
                        "lit_volume": _safe_int(get("lit_volume")) if get("lit_volume") else None,
                        "off_lit_volume": (
                            _safe_int(get("off_lit_volume") or get("dark_volume"))
                            if get("off_lit_volume") or get("dark_volume")
                            else None
                        ),
                    }
                )

            logger.info("uw_off_lit_levels_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_off_lit_levels_failed", symbol=symbol, error=str(e))
            raise

    async def get_flow_per_strike(self, symbol: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get flow data aggregated by strike price."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            # Default to today (ET) if not provided
            if not date_str:
                date_str = datetime.now(_ET).strftime("%Y-%m-%d")

            response = await self._call_sync(
                stock.get_flow_per_strike_intraday.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=date_str,
            )
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "strike": float(get("strike") or 0) if get("strike") else None,
                        "call_premium": (float(get("call_premium") or 0) if get("call_premium") else None),
                        "put_premium": (float(get("put_premium") or 0) if get("put_premium") else None),
                        "call_volume": _safe_int(get("call_volume")) if get("call_volume") else None,
                        "put_volume": _safe_int(get("put_volume")) if get("put_volume") else None,
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                    }
                )

            logger.info("uw_flow_per_strike_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_flow_per_strike_failed", symbol=symbol, error=str(e))
            raise

    async def get_flow_per_expiry(self, symbol: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get flow data aggregated by expiration date."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            # Default to today (ET) if not provided
            if not date_str:
                date_str = datetime.now(_ET).strftime("%Y-%m-%d")

            response = await self._call_sync(
                stock.get_oi_per_expiry.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=date_str,
            )
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "expiry": get("expiry") or get("expiration"),
                        "call_premium": (float(get("call_premium") or 0) if get("call_premium") else None),
                        "put_premium": (float(get("put_premium") or 0) if get("put_premium") else None),
                        "call_volume": _safe_int(get("call_volume")) if get("call_volume") else None,
                        "put_volume": _safe_int(get("put_volume")) if get("put_volume") else None,
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                    }
                )

            logger.info("uw_flow_per_expiry_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_flow_per_expiry_failed", symbol=symbol, error=str(e))
            raise

    async def get_greek_flow(self, symbol: str) -> list[dict[str, Any]]:
        """Get greek flow data for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_greek_flow.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "timestamp": str(get("timestamp") or get("date") or ""),
                        "gamma_flow": float(get("gamma_flow") or 0) if get("gamma_flow") else None,
                        "delta_flow": float(get("delta_flow") or 0) if get("delta_flow") else None,
                        "vanna_flow": float(get("vanna_flow") or 0) if get("vanna_flow") else None,
                        "charm_flow": float(get("charm_flow") or 0) if get("charm_flow") else None,
                    }
                )

            logger.info("uw_greek_flow_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_greek_flow_failed", symbol=symbol, error=str(e))
            raise

    async def get_net_flow_expiry(self) -> list[dict[str, Any]]:
        """Get net premium flow by expiration category."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(market.get_net_flow_by_expiry.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "expiry_category": get("expiry_category") or get("category"),
                        "call_premium": (float(get("call_premium") or 0) if get("call_premium") else None),
                        "put_premium": (float(get("put_premium") or 0) if get("put_premium") else None),
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                        "bullish_premium": (float(get("bullish_premium") or 0) if get("bullish_premium") else None),
                        "bearish_premium": (float(get("bearish_premium") or 0) if get("bearish_premium") else None),
                    }
                )

            logger.info("uw_net_flow_expiry_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_net_flow_expiry_failed", error=str(e))
            raise

    def _parse_sector_tide_items(self, items: list[Any], sector: str) -> list[dict]:
        """Parse raw sector tide data items into normalized dicts."""
        tides = []
        for item in items:
            get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)

            net_call = float(get("net_call_premium") or 0)
            net_put = float(get("net_put_premium") or 0)

            if net_call > abs(net_put):
                sentiment = "bullish"
            elif abs(net_put) > net_call:
                sentiment = "bearish"
            else:
                sentiment = "neutral"

            tides.append(
                {
                    "tide_type": "sector",
                    "sector": sector,
                    "date": get("date"),
                    "timestamp": get("timestamp"),
                    "net_call_premium": net_call,
                    "net_put_premium": net_put,
                    "net_volume": _safe_int(get("net_volume")) if get("net_volume") else None,
                    "sentiment": sentiment,
                }
            )
        return tides

    async def _get_sector_tide_raw(self, sector: str, date_str: str | None = None) -> list[dict]:
        """Fetch sector tide via raw HTTP when SDK version lacks get_sector_tide."""
        if not self._client:
            return []

        try:
            http_client = self._client.get_httpx_client()
            path = f"/api/market/{quote(sector, safe='')}/sector-tide"
            params: dict[str, str] = {}
            if date_str:
                params["date"] = date_str

            response = await self._call_sync(
                http_client.get,
                path,
                params=params,
            )
            response.raise_for_status()
            payload = self._json_payload(response)
        except Exception as e:
            logger.error(
                "uw_sector_tide_raw_failed",
                sector=sector,
                error=str(e),
                **_uw_error_context(e, provider_endpoint="sector_tide_raw", path=path),
            )
            raise

        data_items = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(data_items, list):
            data_items = [data_items] if isinstance(data_items, dict) else []

        return self._parse_sector_tide_items(data_items, sector)

    async def get_sector_tide(self, sector: str, date_str: str | None = None) -> list[dict]:
        """Get market tide for a specific sector.

        Returns data in same format as market/tide (Daily Market Tide schema).
        Falls back to raw HTTP if the installed SDK version lacks get_sector_tide
        (requires unusualwhales-python-client >= 5.1).
        """
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            if not hasattr(market, "get_sector_tide"):
                logger.debug(
                    "uw_sector_tide_sdk_missing",
                    detail="unusualwhales-python-client < 5.1 — falling back to raw HTTP",
                )
                return await self._get_sector_tide_raw(sector, date_str)

            kwargs = {"sector": sector}
            if date_str:
                kwargs["date"] = date_str
            response = await self._call_sync(market.get_sector_tide.sync, client=self._client, **kwargs)

            tides = self._parse_sector_tide_items(list(self._extract_data(response)), sector)

            logger.info("uw_sector_tide_fetched", sector=sector, count=len(tides))
            return tides

        except Exception as e:
            logger.error("uw_sector_tide_failed", sector=sector, error=str(e))
            raise

    async def get_top_net_impact(self, limit: int = 20) -> dict[str, list[dict[str, Any]]]:
        """Get top tickers by net premium (bullish and bearish)."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(market.get_top_net_premium.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                return {"bullish": [], "bearish": []}

            bullish = []
            bearish = []

            for item in data.get("bullish", [])[:limit]:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                bullish.append(
                    {
                        "symbol": get("ticker") or get("symbol"),
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                        "call_premium": (float(get("call_premium") or 0) if get("call_premium") else None),
                    }
                )

            for item in data.get("bearish", [])[:limit]:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                bearish.append(
                    {
                        "symbol": get("ticker") or get("symbol"),
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                        "put_premium": (float(get("put_premium") or 0) if get("put_premium") else None),
                    }
                )

            logger.info("uw_top_net_impact_fetched", bullish=len(bullish), bearish=len(bearish))
            return {"bullish": bullish, "bearish": bearish}

        except Exception as e:
            logger.error("uw_top_net_impact_failed", error=str(e))
            raise

    def _parse_etf_tide_items(self, items: list[Any], symbol: str) -> list[dict]:
        """Parse raw ETF tide data items into normalized dicts."""
        tides = []
        for item in items:
            get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)

            net_call = float(get("net_call_premium") or 0)
            net_put = float(get("net_put_premium") or 0)

            if net_call > abs(net_put):
                sentiment = "bullish"
            elif abs(net_put) > net_call:
                sentiment = "bearish"
            else:
                sentiment = "neutral"

            tides.append(
                {
                    "tide_type": "etf",
                    "ticker": symbol.upper(),
                    "date": get("date"),
                    "timestamp": get("timestamp"),
                    "net_call_premium": net_call,
                    "net_put_premium": net_put,
                    "net_volume": _safe_int(get("net_volume")) if get("net_volume") else None,
                    "sentiment": sentiment,
                }
            )
        return tides

    async def _get_etf_tide_raw(self, symbol: str, date_str: str | None = None) -> list[dict]:
        """Fetch ETF tide via raw HTTP when SDK version lacks get_etf_tide."""
        if not self._client:
            return []

        try:
            http_client = self._client.get_httpx_client()
            path = f"/api/etf/{quote(symbol.upper(), safe='')}/tide"
            params: dict[str, str] = {}
            if date_str:
                params["date"] = date_str

            response = await self._call_sync(
                http_client.get,
                path,
                params=params,
            )
            response.raise_for_status()
            payload = self._json_payload(response)
        except Exception as e:
            logger.error(
                "uw_etf_tide_raw_failed",
                error=str(e),
                **_uw_error_context(e, provider_endpoint="etf_tide_raw", path=path, symbol=symbol),
            )
            raise

        data_items = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(data_items, list):
            data_items = [data_items] if isinstance(data_items, dict) else []

        return self._parse_etf_tide_items(data_items, symbol)

    async def get_etf_tide(self, symbol: str, date_str: str | None = None) -> list[dict]:
        """Get ETF-level tide data (premium flow sentiment).

        Returns data in same format as market/tide (Daily Market Tide schema).
        Falls back to raw HTTP if the installed SDK version lacks get_etf_tide.
        """
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            if not hasattr(market, "get_etf_tide"):
                logger.debug(
                    "uw_etf_tide_sdk_missing",
                    detail="unusualwhales-python-client lacks get_etf_tide — falling back to raw HTTP",
                )
                return await self._get_etf_tide_raw(symbol, date_str)

            kwargs = {"ticker": symbol.upper()}
            if date_str:
                kwargs["date"] = date_str
            response = await self._call_sync(market.get_etf_tide.sync, client=self._client, **kwargs)

            tides = self._parse_etf_tide_items(list(self._extract_data(response)), symbol)

            logger.info("uw_etf_tide_fetched", symbol=symbol, count=len(tides))
            return tides

        except Exception as e:
            logger.error("uw_etf_tide_failed", symbol=symbol, error=str(e))
            raise

    async def get_full_tape(self, _date_str: str) -> list[dict[str, Any]]:
        """Get full options trade tape for a date."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        # SDK doesn't have option_trades module - this functionality is not available
        logger.warning("uw_full_tape_not_available", reason="SDK does not include option_trades module")
        return []

    async def get_greek_flow_expiry(self, symbol: str) -> list[dict[str, Any]]:
        """Get greek flow data aggregated by expiration."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_greek_flow_expiry.sync, client=self._client, ticker=symbol.upper()
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "expiry": get("expiry") or get("expiration"),
                        "gamma_flow": float(get("gamma_flow") or 0) if get("gamma_flow") else None,
                        "delta_flow": float(get("delta_flow") or 0) if get("delta_flow") else None,
                        "vanna_flow": float(get("vanna_flow") or 0) if get("vanna_flow") else None,
                        "charm_flow": float(get("charm_flow") or 0) if get("charm_flow") else None,
                    }
                )

            logger.info("uw_greek_flow_expiry_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_greek_flow_expiry_failed", symbol=symbol, error=str(e))
            raise

    async def get_market_tide_by_etf(self, etf: str) -> list[dict]:
        """Get market tide data for a specific ETF."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_market_tide_by_etf

        try:
            response = await self._call_sync(get_market_tide_by_etf.sync, etf.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_market_tide_etf_failed", error=str(e), etf=etf)
            raise

    async def get_flow_per_strike_intraday(self, symbol: str) -> list[dict]:
        """Get flow per strike for intraday data."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_flow_per_strike_intraday

        try:
            response = await self._call_sync(get_flow_per_strike_intraday.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_flow_per_strike_intraday_failed", error=str(e), symbol=symbol)
            raise

    async def get_greek_flow_by_expiry(self, symbol: str, expiry: str) -> list[dict]:
        """Get greek flow by expiry."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_greek_flow_expiry

        try:
            response = await self._call_sync(get_greek_flow_expiry.sync, symbol.upper(), expiry, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_greek_flow_by_expiry_failed", error=str(e), symbol=symbol, expiry=expiry)
            raise

    async def get_contract_flow(self, option_symbol: str) -> list[dict]:
        """Get flow for a specific contract."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.flow import get_contract_flow

        try:
            response = await self._call_sync(get_contract_flow.sync, option_symbol, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_contract_flow_failed", error=str(e), option_symbol=option_symbol)
            raise

    async def get_full_tape_simple(self, limit: int = 100) -> list[dict]:
        """Get full options tape."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.flow import get_full_tape

        try:
            response = await self._call_sync(get_full_tape.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_full_tape_failed", error=str(e))
            raise

    async def get_flow_recent(self, symbol: str) -> list[dict]:
        """Get recent flow for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_flow_recent

        try:
            response = await self._call_sync(get_flow_recent.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_flow_recent_failed", error=str(e), symbol=symbol)
            raise
