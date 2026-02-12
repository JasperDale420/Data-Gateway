"""Unusual Whales data provider for flow and darkpool data."""

import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter
from typing import Any

import structlog
from unusualwhales.types import UNSET, Unset

from gateway.core.metrics import (
    dec_provider_sync_call_inflight,
    inc_provider_sync_call_inflight,
    record_provider_sync_call_exec,
    record_provider_sync_call_wait,
)
from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.schemas import (
    NormalizedDarkpoolTrade,
    NormalizedFlowAlert,
    NormalizedIVRank,
    NormalizedMarketTide,
    NormalizedVolatilityStats,
)

logger = structlog.get_logger()

# Error message constants
ERR_NOT_INITIALIZED = "Provider not initialized"
DEFAULT_UW_MAX_INFLIGHT_CALLS = 32

# Timezone constants
TZ_UTC_SUFFIX = "+00:00"


def _or_unset[T](value: T | None) -> T | Unset:
    """Convert None to UNSET for SDK compatibility."""
    return UNSET if value is None else value


class UnusualWhalesProvider(DataProvider):
    """Unusual Whales data provider for options flow and darkpool data."""

    def __init__(self):
        self._client = None
        self._api_key: str = ""
        self._initialized: bool = False
        self._max_inflight_calls: int = DEFAULT_UW_MAX_INFLIGHT_CALLS
        self._call_sync_semaphore: asyncio.Semaphore = asyncio.Semaphore(DEFAULT_UW_MAX_INFLIGHT_CALLS)

    @property
    def name(self) -> str:
        return "unusual_whales"

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=False,
            supports_quotes=False,
            supports_trades=False,
            supports_streaming=False,
            supports_historical=True,
        )

    @property
    def supported_feeds(self) -> list[str]:
        return ["flow", "darkpool", "institutions", "congress", "insiders"]

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize the Unusual Whales client."""
        api_key_env = config.get("api_key_env", "UNUSUAL_WHALES_API_KEY")
        self._api_key = os.environ.get(api_key_env, "")
        raw_max_inflight = config.get("max_inflight_calls", DEFAULT_UW_MAX_INFLIGHT_CALLS)
        try:
            self._max_inflight_calls = max(1, int(raw_max_inflight))
        except (TypeError, ValueError):
            logger.warning(
                "uw_invalid_max_inflight_calls",
                value=raw_max_inflight,
                default=DEFAULT_UW_MAX_INFLIGHT_CALLS,
            )
            self._max_inflight_calls = DEFAULT_UW_MAX_INFLIGHT_CALLS
        self._call_sync_semaphore = asyncio.Semaphore(self._max_inflight_calls)

        if not self._api_key:
            logger.warning(
                "uw_api_key_not_set",
                env_var=api_key_env,
            )
            return

        try:
            from unusualwhales import UnusualWhalesClient

            # SDK requires base_url and token parameters
            self._client = UnusualWhalesClient(
                base_url="https://api.unusualwhales.com",
                token=self._api_key,
            )
            self._initialized = True
            logger.info("uw_provider_initialized")
        except ImportError:
            logger.error("uw_client_import_failed")
        except Exception as e:
            logger.error("uw_provider_init_failed", error=str(e))

    async def shutdown(self) -> None:
        """Cleanup resources."""
        self._client = None
        self._initialized = False
        logger.info("uw_provider_shutdown")

    async def health_check(self) -> HealthStatus:
        """Check provider health."""
        if not self._initialized or not self._client:
            return HealthStatus(
                healthy=False,
                error="Provider not initialized",
            )

        # Lightweight health check to validate upstream connectivity
        try:
            from unusualwhales.api import market

            start = datetime.now(UTC)
            await self._call_sync(market.get_market_tide.sync, client=self._client)
            latency = (datetime.now(UTC) - start).total_seconds() * 1000
            return HealthStatus(
                healthy=True,
                latency_ms=latency,
                last_check=datetime.now(UTC),
            )
        except Exception as e:
            return HealthStatus(
                healthy=False,
                error=str(e),
                last_check=datetime.now(UTC),
            )

    def _extract_data(self, response: Any) -> list[dict[str, Any]]:
        """Extract data from SDK response - handles additional_properties or data attribute.

        Converts SDK typed objects (with to_dict method) to dicts for uniform access.
        """
        if response is None:
            return []

        # First try additional_properties['data']
        if hasattr(response, "additional_properties") and response.additional_properties:
            data = response.additional_properties.get("data", [])
            if data:
                # Handle both list and single-object cases
                if isinstance(data, list):
                    return [item.to_dict() if hasattr(item, "to_dict") else item for item in data]
                else:
                    # Single object - wrap in list
                    return [data.to_dict() if hasattr(data, "to_dict") else data]

        # Then try response.data
        if hasattr(response, "data") and response.data:
            data = response.data
            # Check if it's a list or a single object (e.g., TickerInfo, MarketTide)
            if isinstance(data, list):
                return [item.to_dict() if hasattr(item, "to_dict") else item for item in data]
            else:
                # Single object response - wrap in list after converting
                return [data.to_dict() if hasattr(data, "to_dict") else data]

        return []

    def _get_data_safe(self, response: Any) -> Any:
        """Safely get .data from response, handling None/ErrorMessage/empty arrays.

        Returns:
            - dict: If response.data is a dict (expected case)
            - None: If response is None, has no data, data is empty list, or data is ErrorMessage
        """
        if not response or not hasattr(response, "data"):
            logger.debug("uw_response_no_data", response_type=type(response).__name__ if response else "None")
            return None
        data = response.data
        # Return None for empty lists - callers expect dict with .get()
        if isinstance(data, list):
            if len(data) == 0:
                logger.debug("uw_response_empty_list")
            return data[0] if len(data) == 1 else None if len(data) == 0 else data
        return data

    async def _call_sync(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run blocking SDK calls off the event loop."""
        wait_start = perf_counter()
        async with self._call_sync_semaphore:
            wait_seconds = perf_counter() - wait_start
            record_provider_sync_call_wait(self.name, wait_seconds)

            inc_provider_sync_call_inflight(self.name)
            exec_start = perf_counter()
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            finally:
                record_provider_sync_call_exec(self.name, perf_counter() - exec_start)
                dec_provider_sync_call_inflight(self.name)

    async def _call_sync_with_optional_offset(
        self,
        func: Any,
        *,
        call_args: tuple[Any, ...] = (),
        kwargs: dict[str, Any],
        limit: int | None,
        offset: int,
    ) -> tuple[Any, bool]:
        """Call SDK method with optional native offset support and fallback slicing signal."""
        base_kwargs = dict(kwargs)
        if limit is not None:
            base_kwargs["limit"] = limit

        if offset <= 0:
            try:
                return await self._call_sync(func, *call_args, **base_kwargs), False
            except TypeError:
                if "limit" in base_kwargs:
                    base_kwargs.pop("limit", None)
                    return await self._call_sync(func, *call_args, **base_kwargs), False
                raise

        for offset_key in ("offset", "page"):
            try:
                return (
                    await self._call_sync(
                        func,
                        *call_args,
                        **{
                            **base_kwargs,
                            offset_key: offset,
                        },
                    ),
                    False,
                )
            except TypeError:
                continue

        fallback_kwargs = dict(kwargs)
        if limit is not None:
            fallback_kwargs["limit"] = limit + offset
        try:
            return await self._call_sync(func, *call_args, **fallback_kwargs), True
        except TypeError:
            if "limit" in fallback_kwargs:
                fallback_kwargs.pop("limit", None)
                return await self._call_sync(func, *call_args, **fallback_kwargs), True
            raise

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
            logger.error("uw_flow_alerts_failed", error=str(e), exc_info=True)
            return []

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
            logger.error("uw_ticker_flow_failed", symbol=symbol, error=str(e))
            return []

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
            data_items = []
            if response is not None and hasattr(response, "additional_properties") and response.additional_properties:
                data_items = response.additional_properties.get("data", [])
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            for item in data_items:
                trade = self._normalize_darkpool_trade(item)
                if trade:
                    trades.append(trade)

            logger.info("uw_darkpool_recent_fetched", count=len(trades))
            return trades

        except Exception as e:
            logger.error("uw_darkpool_recent_failed", error=str(e))
            return []

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

            trades = []
            data_items = []
            if response is not None and hasattr(response, "additional_properties") and response.additional_properties:
                data_items = response.additional_properties.get("data", [])
            if used_local_offset and offset > 0:
                data_items = data_items[offset:]

            for item in data_items:
                trade = self._normalize_darkpool_trade(item)
                if trade:
                    trades.append(trade)

            logger.info("uw_darkpool_ticker_fetched", symbol=symbol, count=len(trades))
            return trades

        except Exception as e:
            logger.error("uw_darkpool_ticker_failed", symbol=symbol, error=str(e))
            return []

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
            logger.error("uw_market_tide_failed", error=str(e))
            return []

    async def get_institutions(
        self,
        symbol: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict]:
        """Get 13F institutional holdings for a ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

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
            return []

    async def get_congress_trades(self, symbol: str | None = None, limit: int = 100) -> list[dict]:
        """Get congressional trades, optionally filtered by ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

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
            return []

    async def get_insiders(self, symbol: str | None = None, limit: int = 100) -> list[dict]:
        """Get insider transactions, optionally filtered by ticker."""
        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

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
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 1: Greek Exposure (GEX)
    # ─────────────────────────────────────────────────────────────────

    async def get_greek_exposure(self, symbol: str, date_str: str | None = None) -> list:
        """Get Greek exposure (GEX) data for a ticker."""
        from gateway.schemas import NormalizedGreekExposure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_greek_exposure.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                timestamp_str = get("timestamp") or get("date")
                timestamp = (
                    datetime.fromisoformat(str(timestamp_str).replace("Z", TZ_UTC_SUFFIX))
                    if timestamp_str
                    else datetime.now(UTC)
                )
                results.append(
                    NormalizedGreekExposure(
                        symbol=symbol.upper(),
                        timestamp=timestamp,
                        gamma_exposure=Decimal(str(get("gamma_exposure") or get("gex") or 0)),
                        delta_exposure=(
                            Decimal(str(get("delta_exposure") or get("dex") or 0))
                            if get("delta_exposure") or get("dex")
                            else None
                        ),
                        vanna_exposure=(
                            Decimal(str(get("vanna_exposure") or get("vex") or 0))
                            if get("vanna_exposure") or get("vex")
                            else None
                        ),
                        charm_exposure=(
                            Decimal(str(get("charm_exposure") or get("cex") or 0))
                            if get("charm_exposure") or get("cex")
                            else None
                        ),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_greek_exposure_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_greek_exposure_failed", symbol=symbol, error=str(e))
            return []

    async def get_greek_exposure_by_strike(self, symbol: str, date_str: str | None = None) -> list:
        """Get Greek exposure by strike price for a ticker."""
        from gateway.schemas import NormalizedGreekExposure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_greek_exposure_by_strike.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedGreekExposure(
                        symbol=symbol.upper(),
                        timestamp=datetime.now(UTC),
                        gamma_exposure=Decimal(str(get("gamma_exposure") or get("gex") or 0)),
                        delta_exposure=(Decimal(str(get("delta_exposure") or 0)) if get("delta_exposure") else None),
                        strike=Decimal(str(get("strike") or 0)),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_greek_exposure_strike_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_greek_exposure_strike_failed", symbol=symbol, error=str(e))
            return []

    async def get_greek_exposure_by_expiry(self, symbol: str, date_str: str | None = None) -> list:
        """Get Greek exposure by expiration date for a ticker."""
        from gateway.schemas import NormalizedGreekExposure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_greek_exposure_by_expiry.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedGreekExposure(
                        symbol=symbol.upper(),
                        timestamp=datetime.now(UTC),
                        gamma_exposure=Decimal(str(get("gamma_exposure") or get("gex") or 0)),
                        delta_exposure=(Decimal(str(get("delta_exposure") or 0)) if get("delta_exposure") else None),
                        expiry=str(get("expiry") or get("expiration_date") or ""),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_greek_exposure_expiry_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_greek_exposure_expiry_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 1: Earnings Calendar
    # ─────────────────────────────────────────────────────────────────

    async def get_earnings_premarket(self, date_str: str | None = None) -> list:
        """Get premarket earnings for a date."""
        from gateway.schemas import NormalizedEarnings

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import earnings

            response = await self._call_sync(
                earnings.get_premarket.sync,
                client=self._client,
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedEarnings(
                        symbol=get("ticker") or get("symbol") or "",
                        date=str(get("date") or get("earnings_date") or ""),
                        time="premarket",
                        eps_estimate=(Decimal(str(get("eps_estimate") or 0)) if get("eps_estimate") else None),
                        eps_actual=(Decimal(str(get("eps_actual") or 0)) if get("eps_actual") else None),
                        revenue_estimate=(
                            Decimal(str(get("revenue_estimate") or 0)) if get("revenue_estimate") else None
                        ),
                        revenue_actual=(Decimal(str(get("revenue_actual") or 0)) if get("revenue_actual") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_earnings_premarket_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_earnings_premarket_failed", error=str(e))
            return []

    async def get_earnings_afterhours(self, date_str: str | None = None) -> list:
        """Get afterhours earnings for a date."""
        from gateway.schemas import NormalizedEarnings

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import earnings

            response = await self._call_sync(
                earnings.get_afterhours.sync,
                client=self._client,
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedEarnings(
                        symbol=get("ticker") or get("symbol") or "",
                        date=str(get("date") or get("earnings_date") or ""),
                        time="afterhours",
                        eps_estimate=(Decimal(str(get("eps_estimate") or 0)) if get("eps_estimate") else None),
                        eps_actual=(Decimal(str(get("eps_actual") or 0)) if get("eps_actual") else None),
                        revenue_estimate=(
                            Decimal(str(get("revenue_estimate") or 0)) if get("revenue_estimate") else None
                        ),
                        revenue_actual=(Decimal(str(get("revenue_actual") or 0)) if get("revenue_actual") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_earnings_afterhours_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_earnings_afterhours_failed", error=str(e))
            return []

    async def get_earnings_ticker(self, symbol: str) -> list:
        """Get historical earnings for a specific ticker."""
        from gateway.schemas import NormalizedEarnings

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import earnings

            response = await self._call_sync(
                earnings.get_ticker_earnings.sync,
                client=self._client,
                ticker=symbol.upper(),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedEarnings(
                        symbol=symbol.upper(),
                        date=str(get("date") or get("earnings_date") or ""),
                        time=str(get("time") or get("timing") or "unknown"),
                        eps_estimate=(Decimal(str(get("eps_estimate") or 0)) if get("eps_estimate") else None),
                        eps_actual=(Decimal(str(get("eps_actual") or 0)) if get("eps_actual") else None),
                        revenue_estimate=(
                            Decimal(str(get("revenue_estimate") or 0)) if get("revenue_estimate") else None
                        ),
                        revenue_actual=(Decimal(str(get("revenue_actual") or 0)) if get("revenue_actual") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_earnings_ticker_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_earnings_ticker_failed", symbol=symbol, error=str(e))
            return []

    # Raw earnings methods for Heber integration (return all UW fields)
    async def get_raw_earnings_premarket(self, date_str: str | None = None, limit: int = 50) -> list[dict]:
        """Get raw premarket earnings for Heber ingestion.

        Returns all UW API fields with earnings_type='premarket'.
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api import earnings

        try:
            response = await self._call_sync(
                earnings.get_premarket.sync,
                client=self._client,
                date=_or_unset(date_str),
                limit=limit,
            )
            data = self._extract_data(response)
            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    {
                        "symbol": get("symbol") or get("ticker") or "",
                        "earnings_type": "premarket",
                        "report_date": get("report_date"),
                        "report_time": get("report_time"),
                        "actual_eps": get("actual_eps"),
                        "street_mean_est": get("street_mean_est"),
                        "source": get("source"),
                        "ending_fiscal_quarter": get("ending_fiscal_quarter"),
                        "expected_move": get("expected_move"),
                        "expected_move_perc": get("expected_move_perc"),
                        "full_name": get("full_name"),
                        "sector": get("sector"),
                        "marketcap": get("marketcap"),
                        "has_options": get("has_options"),
                        "is_s_p_500": get("is_s_p_500"),
                        "continent": get("continent"),
                        "country_code": get("country_code"),
                        "country_name": get("country_name"),
                        "pre_earnings_close": get("pre_earnings_close"),
                        "pre_earnings_date": get("pre_earnings_date"),
                        "post_earnings_close": get("post_earnings_close"),
                        "post_earnings_date": get("post_earnings_date"),
                        "reaction": get("reaction"),
                    }
                )
            logger.info("uw_raw_earnings_premarket_fetched", count=len(results))
            return results
        except Exception as e:
            logger.error("uw_raw_earnings_premarket_failed", error=str(e))
            raise

    async def get_raw_earnings_afterhours(self, date_str: str | None = None, limit: int = 50) -> list[dict]:
        """Get raw afterhours earnings for Heber ingestion.

        Returns all UW API fields with earnings_type='afterhours'.
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api import earnings

        try:
            response = await self._call_sync(
                earnings.get_afterhours.sync,
                client=self._client,
                date=_or_unset(date_str),
                limit=limit,
            )
            data = self._extract_data(response)
            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    {
                        "symbol": get("symbol") or get("ticker") or "",
                        "earnings_type": "afterhours",
                        "report_date": get("report_date"),
                        "report_time": get("report_time"),
                        "actual_eps": get("actual_eps"),
                        "street_mean_est": get("street_mean_est"),
                        "source": get("source"),
                        "ending_fiscal_quarter": get("ending_fiscal_quarter"),
                        "expected_move": get("expected_move"),
                        "expected_move_perc": get("expected_move_perc"),
                        "full_name": get("full_name"),
                        "sector": get("sector"),
                        "marketcap": get("marketcap"),
                        "has_options": get("has_options"),
                        "is_s_p_500": get("is_s_p_500"),
                        "continent": get("continent"),
                        "country_code": get("country_code"),
                        "country_name": get("country_name"),
                        "pre_earnings_close": get("pre_earnings_close"),
                        "pre_earnings_date": get("pre_earnings_date"),
                        "post_earnings_close": get("post_earnings_close"),
                        "post_earnings_date": get("post_earnings_date"),
                        "reaction": get("reaction"),
                    }
                )
            logger.info("uw_raw_earnings_afterhours_fetched", count=len(results))
            return results
        except Exception as e:
            logger.error("uw_raw_earnings_afterhours_failed", error=str(e))
            raise

    async def get_raw_earnings_ticker(self, symbol: str) -> list[dict]:
        """Get raw historical earnings for a ticker for Heber ingestion.

        Returns all UW API fields with earnings_type='historical'.
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api import earnings

        try:
            response = await self._call_sync(
                earnings.get_ticker_earnings.sync,
                client=self._client,
                ticker=symbol,
            )
            data = self._extract_data(response)
            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    {
                        "symbol": symbol,
                        "earnings_type": "historical",
                        "report_date": get("report_date"),
                        "report_time": get("report_time"),
                        "actual_eps": get("actual_eps"),
                        "street_mean_est": get("street_mean_est"),
                        "source": get("source"),
                        "ending_fiscal_quarter": get("ending_fiscal_quarter"),
                        "expected_move": get("expected_move"),
                        "expected_move_perc": get("expected_move_perc"),
                        # Historical-specific fields
                        "pre_earnings_move_1d": get("pre_earnings_move_1d"),
                        "pre_earnings_move_3d": get("pre_earnings_move_3d"),
                        "pre_earnings_move_1w": get("pre_earnings_move_1w"),
                        "pre_earnings_move_2w": get("pre_earnings_move_2w"),
                        "post_earnings_move_1d": get("post_earnings_move_1d"),
                        "post_earnings_move_3d": get("post_earnings_move_3d"),
                        "post_earnings_move_1w": get("post_earnings_move_1w"),
                        "post_earnings_move_2w": get("post_earnings_move_2w"),
                        "long_straddle_1d": get("long_straddle_1d"),
                        "long_straddle_1w": get("long_straddle_1w"),
                        "short_straddle_1d": get("short_straddle_1d"),
                        "short_straddle_1w": get("short_straddle_1w"),
                    }
                )
            logger.info("uw_raw_earnings_ticker_fetched", symbol=symbol, count=len(results))
            return results
        except Exception as e:
            logger.error("uw_raw_earnings_ticker_failed", symbol=symbol, error=str(e))
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

    # ─────────────────────────────────────────────────────────────────
    # Phase 1: Screeners
    # ─────────────────────────────────────────────────────────────────

    async def get_stock_screener(self, limit: int = 20) -> list:
        """Get stock screener results."""
        from gateway.schemas import NormalizedScreenerResult

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import screener

            response = await self._call_sync(
                screener.get_stocks.sync,
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedScreenerResult(
                        symbol=get("ticker") or get("symbol") or "",
                        price=Decimal(str(get("price") or 0)) if get("price") else None,
                        volume=int(get("volume") or 0) if get("volume") else None,
                        market_cap=(Decimal(str(get("market_cap") or 0)) if get("market_cap") else None),
                        sector=get("sector"),
                        call_volume=int(get("call_volume") or 0) if get("call_volume") else None,
                        put_volume=int(get("put_volume") or 0) if get("put_volume") else None,
                        iv_rank=Decimal(str(get("iv_rank") or 0)) if get("iv_rank") else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_stock_screener_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_stock_screener_failed", error=str(e))
            return []

    async def get_hottest_chains(self, limit: int = 20) -> list:
        """Get hottest option chains/contracts."""
        from gateway.schemas import NormalizedHottestChain

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import screener

            response = await self._call_sync(
                screener.get_option_contracts.sync,
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedHottestChain(
                        contract_symbol=get("option_symbol") or get("contract") or "",
                        underlying=get("ticker") or get("underlying") or "",
                        strike=Decimal(str(get("strike") or 0)),
                        expiry=str(get("expiry") or get("expiration_date") or ""),
                        option_type=str(get("put_call") or get("option_type") or "call").lower(),
                        volume=int(get("volume") or 0),
                        open_interest=int(get("open_interest") or get("oi") or 0),
                        premium=Decimal(str(get("premium") or get("total_premium") or 0)),
                        iv=(
                            Decimal(str(get("iv") or get("implied_volatility") or 0))
                            if get("iv") or get("implied_volatility")
                            else None
                        ),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_hottest_chains_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_hottest_chains_failed", error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 2: Options Analytics
    # ─────────────────────────────────────────────────────────────────

    async def get_net_premium_ticks(self, symbol: str, date_str: str | None = None) -> list:
        """Get net premium ticks for a ticker."""
        from gateway.schemas import NormalizedNetPremiumTick

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_net_premium_ticks.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                timestamp_str = get("timestamp") or get("time")
                timestamp = (
                    datetime.fromisoformat(str(timestamp_str).replace("Z", TZ_UTC_SUFFIX))
                    if timestamp_str
                    else datetime.now(UTC)
                )
                results.append(
                    NormalizedNetPremiumTick(
                        symbol=symbol.upper(),
                        timestamp=timestamp,
                        net_call_premium=Decimal(str(get("net_call_premium") or get("call_premium") or 0)),
                        net_put_premium=Decimal(str(get("net_put_premium") or get("put_premium") or 0)),
                        call_volume=int(get("call_volume") or 0),
                        put_volume=int(get("put_volume") or 0),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_net_premium_ticks_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_net_premium_ticks_failed", symbol=symbol, error=str(e))
            return []

    async def get_max_pain(self, symbol: str, expiry: str | None = None) -> list:
        """Get max pain data for a ticker."""
        from gateway.schemas import NormalizedMaxPain

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_max_pain.sync,
                client=self._client,
                ticker=symbol.upper(),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedMaxPain(
                        symbol=symbol.upper(),
                        expiry=str(get("expiry") or get("expiration_date") or ""),
                        max_pain_strike=Decimal(str(get("max_pain") or get("max_pain_strike") or 0)),
                        call_oi=int(get("call_oi") or 0) if get("call_oi") else None,
                        put_oi=int(get("put_oi") or 0) if get("put_oi") else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_max_pain_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_max_pain_failed", symbol=symbol, error=str(e))
            return []

    async def get_iv_rank(self, symbol: str, date_str: str | None = None) -> NormalizedIVRank | None:
        """Get IV rank for a ticker.

        Args:
            symbol: Stock ticker symbol
            date_str: Date in YYYY-MM-DD format (defaults to today)
        """
        from gateway.schemas import NormalizedIVRank

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return None

        try:
            # Default date to today if not provided
            if not date_str:
                date_str = datetime.now(UTC).strftime("%Y-%m-%d")

            # The SDK response parser for this endpoint is incompatible with
            # the live payload shape, so fetch raw JSON directly.
            http_client = self._client.get_httpx_client()
            response = await self._call_sync(
                http_client.get,
                f"/api/stock/{symbol.upper()}/iv-rank",
                params={"date": date_str},
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") if isinstance(payload, dict) else payload

            if isinstance(data, list):
                if not data:
                    return None
                latest = data[-1]
            else:
                latest = data

            if not isinstance(latest, dict):
                logger.warning(
                    "uw_iv_rank_unexpected_payload",
                    symbol=symbol,
                    payload_type=type(latest).__name__,
                )
                return None

            get = latest.get
            iv_rank_value = get("iv_rank") or get("iv_rank_1y")
            if iv_rank_value in (None, ""):
                return None

            result = NormalizedIVRank(
                symbol=symbol.upper(),
                iv_rank=Decimal(str(iv_rank_value)),
                iv_percentile=(Decimal(str(get("iv_percentile") or 0)) if get("iv_percentile") else None),
                current_iv=(
                    Decimal(str(get("current_iv") or get("iv") or get("volatility") or 0))
                    if get("current_iv") or get("iv") or get("volatility")
                    else None
                ),
                one_year_high=(
                    Decimal(str(get("one_year_high") or get("iv_1y_high") or 0))
                    if get("one_year_high") or get("iv_1y_high")
                    else None
                ),
                one_year_low=(
                    Decimal(str(get("one_year_low") or get("iv_1y_low") or 0))
                    if get("one_year_low") or get("iv_1y_low")
                    else None
                ),
                provider="unusual_whales",
            )

            logger.info("uw_iv_rank_fetched", symbol=symbol)
            return result

        except Exception as e:
            logger.error("uw_iv_rank_failed", symbol=symbol, error=str(e))
            return None

    async def get_oi_change(self, symbol: str, date_str: str | None = None) -> list:
        """Get OI change data for a ticker."""
        from gateway.schemas import NormalizedOIChange

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_open_interest_change.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedOIChange(
                        symbol=symbol.upper(),
                        date=str(get("date") or ""),
                        call_oi=int(get("call_oi") or 0),
                        put_oi=int(get("put_oi") or 0),
                        call_oi_change=int(get("call_oi_change") or 0),
                        put_oi_change=int(get("put_oi_change") or 0),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_oi_change_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_oi_change_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 2: ETF Analytics
    # ─────────────────────────────────────────────────────────────────

    async def get_etf_holdings(self, symbol: str) -> list:
        """Get ETF holdings."""
        from gateway.schemas import NormalizedETFHolding

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

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
                        shares=int(get("shares") or 0) if get("shares") else None,
                        market_value=(Decimal(str(get("market_value") or 0)) if get("market_value") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_etf_holdings_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_etf_holdings_failed", symbol=symbol, error=str(e))
            return []

    async def get_etf_exposure(self, symbol: str) -> list:
        """Get ETF exposure for a stock (which ETFs hold it)."""
        from gateway.schemas import NormalizedETFHolding

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

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
                        shares=int(get("shares") or 0) if get("shares") else None,
                        market_value=(Decimal(str(get("market_value") or 0)) if get("market_value") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_etf_exposure_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_etf_exposure_failed", symbol=symbol, error=str(e))
            return []

    async def get_etf_flows(self, symbol: str) -> list:
        """Get ETF inflow/outflow data."""
        from gateway.schemas import NormalizedETFFlow

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

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
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 2: Shorts Data
    # ─────────────────────────────────────────────────────────────────

    async def get_short_interest(self, symbol: str) -> list:
        """Get short interest data."""
        from gateway.schemas import NormalizedShortData

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import shorts

            response = await self._call_sync(
                shorts.get_data.sync,
                symbol.upper(),
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedShortData(
                        symbol=symbol.upper(),
                        date=str(get("date") or ""),
                        short_interest=int(get("short_interest") or 0),
                        days_to_cover=(Decimal(str(get("days_to_cover") or 0)) if get("days_to_cover") else None),
                        short_percent_float=(
                            Decimal(str(get("short_percent_float") or 0)) if get("short_percent_float") else None
                        ),
                        short_percent_outstanding=(
                            Decimal(str(get("short_percent_outstanding") or 0))
                            if get("short_percent_outstanding")
                            else None
                        ),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_short_interest_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_short_interest_failed", symbol=symbol, error=str(e))
            return []

    async def get_ftds(self, symbol: str) -> list:
        """Get failures to deliver data."""
        from gateway.schemas import NormalizedFTD

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import shorts

            response = await self._call_sync(
                shorts.get_ftds.sync,
                symbol.upper(),
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                quantity = int(get("quantity") or get("fails") or 0)
                price = Decimal(str(get("price") or 0)) if get("price") else None
                results.append(
                    NormalizedFTD(
                        symbol=symbol.upper(),
                        date=str(get("date") or ""),
                        quantity=quantity,
                        price=price,
                        value=price * quantity if price else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_ftds_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_ftds_failed", symbol=symbol, error=str(e))
            return []

    async def get_short_volume(self, symbol: str) -> list:
        """Get short volume data."""
        from gateway.schemas import NormalizedShortData

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import shorts

            response = await self._call_sync(
                shorts.get_volume_and_ratio.sync,
                symbol.upper(),
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedShortData(
                        symbol=symbol.upper(),
                        date=str(get("date") or ""),
                        short_interest=int(get("short_volume") or 0),
                        short_percent_float=(Decimal(str(get("short_ratio") or 0)) if get("short_ratio") else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_short_volume_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_short_volume_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Volatility Analytics
    # ─────────────────────────────────────────────────────────────────

    async def get_iv_term_structure(self, symbol: str) -> list:
        """Get IV term structure for a ticker."""
        from gateway.schemas import NormalizedIVTermStructure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_volatility_term_structure.sync,
                client=self._client,
                ticker=symbol.upper(),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedIVTermStructure(
                        symbol=symbol.upper(),
                        expiry=str(get("expiry") or get("expiration_date") or ""),
                        iv=Decimal(str(get("iv") or get("implied_volatility") or 0)),
                        days_to_expiry=int(get("days_to_expiry") or get("dte") or 0),
                        call_iv=Decimal(str(get("call_iv") or 0)) if get("call_iv") else None,
                        put_iv=Decimal(str(get("put_iv") or 0)) if get("put_iv") else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_iv_term_structure_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_iv_term_structure_failed", symbol=symbol, error=str(e))
            return []

    async def get_realized_volatility(self, symbol: str) -> NormalizedVolatilityStats | None:
        """Get realized volatility for a ticker."""
        from gateway.schemas import NormalizedVolatilityStats

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return None

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_candles.sync,
                client=self._client,
                ticker=symbol.upper(),
                candle_size="1D",
            )

            data = self._get_data_safe(response)
            if not data:
                return None

            get = data.get if isinstance(data, dict) else lambda k, d=None: getattr(data, k, d)
            result = NormalizedVolatilityStats(
                symbol=symbol.upper(),
                realized_vol_30d=(
                    Decimal(str(get("realized_vol_30d") or get("rv_30") or 0))
                    if get("realized_vol_30d") or get("rv_30")
                    else None
                ),
                realized_vol_60d=(
                    Decimal(str(get("realized_vol_60d") or get("rv_60") or 0))
                    if get("realized_vol_60d") or get("rv_60")
                    else None
                ),
                realized_vol_90d=(
                    Decimal(str(get("realized_vol_90d") or get("rv_90") or 0))
                    if get("realized_vol_90d") or get("rv_90")
                    else None
                ),
                provider="unusual_whales",
            )

            logger.info("uw_realized_volatility_fetched", symbol=symbol)
            return result

        except Exception as e:
            logger.error("uw_realized_volatility_failed", symbol=symbol, error=str(e))
            return None

    async def get_volatility_stats(self, symbol: str) -> NormalizedVolatilityStats | None:
        """Get volatility stats for a ticker."""
        from gateway.schemas import NormalizedVolatilityStats

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return None

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_info.sync,
                client=self._client,
                ticker=symbol.upper(),
            )

            data = self._get_data_safe(response)
            if not data:
                return None

            get = data.get if isinstance(data, dict) else lambda k, d=None: getattr(data, k, d)
            result = NormalizedVolatilityStats(
                symbol=symbol.upper(),
                realized_vol_30d=(
                    Decimal(str(get("realized_vol_30d") or get("hv_30") or 0))
                    if get("realized_vol_30d") or get("hv_30")
                    else None
                ),
                iv_30d=(Decimal(str(get("iv_30d") or get("iv_30") or 0)) if get("iv_30d") or get("iv_30") else None),
                iv_percentile=(Decimal(str(get("iv_percentile") or 0)) if get("iv_percentile") else None),
                hv_iv_ratio=Decimal(str(get("hv_iv_ratio") or 0)) if get("hv_iv_ratio") else None,
                provider="unusual_whales",
            )

            logger.info("uw_volatility_stats_fetched", symbol=symbol)
            return result

        except Exception as e:
            logger.error("uw_volatility_stats_failed", symbol=symbol, error=str(e))
            return None

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Seasonality
    # ─────────────────────────────────────────────────────────────────

    async def get_market_seasonality(self) -> list:
        """Get market-wide seasonality data."""
        from gateway.schemas import NormalizedSeasonality

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import seasonality

            response = await self._call_sync(seasonality.get_market_average_returns_by_month.sync, client=self._client)

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedSeasonality(
                        symbol=None,
                        month=int(get("month") or 0),
                        avg_return=Decimal(str(get("avg_return") or get("average_return") or 0)),
                        median_return=(Decimal(str(get("median_return") or 0)) if get("median_return") else None),
                        win_rate=Decimal(str(get("win_rate") or get("positive_rate") or 0)),
                        sample_years=(
                            int(get("sample_years") or get("years") or 0)
                            if get("sample_years") or get("years")
                            else None
                        ),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_market_seasonality_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_market_seasonality_failed", error=str(e))
            return []

    async def get_monthly_returns(self, symbol: str) -> list:
        """Get monthly returns for a ticker."""
        from gateway.schemas import NormalizedSeasonality

        if not self._client:
            logger.warning("uw_client_not_initialized")
            return []

        try:
            from unusualwhales.api import seasonality

            response = await self._call_sync(
                seasonality.get_monthly_average_returns.sync,
                symbol.upper(),
                client=self._client,
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedSeasonality(
                        symbol=symbol.upper(),
                        month=int(get("month") or 0),
                        avg_return=Decimal(str(get("avg_return") or get("average_return") or 0)),
                        median_return=(Decimal(str(get("median_return") or 0)) if get("median_return") else None),
                        win_rate=Decimal(str(get("win_rate") or get("positive_rate") or 0)),
                        sample_years=(
                            int(get("sample_years") or get("years") or 0)
                            if get("sample_years") or get("years")
                            else None
                        ),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_monthly_returns_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_monthly_returns_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 5: Options Data
    # ─────────────────────────────────────────────────────────────────

    async def get_historic_option_volume(self, symbol: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get historic option volume and premium for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_volume_open_interest_by_expiry.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "date": str(get("date") or date_str or ""),
                        "expiry": get("expiry"),
                        "volume": int(get("volume") or 0) if get("volume") else None,
                        "open_interest": (
                            int(get("open_interest") or get("oi") or 0) if get("open_interest") or get("oi") else None
                        ),
                        "call_volume": int(get("call_volume") or 0) if get("call_volume") else None,
                        "put_volume": int(get("put_volume") or 0) if get("put_volume") else None,
                        "premium": float(get("premium") or 0) if get("premium") else None,
                    }
                )

            logger.info("uw_historic_option_volume_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_historic_option_volume_failed", symbol=symbol, error=str(e))
            return []

    async def get_intraday_option_data(self, contract_id: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get intraday 1-min OHLC for an option contract."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import contract

            response = await self._call_sync(
                contract.get_price_history.sync,
                client=self._client,
                option_symbol=contract_id,
                date=date_str,
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "contract": contract_id,
                        "timestamp": str(get("timestamp") or get("time") or ""),
                        "open": float(get("open") or 0) if get("open") else None,
                        "high": float(get("high") or 0) if get("high") else None,
                        "low": float(get("low") or 0) if get("low") else None,
                        "close": float(get("close") or 0) if get("close") else None,
                        "volume": int(get("volume") or 0) if get("volume") else None,
                    }
                )

            logger.info("uw_intraday_option_data_fetched", contract=contract_id, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_intraday_option_data_failed", contract=contract_id, error=str(e))
            return []

    async def get_options_screener(
        self, min_volume: int | None = None, min_premium: float | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Get option contracts screener results."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import screener

            response = await self._call_sync(
                screener.get_option_contracts.sync,
                client=self._client,
                min_volume=_or_unset(min_volume),
                min_premium=_or_unset(int(min_premium) if min_premium else None),
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data[:limit]:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": get("underlying") or get("ticker") or get("symbol"),
                        "contract": get("option_symbol") or get("contract"),
                        "strike": float(get("strike") or 0) if get("strike") else None,
                        "expiry": get("expiry") or get("expiration"),
                        "type": get("option_type") or get("type"),
                        "volume": int(get("volume") or 0) if get("volume") else None,
                        "open_interest": (
                            int(get("open_interest") or get("oi") or 0) if get("open_interest") or get("oi") else None
                        ),
                        "premium": float(get("premium") or 0) if get("premium") else None,
                        "iv": (
                            float(get("iv") or get("implied_volatility") or 0)
                            if get("iv") or get("implied_volatility")
                            else None
                        ),
                    }
                )

            logger.info("uw_options_screener_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_options_screener_failed", error=str(e))
            return []

    async def get_iv_surface(self, symbol: str) -> list[dict[str, Any]]:
        """Get implied volatility surface for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_implied_volatility_surface.sync,
                client=self._client,
                ticker=symbol.upper(),
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
                        "strike": float(get("strike") or 0) if get("strike") else None,
                        "call_iv": (
                            float(get("call_iv") or get("atm_call_iv") or 0)
                            if get("call_iv") or get("atm_call_iv")
                            else None
                        ),
                        "put_iv": (
                            float(get("put_iv") or get("atm_put_iv") or 0)
                            if get("put_iv") or get("atm_put_iv")
                            else None
                        ),
                        "mid_iv": (
                            float(get("mid_iv") or get("atm_iv") or 0) if get("mid_iv") or get("atm_iv") else None
                        ),
                        "dte": int(get("dte") or 0) if get("dte") else None,
                    }
                )

            logger.info("uw_iv_surface_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_iv_surface_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 6: Market Intelligence
    # ─────────────────────────────────────────────────────────────────

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

            # Default date to today if not provided
            if not date_str:
                date_str = datetime.now().strftime("%Y-%m-%d")

            response = await self._call_sync(
                stock.get_stock_volume_price_levels.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=date_str,
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
                        "price": float(get("price") or 0) if get("price") else None,
                        "volume": int(get("volume") or 0) if get("volume") else None,
                        "lit_volume": int(get("lit_volume") or 0) if get("lit_volume") else None,
                        "off_lit_volume": (
                            int(get("off_lit_volume") or get("dark_volume") or 0)
                            if get("off_lit_volume") or get("dark_volume")
                            else None
                        ),
                    }
                )

            logger.info("uw_off_lit_levels_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_off_lit_levels_failed", symbol=symbol, error=str(e))
            return []

    async def get_recent_congress_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent congressional trades across all tickers."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import congress

            response = await self._call_sync(congress.get_trades.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

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
            return []

    async def get_nope(self, symbol: str) -> dict[str, Any]:
        """Get NOPE (Net Options Pricing Effect) for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_nope.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, dict) else {}

            get = data.get if isinstance(data, dict) else lambda k, d=None: getattr(data, k, d)

            result = {
                "symbol": symbol.upper(),
                "nope": float(get("nope") or 0) if get("nope") else None,
                "call_delta": float(get("call_delta") or 0) if get("call_delta") else None,
                "put_delta": float(get("put_delta") or 0) if get("put_delta") else None,
                "timestamp": str(get("timestamp") or get("date") or ""),
            }

            logger.info("uw_nope_fetched", symbol=symbol)
            return result

        except Exception as e:
            logger.error("uw_nope_failed", symbol=symbol, error=str(e))
            return {}

    async def get_put_call_ratio(self, symbol: str) -> list[dict[str, Any]]:
        """Get historical put/call ratio for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_put_call_ratio.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "date": str(get("date") or ""),
                        "put_call_ratio": (
                            float(get("put_call_ratio") or get("pcr") or 0)
                            if get("put_call_ratio") or get("pcr")
                            else None
                        ),
                        "put_volume": int(get("put_volume") or 0) if get("put_volume") else None,
                        "call_volume": int(get("call_volume") or 0) if get("call_volume") else None,
                    }
                )

            logger.info("uw_put_call_ratio_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_put_call_ratio_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 7: Flow Analytics
    # ─────────────────────────────────────────────────────────────────

    async def get_spot_exposures_by_strike(self, symbol: str) -> list[dict[str, Any]]:
        """Get gamma and options volume confluence per strike."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_spot_exposures_by_strike.sync, client=self._client, ticker=symbol.upper()
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
                        "strike": float(get("strike") or 0) if get("strike") else None,
                        "gamma_exposure": (
                            float(get("gamma_exposure") or get("gex") or 0)
                            if get("gamma_exposure") or get("gex")
                            else None
                        ),
                        "call_volume": int(get("call_volume") or 0) if get("call_volume") else None,
                        "put_volume": int(get("put_volume") or 0) if get("put_volume") else None,
                        "call_oi": int(get("call_oi") or 0) if get("call_oi") else None,
                        "put_oi": int(get("put_oi") or 0) if get("put_oi") else None,
                    }
                )

            logger.info("uw_spot_exposures_strike_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_spot_exposures_strike_failed", symbol=symbol, error=str(e))
            return []

    async def get_flow_per_strike(self, symbol: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get flow data aggregated by strike price."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from datetime import datetime

            from unusualwhales.api import stock

            # Default to today if not provided
            if not date_str:
                date_str = datetime.now().strftime("%Y-%m-%d")

            response = await self._call_sync(
                stock.get_flow_per_strike_intraday.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=date_str,
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
                        "strike": float(get("strike") or 0) if get("strike") else None,
                        "call_premium": (float(get("call_premium") or 0) if get("call_premium") else None),
                        "put_premium": (float(get("put_premium") or 0) if get("put_premium") else None),
                        "call_volume": int(get("call_volume") or 0) if get("call_volume") else None,
                        "put_volume": int(get("put_volume") or 0) if get("put_volume") else None,
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                    }
                )

            logger.info("uw_flow_per_strike_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_flow_per_strike_failed", symbol=symbol, error=str(e))
            return []

    async def get_flow_per_expiry(self, symbol: str, date_str: str | None = None) -> list[dict[str, Any]]:
        """Get flow data aggregated by expiration date."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from datetime import datetime

            from unusualwhales.api import stock

            # Default to today if not provided
            if not date_str:
                date_str = datetime.now().strftime("%Y-%m-%d")

            response = await self._call_sync(
                stock.get_oi_per_expiry.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=date_str,
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
                        "call_premium": (float(get("call_premium") or 0) if get("call_premium") else None),
                        "put_premium": (float(get("put_premium") or 0) if get("put_premium") else None),
                        "call_volume": int(get("call_volume") or 0) if get("call_volume") else None,
                        "put_volume": int(get("put_volume") or 0) if get("put_volume") else None,
                        "net_premium": (float(get("net_premium") or 0) if get("net_premium") else None),
                    }
                )

            logger.info("uw_flow_per_expiry_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_flow_per_expiry_failed", symbol=symbol, error=str(e))
            return []

    async def get_greek_flow(self, symbol: str) -> list[dict[str, Any]]:
        """Get greek flow data for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_greek_flow.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

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
            return []

    async def get_net_flow_expiry(self) -> list[dict[str, Any]]:
        """Get net premium flow by expiration category."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(market.get_net_flow_by_expiry.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

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
            return []

    async def get_interpolated_iv(self, symbol: str) -> list[dict[str, Any]]:
        """Get interpolated implied volatility for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_interpolated_iv.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "dte": int(get("dte") or 0) if get("dte") else None,
                        "iv": (
                            float(get("iv") or get("implied_volatility") or 0)
                            if get("iv") or get("implied_volatility")
                            else None
                        ),
                        "call_iv": float(get("call_iv") or 0) if get("call_iv") else None,
                        "put_iv": float(get("put_iv") or 0) if get("put_iv") else None,
                    }
                )

            logger.info("uw_interpolated_iv_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_interpolated_iv_failed", symbol=symbol, error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 8: Market Intelligence
    # ─────────────────────────────────────────────────────────────────

    async def get_economic_calendar(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Get economic calendar events."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(
                market.get_economic_calendar.sync,
                client=self._client,
                start_date=start_date,
                end_date=end_date,
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "date": str(get("date") or get("event_date") or ""),
                        "time": get("time"),
                        "event": get("event") or get("name"),
                        "country": get("country"),
                        "impact": get("impact") or get("importance"),
                        "previous": get("previous"),
                        "forecast": get("forecast"),
                        "actual": get("actual"),
                    }
                )

            logger.info("uw_economic_calendar_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_economic_calendar_failed", error=str(e))
            return []

    async def get_sector_tide(self, sector: str, date_str: str | None = None) -> list[dict]:
        """Get market tide for a specific sector.

        Returns data in same format as market/tide (Daily Market Tide schema).
        """
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            kwargs = {"sector": sector}
            if date_str:
                kwargs["date"] = date_str
            response = await self._call_sync(market.get_sector_tide.sync, client=self._client, **kwargs)

            tides = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)

                net_call = float(get("net_call_premium") or 0)
                net_put = float(get("net_put_premium") or 0)

                # Compute sentiment
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
                        "net_volume": int(get("net_volume") or 0) if get("net_volume") else None,
                        "sentiment": sentiment,
                    }
                )

            logger.info("uw_sector_tide_fetched", sector=sector, count=len(tides))
            return tides

        except Exception as e:
            logger.error("uw_sector_tide_failed", sector=sector, error=str(e))
            return []

    async def get_top_net_impact(self, limit: int = 20) -> dict[str, list[dict[str, Any]]]:
        """Get top tickers by net premium (bullish and bearish)."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(market.get_top_net_premium.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, dict) else {}

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
            return {"bullish": [], "bearish": []}

    async def get_custom_alerts(
        self, _min_premium: float | None = None, _min_volume: int | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Get custom filtered flow alerts."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import alerts

            response = await self._call_sync(alerts.get_alerts.sync, client=self._client)
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data[:limit]:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": get("ticker") or get("symbol"),
                        "strike": float(get("strike") or 0) if get("strike") else None,
                        "expiry": get("expiry") or get("expiration"),
                        "type": get("option_type") or get("type"),
                        "premium": float(get("premium") or 0) if get("premium") else None,
                        "volume": int(get("volume") or 0) if get("volume") else None,
                        "rule": get("rule_name") or get("alert_type"),
                        "timestamp": str(get("timestamp") or get("date") or ""),
                    }
                )

            logger.info("uw_custom_alerts_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_custom_alerts_failed", error=str(e))
            return []

    async def get_market_correlations(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Get cross-asset correlations."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            response = await self._call_sync(
                market.get_correlations.sync,
                client=self._client,
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol_1": get("symbol_1") or get("ticker_1"),
                        "symbol_2": get("symbol_2") or get("ticker_2"),
                        "correlation": (float(get("correlation") or 0) if get("correlation") else None),
                        "period": get("period"),
                    }
                )

            logger.info("uw_market_correlations_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_market_correlations_failed", error=str(e))
            return []

    # ─────────────────────────────────────────────────────────────────
    # Phase 9: Final Gaps
    # ─────────────────────────────────────────────────────────────────

    async def get_etf_tide(self, symbol: str, date_str: str | None = None) -> list[dict]:
        """Get ETF-level tide data (premium flow sentiment).

        Returns data in same format as market/tide (Daily Market Tide schema).
        """
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import market

            kwargs = {"ticker": symbol.upper()}
            if date_str:
                kwargs["date"] = date_str
            response = await self._call_sync(market.get_etf_tide.sync, client=self._client, **kwargs)

            tides = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)

                net_call = float(get("net_call_premium") or 0)
                net_put = float(get("net_put_premium") or 0)

                # Compute sentiment
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
                        "net_volume": int(get("net_volume") or 0) if get("net_volume") else None,
                        "sentiment": sentiment,
                    }
                )

            logger.info("uw_etf_tide_fetched", symbol=symbol, count=len(tides))
            return tides

        except Exception as e:
            logger.error("uw_etf_tide_failed", symbol=symbol, error=str(e))
            return []

    async def get_option_volume_levels(self, symbol: str) -> list[dict[str, Any]]:
        """Get option volume levels per price."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_option_volume_levels.sync, client=self._client, ticker=symbol.upper()
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
                        "price": float(get("price") or 0) if get("price") else None,
                        "call_volume": int(get("call_volume") or 0) if get("call_volume") else None,
                        "put_volume": int(get("put_volume") or 0) if get("put_volume") else None,
                        "total_volume": (int(get("total_volume") or 0) if get("total_volume") else None),
                    }
                )

            logger.info("uw_option_volume_levels_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_option_volume_levels_failed", symbol=symbol, error=str(e))
            return []

    async def get_full_tape(self, _date_str: str) -> list[dict[str, Any]]:
        """Get full options trade tape for a date."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        # SDK doesn't have option_trades module - this functionality is not available
        logger.warning("uw_full_tape_not_available", reason="SDK does not include option_trades module")
        return []

    async def get_volume_profile(self, contract_id: str) -> list[dict[str, Any]]:
        """Get volume profile of an option contract by fill price."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import contract

            response = await self._call_sync(
                contract.get_volume_profile.sync, client=self._client, option_symbol=contract_id
            )
            data = self._get_data_safe(response)
            if not data:
                data = response if isinstance(response, list) else []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "contract": contract_id,
                        "fill_price": (
                            float(get("fill_price") or get("price") or 0) if get("fill_price") or get("price") else None
                        ),
                        "volume": int(get("volume") or 0) if get("volume") else None,
                        "percentage": (
                            float(get("percentage") or get("pct") or 0) if get("percentage") or get("pct") else None
                        ),
                    }
                )

            logger.info("uw_volume_profile_fetched", contract=contract_id, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_volume_profile_failed", contract=contract_id, error=str(e))
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
            return []

    # ─────────────────────────────────────────────────────────────────
    # Normalization Helpers
    # ─────────────────────────────────────────────────────────────────

    def _normalize_flow_alert(self, data: Any) -> NormalizedFlowAlert | None:
        """Normalize a flow alert from UW API response."""
        try:
            # Handle both dict and object access
            get = data.get if isinstance(data, dict) else lambda k, d=None: getattr(data, k, d)

            timestamp_str = get("timestamp") or get("created_at") or get("executed_at")
            timestamp = (
                datetime.fromisoformat(str(timestamp_str).replace("Z", TZ_UTC_SUFFIX))
                if timestamp_str
                else datetime.now(UTC)
            )

            # Parse optional decimal fields
            price = None
            if get("price") is not None:
                price = Decimal(str(get("price")))

            underlying_price = None
            if get("underlying_price") is not None:
                underlying_price = Decimal(str(get("underlying_price")))

            volume_oi_ratio = None
            if get("volume_oi_ratio") is not None:
                volume_oi_ratio = Decimal(str(get("volume_oi_ratio")))

            ask_prem = None
            if get("total_ask_side_prem") is not None:
                ask_prem = Decimal(str(get("total_ask_side_prem")))

            bid_prem = None
            if get("total_bid_side_prem") is not None:
                bid_prem = Decimal(str(get("total_bid_side_prem")))

            return NormalizedFlowAlert(
                symbol=get("ticker") or get("symbol") or "",
                timestamp=timestamp,
                strike=Decimal(str(get("strike") or 0)),
                expiry=str(get("expiry") or get("expiration_date") or ""),
                put_call=str(get("type") or get("put_call") or get("option_type") or "").lower(),
                premium=Decimal(str(get("total_premium") or get("premium") or 0)),
                volume=int(get("volume") or get("size") or 0),
                open_interest=int(get("open_interest") or get("oi") or 0),
                side=str(get("side") or get("aggressor_side") or "mid").lower(),
                is_sweep=bool(get("has_sweep") or get("is_sweep") or get("sweep")),
                is_unusual=bool(get("is_unusual") or get("unusual")),
                sentiment=get("sentiment"),
                # Additional UW fields
                option_chain=get("option_chain"),
                price=price,
                underlying_price=underlying_price,
                alert_rule=get("alert_rule"),
                total_size=int(get("total_size")) if get("total_size") is not None else None,
                trade_count=int(get("trade_count")) if get("trade_count") is not None else None,
                volume_oi_ratio=volume_oi_ratio,
                total_ask_side_prem=ask_prem,
                total_bid_side_prem=bid_prem,
                all_opening_trades=bool(get("all_opening_trades", False)),
                has_floor=bool(get("has_floor", False)),
                has_multileg=bool(get("has_multileg", False)),
                has_singleleg=bool(get("has_singleleg", True)),
                expiry_count=int(get("expiry_count")) if get("expiry_count") is not None else None,
                provider="unusual_whales",
            )
        except Exception as e:
            logger.warning("uw_normalize_flow_failed", error=str(e))
            return None

    def _normalize_darkpool_trade(self, data: Any) -> NormalizedDarkpoolTrade | None:
        """Normalize a darkpool trade from UW API response."""
        try:
            get = data.get if isinstance(data, dict) else lambda k, d=None: getattr(data, k, d)

            timestamp_str = get("timestamp") or get("executed_at")
            timestamp = (
                datetime.fromisoformat(str(timestamp_str).replace("Z", TZ_UTC_SUFFIX))
                if timestamp_str
                else datetime.now(UTC)
            )

            price = Decimal(str(get("price") or 0))
            size = int(get("size") or get("volume") or 0)
            notional = get("notional") or get("premium")
            if notional is None:
                notional = price * size

            # Extract NBBO data
            nbbo_bid = None
            nbbo_ask = None
            if get("nbbo_bid") is not None:
                nbbo_bid = Decimal(str(get("nbbo_bid")))
            if get("nbbo_ask") is not None:
                nbbo_ask = Decimal(str(get("nbbo_ask")))

            # Extract tracking_id as string
            tracking_id = None
            if get("tracking_id") is not None:
                tracking_id = str(get("tracking_id"))

            return NormalizedDarkpoolTrade(
                symbol=get("ticker") or get("symbol") or "",
                timestamp=timestamp,
                price=price,
                size=size,
                notional=Decimal(str(notional)),
                exchange=get("market_center") or get("exchange") or get("venue"),
                tracking_id=tracking_id,
                nbbo_bid=nbbo_bid,
                nbbo_ask=nbbo_ask,
                ext_hours=get("ext_hour_sold_codes"),
                trade_settlement=get("trade_settlement"),
                canceled=bool(get("canceled", False)),
                provider="unusual_whales",
            )
        except Exception as e:
            logger.warning("uw_normalize_darkpool_failed", error=str(e))
            return None

    def _normalize_market_tide(self, data: Any) -> NormalizedMarketTide | None:
        """Normalize market tide data from UW API response."""
        try:
            get = data.get if isinstance(data, dict) else lambda k, d=None: getattr(data, k, d)

            timestamp_str = get("timestamp") or get("time")
            timestamp = (
                datetime.fromisoformat(str(timestamp_str).replace("Z", TZ_UTC_SUFFIX))
                if timestamp_str
                else datetime.now(UTC)
            )

            net_call = Decimal(str(get("net_call_premium") or get("call_premium") or 0))
            net_put = Decimal(str(get("net_put_premium") or get("put_premium") or 0))

            # Extract net_volume from UW API
            net_volume = None
            if get("net_volume") is not None:
                net_volume = int(get("net_volume"))

            # Extract date from UW API
            date_str = get("date")

            # Determine sentiment based on premium comparison
            if net_call > abs(net_put):
                sentiment = "bullish"
            elif abs(net_put) > net_call:
                sentiment = "bearish"
            else:
                sentiment = "neutral"

            return NormalizedMarketTide(
                timestamp=timestamp,
                date=date_str,
                net_call_premium=net_call,
                net_put_premium=net_put,
                net_volume=net_volume,
                sentiment=sentiment,
                provider="unusual_whales",
            )
        except Exception as e:
            logger.warning("uw_normalize_tide_failed", error=str(e))
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 1: News Endpoints
    # ─────────────────────────────────────────────────────────────────────────

    async def get_news_headlines(
        self,
        sources: list[str] | None = None,
        search_term: str | None = None,
        major_only: bool | None = None,
        limit: int = 50,
        page: int | None = None,
    ) -> list[dict]:
        """Get latest news headlines for financial markets."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.news import get_headlines

        try:
            response = await self._call_sync(
                get_headlines.sync,
                client=self._client,
                sources=_or_unset(sources),
                search_term=_or_unset(search_term),
                major_only=_or_unset(major_only),
                limit=limit,
                page=_or_unset(page),
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_news_headlines_failed", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 1: Politician Portfolios Endpoints (Enterprise-only)
    # ─────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 2: Market Calendar & Data Endpoints
    # ─────────────────────────────────────────────────────────────────────────

    async def get_economic_calendar_simple(self) -> list[dict]:
        """Get economic calendar events."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_economic_calendar

        try:
            response = await self._call_sync(get_economic_calendar.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_economic_calendar_failed", error=str(e))
            raise

    async def get_fda_calendar(self) -> list[dict]:
        """Get FDA calendar events."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_fda_calendar

        try:
            response = await self._call_sync(get_fda_calendar.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_fda_calendar_failed", error=str(e))
            raise

    async def get_market_holidays(self) -> list[dict]:
        """Get market holidays."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_holidays

        try:
            response = await self._call_sync(get_holidays.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_market_holidays_failed", error=str(e))
            raise

    async def get_market_imbalances(self) -> list[dict]:
        """Get market imbalances data."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_imbalances

        try:
            response = await self._call_sync(get_imbalances.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_market_imbalances_failed", error=str(e))
            raise

    async def get_market_options_volume(self) -> list[dict]:
        """Get total market options volume."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_market_options_volume

        try:
            response = await self._call_sync(get_market_options_volume.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_market_options_volume_failed", error=str(e))
            raise

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

    async def get_sector_stats(self) -> list[dict]:
        """Get sector statistics."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_sector_stats

        try:
            response = await self._call_sync(get_sector_stats.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_sector_stats_failed", error=str(e))
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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 3: Institution Endpoints
    # ─────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 3: Insider Endpoints
    # ─────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 5: ETF Endpoints
    # ─────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 5: Screener/Alerts Endpoints
    # ─────────────────────────────────────────────────────────────────────────

    async def get_all_alerts(self, limit: int = 50) -> list[dict]:
        """Get all alerts."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.alerts import get_alerts

        try:
            response = await self._call_sync(get_alerts.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_alerts_failed", error=str(e))
            raise

    async def get_alerts_configuration(self) -> dict | None:
        """Get alerts configuration."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.alerts import get_configurations

        try:
            response = await self._call_sync(get_configurations.sync, client=self._client)
            data = self._extract_data(response)
            return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        except Exception as e:
            logger.error("uw_alerts_config_failed", error=str(e))
            raise

    async def get_analyst_ratings(self, limit: int = 50) -> list[dict]:
        """Get analyst ratings from screener."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.screener import get_analyst_ratings

        try:
            response = await self._call_sync(get_analyst_ratings.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_analyst_ratings_failed", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 4: Stock Module - Additional Endpoints
    # ─────────────────────────────────────────────────────────────────────────

    async def get_stock_info(self, symbol: str) -> dict | None:
        """Get stock/ticker information."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_info

        try:
            response = await self._call_sync(get_info.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        except Exception as e:
            logger.error("uw_stock_info_failed", error=str(e), symbol=symbol)
            raise

    async def get_stock_candles(self, symbol: str, timeframe: str = "1d") -> list[dict]:
        """Get OHLC candle data for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_candles

        try:
            response = await self._call_sync(get_candles.sync, symbol.upper(), client=self._client, timeframe=timeframe)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_stock_candles_failed", error=str(e), symbol=symbol)
            raise

    async def get_stock_option_chains(self, symbol: str) -> list[dict]:
        """Get tradeable option contracts for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_option_chains

        try:
            response = await self._call_sync(get_option_chains.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_option_chains_failed", error=str(e), symbol=symbol)
            raise

    async def get_stock_option_contracts(self, symbol: str) -> list[dict]:
        """Get all option contracts for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_option_contracts

        try:
            response = await self._call_sync(get_option_contracts.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_option_contracts_failed", error=str(e), symbol=symbol)
            raise

    async def get_oi_per_strike(self, symbol: str) -> list[dict]:
        """Get open interest per strike for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_oi_per_strike

        try:
            response = await self._call_sync(get_oi_per_strike.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_oi_per_strike_failed", error=str(e), symbol=symbol)
            raise

    async def get_oi_per_expiry(self, symbol: str) -> list[dict]:
        """Get open interest per expiry for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_oi_per_expiry

        try:
            response = await self._call_sync(get_oi_per_expiry.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_oi_per_expiry_failed", error=str(e), symbol=symbol)
            raise

    async def get_greeks_by_strike_expiry(self, symbol: str, expiry: str) -> list[dict]:
        """Get option greeks by strike for a specific expiry."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_greeks_by_strike_expiry

        try:
            response = await self._call_sync(
                get_greeks_by_strike_expiry.sync,
                symbol.upper(),
                self._client,
                expiry=expiry,
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_greeks_by_strike_expiry_failed", error=str(e), symbol=symbol, expiry=expiry)
            raise

    async def get_greek_exposure_by_strike_expiry(self, symbol: str, expiry: str) -> list[dict]:
        """Get greek exposure by strike for a specific expiry."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_greek_exposure_by_strike_expiry

        try:
            response = await self._call_sync(
                get_greek_exposure_by_strike_expiry.sync,
                symbol.upper(),
                self._client,
                expiry=expiry,
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error(
                "uw_greek_exposure_by_strike_expiry_failed",
                error=str(e),
                symbol=symbol,
                expiry=expiry,
            )
            raise

    async def get_atm_option_contracts(self, symbol: str) -> list[dict]:
        """Get ATM option contracts for all expiries."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_atm_option_contracts_for_expiries

        try:
            response = await self._call_sync(
                get_atm_option_contracts_for_expiries.sync, symbol.upper(), client=self._client
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_atm_options_failed", error=str(e), symbol=symbol)
            raise

    async def get_daily_expiry_breakdown(self, symbol: str) -> list[dict]:
        """Get option order flow grouped by expiry for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_daily_expiry_breakdown

        try:
            response = await self._call_sync(get_daily_expiry_breakdown.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_daily_expiry_breakdown_failed", error=str(e), symbol=symbol)
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

    async def get_risk_reversal_skew(self, symbol: str, expiry: str) -> list[dict]:
        """Get historical risk reversal skew by expiry."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_risk_reversal_skew

        try:
            response = await self._call_sync(
                get_risk_reversal_skew.sync,
                symbol.upper(),
                self._client,
                expiry=expiry,
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_risk_reversal_skew_failed", error=str(e), symbol=symbol, expiry=expiry)
            raise

    async def get_sector_tickers(self, sector: str) -> list[dict]:
        """Get tickers for a given sector."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_sector_tickers

        try:
            response = await self._call_sync(
                get_sector_tickers.sync,
                sector,
                client=self._client,
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_sector_tickers_failed", error=str(e), sector=sector)
            raise

    async def get_stock_state(self, symbol: str) -> dict | None:
        """Get stock OHLC and volume state."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_stock_state

        try:
            response = await self._call_sync(get_stock_state.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        except Exception as e:
            logger.error("uw_stock_state_failed", error=str(e), symbol=symbol)
            raise

    async def get_stock_volume_price_levels(self, symbol: str) -> list[dict]:
        """Get stock volume price levels."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_stock_volume_price_levels

        try:
            response = await self._call_sync(get_stock_volume_price_levels.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_stock_volume_price_levels_failed", error=str(e), symbol=symbol)
            raise

    async def get_option_volume_by_price_level(self, symbol: str) -> list[dict]:
        """Get call and put volume per price level."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_option_volume_by_price_level

        try:
            response = await self._call_sync(get_option_volume_by_price_level.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_option_volume_by_price_failed", error=str(e), symbol=symbol)
            raise

    async def get_volume_oi_by_expiry(self, symbol: str) -> list[dict]:
        """Get volume and OI per expiry."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_volume_open_interest_by_expiry

        try:
            response = await self._call_sync(
                get_volume_open_interest_by_expiry.sync, symbol.upper(), client=self._client
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_volume_oi_by_expiry_failed", error=str(e), symbol=symbol)
            raise

    async def get_spot_exposures(self, symbol: str) -> list[dict]:
        """Get spot GEX exposures per minute."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_spot_exposures

        try:
            response = await self._call_sync(get_spot_exposures.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_spot_exposures_failed", error=str(e), symbol=symbol)
            raise

    async def get_options_volume(self, symbol: str) -> list[dict]:
        """Get options volume and premium for a trading date."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_options_volume

        try:
            response = await self._call_sync(get_options_volume.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_options_volume_failed", error=str(e), symbol=symbol)
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

    # ─────────────────────────────────────────────────────────────────────────
    # Final Batch: Remaining SDK Methods for True 100% Coverage
    # ─────────────────────────────────────────────────────────────────────────

    # --- Option Contract Module ---

    async def get_option_contract_flow(self, option_symbol: str) -> list[dict]:
        """Get flow for a specific option contract."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.option_contract import get_flow

        try:
            response = await self._call_sync(get_flow.sync, option_symbol, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_option_contract_flow_failed", error=str(e), option_symbol=option_symbol)
            raise

    async def get_option_contract_historic(self, option_symbol: str) -> list[dict]:
        """Get historic data for a specific option contract."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.option_contract import get_historic

        try:
            response = await self._call_sync(get_historic.sync, option_symbol, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_option_contract_historic_failed", error=str(e), option_symbol=option_symbol)
            raise

    async def get_option_contract_intraday(self, option_symbol: str) -> list[dict]:
        """Get intraday data for a specific option contract."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.option_contract import get_intraday

        try:
            response = await self._call_sync(get_intraday.sync, option_symbol, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_option_contract_intraday_failed", error=str(e), option_symbol=option_symbol)
            raise

    async def get_option_contract_volume_profile(self, option_symbol: str) -> list[dict]:
        """Get volume profile for a specific option contract."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.option_contract import get_volume_profile

        try:
            response = await self._call_sync(get_volume_profile.sync, option_symbol, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error(
                "uw_option_contract_volume_profile_failed",
                error=str(e),
                option_symbol=option_symbol,
            )
            raise

    # --- Contract Module ---

    async def get_contract_price_history(self, option_symbol: str) -> list[dict]:
        """Get price history for a contract."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.contract import get_price_history

        try:
            response = await self._call_sync(get_price_history.sync, option_symbol, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_contract_price_history_failed", error=str(e), option_symbol=option_symbol)
            raise

    # --- Congress Module ---

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

    # --- Flow Module ---

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

    # --- Screener Module ---

    async def get_screener_option_contracts(self, limit: int = 50) -> list[dict]:
        """Get option contracts from screener."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.screener import get_option_contracts

        try:
            response = await self._call_sync(get_option_contracts.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_screener_option_contracts_failed", error=str(e))
            raise

    async def get_screener_stocks(self, limit: int = 50) -> list[dict]:
        """Get stocks from screener."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.screener import get_stocks

        try:
            response = await self._call_sync(get_stocks.sync, client=self._client)
            data = self._extract_data(response)
            result = data
            return result[:limit]
        except Exception as e:
            logger.error("uw_screener_stocks_failed", error=str(e))
            raise

    # --- Seasonality Module ---

    async def get_monthly_top_performers(self, month: int) -> list[dict]:
        """Get top performing stocks by month."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.seasonality import get_monthly_top_performers

        try:
            response = await self._call_sync(
                get_monthly_top_performers.sync,
                month,
                client=self._client,
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_monthly_top_performers_failed", error=str(e), month=month)
            raise

    async def get_price_changes_by_month_year(self, symbol: str) -> list[dict]:
        """Get price changes by month and year for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.seasonality import get_price_changes_by_month_and_year

        try:
            response = await self._call_sync(
                get_price_changes_by_month_and_year.sync, symbol.upper(), client=self._client
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_price_changes_by_month_year_failed", error=str(e), symbol=symbol)
            raise

    # --- Shorts Module ---

    async def get_shorts_data(self, symbol: str) -> list[dict]:
        """Get short data for a ticker."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.shorts import get_data

        try:
            response = await self._call_sync(get_data.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_shorts_data_failed", error=str(e), symbol=symbol)
            raise

    async def get_short_interest_float(self, symbol: str) -> dict | None:
        """Get short interest as percentage of float."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.shorts import get_interest_float

        try:
            response = await self._call_sync(get_interest_float.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        except Exception as e:
            logger.error("uw_short_interest_float_failed", error=str(e), symbol=symbol)
            raise

    async def get_short_volumes_by_exchange(self, symbol: str) -> list[dict]:
        """Get short volumes by exchange."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.shorts import get_volumes_by_exchange

        try:
            response = await self._call_sync(get_volumes_by_exchange.sync, symbol.upper(), client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_short_volumes_by_exchange_failed", error=str(e), symbol=symbol)
            raise

    # --- Market Module ---

    async def get_market_spike(self) -> list[dict]:
        """Get market spike data."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_spike

        try:
            response = await self._call_sync(get_spike.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_market_spike_failed", error=str(e))
            raise

    async def get_sector_etfs(self) -> list[dict]:
        """Get current trading day statistics for SPDR sector ETFs."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.market import get_sector_etfs

        try:
            response = await self._call_sync(get_sector_etfs.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_sector_etfs_failed", error=str(e))
            raise

    # --- Stock Module (remaining) ---

    async def get_spot_exposures_by_expiry_strike(self, symbol: str, expiry: str) -> list[dict]:
        """Get spot GEX exposures by expiry and strike."""
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_spot_exposures_by_expiry_strike

        try:
            response = await self._call_sync(
                get_spot_exposures_by_expiry_strike.sync,
                symbol.upper(),
                expiry,
                client=self._client,
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error(
                "uw_spot_exposures_by_expiry_strike_failed",
                error=str(e),
                symbol=symbol,
                expiry=expiry,
            )
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
