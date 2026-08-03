"""UW Base mixin — shared init, helpers, constants, and normalization."""

import asyncio
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter
from typing import TYPE_CHECKING, Any

from unusualwhales.types import UNSET, Unset

from gateway.core.logger import logger
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
)

from .transient import _uw_error_context

if TYPE_CHECKING:
    from unusualwhales import UnusualWhalesClient

# Error message constants
ERR_NOT_INITIALIZED = "Provider not initialized"
DEFAULT_UW_MAX_INFLIGHT_CALLS = 32

# Timezone constants
TZ_UTC_SUFFIX = "+00:00"


def _or_unset[T](value: T | None) -> T | Unset:
    """Convert None to UNSET for SDK compatibility."""
    return UNSET if value is None else value


def _safe_int(value: Any) -> int:
    """Parse a value to int, handling float strings from the UW API.

    The UW API occasionally returns numeric fields as float strings
    (e.g., '12345.67' for volume). Bare int() crashes on these;
    int(float()) handles both '12345' and '12345.67' correctly.
    """
    if value is None or value == "":
        return 0
    return int(float(value))


def _safe_float(value: Any) -> float | None:
    """Parse a value to float, returning None for missing/empty values.

    The UW API returns numeric greek fields as strings (e.g. '-32823586.24').
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any, default: bool = False) -> bool:
    """Parse a value to bool, handling string 'false'/'true' from APIs.

    Python's bool("false") is True because non-empty strings are truthy.
    This function correctly handles string representations of booleans.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


class UWBaseMixin(DataProvider):
    """Base mixin providing init, lifecycle, helpers, and normalization for the UW provider."""

    def __init__(self):
        self._client: UnusualWhalesClient | None = None
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

    @staticmethod
    def _extract_http_status_code(exc: Exception) -> int | None:
        """Extract HTTP status code from HTTP-style exceptions when available."""
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        return status_code if isinstance(status_code, int) else None

    @staticmethod
    def _json_payload(response: Any) -> Any:
        """Parse JSON while preserving the response for failure logs."""
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            object.__setattr__(exc, "_uw_response", response)
            raise

    async def _raw_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Authenticated GET against the UW REST API, returning the JSON ``data`` payload.

        Shared primitive for endpoints not covered by the vendored SDK (v5.1). The SDK's
        httpx client already carries the base URL and bearer auth. ``None``-valued params
        are dropped; remaining values are stringified. Returns ``payload["data"]`` when the
        response is the standard ``{"data": ...}`` envelope, otherwise the raw JSON body.
        """
        if not self._client:
            logger.warning("uw_client_not_initialized", path=path)
            return []

        # Drop None params; pass lists/tuples through so httpx emits repeated keys
        # (e.g. ``ticker[]=AAPL&ticker[]=MSFT``). Scalars (str/int/float/bool) are left
        # for httpx to serialize — notably bool -> "true"/"false".
        clean: dict[str, Any] = {}
        for key, value in (params or {}).items():
            if value is None:
                continue
            clean[key] = [str(item) for item in value] if isinstance(value, list | tuple) else value
        try:
            http_client = self._client.get_httpx_client()
            response = await self._call_sync(http_client.get, path, params=clean)
            response.raise_for_status()
            payload = self._json_payload(response)
        except Exception as e:
            status_code = self._extract_http_status_code(e)
            context = _uw_error_context(e, provider_endpoint="raw_get", path=path)
            if status_code is not None and status_code >= 500:
                logger.warning("uw_raw_get_upstream_unavailable", **context)
            else:
                logger.error("uw_raw_get_failed", error=str(e), **context)
            raise

        if isinstance(payload, dict) and "data" in payload:
            return payload["data"]
        return payload

    def _parse_iv_rank_payload(self, symbol: str, payload: Any) -> NormalizedIVRank | None:
        """Parse raw IV-rank payload into canonical schema."""
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

        return NormalizedIVRank(
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
            date=get("date") or None,
            updated_at=get("updated_at") or None,
        )

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
                volume=_safe_int(get("volume") or get("size")),
                open_interest=_safe_int(get("open_interest") or get("oi")),
                side=str(get("side") or get("aggressor_side") or "mid").lower(),
                is_sweep=_safe_bool(get("has_sweep") or get("is_sweep") or get("sweep")),
                is_unusual=_safe_bool(get("is_unusual") or get("unusual")),
                sentiment=get("sentiment"),
                # Additional UW fields
                option_chain=get("option_chain"),
                price=price,
                underlying_price=underlying_price,
                alert_rule=get("alert_rule"),
                total_size=_safe_int(get("total_size")) if get("total_size") is not None else None,
                trade_count=_safe_int(get("trade_count")) if get("trade_count") is not None else None,
                volume_oi_ratio=volume_oi_ratio,
                total_ask_side_prem=ask_prem,
                total_bid_side_prem=bid_prem,
                all_opening_trades=_safe_bool(get("all_opening_trades")),
                has_floor=_safe_bool(get("has_floor")),
                has_multileg=_safe_bool(get("has_multileg")),
                has_singleleg=_safe_bool(get("has_singleleg"), default=True),
                expiry_count=_safe_int(get("expiry_count")) if get("expiry_count") is not None else None,
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
            size = _safe_int(get("size") or get("volume"))
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

            # Extract NBBO sizes
            nbbo_bid_size = None
            nbbo_ask_size = None
            if get("nbbo_bid_quantity") is not None:
                nbbo_bid_size = _safe_int(get("nbbo_bid_quantity"))
            if get("nbbo_ask_quantity") is not None:
                nbbo_ask_size = _safe_int(get("nbbo_ask_quantity"))

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
                nbbo_bid_size=nbbo_bid_size,
                nbbo_ask_size=nbbo_ask_size,
                ext_hours=get("ext_hour_sold_codes"),
                sale_cond_codes=get("sale_cond_codes"),
                trade_code=get("trade_code"),
                trade_settlement=get("trade_settlement"),
                canceled=_safe_bool(get("canceled")),
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
                net_volume = _safe_int(get("net_volume"))

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


if TYPE_CHECKING:
    # Feature mixins inherit this alias so type checkers can see the shared
    # UWBaseMixin surface (_client, _call_sync, _extract_data, _raw_get, ...).
    # It must have no runtime footprint: UnusualWhalesProvider lists UWBaseMixin
    # LAST in its bases, so any real base with members here (including a
    # Protocol with method stubs) would sit before UWBaseMixin in the MRO and
    # shadow the real implementations.
    _UWMixinBase = UWBaseMixin
else:
    _UWMixinBase = object
