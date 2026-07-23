"""UW Options mixin — option chains, contracts, greeks, OI, premium, max pain, IV, screener."""

import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from urllib.parse import quote

from gateway.core.logger import logger
from gateway.schemas import NormalizedIVRank

from ._base import ERR_NOT_INITIALIZED, TZ_UTC_SUFFIX, _or_unset, _safe_float, _safe_int, _UWMixinBase
from .transient import _uw_error_context, is_transient_upstream_error

# OCC option symbol: the contract type (C/P) is the single char immediately
# before the 8-digit strike at the end (e.g. AAPL260116C00190000 -> C).
_OCC_TYPE_RE = re.compile(r"[CP](?=\d{8}$)")


def _occ_contract_type(option_symbol: str | None) -> str | None:
    """Return 'call' or 'put' from an OCC option symbol, or None if unparseable."""
    if not option_symbol:
        return None
    match = _OCC_TYPE_RE.search(option_symbol.replace(" ", ""))
    if not match:
        return None
    return "call" if match.group() == "C" else "put"


def _parse_uw_timestamp(value: Any) -> datetime:
    if not value:
        return datetime.now(UTC)
    timestamp = datetime.fromisoformat(str(value).replace("Z", TZ_UTC_SUFFIX))
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


class UWOptionsMixin(_UWMixinBase):
    """Mixin providing options analytics, chains, contracts, greeks, and screener endpoints."""

    async def get_greek_exposure(self, symbol: str, date_str: str | None = None, timeframe: str | None = None) -> list:
        """Get Greek exposure (GEX) data for a ticker.

        ``timeframe`` widens the returned daily series (UW default 1Y; ``2Y``,
        ``3Y`` verified working). ``date`` alone returns nothing for historical
        dates, so backfill callers should pass ``timeframe`` rather than ``date``.
        """
        from gateway.schemas import NormalizedGreekExposure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_greek_exposure.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=_or_unset(date_str),
                timeframe=_or_unset(timeframe),
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                timestamp_str = get("timestamp") or get("date")
                timestamp = _parse_uw_timestamp(timestamp_str)
                results.append(
                    NormalizedGreekExposure(
                        symbol=symbol.upper(),
                        timestamp=timestamp,
                        call_gamma=Decimal(str(get("call_gamma") or 0)),
                        put_gamma=(Decimal(str(get("put_gamma") or 0)) if get("put_gamma") is not None else None),
                        call_delta=(Decimal(str(get("call_delta") or 0)) if get("call_delta") is not None else None),
                        put_delta=(Decimal(str(get("put_delta") or 0)) if get("put_delta") is not None else None),
                        call_vanna=(Decimal(str(get("call_vanna") or 0)) if get("call_vanna") is not None else None),
                        put_vanna=(Decimal(str(get("put_vanna") or 0)) if get("put_vanna") is not None else None),
                        call_charm=(Decimal(str(get("call_charm") or 0)) if get("call_charm") is not None else None),
                        put_charm=(Decimal(str(get("put_charm") or 0)) if get("put_charm") is not None else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_greek_exposure_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning(
                    "uw_greek_exposure_failed",
                    error=str(e),
                    **_uw_error_context(e, provider_endpoint="greek_exposure", symbol=symbol),
                )
            else:
                logger.error(
                    "uw_greek_exposure_failed",
                    error=str(e),
                    exc_info=True,
                    **_uw_error_context(e, provider_endpoint="greek_exposure", symbol=symbol),
                )
            raise

    async def get_greek_exposure_by_strike(self, symbol: str, date_str: str | None = None) -> list:
        """Get Greek exposure by strike price for a ticker."""
        from gateway.schemas import NormalizedGreekExposure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        call_gamma=Decimal(str(get("call_gamma") or 0)),
                        put_gamma=(Decimal(str(get("put_gamma") or 0)) if get("put_gamma") is not None else None),
                        call_delta=(Decimal(str(get("call_delta") or 0)) if get("call_delta") is not None else None),
                        put_delta=(Decimal(str(get("put_delta") or 0)) if get("put_delta") is not None else None),
                        call_vanna=(Decimal(str(get("call_vanna") or 0)) if get("call_vanna") is not None else None),
                        put_vanna=(Decimal(str(get("put_vanna") or 0)) if get("put_vanna") is not None else None),
                        call_charm=(Decimal(str(get("call_charm") or 0)) if get("call_charm") is not None else None),
                        put_charm=(Decimal(str(get("put_charm") or 0)) if get("put_charm") is not None else None),
                        strike=Decimal(str(get("strike") or 0)),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_greek_exposure_strike_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning(
                    "uw_greek_exposure_strike_failed",
                    error=str(e),
                    **_uw_error_context(e, provider_endpoint="greek_exposure_strike", symbol=symbol),
                )
            else:
                logger.error(
                    "uw_greek_exposure_strike_failed",
                    error=str(e),
                    exc_info=True,
                    **_uw_error_context(e, provider_endpoint="greek_exposure_strike", symbol=symbol),
                )
            raise

    async def get_greek_exposure_by_expiry(self, symbol: str, date_str: str | None = None) -> list:
        """Get Greek exposure by expiration date for a ticker."""
        from gateway.schemas import NormalizedGreekExposure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        call_gamma=Decimal(str(get("call_gamma") or 0)),
                        put_gamma=(Decimal(str(get("put_gamma") or 0)) if get("put_gamma") is not None else None),
                        call_delta=(Decimal(str(get("call_delta") or 0)) if get("call_delta") is not None else None),
                        put_delta=(Decimal(str(get("put_delta") or 0)) if get("put_delta") is not None else None),
                        call_vanna=(Decimal(str(get("call_vanna") or 0)) if get("call_vanna") is not None else None),
                        put_vanna=(Decimal(str(get("put_vanna") or 0)) if get("put_vanna") is not None else None),
                        call_charm=(Decimal(str(get("call_charm") or 0)) if get("call_charm") is not None else None),
                        put_charm=(Decimal(str(get("put_charm") or 0)) if get("put_charm") is not None else None),
                        expiry=str(get("expiry") or get("expiration_date") or ""),
                        dte=_safe_int(get("dte")) if get("dte") is not None else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_greek_exposure_expiry_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_greek_exposure_expiry_failed", symbol=symbol, error=str(e))
            raise

    async def get_hottest_chains(self, limit: int = 20) -> list:
        """Get hottest option chains/contracts."""
        from gateway.schemas import NormalizedHottestChain

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        volume=_safe_int(get("volume")),
                        open_interest=_safe_int(get("open_interest") or get("oi")),
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
            raise

    async def get_net_premium_ticks(self, symbol: str, date_str: str | None = None) -> list:
        """Get net premium ticks for a ticker."""
        from gateway.schemas import NormalizedNetPremiumTick

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                timestamp = _parse_uw_timestamp(timestamp_str)
                results.append(
                    NormalizedNetPremiumTick(
                        symbol=symbol.upper(),
                        timestamp=timestamp,
                        net_call_premium=Decimal(str(get("net_call_premium") or get("call_premium") or 0)),
                        net_put_premium=Decimal(str(get("net_put_premium") or get("put_premium") or 0)),
                        call_volume=_safe_int(get("call_volume")),
                        put_volume=_safe_int(get("put_volume")),
                        net_delta=(Decimal(str(get("net_delta"))) if get("net_delta") is not None else None),
                        net_call_volume=(
                            _safe_int(get("net_call_volume")) if get("net_call_volume") is not None else None
                        ),
                        net_put_volume=(
                            _safe_int(get("net_put_volume")) if get("net_put_volume") is not None else None
                        ),
                        call_volume_ask_side=(
                            _safe_int(get("call_volume_ask_side")) if get("call_volume_ask_side") is not None else None
                        ),
                        call_volume_bid_side=(
                            _safe_int(get("call_volume_bid_side")) if get("call_volume_bid_side") is not None else None
                        ),
                        put_volume_ask_side=(
                            _safe_int(get("put_volume_ask_side")) if get("put_volume_ask_side") is not None else None
                        ),
                        put_volume_bid_side=(
                            _safe_int(get("put_volume_bid_side")) if get("put_volume_bid_side") is not None else None
                        ),
                        tape_time=get("tape_time"),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_net_premium_ticks_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_net_premium_ticks_failed", symbol=symbol, error=str(e))
            raise

    async def get_max_pain(self, symbol: str, expiry: str | None = None) -> list:
        """Get max pain data for a ticker."""
        from gateway.schemas import NormalizedMaxPain

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        call_oi=_safe_int(get("call_oi")) if get("call_oi") else None,
                        put_oi=_safe_int(get("put_oi")) if get("put_oi") else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_max_pain_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_max_pain_failed", symbol=symbol, error=str(e))
            raise

    async def get_iv_rank(self, symbol: str, date_str: str | None = None) -> NormalizedIVRank | None:
        """Get IV rank for a ticker.

        Args:
            symbol: Stock ticker symbol
            date_str: Date in YYYY-MM-DD format (defaults to today)
        """

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            # The SDK response parser for this endpoint is incompatible with
            # the live payload shape, so fetch raw JSON directly.
            path = f"/api/stock/{quote(symbol.upper(), safe='')}/iv-rank"
            http_client = self._client.get_httpx_client()
            params = {"date": date_str} if date_str else None

            try:
                response = await self._call_sync(
                    http_client.get,
                    path,
                    params=params,
                )
                response.raise_for_status()
                payload = self._json_payload(response)
            except Exception as e:
                status_code = self._extract_http_status_code(e)
                if date_str and status_code == 422:
                    logger.warning(
                        "uw_iv_rank_date_unprocessable_retrying_without_date",
                        symbol=symbol,
                        date=date_str,
                        status_code=status_code,
                    )
                    response = await self._call_sync(
                        http_client.get,
                        path,
                    )
                    response.raise_for_status()
                    payload = self._json_payload(response)
                else:
                    raise

            result = self._parse_iv_rank_payload(symbol=symbol, payload=payload)
            if not result:
                return None

            logger.info("uw_iv_rank_fetched", symbol=symbol)
            return result

        except Exception as e:
            status_code = self._extract_http_status_code(e)
            if status_code == 422:
                logger.warning(
                    "uw_iv_rank_unprocessable",
                    symbol=symbol,
                    date=date_str,
                    status_code=status_code,
                    error=str(e),
                )
                return None  # Keep returning None for 422 specifically
            else:
                logger.error(
                    "uw_iv_rank_failed",
                    error=str(e),
                    **_uw_error_context(e, provider_endpoint="iv_rank", path=path, symbol=symbol),
                )
                raise

    async def get_oi_change(self, symbol: str, date_str: str | None = None) -> list:
        """Get OI change data for a ticker."""
        from gateway.schemas import NormalizedOIChange

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                # Each upstream row is ONE option contract (a call or a put), keyed
                # by its OCC option_symbol. Route its open interest to the correct
                # side so a sum over rows is a real call-vs-put aggregate. The
                # absolute OI change is ``oi_diff_plain`` (curr_oi - last_oi);
                # ``oi_change`` upstream is a ratio, not a contract count — never sum it.
                # Unparseable symbols fall back to the call side (option_symbol is
                # always present in practice).
                is_put = _occ_contract_type(get("option_symbol")) == "put"
                curr_oi = int(float(get("curr_oi") or get("call_oi") or 0))
                oi_diff = int(float(get("oi_diff_plain") or 0))
                results.append(
                    NormalizedOIChange(
                        symbol=symbol.upper(),
                        date=str(get("date") or get("curr_date") or ""),
                        call_oi=0 if is_put else curr_oi,
                        put_oi=curr_oi if is_put else 0,
                        call_oi_change=oi_diff if not is_put else 0,
                        put_oi_change=oi_diff if is_put else 0,
                        avg_price=(Decimal(str(get("avg_price"))) if get("avg_price") is not None else None),
                        prev_oi=(int(float(get("last_oi") or 0)) if get("last_oi") is not None else None),
                        option_symbol=get("option_symbol"),
                        volume=(int(float(get("volume") or 0)) if get("volume") is not None else None),
                        trades=(int(float(get("trades") or 0)) if get("trades") is not None else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_oi_change_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_oi_change_failed", symbol=symbol, error=str(e))
            raise

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
                return []

            # UW's volume-oi-expiry rows carry no date/timestamp -- only expires/volume/oi.
            # Anchor ts_event to the snapshot day (date_str for backfill, today for the
            # live EOD poller); never stamp wall-clock now(), which makes ts_event -- and
            # so the event_id -- drift on every re-fetch, duplicating this per-day snapshot
            # at Heber.
            # ponytail: today() on the live path is day-granular and stable per fetch-day;
            # pass an explicit ET trading date from the poller if the UTC-midnight boundary
            # ever matters.
            snapshot_date = str(date_str or datetime.now(UTC).date().isoformat())
            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                raw_ts = get("timestamp")
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "timestamp": str(raw_ts) if raw_ts else snapshot_date,
                        "date": str(get("date") or snapshot_date),
                        # UW returns the expiry as `expires`; reading only `expiry` yielded
                        # None for every row, collapsing all per-expiry rows to one id.
                        "expiry": get("expiry") or get("expires"),
                        "volume": _safe_int(get("volume")) if get("volume") is not None else 0,
                        "open_interest": (
                            _safe_int(get("open_interest") or get("oi"))
                            if get("open_interest") is not None or get("oi") is not None
                            else None
                        ),
                        "call_volume": _safe_int(get("call_volume")) if get("call_volume") is not None else None,
                        "put_volume": _safe_int(get("put_volume")) if get("put_volume") is not None else None,
                        "premium": float(get("premium") or 0) if get("premium") is not None else None,
                    }
                )

            logger.info("uw_historic_option_volume_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_historic_option_volume_failed", symbol=symbol, error=str(e))
            raise

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
                date=_or_unset(date_str),
            )
            data = self._get_data_safe(response)
            if not data:
                return []

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
                        "volume": _safe_int(get("volume")) if get("volume") else None,
                    }
                )

            logger.info("uw_intraday_option_data_fetched", contract=contract_id, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_intraday_option_data_failed", contract=contract_id, error=str(e))
            raise

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
                min_premium=_or_unset(min_premium),
            )
            data = self._get_data_safe(response)
            if not data:
                return []

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
                        "volume": _safe_int(get("volume")) if get("volume") else None,
                        "open_interest": (
                            _safe_int(get("open_interest") or get("oi")) if get("open_interest") or get("oi") else None
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
            raise

    async def get_iv_surface(self, symbol: str) -> list[dict[str, Any]]:
        """Get implied volatility surface for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                cast(
                    Any, stock
                ).get_implied_volatility_surface.sync,  # not in vendored UW SDK v5.1 (docs/FOLLOW_UPS.md)
                client=self._client,
                ticker=symbol.upper(),
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
                        "dte": _safe_int(get("dte")) if get("dte") else None,
                    }
                )

            logger.info("uw_iv_surface_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_iv_surface_failed", symbol=symbol, error=str(e))
            raise

    async def get_nope(self, symbol: str) -> dict[str, Any]:
        """Get NOPE (Net Options Pricing Effect) for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_nope.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                return {}

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
            raise

    async def get_put_call_ratio(self, symbol: str) -> list[dict[str, Any]]:
        """Get historical put/call ratio for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                cast(Any, stock).get_put_call_ratio.sync,  # not in vendored UW SDK v5.1 (docs/FOLLOW_UPS.md)
                client=self._client,
                ticker=symbol.upper(),
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
                        "date": str(get("date") or ""),
                        "put_call_ratio": (
                            float(get("put_call_ratio") or get("pcr") or 0)
                            if get("put_call_ratio") or get("pcr")
                            else None
                        ),
                        "put_volume": _safe_int(get("put_volume")) if get("put_volume") else None,
                        "call_volume": _safe_int(get("call_volume")) if get("call_volume") else None,
                    }
                )

            logger.info("uw_put_call_ratio_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_put_call_ratio_failed", symbol=symbol, error=str(e))
            raise

    async def get_spot_exposures_by_strike(self, symbol: str) -> list[dict[str, Any]]:
        """Get spot greek (gamma/charm/vanna/delta) exposures per strike.

        UW's ``/stock/{ticker}/spot-exposures/strike`` returns many strike rows, but the
        generated SDK model is a single-row shape, so the rows arrive under
        ``additional_properties['data']``. ``_extract_data`` reads that location; using
        ``_get_data_safe`` (``.data`` only) would silently return [].
        """
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                stock.get_spot_exposures_by_strike.sync, client=self._client, ticker=symbol.upper()
            )

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "strike": _safe_float(get("strike")),
                        "price": _safe_float(get("price")),
                        "date": str(get("date") or ""),
                        "timestamp": str(get("time") or ""),
                        "call_gamma_oi": _safe_float(get("call_gamma_oi")),
                        "call_gamma_vol": _safe_float(get("call_gamma_vol")),
                        "put_gamma_oi": _safe_float(get("put_gamma_oi")),
                        "put_gamma_vol": _safe_float(get("put_gamma_vol")),
                        "call_charm_oi": _safe_float(get("call_charm_oi")),
                        "call_charm_vol": _safe_float(get("call_charm_vol")),
                        "put_charm_oi": _safe_float(get("put_charm_oi")),
                        "put_charm_vol": _safe_float(get("put_charm_vol")),
                        "call_vanna_oi": _safe_float(get("call_vanna_oi")),
                        "call_vanna_vol": _safe_float(get("call_vanna_vol")),
                        "put_vanna_oi": _safe_float(get("put_vanna_oi")),
                        "put_vanna_vol": _safe_float(get("put_vanna_vol")),
                        "call_delta_oi": _safe_float(get("call_delta_oi")),
                        "call_delta_vol": _safe_float(get("call_delta_vol")),
                        "put_delta_oi": _safe_float(get("put_delta_oi")),
                        "put_delta_vol": _safe_float(get("put_delta_vol")),
                    }
                )

            logger.info("uw_spot_exposures_strike_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            if is_transient_upstream_error(e):
                logger.warning("uw_spot_exposures_strike_failed", symbol=symbol, error=str(e))
            else:
                logger.error("uw_spot_exposures_strike_failed", symbol=symbol, error=str(e), exc_info=True)
            raise

    async def get_option_volume_levels(self, symbol: str) -> list[dict[str, Any]]:
        """Get option volume levels per price."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(
                cast(Any, stock).get_option_volume_levels.sync,  # not in vendored UW SDK v5.1 (docs/FOLLOW_UPS.md)
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
                        "price": float(get("price") or 0) if get("price") else None,
                        "call_volume": _safe_int(get("call_volume")) if get("call_volume") else None,
                        "put_volume": _safe_int(get("put_volume")) if get("put_volume") else None,
                        "total_volume": (_safe_int(get("total_volume")) if get("total_volume") else None),
                    }
                )

            logger.info("uw_option_volume_levels_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_option_volume_levels_failed", symbol=symbol, error=str(e))
            raise

    async def get_volume_profile(self, contract_id: str) -> list[dict[str, Any]]:
        """Get volume profile of an option contract by fill price."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import contract

            response = await self._call_sync(
                cast(Any, contract).get_volume_profile.sync,  # not in vendored UW SDK v5.1 (docs/FOLLOW_UPS.md)
                client=self._client,
                option_symbol=contract_id,
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
                        "volume": _safe_int(get("volume")) if get("volume") else None,
                        "percentage": (
                            float(get("percentage") or get("pct") or 0) if get("percentage") or get("pct") else None
                        ),
                    }
                )

            logger.info("uw_volume_profile_fetched", contract=contract_id, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_volume_profile_failed", contract=contract_id, error=str(e))
            raise

    async def get_interpolated_iv(self, symbol: str) -> list[dict[str, Any]]:
        """Get interpolated implied volatility for a ticker."""
        if not self._client:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import stock

            response = await self._call_sync(stock.get_interpolated_iv.sync, client=self._client, ticker=symbol.upper())
            data = self._get_data_safe(response)
            if not data:
                return []

            results = []
            for item in data:
                get = item.get if isinstance(item, dict) else lambda k, d=None, i=item: getattr(i, k, d)
                results.append(
                    {
                        "symbol": symbol.upper(),
                        "dte": _safe_int(get("dte")) if get("dte") else None,
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
            raise

    # --- Phase 4 thin wrappers ---

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

    async def get_options_volume(self, symbol: str, limit: int | None = None) -> list[dict]:
        """Get daily options volume and premium series.

        ``limit`` returns that many trailing trading days (UW caps ~500 ≈ 2yr);
        omit for the default single latest day.
        """
        if not self._initialized:
            raise RuntimeError(ERR_NOT_INITIALIZED)

        from unusualwhales.api.stock import get_options_volume

        try:
            response = await self._call_sync(
                get_options_volume.sync, symbol.upper(), client=self._client, limit=_or_unset(limit)
            )
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_options_volume_failed", error=str(e), symbol=symbol)
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
