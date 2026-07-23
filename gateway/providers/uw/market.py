"""UW Market mixin — shorts, volatility, seasonality, alerts, market data, news, calendar."""

import math
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from gateway.core.logger import logger

from ._base import ERR_NOT_INITIALIZED, _or_unset, _safe_int, _UWMixinBase


def _compute_realized_vol(prices: list[float], window: int) -> Decimal | None:
    """Compute annualised realized volatility from a price series.

    Uses close-to-close log returns over the trailing `window` observations
    and annualises by sqrt(252) (US-equity trading days). Returns `None`
    when there are not enough observations.

    Used as a fallback when the UW realized-volatility endpoint does not
    expose a pre-computed window matching the schema's 30d / 60d / 90d
    fields, so that the gateway still emits non-empty stats instead of
    silently dropping the row.
    """
    if len(prices) <= window:
        return None
    tail = prices[-(window + 1) :]
    log_returns: list[float] = []
    for prev, curr in zip(tail, tail[1:], strict=False):
        if prev <= 0 or curr <= 0:
            return None
        log_returns.append(math.log(curr / prev))
    if not log_returns:
        return None
    mean = sum(log_returns) / len(log_returns)
    variance = sum((r - mean) ** 2 for r in log_returns) / max(1, len(log_returns) - 1)
    sigma = math.sqrt(variance) * math.sqrt(252)
    return Decimal(str(sigma))


class UWMarketMixin(_UWMixinBase):
    """Shorts, volatility, seasonality, alerts, market info, sector, news, economic calendar."""

    # ─────────────────────────────────────────────────────────────────
    # Phase 2: Shorts Data
    # ─────────────────────────────────────────────────────────────────

    async def get_short_interest(self, symbol: str) -> list:
        """Get short interest data."""
        from gateway.schemas import NormalizedShortData

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import shorts

            # shorts.get_data returns borrow/rebate data (short_shares_available,
            # fee_rate) — NOT short interest, so the NormalizedShortData fields all
            # came back empty. The actual short-interest record (shares shorted,
            # days-to-cover, % float) is the interest-float endpoint, which returns
            # ONE latest snapshot (no history / no date param).
            response = await self._call_sync(
                shorts.get_interest_float.sync,
                symbol.upper(),
                client=self._client,
            )

            if response is None:
                logger.info("uw_short_interest_fetched", symbol=symbol, count=0)
                return []

            record = (
                response.to_dict() if hasattr(response, "to_dict") else (response if isinstance(response, dict) else {})
            )
            get = record.get
            # interest-float returns an all-UNSET record on UW tiers that don't
            # include it (observed empty even for high-SI names). Emit nothing
            # rather than a misleading all-zero short-interest row.
            if not (get("si_float_returned") or get("percent_returned") or get("days_to_cover_returned")):
                logger.info("uw_short_interest_fetched", symbol=symbol, count=0)
                return []
            results = [
                NormalizedShortData(
                    symbol=symbol.upper(),
                    date=str(get("market_date") or get("date") or ""),
                    short_interest=_safe_int(get("si_float_returned")),
                    days_to_cover=(
                        Decimal(str(get("days_to_cover_returned"))) if get("days_to_cover_returned") else None
                    ),
                    short_percent_float=(Decimal(str(get("percent_returned"))) if get("percent_returned") else None),
                    # interest-float does not report % of shares outstanding.
                    short_percent_outstanding=None,
                    provider="unusual_whales",
                )
            ]

            logger.info("uw_short_interest_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_short_interest_failed", symbol=symbol, error=str(e))
            raise

    async def get_ftds(self, symbol: str) -> list:
        """Get failures to deliver data."""
        from gateway.schemas import NormalizedFTD

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                quantity = _safe_int(get("quantity") or get("fails"))
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
            raise

    async def get_short_volume(self, symbol: str) -> list:
        """Get short volume data."""
        from gateway.schemas import NormalizedShortData

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import shorts

            response = await self._call_sync(
                shorts.get_volume_and_ratio.sync,
                symbol.upper(),
                client=self._client,
            )

            # The volume-and-ratio endpoint returns rows under the 'si' key, not
            # 'data'/'.data', so _extract_data alone returns [] on every call.
            data_items = self._extract_data(response)
            if not data_items and getattr(response, "additional_properties", None):
                data_items = response.additional_properties.get("si") or []

            results = []
            for item in data_items:
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                short_ratio = get("short_volume_ratio") or get("short_ratio")
                results.append(
                    NormalizedShortData(
                        symbol=symbol.upper(),
                        date=str(get("market_date") or get("date") or ""),
                        short_interest=_safe_int(get("short_volume")),
                        short_percent_float=(Decimal(str(short_ratio)) if short_ratio else None),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_short_volume_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_short_volume_failed", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Volatility Analytics
    # ─────────────────────────────────────────────────────────────────

    async def get_iv_term_structure(self, symbol: str) -> list:
        """Get IV term structure for a ticker."""
        from gateway.schemas import NormalizedIVTermStructure

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        # The UW term-structure endpoint carries the ATM IV in
                        # "volatility" (see ImpliedVolatilityTermStructure in the
                        # vendored SDK); the old "iv"/"implied_volatility" keys
                        # never exist, which stored a literal 0 on every row.
                        iv=Decimal(str(get("volatility") or get("iv") or get("implied_volatility") or 0)),
                        days_to_expiry=_safe_int(get("days_to_expiry") or get("dte")),
                        # No per-side IVs on this endpoint — these stay None.
                        call_iv=Decimal(str(get("call_iv") or 0)) if get("call_iv") else None,
                        put_iv=Decimal(str(get("put_iv") or 0)) if get("put_iv") else None,
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_iv_term_structure_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_iv_term_structure_failed", symbol=symbol, error=str(e))
            raise

    async def get_realized_volatility(self, symbol: str, date: str | None = None):
        """Get realized volatility for a ticker.

        Previously wired to `stock.get_candles.sync`, which returns OHLCV
        rows without any `realized_vol_30d/60d/90d` fields — every record
        produced an empty `NormalizedVolatilityStats`. Now hits the
        dedicated UW endpoint `GET /api/stock/{ticker}/volatility/realized`
        (`unusualwhales.api.stock.get_realized_volatility`), which returns
        a daily series of `{date, price, realized_volatility,
        implied_volatility}`.

        The schema's 30d/60d/90d split is filled by:

          * trying common pre-computed shapes the vendor sometimes emits
            (`rv_30`, `realized_vol_30d`) on the most recent record, and
          * falling back to a client-side close-to-close log-return
            calculation over the returned price series when the vendor
            does not emit pre-aggregated windows.

        Annualisation uses sqrt(252). `date` defaults to today's UTC date
        because the UW endpoint returns history up to that requested date.
        """
        from gateway.schemas import NormalizedVolatilityStats

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api.stock import get_realized_volatility

            target_date = date or datetime.now(UTC).strftime("%Y-%m-%d")

            response = await self._call_sync(
                get_realized_volatility.sync,
                client=self._client,
                ticker=symbol.upper(),
                date=target_date,
            )

            # `sync` returns `List[RealizedVolatility] | ErrorMessage | None`.
            # Coerce to a list of dicts for uniform access.
            if response is None:
                logger.info("uw_realized_volatility_no_data", symbol=symbol, date=target_date)
                return None
            if not isinstance(response, list):
                logger.warning(
                    "uw_realized_volatility_unexpected_response_type",
                    symbol=symbol,
                    response_type=type(response).__name__,
                )
                return None
            rows: list[dict[str, Any]] = [
                row.to_dict() if hasattr(row, "to_dict") else row for row in response if row is not None
            ]
            if not rows:
                return None

            # Order ascending so the trailing window is at the tail.
            rows.sort(key=lambda r: str(r.get("date") or ""))
            prices: list[float] = []
            for row in rows:
                price = row.get("price")
                if price in (None, ""):
                    continue
                try:
                    prices.append(float(price))
                except (TypeError, ValueError):
                    continue

            latest = rows[-1]

            def _vendor_window(*keys: str) -> Decimal | None:
                for key in keys:
                    val = latest.get(key)
                    if val not in (None, ""):
                        return Decimal(str(val))
                return None

            # Prefer vendor pre-aggregated values, fall back to client-side
            # computation so the schema slots are never silently empty.
            rv_30 = _vendor_window("realized_vol_30d", "rv_30") or _compute_realized_vol(prices, 30)
            rv_60 = _vendor_window("realized_vol_60d", "rv_60") or _compute_realized_vol(prices, 60)
            rv_90 = _vendor_window("realized_vol_90d", "rv_90") or _compute_realized_vol(prices, 90)

            result = NormalizedVolatilityStats(
                symbol=symbol.upper(),
                realized_vol_30d=rv_30,
                realized_vol_60d=rv_60,
                realized_vol_90d=rv_90,
                provider="unusual_whales",
            )

            logger.info(
                "uw_realized_volatility_fetched",
                symbol=symbol,
                date=target_date,
                rows=len(rows),
                rv30_source="vendor" if _vendor_window("realized_vol_30d", "rv_30") else "computed",
            )
            return result

        except Exception as e:
            logger.error("uw_realized_volatility_failed", symbol=symbol, error=str(e))
            raise

    async def get_volatility_stats(self, symbol: str):
        """Get volatility stats for a ticker."""
        from gateway.schemas import NormalizedVolatilityStats

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
            raise

    # ─────────────────────────────────────────────────────────────────
    # Phase 3: Seasonality
    # ─────────────────────────────────────────────────────────────────

    async def get_market_seasonality(self) -> list:
        """Get market-wide seasonality data."""
        from gateway.schemas import NormalizedSeasonality

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

        try:
            from unusualwhales.api import seasonality

            response = await self._call_sync(seasonality.get_market_average_returns_by_month.sync, client=self._client)

            results = []
            for item in self._extract_data(response):
                get = item.get if isinstance(item, dict) else lambda k, d=None, _item=item: getattr(_item, k, d)
                results.append(
                    NormalizedSeasonality(
                        symbol=None,
                        month=_safe_int(get("month")),
                        avg_return=Decimal(str(get("avg_return") or get("average_return") or 0)),
                        median_return=(Decimal(str(get("median_return") or 0)) if get("median_return") else None),
                        win_rate=Decimal(str(get("win_rate") or get("positive_rate") or 0)),
                        sample_years=(
                            _safe_int(get("sample_years") or get("years"))
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
            raise

    async def get_monthly_returns(self, symbol: str) -> list:
        """Get monthly returns for a ticker."""
        from gateway.schemas import NormalizedSeasonality

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        month=_safe_int(get("month")),
                        avg_return=Decimal(str(get("avg_return") or get("average_return") or 0)),
                        median_return=(Decimal(str(get("median_return") or 0)) if get("median_return") else None),
                        win_rate=Decimal(str(get("win_rate") or get("positive_rate") or 0)),
                        sample_years=(
                            _safe_int(get("sample_years") or get("years"))
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
            raise

    # ─────────────────────────────────────────────────────────────────
    # Economic Calendar
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
                return []

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
            raise

    # ─────────────────────────────────────────────────────────────────
    # Custom Alerts / Market Correlations
    # ─────────────────────────────────────────────────────────────────

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
                return []

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
                        "volume": _safe_int(get("volume")) if get("volume") else None,
                        "rule": get("rule_name") or get("alert_type"),
                        "timestamp": str(get("timestamp") or get("date") or ""),
                    }
                )

            logger.info("uw_custom_alerts_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_custom_alerts_failed", error=str(e))
            raise

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
                start_date=_or_unset(start_date),
                end_date=_or_unset(end_date),
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
            raise

    # ─────────────────────────────────────────────────────────────────
    # News Headlines
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Phase 2: Market Calendar & Data Endpoints
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Phase 5: Screener/Alerts Endpoints
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Phase 4: Stock Module - Additional Endpoints
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Sector Tickers / Stock State / Volume Price Levels
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Final Batch: Seasonality Module
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Final Batch: Shorts Module
    # ─────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────
    # Final Batch: Market Module
    # ─────────────────────────────────────────────────────────────────

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

        # get_sector_etfs is not in the vendored UW SDK v5.1 (docs/FOLLOW_UPS.md)
        from unusualwhales.api import market

        try:
            response = await self._call_sync(cast(Any, market).get_sector_etfs.sync, client=self._client)
            data = self._extract_data(response)
            return data
        except Exception as e:
            logger.error("uw_sector_etfs_failed", error=str(e))
            raise
