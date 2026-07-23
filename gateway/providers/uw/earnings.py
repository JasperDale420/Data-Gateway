"""UW Earnings mixin — earnings calendar, stock screener."""

from decimal import Decimal

from gateway.core.logger import logger

from ._base import ERR_NOT_INITIALIZED, _or_unset, _safe_int, _UWMixinBase


class UWEarningsMixin(_UWMixinBase):
    """Mixin providing earnings calendar and stock screener endpoints."""

    async def get_earnings_premarket(self, date_str: str | None = None) -> list:
        """Get premarket earnings for a date."""
        from gateway.schemas import NormalizedEarnings

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        date=str(get("date") or get("earnings_date") or get("report_date") or ""),
                        time="premarket",
                        eps_estimate=(
                            Decimal(str(get("eps_estimate") or get("street_mean_est") or 0))
                            if (get("eps_estimate") or get("street_mean_est"))
                            else None
                        ),
                        eps_actual=(
                            Decimal(str(get("eps_actual") or get("actual_eps") or 0))
                            if (get("eps_actual") or get("actual_eps"))
                            else None
                        ),
                        revenue_estimate=(
                            Decimal(str(get("revenue_estimate") or 0)) if get("revenue_estimate") else None
                        ),
                        revenue_actual=(Decimal(str(get("revenue_actual") or 0)) if get("revenue_actual") else None),
                        expected_move=(
                            Decimal(str(get("expected_move"))) if get("expected_move") is not None else None
                        ),
                        expected_move_pct=(
                            Decimal(str(get("expected_move_perc"))) if get("expected_move_perc") is not None else None
                        ),
                        prior_close=(
                            Decimal(str(get("pre_earnings_close"))) if get("pre_earnings_close") is not None else None
                        ),
                        has_options=get("has_options"),
                        market_cap=(Decimal(str(get("marketcap"))) if get("marketcap") is not None else None),
                        sector=get("sector"),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_earnings_premarket_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_earnings_premarket_failed", error=str(e))
            raise

    async def get_earnings_afterhours(self, date_str: str | None = None) -> list:
        """Get afterhours earnings for a date."""
        from gateway.schemas import NormalizedEarnings

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        date=str(get("date") or get("earnings_date") or get("report_date") or ""),
                        time="afterhours",
                        eps_estimate=(
                            Decimal(str(get("eps_estimate") or get("street_mean_est") or 0))
                            if (get("eps_estimate") or get("street_mean_est"))
                            else None
                        ),
                        eps_actual=(
                            Decimal(str(get("eps_actual") or get("actual_eps") or 0))
                            if (get("eps_actual") or get("actual_eps"))
                            else None
                        ),
                        revenue_estimate=(
                            Decimal(str(get("revenue_estimate") or 0)) if get("revenue_estimate") else None
                        ),
                        revenue_actual=(Decimal(str(get("revenue_actual") or 0)) if get("revenue_actual") else None),
                        expected_move=(
                            Decimal(str(get("expected_move"))) if get("expected_move") is not None else None
                        ),
                        expected_move_pct=(
                            Decimal(str(get("expected_move_perc"))) if get("expected_move_perc") is not None else None
                        ),
                        prior_close=(
                            Decimal(str(get("pre_earnings_close"))) if get("pre_earnings_close") is not None else None
                        ),
                        has_options=get("has_options"),
                        market_cap=(Decimal(str(get("marketcap"))) if get("marketcap") is not None else None),
                        sector=get("sector"),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_earnings_afterhours_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_earnings_afterhours_failed", error=str(e))
            raise

    async def get_earnings_ticker(self, symbol: str) -> list:
        """Get historical earnings for a specific ticker."""
        from gateway.schemas import NormalizedEarnings

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        date=str(get("date") or get("earnings_date") or get("report_date") or ""),
                        time=str(get("time") or get("timing") or get("report_time") or "unknown"),
                        eps_estimate=(
                            Decimal(str(get("eps_estimate") or get("street_mean_est") or 0))
                            if (get("eps_estimate") or get("street_mean_est"))
                            else None
                        ),
                        eps_actual=(
                            Decimal(str(get("eps_actual") or get("actual_eps") or 0))
                            if (get("eps_actual") or get("actual_eps"))
                            else None
                        ),
                        revenue_estimate=(
                            Decimal(str(get("revenue_estimate") or 0)) if get("revenue_estimate") else None
                        ),
                        revenue_actual=(Decimal(str(get("revenue_actual") or 0)) if get("revenue_actual") else None),
                        expected_move=(
                            Decimal(str(get("expected_move"))) if get("expected_move") is not None else None
                        ),
                        expected_move_pct=(
                            Decimal(str(get("expected_move_perc"))) if get("expected_move_perc") is not None else None
                        ),
                        prior_close=(
                            Decimal(str(get("pre_earnings_close"))) if get("pre_earnings_close") is not None else None
                        ),
                        has_options=get("has_options"),
                        market_cap=(Decimal(str(get("marketcap"))) if get("marketcap") is not None else None),
                        sector=get("sector"),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_earnings_ticker_fetched", symbol=symbol, count=len(results))
            return results

        except Exception as e:
            logger.error("uw_earnings_ticker_failed", symbol=symbol, error=str(e))
            raise

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

    async def get_stock_screener(self, limit: int = 20) -> list:
        """Get stock screener results."""
        from gateway.schemas import NormalizedScreenerResult

        if not self._client:
            logger.warning("uw_client_not_initialized")
            raise RuntimeError(ERR_NOT_INITIALIZED)

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
                        price=(
                            Decimal(str(get("close") or get("price") or 0)) if (get("close") or get("price")) else None
                        ),
                        volume=_safe_int(get("volume")) if get("volume") else None,
                        market_cap=(
                            Decimal(str(get("marketcap") or get("market_cap") or 0))
                            if (get("marketcap") or get("market_cap"))
                            else None
                        ),
                        sector=get("sector"),
                        call_volume=_safe_int(get("call_volume")) if get("call_volume") else None,
                        put_volume=_safe_int(get("put_volume")) if get("put_volume") else None,
                        iv_rank=Decimal(str(get("iv_rank") or 0)) if get("iv_rank") else None,
                        # IV metrics
                        iv30d=(Decimal(str(get("iv30d"))) if get("iv30d") is not None else None),
                        iv30d_1d=(Decimal(str(get("iv30d_1d"))) if get("iv30d_1d") is not None else None),
                        iv30d_1w=(Decimal(str(get("iv30d_1w"))) if get("iv30d_1w") is not None else None),
                        iv30d_1m=(Decimal(str(get("iv30d_1m"))) if get("iv30d_1m") is not None else None),
                        volatility=(Decimal(str(get("volatility"))) if get("volatility") is not None else None),
                        # Flow / premium metrics
                        bearish_premium=(
                            Decimal(str(get("bearish_premium"))) if get("bearish_premium") is not None else None
                        ),
                        bullish_premium=(
                            Decimal(str(get("bullish_premium"))) if get("bullish_premium") is not None else None
                        ),
                        call_premium=(Decimal(str(get("call_premium"))) if get("call_premium") is not None else None),
                        put_premium=(Decimal(str(get("put_premium"))) if get("put_premium") is not None else None),
                        net_call_premium=(
                            Decimal(str(get("net_call_premium"))) if get("net_call_premium") is not None else None
                        ),
                        net_put_premium=(
                            Decimal(str(get("net_put_premium"))) if get("net_put_premium") is not None else None
                        ),
                        # OI data
                        call_open_interest=(
                            _safe_int(get("call_open_interest")) if get("call_open_interest") is not None else None
                        ),
                        put_open_interest=(
                            _safe_int(get("put_open_interest")) if get("put_open_interest") is not None else None
                        ),
                        total_open_interest=(
                            _safe_int(get("total_open_interest")) if get("total_open_interest") is not None else None
                        ),
                        prev_call_oi=(_safe_int(get("prev_call_oi")) if get("prev_call_oi") is not None else None),
                        prev_put_oi=(_safe_int(get("prev_put_oi")) if get("prev_put_oi") is not None else None),
                        # Volume breakdowns
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
                        # Average volume metrics
                        avg_30_day_call_volume=(
                            _safe_int(get("avg_30_day_call_volume"))
                            if get("avg_30_day_call_volume") is not None
                            else None
                        ),
                        avg_30_day_put_volume=(
                            _safe_int(get("avg_30_day_put_volume"))
                            if get("avg_30_day_put_volume") is not None
                            else None
                        ),
                        avg_3_day_call_volume=(
                            _safe_int(get("avg_3_day_call_volume"))
                            if get("avg_3_day_call_volume") is not None
                            else None
                        ),
                        avg_3_day_put_volume=(
                            _safe_int(get("avg_3_day_put_volume")) if get("avg_3_day_put_volume") is not None else None
                        ),
                        avg_7_day_call_volume=(
                            _safe_int(get("avg_7_day_call_volume"))
                            if get("avg_7_day_call_volume") is not None
                            else None
                        ),
                        avg_7_day_put_volume=(
                            _safe_int(get("avg_7_day_put_volume")) if get("avg_7_day_put_volume") is not None else None
                        ),
                        # Market data
                        prev_close=(Decimal(str(get("prev_close"))) if get("prev_close") is not None else None),
                        week_52_high=(Decimal(str(get("week_52_high"))) if get("week_52_high") is not None else None),
                        week_52_low=(Decimal(str(get("week_52_low"))) if get("week_52_low") is not None else None),
                        relative_volume=(
                            Decimal(str(get("relative_volume"))) if get("relative_volume") is not None else None
                        ),
                        implied_move=(Decimal(str(get("implied_move"))) if get("implied_move") is not None else None),
                        implied_move_perc=(
                            Decimal(str(get("implied_move_perc"))) if get("implied_move_perc") is not None else None
                        ),
                        put_call_ratio=(
                            Decimal(str(get("put_call_ratio"))) if get("put_call_ratio") is not None else None
                        ),
                        # Metadata
                        is_index=get("is_index"),
                        issue_type=get("issue_type"),
                        er_time=get("er_time"),
                        next_earnings_date=str(get("next_earnings_date")) if get("next_earnings_date") else None,
                        next_dividend_date=str(get("next_dividend_date")) if get("next_dividend_date") else None,
                        full_name=get("full_name"),
                        provider="unusual_whales",
                    )
                )

            logger.info("uw_stock_screener_fetched", count=len(results))
            return results

        except Exception as e:
            logger.error("uw_stock_screener_failed", error=str(e))
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
