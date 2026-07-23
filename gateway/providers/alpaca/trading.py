"""Alpaca trading mixin — account, orders, positions, portfolio, watchlists, clock, calendar."""

from datetime import date, datetime
from typing import Any

import httpx
from alpaca.common.enums import Sort
from alpaca.common.exceptions import APIError
from alpaca.trading.enums import (
    AssetClass,
    AssetExchange,
    AssetStatus,
    OrderSide,
    PositionIntent,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.requests import (
    ClosePositionRequest,
    CreateWatchlistRequest,
    GetAssetsRequest,
    GetCalendarRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLimitOrderRequest,
    StopLossRequest,
    StopOrderRequest,
    TakeProfitRequest,
    UpdateWatchlistRequest,
)
from fastapi import HTTPException

from gateway.core.logger import logger
from gateway.providers.alpaca._base import ERR_TRADING_CLIENT_NOT_INITIALIZED, _AlpacaMixinBase

# Alpaca error codes that indicate a terminal (non-retryable) state.
# 40410000 — position does not exist (already closed/expired/never existed)
_POSITION_NOT_FOUND_CODES = {40410000}

# Alpaca error codes for a cancel that lost a benign race with the order's own
# terminal transition — the order already reached a terminal state (filled,
# canceled, expired, rejected) before the cancel landed. Expected during active
# trading, NOT an error. We still re-raise so the caller learns the true state
# (e.g. a fill), but logging these at ERROR floods the error log with thousands
# of non-actionable lines.
# 42210000 — "order is already in \"{state}\" state"
_BENIGN_CANCEL_RACE_CODES = {42210000}


class AlpacaTradingMixin(_AlpacaMixinBase):
    """Trading/account/order management methods."""

    @staticmethod
    def _extract_alpaca_error_code(exc: APIError) -> int | None:
        """Extract the numeric Alpaca error code from an APIError, or None if unparseable."""
        # nosemgrep: empire-no-bare-exception,empire-no-return-none-for-failure -- documented Optional helper: SDK error internals vary; unparseable code yields None
        try:
            return exc.code
        except Exception:
            return None

    @staticmethod
    def _is_benign_cancel_race(exc: APIError) -> bool:
        """True only when a cancel lost a benign race with the order's own
        terminal transition (already filled/canceled/expired/rejected).

        Requires BOTH the terminal error code AND the "already in ... state"
        message signature — Alpaca reuses 42210000 for other rejections, so a
        code-only match could hide an actionable cancel failure at INFO.
        """
        if AlpacaTradingMixin._extract_alpaca_error_code(exc) not in _BENIGN_CANCEL_RACE_CODES:
            return False
        return "already in" in str(exc).lower()

    def get_account(self) -> dict[str, Any]:
        """Get account information."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            account = self._trading_client.get_account()
            data = self._model_to_dict(account)
            logger.info("alpaca_account_fetched", status=data.get("status"))
            return data
        except APIError as e:
            logger.error("alpaca_account_error", error=str(e))
            raise

    def create_order(
        self,
        symbol: str,
        qty: float | None = None,
        notional: float | None = None,
        side: str = "buy",
        order_type: str = "market",
        time_in_force: str = "day",
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
        extended_hours: bool = False,
        order_class: str | None = None,
        take_profit_limit_price: float | None = None,
        stop_loss_stop_price: float | None = None,
        stop_loss_limit_price: float | None = None,
        position_intent: str | None = None,
    ) -> dict[str, Any]:
        """Create a new order using SDK."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        # Map string enums to SDK enums
        side_lower = side.lower()
        if side_lower not in ("buy", "sell"):
            raise ValueError(f"Invalid order side: {side!r}. Must be 'buy' or 'sell'.")
        order_side = OrderSide.BUY if side_lower == "buy" else OrderSide.SELL

        # Optional position_intent (buy_to_open / buy_to_close / sell_to_open /
        # sell_to_close). Lets callers force reduce-only semantics so Alpaca
        # never converts a close into an opening (e.g. naked short) position.
        pi: PositionIntent | None = None
        if position_intent is not None:
            try:
                pi = PositionIntent(position_intent.lower())
            except ValueError as e:
                raise ValueError(
                    f"Invalid position_intent: {position_intent!r}. Must be one of {[e.value for e in PositionIntent]}."
                ) from e
        tif_map = {
            "day": TimeInForce.DAY,
            "gtc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK,
            "opg": TimeInForce.OPG,
            "cls": TimeInForce.CLS,
        }
        tif = tif_map.get(time_in_force.lower(), TimeInForce.DAY)

        # Build bracket sub-objects when order_class is set
        tp_request = None
        sl_request = None
        if order_class and order_class.lower() in ("bracket", "oco", "oto"):
            if take_profit_limit_price is not None:
                tp_request = TakeProfitRequest(limit_price=take_profit_limit_price)
            if stop_loss_stop_price is not None:
                sl_request = StopLossRequest(
                    stop_price=stop_loss_stop_price,
                    limit_price=stop_loss_limit_price,
                )

        from alpaca.trading.enums import OrderClass as AlpacaOrderClass

        oc = AlpacaOrderClass(order_class.lower()) if order_class else None

        try:
            request: MarketOrderRequest | LimitOrderRequest | StopOrderRequest | StopLimitOrderRequest
            if order_type.lower() == "market":
                request = MarketOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    notional=notional,
                    side=order_side,
                    time_in_force=tif,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                    order_class=oc,
                    take_profit=tp_request,
                    stop_loss=sl_request,
                    position_intent=pi,
                )
            elif order_type.lower() == "limit":
                request = LimitOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    notional=notional,
                    side=order_side,
                    time_in_force=tif,
                    limit_price=limit_price,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                    order_class=oc,
                    take_profit=tp_request,
                    stop_loss=sl_request,
                    position_intent=pi,
                )
            elif order_type.lower() == "stop":
                request = StopOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    notional=notional,
                    side=order_side,
                    time_in_force=tif,
                    stop_price=stop_price,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                    position_intent=pi,
                )
            elif order_type.lower() == "stop_limit":
                request = StopLimitOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    side=order_side,
                    time_in_force=tif,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    extended_hours=extended_hours,
                    client_order_id=client_order_id,
                    position_intent=pi,
                )
            else:
                logger.error("alpaca_order_unsupported_type", order_type=order_type, symbol=symbol)
                raise ValueError(f"Unsupported order type: {order_type}")

            order = self._trading_client.submit_order(request)
            data = self._model_to_dict(order)
            logger.info(
                "alpaca_order_created",
                order_id=data.get("id"),
                symbol=symbol,
                side=side,
                order_class=order_class,
            )
            return data

        except APIError as e:
            logger.warning("alpaca_order_create_error", error=str(e))
            raise

    def get_orders(
        self,
        status: str = "open",
        limit: int = 100,
        direction: str = "desc",
        symbols: list[str] | None = None,
        nested: bool = True,
        side: str | None = None,
        after: datetime | None = None,
        until: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get all orders with optional filters.

        ``after`` / ``until`` filter by the order's ``submitted_at`` timestamp
        (Alpaca's GetOrdersRequest semantics — NOT filled_at). They are the
        pagination cursor: Alpaca caps each page at 500 orders and exposes no
        page_token for orders, so a caller pages by advancing the
        ``after``/``until`` window across the submitted_at range.
        """
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = GetOrdersRequest(
                status=QueryOrderStatus(status),
                limit=min(limit, 500),
                direction=Sort(direction),
                symbols=symbols,
                nested=nested,
                side=OrderSide(side) if side else None,
                after=after,
                until=until,
            )
            orders = self._trading_client.get_orders(request)
            result = [self._model_to_dict(o) for o in orders]
            logger.info("alpaca_orders_fetched", count=len(result), status=status)
            return result

        except APIError as e:
            logger.error("alpaca_orders_error", error=str(e))
            raise

    def get_order(self, order_id: str) -> dict[str, Any]:
        """Get a specific order by ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            order = self._trading_client.get_order_by_id(order_id)
            return self._model_to_dict(order)
        except APIError as e:
            logger.error("alpaca_order_get_error", order_id=order_id, error=str(e))
            raise

    def get_order_by_client_id(self, client_order_id: str) -> dict[str, Any]:
        """Get order by client order ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            order = self._trading_client.get_order_by_client_id(client_order_id)
            return self._model_to_dict(order)
        except APIError:
            logger.error("alpaca_order_get_by_client_error", client_order_id=client_order_id)
            raise

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            self._trading_client.cancel_order_by_id(order_id)
            logger.info("alpaca_order_cancelled", order_id=order_id)
            return True
        except APIError as e:
            if self._is_benign_cancel_race(e):
                # Cancel raced the order's terminal transition (already
                # filled/canceled). Expected — log at INFO, still re-raise so
                # the caller sees the real 422 and learns the order's true state.
                logger.info("alpaca_order_cancel_noop", order_id=order_id, error=str(e))
            else:
                logger.error("alpaca_order_cancel_error", order_id=order_id, error=str(e))
            raise

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        """Cancel all open orders."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            cancelled = self._trading_client.cancel_orders()
            result = [self._model_to_dict(c) for c in cancelled]
            logger.info("alpaca_orders_cancelled_all", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_orders_cancel_all_error", error=str(e))
            raise

    def replace_order(
        self,
        order_id: str,
        qty: float | None = None,
        limit_price: float | None = None,
        stop_price: float | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Replace/modify an existing order."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            tif = TimeInForce(time_in_force) if time_in_force else None
            request = ReplaceOrderRequest(
                qty=qty if qty else None,
                limit_price=limit_price,
                stop_price=stop_price,
                time_in_force=tif,
                client_order_id=client_order_id,
            )
            order = self._trading_client.replace_order_by_id(order_id, request)
            return self._model_to_dict(order)
        except APIError as e:
            logger.error("alpaca_order_replace_error", order_id=order_id, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Positions
    # ─────────────────────────────────────────────────────────────────

    def get_positions(self) -> list[dict[str, Any]]:
        """Get all open positions."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            positions = self._trading_client.get_all_positions()
            result = [self._model_to_dict(p) for p in positions]
            logger.info("alpaca_positions_fetched", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_positions_error", error=str(e))
            raise

    def get_position(self, symbol: str) -> dict[str, Any]:
        """Get position for a specific symbol."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            position = self._trading_client.get_open_position(symbol.upper())
            return self._model_to_dict(position)
        except APIError as e:
            alpaca_code = self._extract_alpaca_error_code(e)
            if alpaca_code in _POSITION_NOT_FOUND_CODES:
                logger.warning("alpaca_position_not_found", symbol=symbol, alpaca_code=alpaca_code)
                raise HTTPException(
                    status_code=404,
                    detail={
                        "code": "POSITION_NOT_FOUND",
                        "message": f"No open position for '{symbol}' at broker",
                        "alpaca_code": alpaca_code,
                        "symbol": symbol,
                    },
                ) from e
            logger.error("alpaca_position_error", symbol=symbol, error=str(e))
            raise

    def close_position(
        self,
        symbol: str,
        qty: float | None = None,
        percentage: float | None = None,
    ) -> dict[str, Any]:
        """Close a position (fully or partially)."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        # Default to closing the entire position when neither qty nor
        # percentage is supplied. Alpaca's ClosePositionRequest requires
        # exactly one to be set; the previous code passed both as None and
        # raised a ValidationError that surfaced upstream as a 502, leaving
        # callers' positions open after a "DELETE /positions/<symbol>".
        if qty is None and percentage is None:
            percentage = 100.0
        # Use ``is not None`` rather than truthiness so that qty=0 / pct=0
        # round-trip to the SDK and surface as Alpaca's own validation error
        # rather than being silently rewritten to None and triggering the
        # same ValidationError → 502 the naked-call default above guards
        # against. (qty=0 / pct=0 are nonsense for a close; the SDK will
        # reject them — that's the right place for the rejection.)
        try:
            request = ClosePositionRequest(
                qty=str(qty) if qty is not None else None,
                percentage=str(percentage) if percentage is not None else None,
            )
            order = self._trading_client.close_position(symbol.upper(), close_options=request)
            data = self._model_to_dict(order)
            logger.info("alpaca_position_closed", symbol=symbol, qty=qty, percentage=percentage)
            return data
        except APIError as e:
            alpaca_code = self._extract_alpaca_error_code(e)
            if alpaca_code in _POSITION_NOT_FOUND_CODES:
                logger.warning(
                    "alpaca_position_not_found",
                    symbol=symbol,
                    alpaca_code=alpaca_code,
                )
                raise HTTPException(
                    status_code=404,
                    detail={
                        "code": "POSITION_NOT_FOUND",
                        "message": f"Position for '{symbol}' does not exist at broker (already closed or expired)",
                        "alpaca_code": alpaca_code,
                        "symbol": symbol,
                    },
                ) from e
            logger.error("alpaca_position_close_error", symbol=symbol, error=str(e))
            raise

    def close_all_positions(self, cancel_orders: bool = True) -> list[dict[str, Any]]:
        """Close all open positions."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            closed = self._trading_client.close_all_positions(cancel_orders=cancel_orders)
            result = [self._model_to_dict(c) for c in closed]
            logger.info("alpaca_positions_closed_all", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_positions_close_all_error", error=str(e))
            raise

    def exercise_option_position(self, symbol_or_contract_id: str) -> dict[str, Any]:
        """Exercise an options position.

        Args:
            symbol_or_contract_id: The OCC symbol or contract ID to exercise

        Returns:
            Response from the exercise request
        """
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            # The Alpaca SDK exercise_options_position returns None (void)
            self._trading_client.exercise_options_position(symbol_or_contract_id)
            logger.info("alpaca_option_exercised", symbol=symbol_or_contract_id)
            return {"status": "exercised", "symbol": symbol_or_contract_id}
        except APIError as e:
            logger.error(
                "alpaca_option_exercise_error",
                symbol=symbol_or_contract_id,
                error=str(e),
            )
            raise

    def do_not_exercise_option(self, symbol_or_contract_id: str) -> dict[str, Any]:
        """Mark an options position as do-not-exercise.

        Args:
            symbol_or_contract_id: The OCC symbol or contract ID to mark as DNE

        Returns:
            Response from the DNE request
        """
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            # This endpoint is not in alpaca-py SDK, use REST directly.
            # An explicit timeout is REQUIRED here: httpx.post defaults to no
            # timeout, so a hung trading endpoint would block the calling thread
            # forever (the same leak the SDK-session timeout in _base guards
            # against, on the one path that bypasses the SDK).
            from gateway.config import get_settings

            response = httpx.post(
                f"{self._trading_base_url}/v2/positions/{symbol_or_contract_id}/do-not-exercise",
                headers={
                    "APCA-API-KEY-ID": self._api_key,
                    "APCA-API-SECRET-KEY": self._secret_key,
                },
                timeout=get_settings().alpaca_trading_http_timeout_seconds,
            )
            response.raise_for_status()
            data = response.json() if response.content else {"status": "do_not_exercise"}
            logger.info("alpaca_option_dne", symbol=symbol_or_contract_id)
            return data
        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_dne_error",
                symbol=symbol_or_contract_id,
                status=e.response.status_code,
                error=str(e),
            )
            raise

    # ─────────────────────────────────────────────────────────────────
    # Portfolio & Assets
    # ─────────────────────────────────────────────────────────────────

    def get_portfolio_history(
        self,
        period: str | None = None,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        extended_hours: bool = False,
    ) -> dict[str, Any]:
        """Get account portfolio history."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            history = self._trading_client.get_portfolio_history(  # type: ignore[call-arg]  # kwargs do not match alpaca-py signature (docs/FOLLOW_UPS.md)
                period=period,
                timeframe=timeframe,
                date_start=start,
                date_end=end,
                extended_hours=extended_hours,
            )
            data = self._model_to_dict(history)
            logger.info("alpaca_portfolio_history_fetched", period=period)
            return data
        except APIError as e:
            logger.error("alpaca_portfolio_history_error", error=str(e))
            raise

    def get_assets(
        self,
        status: str | None = None,
        asset_class: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get available assets."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = GetAssetsRequest(
                status=AssetStatus(status) if status else None,
                asset_class=AssetClass(asset_class) if asset_class else None,
                exchange=AssetExchange(exchange) if exchange else None,
            )
            assets = self._trading_client.get_all_assets(request)
            result = [self._model_to_dict(a) for a in assets]
            logger.info("alpaca_assets_fetched", count=len(result))
            return result
        except APIError as e:
            logger.error("alpaca_assets_error", error=str(e))
            raise

    def get_asset(self, symbol: str) -> dict[str, Any]:
        """Get a specific asset by symbol."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            asset = self._trading_client.get_asset(symbol.upper())
            return self._model_to_dict(asset)
        except APIError as e:
            logger.error("alpaca_asset_error", symbol=symbol, error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Clock & Calendar
    # ─────────────────────────────────────────────────────────────────

    def get_clock(self) -> dict[str, Any]:
        """Get market clock."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            clock = self._trading_client.get_clock()
            return self._model_to_dict(clock)
        except APIError as e:
            logger.error("alpaca_clock_error", error=str(e))
            raise

    def get_calendar(
        self,
        start: date | None = None,
        end: date | None = None,
    ) -> list[dict[str, Any]]:
        """Get trading calendar."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = GetCalendarRequest(start=start, end=end)
            calendar = self._trading_client.get_calendar(request)
            return [self._model_to_dict(c) for c in calendar]
        except APIError as e:
            logger.error("alpaca_calendar_error", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Account Configuration & Activities
    # ─────────────────────────────────────────────────────────────────

    def get_account_configurations(self) -> dict[str, Any]:
        """Get account configuration settings."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            config = self._trading_client.get_account_configurations()
            return self._model_to_dict(config)
        except APIError as e:
            logger.error("alpaca_account_config_error", error=str(e))
            raise

    def set_account_configurations(
        self,
        dtbp_check: str | None = None,
        trade_confirm_email: str | None = None,
        suspend_trade: bool | None = None,
        no_shorting: bool | None = None,
        fractional_trading: bool | None = None,
        max_margin_multiplier: str | None = None,
        pdt_check: str | None = None,
    ) -> dict[str, Any]:
        """Update account configuration settings."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            config = self._trading_client.set_account_configurations(  # type: ignore[call-arg]  # kwargs do not match alpaca-py signature (docs/FOLLOW_UPS.md)
                dtbp_check=dtbp_check,
                trade_confirm_email=trade_confirm_email,
                suspend_trade=suspend_trade,
                no_shorting=no_shorting,
                fractional_trading=fractional_trading,
                max_margin_multiplier=max_margin_multiplier,
                pdt_check=pdt_check,
            )
            return self._model_to_dict(config)
        except APIError as e:
            logger.error("alpaca_account_config_set_error", error=str(e))
            raise

    def get_account_activities(
        self,
        activity_types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Get account activities."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            # alpaca-py >=0.41 removed TradingClient.get_account_activities (it
            # moved to BrokerClient, which needs Broker-API creds the gateway
            # does not hold). Call the Trading API endpoint directly via the
            # SDK's low-level client, which returns raw activity dicts already.
            params: dict[str, Any] = {}
            if activity_types:
                params["activity_types"] = ",".join(activity_types)
            # ponytail: one page (Alpaca caps page_size at 100), newest-first by
            # default — enough for close-recon's "a recent close aged out" case.
            activities = self._trading_client.get("/account/activities", data=params or None)
            return list(activities or [])
        except APIError as e:
            logger.error("alpaca_activities_error", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────
    # Watchlists
    # ─────────────────────────────────────────────────────────────────

    def get_watchlists(self) -> list[dict[str, Any]]:
        """Get all watchlists."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlists = self._trading_client.get_watchlists()
            return [self._model_to_dict(w) for w in watchlists]
        except APIError as e:
            logger.error("alpaca_watchlists_error", error=str(e))
            raise

    def create_watchlist(self, name: str, symbols: list[str] | None = None) -> dict[str, Any]:
        """Create a new watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = CreateWatchlistRequest(name=name, symbols=symbols or [])
            watchlist = self._trading_client.create_watchlist(request)
            return self._model_to_dict(watchlist)
        except APIError as e:
            logger.error("alpaca_watchlist_create_error", error=str(e))
            raise

    def get_watchlist(self, watchlist_id: str) -> dict[str, Any]:
        """Get a specific watchlist by ID."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlist = self._trading_client.get_watchlist_by_id(watchlist_id)
            return self._model_to_dict(watchlist)
        except APIError as e:
            logger.error("alpaca_watchlist_error", watchlist_id=watchlist_id, error=str(e))
            raise

    def update_watchlist(
        self,
        watchlist_id: str,
        name: str | None = None,
        symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        """Update a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            request = UpdateWatchlistRequest(name=name, symbols=symbols)
            watchlist = self._trading_client.update_watchlist_by_id(watchlist_id, request)
            return self._model_to_dict(watchlist)
        except APIError as e:
            logger.error("alpaca_watchlist_update_error", watchlist_id=watchlist_id, error=str(e))
            raise

    def delete_watchlist(self, watchlist_id: str) -> bool:
        """Delete a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            self._trading_client.delete_watchlist_by_id(watchlist_id)
            logger.info("alpaca_watchlist_deleted", watchlist_id=watchlist_id)
            return True
        except APIError as e:
            logger.error("alpaca_watchlist_delete_error", watchlist_id=watchlist_id, error=str(e))
            raise

    def add_asset_to_watchlist(self, watchlist_id: str, symbol: str) -> dict[str, Any]:
        """Add an asset to a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlist = self._trading_client.add_asset_to_watchlist_by_id(watchlist_id, symbol)
            return self._model_to_dict(watchlist)
        except APIError:
            logger.error("alpaca_watchlist_add_asset_error", watchlist_id=watchlist_id, symbol=symbol)
            raise

    def remove_asset_from_watchlist(self, watchlist_id: str, symbol: str) -> dict[str, Any]:
        """Remove an asset from a watchlist."""
        if not self._trading_client:
            raise RuntimeError(ERR_TRADING_CLIENT_NOT_INITIALIZED)

        try:
            watchlist = self._trading_client.remove_asset_from_watchlist_by_id(watchlist_id, symbol)
            return self._model_to_dict(watchlist)
        except APIError:
            logger.error("alpaca_watchlist_remove_asset_error", watchlist_id=watchlist_id, symbol=symbol)
            raise
