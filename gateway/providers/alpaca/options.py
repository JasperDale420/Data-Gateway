"""Alpaca options mixin — option chain, bars, quotes, trades, snapshots, OCC parsing."""

import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog

from gateway.core.http_client import http_retry
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade

logger = structlog.get_logger()


class AlpacaOptionsMixin:
    """Option data methods."""

    @http_retry
    async def get_option_chain(
        self,
        underlying: str,
        expiration_date: str | None = None,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        option_type: str | None = None,
        limit: int | None = None,
    ) -> list:
        """Get option chain with greeks for an underlying."""
        from gateway.schemas import NormalizedOptionContract

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        request_limit = max(1, min(limit, 1000)) if limit is not None else 1000
        params: dict[str, Any] = {"feed": self._options_feed, "limit": request_limit}

        results: list[NormalizedOptionContract] = []

        try:
            response = await self._client.get(
                f"/v1beta1/options/snapshots/{underlying.upper()}",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            for contract_symbol, snapshot in data.get("snapshots", {}).items():
                parsed_contract = self._parse_occ_contract(contract_symbol)
                if parsed_contract is None:
                    continue
                if not self._matches_option_chain_filters(
                    parsed_contract,
                    expiration_date=expiration_date,
                    expiration_gte=expiration_gte,
                    expiration_lte=expiration_lte,
                    strike_gte=strike_gte,
                    strike_lte=strike_lte,
                    option_type=option_type,
                ):
                    continue
                contract = self._normalize_option_contract(
                    contract_symbol,
                    snapshot,
                    parsed_contract=parsed_contract,
                )
                if contract:
                    results.append(contract)

            logger.info(
                "alpaca_option_chain_fetched",
                underlying=underlying,
                contracts=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_chain_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_option_bars(
        self,
        contracts: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[NormalizedBar]:
        """Get historical bars for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedBar] = []
        symbols_param = ",".join(contracts)

        alpaca_timeframe = self._convert_timeframe(timeframe)
        params: dict[str, str | int] = {
            "symbols": symbols_param,
            "timeframe": alpaca_timeframe,
            "start": start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat(),
            "end": end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat(),
            "limit": limit,
        }

        try:
            response = await self._client.get(
                "/v1beta1/options/bars",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            for symbol, bars in data.get("bars", {}).items():
                for bar in bars:
                    results.append(self._normalize_bar(symbol, bar, timeframe=alpaca_timeframe))

            logger.info(
                "alpaca_option_bars_fetched",
                contracts=len(contracts),
                bars=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_bars_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_option_quotes(self, contracts: list[str]) -> list[NormalizedQuote]:
        """Get latest quotes for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedQuote] = []
        symbols_param = ",".join(contracts)

        try:
            response = await self._client.get(
                "/v1beta1/options/quotes/latest",
                params={"symbols": symbols_param, "feed": self._options_feed},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, quote in data.get("quotes", {}).items():
                results.append(self._normalize_quote(symbol, quote))

            logger.info(
                "alpaca_option_quotes_fetched",
                contracts=len(contracts),
                quotes=len(results),
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_option_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    async def get_historical_option_quotes(
        self,
        contracts: list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10000,
    ) -> list[NormalizedQuote]:
        """Get historical quotes for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedQuote] = []
        symbols_param = ",".join(contracts)
        request_limit = max(1, min(limit, 10000))

        params: dict[str, str | int] = {
            "symbols": symbols_param,
            "feed": self._options_feed,
            "limit": request_limit,
        }
        if start:
            params["start"] = start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat()
        if end:
            params["end"] = end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat()

        try:
            while True:
                response = await self._client.get(
                    "/v1beta1/options/quotes",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                for symbol, quotes in data.get("quotes", {}).items():
                    for quote in quotes:
                        results.append(self._normalize_quote(symbol, quote))

                next_token = data.get("next_page_token")
                if not next_token:
                    break
                params["page_token"] = next_token

            logger.info("alpaca_historical_option_quotes_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_historical_option_quotes_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    @http_retry
    async def get_option_trades(
        self,
        contracts: list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[NormalizedTrade]:
        """Get historical trades for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []
        symbols_param = ",".join(contracts)
        params: dict[str, Any] = {"symbols": symbols_param, "feed": self._options_feed, "limit": limit}
        if start:
            params["start"] = start.replace(tzinfo=UTC).isoformat() if start.tzinfo is None else start.isoformat()
        if end:
            params["end"] = end.replace(tzinfo=UTC).isoformat() if end.tzinfo is None else end.isoformat()

        try:
            response = await self._client.get("/v1beta1/options/trades", params=params)
            response.raise_for_status()
            data = response.json()

            for symbol, trades in data.get("trades", {}).items():
                for trade in trades:
                    results.append(self._normalize_trade(symbol, trade))

            logger.info("alpaca_option_trades_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_option_trades_error", status=e.response.status_code)
            raise

        return results

    @http_retry
    async def get_option_latest_trades(self, contracts: list[str]) -> list[NormalizedTrade]:
        """Get latest trades for option contracts."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedTrade] = []
        symbols_param = ",".join(contracts)

        try:
            response = await self._client.get(
                "/v1beta1/options/trades/latest",
                params={"symbols": symbols_param, "feed": self._options_feed},
            )
            response.raise_for_status()
            data = response.json()

            for symbol, trade in data.get("trades", {}).items():
                results.append(self._normalize_trade(symbol, trade))

            logger.info("alpaca_option_latest_trades_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_option_latest_trades_error", status=e.response.status_code)
            raise

        return results

    @http_retry
    async def get_option_snapshots(self, underlying: str) -> dict[str, Any]:
        """Get snapshots for all options on an underlying."""
        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        try:
            response = await self._client.get(
                f"/v1beta1/options/snapshots/{underlying.upper()}",
                params={"feed": self._options_feed},
            )
            response.raise_for_status()
            data = response.json()

            logger.info(
                "alpaca_option_snapshots_fetched",
                underlying=underlying,
                count=len(data.get("snapshots", {})),
            )
            return data.get("snapshots", {})

        except httpx.HTTPStatusError as e:
            logger.error("alpaca_option_snapshots_error", status=e.response.status_code)
            raise

    async def get_option_snapshot_contracts(self, underlying: str) -> list:
        """Get the latest full underlying snapshot as normalized option contracts."""
        from gateway.schemas import NormalizedOptionContract

        snapshots = await self.get_option_snapshots(underlying)
        results: list[NormalizedOptionContract] = []
        for contract_symbol, snapshot in snapshots.items():
            parsed_contract = self._parse_occ_contract(contract_symbol)
            if parsed_contract is None:
                continue
            contract = self._normalize_option_contract(
                contract_symbol,
                snapshot,
                parsed_contract=parsed_contract,
            )
            if contract:
                results.append(contract)

        logger.info(
            "alpaca_option_snapshot_contracts_fetched",
            underlying=underlying,
            contracts=len(results),
        )
        return results

    # ─────────────────────────────────────────────────────────────────
    # OCC parsing helpers
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_occ_contract(contract_symbol: str) -> dict[str, Any] | None:
        """Parse OCC option contract symbol into components."""
        match = re.match(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$", contract_symbol.upper())
        if not match:
            return None

        underlying, expiry_yy_mm_dd, cp_flag, strike_digits = match.groups()
        try:
            expiry = datetime.strptime(expiry_yy_mm_dd, "%y%m%d").date().isoformat()
            strike = Decimal(int(strike_digits)) / Decimal(1000)
        except (ValueError, ArithmeticError):
            return None

        return {
            "underlying": underlying,
            "expiration": expiry,
            "option_type": "call" if cp_flag == "C" else "put",
            "strike": strike,
        }

    @staticmethod
    def _matches_option_chain_filters(
        parsed_contract: dict[str, Any],
        *,
        expiration_date: str | None,
        expiration_gte: str | None,
        expiration_lte: str | None,
        strike_gte: float | None,
        strike_lte: float | None,
        option_type: str | None,
    ) -> bool:
        """Apply API-level filters to parsed OCC contract metadata."""
        expiration = str(parsed_contract["expiration"])
        strike = Decimal(parsed_contract["strike"])
        contract_option_type = str(parsed_contract["option_type"])

        if expiration_date and expiration != expiration_date:
            return False
        if expiration_gte and expiration < expiration_gte:
            return False
        if expiration_lte and expiration > expiration_lte:
            return False
        if strike_gte is not None and strike < Decimal(str(strike_gte)):
            return False
        if strike_lte is not None and strike > Decimal(str(strike_lte)):
            return False
        return not (option_type and contract_option_type != option_type.lower())

    def _normalize_option_contract(
        self,
        contract_symbol: str,
        snapshot: dict[str, Any],
        parsed_contract: dict[str, Any] | None = None,
    ):
        """Normalize option snapshot to NormalizedOptionContract."""
        from gateway.schemas import NormalizedOptionContract

        try:

            def _decimal_or_none(value: Any) -> Decimal | None:
                return Decimal(str(value)) if value is not None else None

            def _first_present(*values: Any) -> Any:
                for value in values:
                    if value is not None:
                        return value
                return None

            quote = snapshot.get("latestQuote", {})
            trade = snapshot.get("latestTrade", {})
            greeks = snapshot.get("greeks", {})
            day = snapshot.get("day", {})
            parsed = parsed_contract or self._parse_occ_contract(contract_symbol)
            if parsed is None:
                logger.warning(
                    "alpaca_option_contract_parse_failed",
                    contract=contract_symbol,
                )
                return None

            underlying = str(parsed["underlying"])
            expiration = str(parsed["expiration"])
            strike = Decimal(parsed["strike"])
            option_type = str(parsed["option_type"])
            volume = _first_present(snapshot.get("volume"), day.get("volume"), day.get("v"))
            open_interest = _first_present(snapshot.get("open_interest"), snapshot.get("openInterest"))
            underlying_price = _first_present(
                snapshot.get("underlying_price"),
                snapshot.get("underlyingPrice"),
                snapshot.get("underlying_asset_price"),
                snapshot.get("underlyingAssetPrice"),
            )
            implied_volatility = _first_present(
                snapshot.get("impliedVolatility"),
                snapshot.get("implied_volatility"),
            )

            return NormalizedOptionContract(
                contract_symbol=contract_symbol,
                underlying=underlying,
                expiration=expiration,
                strike=strike,
                option_type=option_type,
                bid=Decimal(str(quote.get("bp", 0))),
                ask=Decimal(str(quote.get("ap", 0))),
                last=Decimal(str(trade.get("p", 0))),
                volume=int(volume or 0),
                open_interest=int(open_interest or 0),
                underlying_price=_decimal_or_none(underlying_price),
                delta=_decimal_or_none(greeks.get("delta")),
                gamma=_decimal_or_none(greeks.get("gamma")),
                theta=_decimal_or_none(greeks.get("theta")),
                vega=_decimal_or_none(greeks.get("vega")),
                rho=_decimal_or_none(greeks.get("rho")),
                iv=_decimal_or_none(implied_volatility),
                provider="alpaca",
                timestamp=datetime.now(UTC),
            )
        except Exception as e:
            logger.warning(
                "alpaca_option_contract_normalize_failed",
                contract=contract_symbol,
                error=str(e),
            )
            return None
