"""Background capture of live Alpaca option chain snapshots plus tape subscriptions."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

import structlog

from gateway.config import Settings, get_settings
from gateway.core.calendar import TradingCalendar
from gateway.core.envelope import wrap_event
from gateway.core.registry import ProviderRegistry
from gateway.core.stream import AlpacaStreamType, StreamMultiplexer

logger = structlog.get_logger()

HEBER_STREAM = "heber:events"
CAPTURE_CLIENT_ID = "__option_capture__"
DEFAULT_LOOP_SLEEP_SECONDS = 1.0


class SupportsMarketHours(Protocol):
    """Calendar protocol used by option capture tests and runtime wiring."""

    def is_market_open(self, dt: datetime | None = None) -> bool:
        """Return whether the market is open for the given timestamp."""


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


@dataclass
class CaptureCycleSummary:
    published: int
    failed_symbols: list[str]
    skipped_market_closed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "published": self.published,
            "failed_symbols": self.failed_symbols,
            "skipped_market_closed": self.skipped_market_closed,
        }


class OptionCaptureService:
    """Capture minute chain snapshots and keep tape subscriptions in sync."""

    def __init__(
        self,
        *,
        alpaca_provider: Any,
        multiplexer: StreamMultiplexer | Any | None,
        sink_registry: Any,
        symbols: list[str],
        interval_seconds: int = 60,
        market_hours_only: bool = True,
        snapshot_timeout_seconds: float = 10.0,
        websocket_enabled: bool = True,
        option_ws_contract_limit_per_symbol: int = 40,
        calendar: SupportsMarketHours | None = None,
        now_fn: Any | None = None,
        loop_sleep_seconds: float = DEFAULT_LOOP_SLEEP_SECONDS,
    ) -> None:
        self._alpaca_provider = alpaca_provider
        self._multiplexer = multiplexer
        self._sink_registry = sink_registry
        self.symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        self.interval_seconds = max(1, int(interval_seconds))
        self.market_hours_only = market_hours_only
        self.snapshot_timeout_seconds = max(0.5, float(snapshot_timeout_seconds))
        self.websocket_enabled = websocket_enabled
        self.option_ws_contract_limit_per_symbol = max(1, int(option_ws_contract_limit_per_symbol))
        self._calendar = calendar or TradingCalendar()
        self._now_fn = now_fn or (lambda: datetime.now(UTC))
        self._loop_sleep_seconds = max(0.1, float(loop_sleep_seconds))
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._last_bucket: int | None = None
        self._symbol_contracts: dict[str, set[str]] = {}
        self._subscribed_contracts: set[str] = set()

    @classmethod
    def from_settings(
        cls,
        *,
        settings: Settings,
        alpaca_provider: Any,
        multiplexer: StreamMultiplexer | Any | None,
        sink_registry: Any,
    ) -> OptionCaptureService:
        return cls(
            alpaca_provider=alpaca_provider,
            multiplexer=multiplexer,
            sink_registry=sink_registry,
            symbols=settings.option_capture_symbol_list,
            interval_seconds=settings.option_capture_interval_seconds,
            market_hours_only=settings.option_capture_market_hours_only,
            snapshot_timeout_seconds=settings.option_capture_snapshot_timeout_seconds,
            websocket_enabled=settings.option_capture_ws_enabled,
            option_ws_contract_limit_per_symbol=settings.option_capture_ws_contract_limit_per_symbol,
        )

    async def start(self) -> None:
        """Start the background capture loop."""
        self._validate_dependencies()
        if self._running:
            logger.warning("option_capture_already_running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "option_capture_started",
            symbols=self.symbols,
            interval_seconds=self.interval_seconds,
            market_hours_only=self.market_hours_only,
            websocket_enabled=self.websocket_enabled,
        )

    async def stop(self) -> None:
        """Stop the background capture loop and unwind synthetic subscriptions."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

        if self.websocket_enabled and self._multiplexer is not None and self._subscribed_contracts:
            with contextlib.suppress(Exception):
                await self._multiplexer.client_disconnect(CAPTURE_CLIENT_ID)
        self._subscribed_contracts.clear()
        self._symbol_contracts.clear()
        logger.info("option_capture_stopped")

    async def run_cycle(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Run one capture cycle for all configured underlyings."""
        current_time = now or self._now_fn()
        if self.market_hours_only and not self._calendar.is_market_open(current_time):
            logger.debug("option_capture_skipped_market_closed", timestamp=current_time.isoformat())
            return CaptureCycleSummary(published=0, failed_symbols=[], skipped_market_closed=True).to_dict()

        envelopes: list[dict[str, Any]] = []
        failed_symbols: list[str] = []

        for symbol in self.symbols:
            try:
                contracts = await asyncio.wait_for(
                    self._alpaca_provider.get_option_snapshot_contracts(symbol),
                    timeout=self.snapshot_timeout_seconds,
                )
                if not contracts:
                    raise RuntimeError("empty_snapshot")

                payload = self._build_snapshot_payload(symbol=symbol, contracts=contracts)
                envelopes.append(
                    wrap_event(
                        event=payload,
                        provider="alpaca",
                        feed="option_chain_snapshot",
                        source="rest",
                        ts_ingest=current_time,
                        instrument_type_override="equity",
                        instrument_key_override=f"equity:{symbol}",
                        symbol_override=symbol,
                    )
                )
                self._symbol_contracts[symbol] = self._select_ws_contract_symbols(
                    contracts,
                    as_of_date=current_time.date(),
                )
            except Exception as exc:
                failed_symbols.append(symbol)
                logger.warning(
                    "option_capture_symbol_failed",
                    symbol=symbol,
                    error=str(exc),
                    snapshot_timeout_seconds=self.snapshot_timeout_seconds,
                )

        published = await self._publish_envelopes(envelopes)
        if self.websocket_enabled:
            await self._reconcile_ws_subscriptions()

        logger.info(
            "option_capture_cycle_complete",
            symbols=self.symbols,
            published=published,
            failed_symbols=failed_symbols,
        )
        return CaptureCycleSummary(published=published, failed_symbols=failed_symbols).to_dict()

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Expose lightweight runtime state for admin surfaces."""
        return {
            "running": self._running,
            "symbols": self.symbols,
            "interval_seconds": self.interval_seconds,
            "market_hours_only": self.market_hours_only,
            "websocket_enabled": self.websocket_enabled,
            "ws_contract_limit_per_symbol": self.option_ws_contract_limit_per_symbol,
            "tracked_contracts": sum(len(contracts) for contracts in self._symbol_contracts.values()),
            "subscribed_contracts": len(self._subscribed_contracts),
        }

    async def _run_loop(self) -> None:
        while self._running:
            now = self._now_fn()
            current_bucket = int(now.timestamp()) // self.interval_seconds
            if current_bucket != self._last_bucket:
                self._last_bucket = current_bucket
                try:
                    await self.run_cycle(now=now)
                except Exception as exc:
                    logger.error("option_capture_cycle_error", error=str(exc), exc_info=True)
            await asyncio.sleep(self._loop_sleep_seconds)

    def _validate_dependencies(self) -> None:
        if not self.symbols:
            raise RuntimeError("Option capture is enabled but no symbols are configured")
        if self._alpaca_provider is None:
            raise RuntimeError("Option capture requires the Alpaca provider")
        if self._sink_registry is None:
            raise RuntimeError("Option capture requires an active data sink")
        if self.websocket_enabled and self._multiplexer is None:
            raise RuntimeError("Option capture websocket mode requires an initialized stream multiplexer")

    async def _publish_envelopes(self, envelopes: list[dict[str, Any]]) -> int:
        if not envelopes:
            return 0

        if hasattr(self._sink_registry, "publish_all_batch"):
            messages = [(HEBER_STREAM, envelope) for envelope in envelopes]
            return int(await self._sink_registry.publish_all_batch(messages))

        published = 0
        for envelope in envelopes:
            await self._sink_registry.publish_all(HEBER_STREAM, envelope)
            published += 1
        return published

    async def _reconcile_ws_subscriptions(self) -> None:
        if not self.websocket_enabled or self._multiplexer is None:
            return

        desired_contracts: set[str] = set()
        for contracts in self._symbol_contracts.values():
            desired_contracts.update(contracts)

        to_add = sorted(desired_contracts - self._subscribed_contracts)
        to_remove = sorted(self._subscribed_contracts - desired_contracts)

        if to_add:
            response = await self._multiplexer.client_subscribe(
                CAPTURE_CLIENT_ID,
                AlpacaStreamType.OPTIONS,
                quotes=to_add,
                trades=to_add,
            )
            if response.get("status") != "ok":
                raise RuntimeError(f"option_capture_subscribe_failed: {response}")
            self._subscribed_contracts.update(to_add)

        if to_remove:
            response = await self._multiplexer.client_unsubscribe(
                CAPTURE_CLIENT_ID,
                AlpacaStreamType.OPTIONS,
                quotes=to_remove,
                trades=to_remove,
            )
            if response.get("status") != "ok":
                raise RuntimeError(f"option_capture_unsubscribe_failed: {response}")
            self._subscribed_contracts.difference_update(to_remove)

    def _build_snapshot_payload(self, *, symbol: str, contracts: list[Any]) -> dict[str, Any]:
        serialized_contracts: list[dict[str, Any]] = []
        expiries: list[date] = []
        timestamps: list[datetime] = []
        call_volumes: list[float] = []
        put_volumes: list[float] = []
        call_open_interest: list[float] = []
        put_open_interest: list[float] = []
        atm_candidates: list[tuple[float, float]] = []

        for contract in sorted(contracts, key=lambda item: self._contract_symbol(item)):
            serialized = self._serialize_contract(contract)
            serialized_contracts.append(serialized)

            expiry = _parse_date(serialized.get("expiration"))
            if expiry is not None:
                expiries.append(expiry)

            ts_value = _parse_timestamp(serialized.get("timestamp"))
            if ts_value is not None:
                timestamps.append(ts_value.astimezone(UTC))

            option_type = str(serialized.get("option_type", "")).lower()
            volume = _to_float(serialized.get("volume")) or 0.0
            open_interest = _to_float(serialized.get("open_interest")) or 0.0
            if option_type == "put":
                put_volumes.append(volume)
                put_open_interest.append(open_interest)
            else:
                call_volumes.append(volume)
                call_open_interest.append(open_interest)

            iv = _to_float(serialized.get("iv"))
            delta = _to_float(serialized.get("delta"))
            if iv is not None and delta is not None:
                atm_candidates.append((abs(abs(delta) - 0.5), iv))

        nearest_expiry = min(expiries).isoformat() if expiries else None
        snapshot_ts = max(timestamps).isoformat() if timestamps else self._now_fn().isoformat()
        atm_iv = min(atm_candidates, key=lambda item: item[0])[1] if atm_candidates else None

        return {
            "timestamp": snapshot_ts,
            "underlying": symbol,
            "expiry": nearest_expiry,
            "chain_json": {"data": {"contracts": serialized_contracts}},
            "total_call_volume": float(sum(call_volumes)),
            "total_put_volume": float(sum(put_volumes)),
            "total_call_oi": float(sum(call_open_interest)),
            "total_put_oi": float(sum(put_open_interest)),
            "atm_iv": float(atm_iv) if atm_iv is not None else None,
        }

    def _select_ws_contract_symbols(self, contracts: list[Any], *, as_of_date: date) -> set[str]:
        ranked_contracts: list[dict[str, Any]] = []
        for contract in contracts:
            symbol = self._contract_symbol(contract)
            if not symbol:
                continue
            ranked_contracts.append(self._serialize_contract(contract))

        atm_strike = self._estimate_atm_strike(ranked_contracts)
        ranked_contracts.sort(
            key=lambda contract: self._ws_contract_rank(
                contract,
                as_of_date=as_of_date,
                atm_strike=atm_strike,
            )
        )
        return {
            self._contract_symbol(contract)
            for contract in ranked_contracts[: self.option_ws_contract_limit_per_symbol]
            if self._contract_symbol(contract)
        }

    def _ws_contract_rank(
        self,
        contract: dict[str, Any],
        *,
        as_of_date: date,
        atm_strike: float | None,
    ) -> tuple[int, float, float, float, float, float, str]:
        expiry = _parse_date(contract.get("expiration"))
        dte = max(0, (expiry - as_of_date).days) if expiry is not None else 3650

        strike = _to_float(contract.get("strike"))
        strike_distance = abs(strike - atm_strike) if strike is not None and atm_strike is not None else 10_000.0

        delta = _to_float(contract.get("delta"))
        delta_distance = abs(abs(delta) - 0.5) if delta is not None else 10.0

        bid = _to_float(contract.get("bid")) or 0.0
        ask = _to_float(contract.get("ask")) or 0.0
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
        spread_pct = ((ask - bid) / mid) if mid > 0 else 10.0

        open_interest = _to_float(contract.get("open_interest")) or 0.0
        volume = _to_float(contract.get("volume")) or 0.0
        return (
            dte,
            strike_distance,
            delta_distance,
            spread_pct,
            -open_interest,
            -volume,
            self._contract_symbol(contract),
        )

    @staticmethod
    def _estimate_atm_strike(contracts: list[dict[str, Any]]) -> float | None:
        strikes = sorted(_to_float(contract.get("strike")) for contract in contracts)
        filtered = [strike for strike in strikes if strike is not None]
        if not filtered:
            return None
        middle = len(filtered) // 2
        if len(filtered) % 2:
            return filtered[middle]
        return (filtered[middle - 1] + filtered[middle]) / 2.0

    @staticmethod
    def _contract_symbol(contract: Any) -> str:
        if hasattr(contract, "contract_symbol"):
            return str(contract.contract_symbol)
        if isinstance(contract, dict):
            raw_symbol = contract.get("contract_symbol") or contract.get("symbol")
            return str(raw_symbol) if raw_symbol else ""
        return ""

    @staticmethod
    def _serialize_contract(contract: Any) -> dict[str, Any]:
        if hasattr(contract, "model_dump"):
            return dict(contract.model_dump(mode="json"))
        if isinstance(contract, dict):
            return dict(contract)
        raise TypeError(f"Unsupported option snapshot contract type: {type(contract)!r}")


_option_capture_service: OptionCaptureService | None = None


def get_option_capture_service() -> OptionCaptureService | None:
    """Return the global option capture service."""
    return _option_capture_service


async def start_option_capture_service(
    *,
    registry: ProviderRegistry,
    multiplexer: StreamMultiplexer | None,
    sink_registry: Any,
    settings: Settings | None = None,
) -> OptionCaptureService:
    """Start the global option capture service."""
    global _option_capture_service

    if _option_capture_service is not None:
        return _option_capture_service

    resolved_settings = settings or get_settings()
    alpaca_provider = registry.get("alpaca")
    service = OptionCaptureService.from_settings(
        settings=resolved_settings,
        alpaca_provider=alpaca_provider,
        multiplexer=multiplexer,
        sink_registry=sink_registry,
    )
    await service.start()
    _option_capture_service = service
    return service


async def stop_option_capture_service() -> None:
    """Stop the global option capture service."""
    global _option_capture_service

    if _option_capture_service is None:
        return
    await _option_capture_service.stop()
    _option_capture_service = None
