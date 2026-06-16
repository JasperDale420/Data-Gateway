"""Alpaca corporate actions mixin — corporate actions and parsing."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from gateway.core.http_client import http_retry
from gateway.core.logger import logger
from gateway.providers.alpaca._base import ERR_PROVIDER_NOT_INITIALIZED


def _derive_ex_date(action: dict[str, Any], *fallback_fields: str, action_type: str) -> str:
    """Resolve the schema-required `ex_date` from vendor payload.

    `NormalizedCorporateAction.ex_date` is required by the shared schema,
    but several Alpaca corporate-action branches (unit_splits, mergers,
    redemptions, name_changes, worthless_removals) do not carry a
    vendor-supplied `ex_date`. Without this helper those branches raise
    ValidationError and the whole endpoint fails for the impacted page.

    Resolution order:
        1. `action["ex_date"]` if present (the canonical field)
        2. each name in `fallback_fields` in order — designed per action
           type to choose the most semantically correct vendor date
        3. today's UTC date as a last resort, with a structured warning
           so the synthesized ex_date is auditable downstream
    """
    primary = action.get("ex_date")
    if primary:
        return str(primary)

    for field in fallback_fields:
        value = action.get(field)
        if value:
            logger.info(
                "alpaca_corporate_action_ex_date_inferred",
                action_type=action_type,
                source_field=field,
                action_id=action.get("id"),
            )
            return str(value)

    synth = datetime.now(UTC).strftime("%Y-%m-%d")
    logger.warning(
        "alpaca_corporate_action_ex_date_synthesized",
        action_type=action_type,
        action_id=action.get("id"),
        synthesized_ex_date=synth,
        available_keys=sorted(action.keys()),
    )
    return synth


class AlpacaCorporateMixin:
    """Corporate action methods."""

    @http_retry
    async def get_corporate_actions(
        self,
        symbols: list[str],
        types: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list:
        """Get corporate actions (splits, dividends, mergers, etc.) for symbols.

        Alpaca API response structure: {"corporate_actions": {<type>: [...]}, "next_page_token": ...}
        Types: reverse_splits, forward_splits, unit_splits, cash_dividends, stock_dividends,
               cash_mergers, stock_mergers, stock_and_cash_mergers, redemptions, spin_offs,
               rights_distributions, name_changes, worthless_removals
        """
        from gateway.schemas import NormalizedCorporateAction

        if not self._client:
            raise RuntimeError(ERR_PROVIDER_NOT_INITIALIZED)

        results: list[NormalizedCorporateAction] = []

        params: dict[str, Any] = {"symbols": ",".join(symbols)}
        if types:
            params["types"] = ",".join(types)
        if start:
            params["start"] = start.strftime("%Y-%m-%d") if isinstance(start, datetime) else start
        if end:
            params["end"] = end.strftime("%Y-%m-%d") if isinstance(end, datetime) else end

        try:
            while True:
                response = await self._client.get("/v1/corporate-actions", params=params)
                response.raise_for_status()
                data = response.json()

                ca_data = data.get("corporate_actions", {})
                results.extend(self._parse_corporate_actions(ca_data))

                next_token = data.get("next_page_token")
                if not next_token:
                    break
                params["page_token"] = next_token

            logger.info("alpaca_corporate_actions_fetched", count=len(results))

        except httpx.HTTPStatusError as e:
            logger.error(
                "alpaca_corporate_actions_error",
                status=e.response.status_code,
                error=str(e),
            )
            raise

        return results

    def _parse_corporate_actions(self, ca_data: dict[str, Any]) -> list:
        """Parse all corporate action types from Alpaca API response."""
        from gateway.schemas import NormalizedCorporateAction

        results: list[NormalizedCorporateAction] = []

        def _dec(val: Any) -> Decimal | None:
            return Decimal(str(val)) if val is not None else None

        # Reverse splits
        for action in ca_data.get("reverse_splits", []):
            ratio_str = None
            if action.get("new_rate") is not None and action.get("old_rate") is not None:
                ratio_str = f"{action['new_rate']}:{action['old_rate']}"
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("symbol", ""),
                    action_type="reverse_split",
                    ex_date=_derive_ex_date(action, "process_date", "payable_date", action_type="reverse_split"),
                    record_date=action.get("record_date"),
                    payable_date=action.get("payable_date"),
                    process_date=action.get("process_date"),
                    new_rate=_dec(action.get("new_rate")),
                    old_rate=_dec(action.get("old_rate")),
                    ratio=ratio_str,
                    old_cusip=action.get("old_cusip"),
                    new_cusip=action.get("new_cusip"),
                    provider="alpaca",
                )
            )

        # Forward splits
        for action in ca_data.get("forward_splits", []):
            ratio_str = None
            if action.get("new_rate") is not None and action.get("old_rate") is not None:
                ratio_str = f"{action['new_rate']}:{action['old_rate']}"
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("symbol", ""),
                    action_type="forward_split",
                    ex_date=_derive_ex_date(action, "process_date", "payable_date", action_type="forward_split"),
                    record_date=action.get("record_date"),
                    payable_date=action.get("payable_date"),
                    process_date=action.get("process_date"),
                    due_bill_redemption_date=action.get("due_bill_redemption_date"),
                    new_rate=_dec(action.get("new_rate")),
                    old_rate=_dec(action.get("old_rate")),
                    ratio=ratio_str,
                    cusip=action.get("cusip"),
                    provider="alpaca",
                )
            )

        # Unit splits — vendor has no ex_date; the security trades under
        # the new unit on `effective_date`, so that is the closest proxy.
        for action in ca_data.get("unit_splits", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("old_symbol", ""),
                    action_type="unit_split",
                    ex_date=_derive_ex_date(
                        action, "effective_date", "process_date", "payable_date", action_type="unit_split"
                    ),
                    process_date=action.get("process_date"),
                    effective_date=action.get("effective_date"),
                    payable_date=action.get("payable_date"),
                    new_rate=_dec(action.get("new_rate")),
                    old_rate=_dec(action.get("old_rate")),
                    old_symbol=action.get("old_symbol"),
                    old_cusip=action.get("old_cusip"),
                    new_symbol=action.get("new_symbol"),
                    new_cusip=action.get("new_cusip"),
                    alternate_symbol=action.get("alternate_symbol"),
                    alternate_cusip=action.get("alternate_cusip"),
                    alternate_rate=_dec(action.get("alternate_rate")),
                    provider="alpaca",
                )
            )

        # Cash dividends
        for action in ca_data.get("cash_dividends", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("symbol", ""),
                    action_type="cash_dividend",
                    ex_date=_derive_ex_date(
                        action, "record_date", "payable_date", "process_date", action_type="cash_dividend"
                    ),
                    record_date=action.get("record_date"),
                    payable_date=action.get("payable_date"),
                    process_date=action.get("process_date"),
                    amount=_dec(action.get("rate")),
                    cusip=action.get("cusip"),
                    special=action.get("special"),
                    foreign=action.get("foreign"),
                    provider="alpaca",
                )
            )

        # Stock dividends
        for action in ca_data.get("stock_dividends", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("symbol", ""),
                    action_type="stock_dividend",
                    ex_date=_derive_ex_date(
                        action, "record_date", "payable_date", "process_date", action_type="stock_dividend"
                    ),
                    record_date=action.get("record_date"),
                    payable_date=action.get("payable_date"),
                    process_date=action.get("process_date"),
                    amount=_dec(action.get("rate")),
                    cusip=action.get("cusip"),
                    provider="alpaca",
                )
            )

        # Cash mergers — merger closes on `effective_date`, after which
        # the acquiree no longer trades; use that as the ex_date proxy.
        for action in ca_data.get("cash_mergers", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("acquiree_symbol", ""),
                    action_type="cash_merger",
                    ex_date=_derive_ex_date(
                        action, "effective_date", "process_date", "payable_date", action_type="cash_merger"
                    ),
                    process_date=action.get("process_date"),
                    effective_date=action.get("effective_date"),
                    payable_date=action.get("payable_date"),
                    amount=_dec(action.get("rate")),
                    acquirer_symbol=action.get("acquirer_symbol"),
                    acquirer_cusip=action.get("acquirer_cusip"),
                    acquiree_symbol=action.get("acquiree_symbol"),
                    acquiree_cusip=action.get("acquiree_cusip"),
                    provider="alpaca",
                )
            )

        # Stock mergers — same as cash mergers: `effective_date` is the
        # merger close, which is the meaningful ex_date for the acquiree.
        for action in ca_data.get("stock_mergers", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("acquiree_symbol", ""),
                    action_type="stock_merger",
                    ex_date=_derive_ex_date(
                        action, "effective_date", "process_date", "payable_date", action_type="stock_merger"
                    ),
                    process_date=action.get("process_date"),
                    effective_date=action.get("effective_date"),
                    payable_date=action.get("payable_date"),
                    acquirer_symbol=action.get("acquirer_symbol"),
                    acquirer_cusip=action.get("acquirer_cusip"),
                    acquirer_rate=_dec(action.get("acquirer_rate")),
                    acquiree_symbol=action.get("acquiree_symbol"),
                    acquiree_cusip=action.get("acquiree_cusip"),
                    acquiree_rate=_dec(action.get("acquiree_rate")),
                    provider="alpaca",
                )
            )

        # Stock and cash mergers — same proxy choice as cash/stock
        # mergers above: `effective_date` is the close.
        for action in ca_data.get("stock_and_cash_mergers", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("acquiree_symbol", ""),
                    action_type="stock_and_cash_merger",
                    ex_date=_derive_ex_date(
                        action,
                        "effective_date",
                        "process_date",
                        "payable_date",
                        action_type="stock_and_cash_merger",
                    ),
                    process_date=action.get("process_date"),
                    effective_date=action.get("effective_date"),
                    payable_date=action.get("payable_date"),
                    cash_rate=_dec(action.get("cash_rate")),
                    acquirer_symbol=action.get("acquirer_symbol"),
                    acquirer_cusip=action.get("acquirer_cusip"),
                    acquirer_rate=_dec(action.get("acquirer_rate")),
                    acquiree_symbol=action.get("acquiree_symbol"),
                    acquiree_cusip=action.get("acquiree_cusip"),
                    acquiree_rate=_dec(action.get("acquiree_rate")),
                    provider="alpaca",
                )
            )

        # Redemptions — `process_date` is when Alpaca recognizes the
        # redemption; `payable_date` is when cash actually moves. Both
        # are reasonable ex_date proxies in the absence of a vendor ex.
        for action in ca_data.get("redemptions", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("symbol", ""),
                    action_type="redemption",
                    ex_date=_derive_ex_date(action, "process_date", "payable_date", action_type="redemption"),
                    process_date=action.get("process_date"),
                    payable_date=action.get("payable_date"),
                    amount=_dec(action.get("rate")),
                    cusip=action.get("cusip"),
                    provider="alpaca",
                )
            )

        # Spin-offs
        for action in ca_data.get("spin_offs", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("source_symbol", ""),
                    action_type="spin_off",
                    ex_date=_derive_ex_date(action, "process_date", "payable_date", action_type="spin_off"),
                    record_date=action.get("record_date"),
                    payable_date=action.get("payable_date"),
                    process_date=action.get("process_date"),
                    due_bill_redemption_date=action.get("due_bill_redemption_date"),
                    source_symbol=action.get("source_symbol"),
                    source_cusip=action.get("source_cusip"),
                    source_rate=_dec(action.get("source_rate")),
                    new_symbol=action.get("new_symbol"),
                    new_cusip=action.get("new_cusip"),
                    new_rate=_dec(action.get("new_rate")),
                    provider="alpaca",
                )
            )

        # Rights distributions
        for action in ca_data.get("rights_distributions", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("source_symbol", ""),
                    action_type="rights_distribution",
                    ex_date=_derive_ex_date(action, "process_date", "payable_date", action_type="rights_distribution"),
                    record_date=action.get("record_date"),
                    payable_date=action.get("payable_date"),
                    process_date=action.get("process_date"),
                    expiration_date=action.get("expiration_date"),
                    amount=_dec(action.get("rate")),
                    source_symbol=action.get("source_symbol"),
                    source_cusip=action.get("source_cusip"),
                    new_symbol=action.get("new_symbol"),
                    new_cusip=action.get("new_cusip"),
                    provider="alpaca",
                )
            )

        # Name changes — vendor only emits `process_date`; that is when
        # the new ticker becomes effective for trading.
        for action in ca_data.get("name_changes", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("new_symbol", action.get("old_symbol", "")),
                    action_type="name_change",
                    ex_date=_derive_ex_date(action, "process_date", action_type="name_change"),
                    process_date=action.get("process_date"),
                    old_symbol=action.get("old_symbol"),
                    old_cusip=action.get("old_cusip"),
                    new_symbol=action.get("new_symbol"),
                    new_cusip=action.get("new_cusip"),
                    provider="alpaca",
                )
            )

        # Worthless removals — vendor only emits `process_date`; the
        # security is removed and stops trading on that date.
        for action in ca_data.get("worthless_removals", []):
            results.append(
                NormalizedCorporateAction(
                    id=action.get("id"),
                    symbol=action.get("symbol", ""),
                    action_type="worthless_removal",
                    ex_date=_derive_ex_date(action, "process_date", action_type="worthless_removal"),
                    process_date=action.get("process_date"),
                    cusip=action.get("cusip"),
                    provider="alpaca",
                )
            )

        return results
