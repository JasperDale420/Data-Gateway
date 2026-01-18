"""Corporate Actions Service.

Provides corporate action history (splits, dividends, spinoffs)
as specified in PRD (lines 1126-1191).
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Action Types
# ─────────────────────────────────────────────────────────────────────────────


class ActionType(str, Enum):
    """Types of corporate actions."""

    SPLIT = "split"
    DIVIDEND = "dividend"
    MERGER = "merger"
    SPINOFF = "spinoff"
    NAME_CHANGE = "name_change"
    RIGHTS_ISSUE = "rights_issue"


# ─────────────────────────────────────────────────────────────────────────────
# Corporate Action
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CorporateAction:
    """A single corporate action event."""

    type: ActionType
    ex_date: date
    record_date: date | None = None
    pay_date: date | None = None

    # Split fields
    ratio: float | None = None
    description: str | None = None

    # Dividend fields
    amount: Decimal | None = None
    currency: str | None = "USD"
    frequency: str | None = None  # "quarterly", "annual", "special"

    # Spinoff/merger fields
    parent_symbol: str | None = None
    child_symbol: str | None = None

    # Name change fields
    old_symbol: str | None = None
    new_symbol: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "type": self.type.value,
            "ex_date": self.ex_date.isoformat(),
        }

        if self.record_date:
            result["record_date"] = self.record_date.isoformat()
        if self.pay_date:
            result["pay_date"] = self.pay_date.isoformat()

        # Type-specific fields
        if self.type == ActionType.SPLIT:
            result["ratio"] = self.ratio
            if self.description:
                result["description"] = self.description

        elif self.type == ActionType.DIVIDEND:
            result["amount"] = float(self.amount) if self.amount else None
            result["currency"] = self.currency
            if self.frequency:
                result["frequency"] = self.frequency

        elif self.type == ActionType.SPINOFF:
            result["parent_symbol"] = self.parent_symbol
            result["child_symbol"] = self.child_symbol
            result["ratio"] = self.ratio
            if self.description:
                result["description"] = self.description

        elif self.type == ActionType.NAME_CHANGE:
            result["old_symbol"] = self.old_symbol
            result["new_symbol"] = self.new_symbol

        return result


# ─────────────────────────────────────────────────────────────────────────────
# Corporate Actions Service
# ─────────────────────────────────────────────────────────────────────────────


class CorporateActionsService:
    """Service for retrieving corporate actions.

    Sources data from:
    - Primary: Alpaca (via corporate actions endpoint)
    - Fallback: yfinance (ticker.actions, ticker.splits)

    Example:
        service = CorporateActionsService()
        actions = await service.get_actions("AAPL", date(2020, 1, 1), date(2024, 1, 1))
        splits = await service.get_splits("AAPL", date(2020, 1, 1), date(2024, 1, 1))
    """

    # Well-known historical splits for fallback/validation
    KNOWN_SPLITS = {
        "AAPL": [
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2020, 8, 31),
                record_date=date(2020, 8, 24),
                ratio=4.0,
                description="4-for-1 stock split",
            ),
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2014, 6, 9),
                record_date=date(2014, 6, 2),
                ratio=7.0,
                description="7-for-1 stock split",
            ),
        ],
        "TSLA": [
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2022, 8, 25),
                ratio=3.0,
                description="3-for-1 stock split",
            ),
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2020, 8, 31),
                ratio=5.0,
                description="5-for-1 stock split",
            ),
        ],
        "GOOGL": [
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2022, 7, 18),
                ratio=20.0,
                description="20-for-1 stock split",
            ),
        ],
        "AMZN": [
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2022, 6, 6),
                ratio=20.0,
                description="20-for-1 stock split",
            ),
        ],
        "NVDA": [
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2024, 6, 10),
                ratio=10.0,
                description="10-for-1 stock split",
            ),
            CorporateAction(
                type=ActionType.SPLIT,
                ex_date=date(2021, 7, 20),
                ratio=4.0,
                description="4-for-1 stock split",
            ),
        ],
    }

    async def get_actions(
        self,
        symbol: str,
        start: date,
        end: date,
        action_types: list[ActionType] | None = None,
    ) -> list[CorporateAction]:
        """Get all corporate actions for a symbol in date range.

        Args:
            symbol: Stock symbol
            start: Start date (inclusive)
            end: End date (inclusive)
            action_types: Optional filter for specific action types

        Returns:
            List of corporate actions sorted by ex_date
        """
        symbol = symbol.upper()
        actions: list[CorporateAction] = []

        # Try to get from known splits first (fallback data)
        if symbol in self.KNOWN_SPLITS:
            for action in self.KNOWN_SPLITS[symbol]:
                if start <= action.ex_date <= end:
                    if action_types is None or action.type in action_types:
                        actions.append(action)

        # TODO: Integrate with Alpaca corporate actions API
        # TODO: Integrate with yfinance as fallback

        # Sort by ex_date
        actions.sort(key=lambda a: a.ex_date)

        logger.info(
            "corporate_actions_fetched",
            symbol=symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            count=len(actions),
        )

        return actions

    async def get_splits(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> list[CorporateAction]:
        """Get only stock splits for a symbol."""
        return await self.get_actions(symbol, start, end, action_types=[ActionType.SPLIT])

    async def get_dividends(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> list[CorporateAction]:
        """Get only dividends for a symbol."""
        return await self.get_actions(symbol, start, end, action_types=[ActionType.DIVIDEND])

    async def get_spinoffs(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> list[CorporateAction]:
        """Get only spinoffs for a symbol."""
        return await self.get_actions(symbol, start, end, action_types=[ActionType.SPINOFF])


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_service: CorporateActionsService | None = None


def get_corporate_actions_service() -> CorporateActionsService:
    """Get singleton CorporateActionsService instance."""
    global _service
    if _service is None:
        _service = CorporateActionsService()
    return _service
