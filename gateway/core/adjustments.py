"""Adjustment Factors Service.

Provides cumulative price adjustment factors for proper backtesting
as specified in PRD (lines 1166-1204).
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from gateway.core.corporate_actions import (
    ActionType,
    CorporateAction,
    get_corporate_actions_service,
)
from gateway.core.logger import logger

# ─────────────────────────────────────────────────────────────────────────────
# Adjustment Factor
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class AdjustmentFactor:
    """Price adjustment factor for a specific date."""

    date: date
    cumulative_factor: Decimal
    split_factor: Decimal
    dividend_factor: Decimal
    event: str | None = None  # Description of what caused the change

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "date": self.date.isoformat(),
            "cumulative_factor": float(self.cumulative_factor),
            "split_factor": float(self.split_factor),
            "dividend_factor": float(self.dividend_factor),
        }
        if self.event:
            result["event"] = self.event
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Adjustment Service
# ─────────────────────────────────────────────────────────────────────────────


class AdjustmentService:
    """Service for computing and applying price adjustments.

    Computes cumulative adjustment factors from corporate actions,
    enabling proper point-in-time (PIT) price adjustments for backtesting.

    Example:
        service = AdjustmentService()
        factors = await service.get_factors("AAPL", date(2020, 1, 1), date(2024, 1, 1))

        # Apply adjustment to a historical price
        adjusted_price = service.apply_adjustment(
            Decimal("100.00"), date(2020, 7, 1), factors
        )
    """

    async def get_factors(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> list[AdjustmentFactor]:
        """Get adjustment factors for a symbol over a date range.

        Args:
            symbol: Stock symbol
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of adjustment factors, one per corporate action date
        """
        # Get corporate actions
        service = get_corporate_actions_service()
        actions = await service.get_actions(symbol, start, end)

        # Compute factors from actions
        factors = self.compute_factors_from_actions(actions, start, end)

        logger.info(
            "adjustment_factors_computed",
            symbol=symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            factor_count=len(factors),
        )

        return factors

    def compute_factors_from_actions(
        self,
        actions: list[CorporateAction],
        start: date,
        end: date,
    ) -> list[AdjustmentFactor]:
        """Compute adjustment factors from corporate actions.

        Args:
            actions: List of corporate actions (should be sorted by date)
            start: Start date for factor computation
            end: End date for factor computation

        Returns:
            List of adjustment factors
        """
        factors: list[AdjustmentFactor] = []

        # Start with factor of 1.0 (no adjustment)
        cumulative_split = Decimal("1.0")
        cumulative_dividend = Decimal("1.0")

        # Add initial factor
        factors.append(
            AdjustmentFactor(
                date=start,
                cumulative_factor=Decimal("1.0"),
                split_factor=Decimal("1.0"),
                dividend_factor=Decimal("1.0"),
            )
        )

        # Process each action chronologically
        for action in sorted(actions, key=lambda a: a.ex_date):
            if action.ex_date < start or action.ex_date > end:
                continue

            if action.type == ActionType.SPLIT and action.ratio:
                # Split factor: divide by ratio (e.g., 4:1 split means factor = 0.25)
                split_factor = Decimal("1.0") / Decimal(str(action.ratio))
                cumulative_split *= split_factor

                factors.append(
                    AdjustmentFactor(
                        date=action.ex_date,
                        cumulative_factor=cumulative_split * cumulative_dividend,
                        split_factor=cumulative_split,
                        dividend_factor=cumulative_dividend,
                        event=action.description or f"{action.ratio}:1 split",
                    )
                )

            elif action.type == ActionType.DIVIDEND and action.amount:
                # For dividend adjustment, we'd need the stock price at ex-date
                # This is a simplified version that tracks dividend events
                # Full adjustment would require: factor = 1 - (dividend / price)
                factors.append(
                    AdjustmentFactor(
                        date=action.ex_date,
                        cumulative_factor=cumulative_split * cumulative_dividend,
                        split_factor=cumulative_split,
                        dividend_factor=cumulative_dividend,
                        event=f"Dividend ${action.amount}",
                    )
                )

        return factors

    def apply_adjustment(
        self,
        price: Decimal,
        as_of_date: date,
        factors: list[AdjustmentFactor],
    ) -> Decimal:
        """Apply adjustment factor to a price as of a specific date.

        Args:
            price: Unadjusted price
            as_of_date: Date of the price
            factors: List of adjustment factors

        Returns:
            Adjusted price
        """
        # Find the applicable factor for the date
        applicable_factor = Decimal("1.0")

        for factor in sorted(factors, key=lambda f: f.date, reverse=True):
            if factor.date <= as_of_date:
                applicable_factor = factor.cumulative_factor
                break

        return price * applicable_factor

    def adjust_bars(
        self,
        bars: list[dict],
        factors: list[AdjustmentFactor],
    ) -> list[dict]:
        """Adjust prices in a list of bars.

        Args:
            bars: List of bar dictionaries with 'timestamp', 'open', 'high', 'low', 'close'
            factors: List of adjustment factors

        Returns:
            List of adjusted bars
        """
        adjusted_bars = []

        for bar in bars:
            # Get date from bar
            bar_date = bar.get("timestamp")
            if isinstance(bar_date, str):
                bar_date = date.fromisoformat(bar_date[:10])
            elif hasattr(bar_date, "date"):
                bar_date = bar_date.date()

            # Find applicable factor
            factor = self._get_factor_for_date(bar_date, factors)

            # Adjust prices
            adjusted_bar = dict(bar)
            for field in ["open", "high", "low", "close"]:
                if field in adjusted_bar:
                    original = Decimal(str(adjusted_bar[field]))
                    adjusted_bar[field] = float(original * factor)

            # Adjust volume inversely
            if "volume" in adjusted_bar and factor != Decimal("1.0"):
                adjusted_bar["volume"] = int(adjusted_bar["volume"] / float(factor))

            adjusted_bar["adjusted"] = True
            adjusted_bar["adjustment_factor"] = float(factor)
            adjusted_bars.append(adjusted_bar)

        return adjusted_bars

    def _get_factor_for_date(
        self,
        target_date: date,
        factors: list[AdjustmentFactor],
    ) -> Decimal:
        """Get the applicable adjustment factor for a specific date."""
        applicable_factor = Decimal("1.0")

        for factor in sorted(factors, key=lambda f: f.date, reverse=True):
            if factor.date <= target_date:
                applicable_factor = factor.cumulative_factor
                break

        return applicable_factor


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_service: AdjustmentService | None = None


def get_adjustment_service() -> AdjustmentService:
    """Get singleton AdjustmentService instance."""
    global _service
    if _service is None:
        _service = AdjustmentService()
    return _service
