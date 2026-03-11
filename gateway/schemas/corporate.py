"""Corporate actions models."""

from decimal import Decimal

from pydantic import BaseModel

__all__ = [
    "NormalizedCorporateAction",
]


class NormalizedCorporateAction(BaseModel):
    """Corporate action data (splits, dividends, mergers, spinoffs, etc.).

    Alpaca returns corporate actions grouped by type under /v1/corporate-actions.
    The action_type field maps to the Alpaca category:
        reverse_split, forward_split, unit_split, cash_dividend, stock_dividend,
        cash_merger, stock_merger, stock_and_cash_merger, redemption, spin_off,
        rights_distribution, name_change, worthless_removal
    """

    id: str | None = None  # Alpaca: unique corporate action ID (UUID)
    symbol: str
    action_type: str  # One of the Alpaca corporate action types listed above
    # Dates (vary by action type)
    ex_date: str | None = None
    record_date: str | None = None
    payable_date: str | None = None
    process_date: str | None = None  # Alpaca: date the action is processed
    effective_date: str | None = None  # Alpaca: mergers, unit_splits
    due_bill_redemption_date: str | None = None  # Alpaca: forward_split, spin_off
    expiration_date: str | None = None  # Alpaca: rights_distribution
    # Financial details
    amount: Decimal | None = None  # Dividend rate or merger cash rate
    ratio: str | None = None  # For splits: "new_rate:old_rate" (e.g. "4:1")
    new_rate: Decimal | None = None  # Alpaca: new_rate (splits, spinoffs, mergers)
    old_rate: Decimal | None = None  # Alpaca: old_rate (splits)
    cash_rate: Decimal | None = None  # Alpaca: stock_and_cash_merger cash component
    # CUSIP identifiers
    cusip: str | None = None  # Primary CUSIP (dividends, forward splits, redemptions)
    old_cusip: str | None = None  # Alpaca: reverse_split, unit_split, name_change
    new_cusip: str | None = None  # Alpaca: reverse_split, unit_split, name_change, spin_off
    # Merger-specific fields
    acquirer_symbol: str | None = None  # Alpaca: mergers
    acquirer_cusip: str | None = None
    acquirer_rate: Decimal | None = None  # Alpaca: stock_merger, stock_and_cash_merger
    acquiree_symbol: str | None = None  # Alpaca: mergers
    acquiree_cusip: str | None = None
    acquiree_rate: Decimal | None = None  # Alpaca: stock_merger, stock_and_cash_merger
    # Spin-off / rights distribution fields
    source_symbol: str | None = None  # Alpaca: spin_off, rights_distribution
    source_cusip: str | None = None
    source_rate: Decimal | None = None  # Alpaca: spin_off
    new_symbol: str | None = None  # Alpaca: spin_off, rights_distribution, unit_split, name_change
    # Unit split alternate security
    alternate_symbol: str | None = None  # Alpaca: unit_split
    alternate_cusip: str | None = None
    alternate_rate: Decimal | None = None
    # Name change
    old_symbol: str | None = None  # Alpaca: unit_split, name_change
    # Dividend flags
    special: bool | None = None  # Alpaca: cash_dividend special flag
    foreign: bool | None = None  # Alpaca: cash_dividend foreign flag
    # Backward compatibility
    provider: str
