from __future__ import annotations

from gateway.api.alpaca.common import parse_comma_values


def test_parse_comma_values_trims_and_uppercases() -> None:
    values = parse_comma_values(" aapl, msft ", uppercase=True)
    assert values == ["AAPL", "MSFT"]


def test_parse_comma_values_preserves_empty_entries_by_default() -> None:
    values = parse_comma_values("AAPL,,MSFT", uppercase=True)
    assert values == ["AAPL", "", "MSFT"]


def test_parse_comma_values_can_drop_empty_entries() -> None:
    values = parse_comma_values("AAPL,,MSFT", uppercase=True, drop_empty=True)
    assert values == ["AAPL", "MSFT"]
