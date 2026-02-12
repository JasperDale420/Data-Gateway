from __future__ import annotations

import pytest
from pydantic import ValidationError

from gateway.api import symbology


def test_batch_resolve_request_enforces_symbol_limit() -> None:
    with pytest.raises(ValidationError):
        symbology.BatchResolveRequest(symbols=[f"SYM{i:03d}" for i in range(501)])


def test_batch_resolve_request_accepts_max_symbols() -> None:
    request = symbology.BatchResolveRequest(symbols=[f"SYM{i:03d}" for i in range(500)])
    assert len(request.symbols) == 500


@pytest.mark.asyncio
async def test_batch_resolve_records_batch_size_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded_sizes: list[int] = []

    def _record(size: int) -> None:
        recorded_sizes.append(size)

    monkeypatch.setattr(symbology, "record_symbology_batch_size", _record)

    response = await symbology.batch_resolve_symbols(
        symbology.BatchResolveRequest(symbols=["AAPL", "BTC/USD", "INVALID_SYMBOL"])
    )

    assert recorded_sizes == [3]
    assert len(response.results) + len(response.errors) == 3
