from __future__ import annotations

import pytest

from gateway.api import quality as quality_api


@pytest.mark.asyncio
async def test_analyze_data_uses_collected_issues_not_detect_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Result:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def to_dict(self) -> dict:
            return self._payload

    class _FakeAnalyzer:
        def analyze_bars(self, bars, expected_count=None, timeframe="1Min", issues_out=None):
            if issues_out is not None:
                issues_out.append(type("Issue", (), {"to_dict": lambda self: {"code": "Q004"}})())
            return _Result({"bars": len(bars)})

        def analyze_quotes(self, quotes, issues_out=None):
            if issues_out is not None:
                issues_out.append(type("Issue", (), {"to_dict": lambda self: {"code": "Q002"}})())
            return _Result({"quotes": len(quotes)})

        def analyze_trades(self, trades):
            return _Result({"trades": len(trades)})

        def detect_issues(self, bars=None, quotes=None, trades=None):
            raise AssertionError("detect_issues should not be called")

    monkeypatch.setattr(quality_api, "get_quality_analyzer", lambda: _FakeAnalyzer())

    result = await quality_api.analyze_data(
        bars=[{"close": 100, "volume": 0}],
        quotes=[{"bid_price": 105, "ask_price": 100}],
        trades=[{"size": 100}],
    )

    assert result["bars"] == {"bars": 1}
    assert result["quotes"] == {"quotes": 1}
    assert result["trades"] == {"trades": 1}
    assert result["issues"] == [{"code": "Q004"}, {"code": "Q002"}]


@pytest.mark.asyncio
async def test_analyze_data_returns_crossed_quote_issue() -> None:
    result = await quality_api.analyze_data(
        quotes=[
            {"timestamp": "2024-01-15T10:00:00Z", "bid_price": 105, "ask_price": 100},
        ]
    )
    assert result["quotes"]["crossed"] == 1
    assert any(issue["code"] == "Q002" for issue in result["issues"])
