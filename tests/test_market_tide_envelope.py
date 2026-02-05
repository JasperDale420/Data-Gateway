from datetime import UTC, datetime

from gateway.core.envelope import wrap_event


def test_market_tide_sets_placeholder_symbol() -> None:
    event = {
        "timestamp": datetime.now(UTC).isoformat(),
        "net_call_premium": "100",
        "net_put_premium": "50",
        "net_volume": 1,
        "sentiment": "bullish",
    }

    envelope = wrap_event(event=event, provider="unusual_whales", feed="market_tide", source="rest")

    assert envelope["symbol"] == "MARKET"
    assert envelope["instrument_key"] == "equity:MARKET"
