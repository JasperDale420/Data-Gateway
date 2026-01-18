"""Test fixture generator for Data Gateway.

Generates mock data for testing as specified in PRD Section 3399-3424.
"""

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def generate_bar(
    symbol: str,
    timestamp: datetime,
    base_price: float = 100.0,
    valid: bool = True,
) -> dict[str, Any]:
    """Generate a single bar record."""
    open_price = base_price + random.uniform(-2, 2)

    if valid:
        high = open_price + random.uniform(0.5, 3)
        low = open_price - random.uniform(0.5, 3)
        if low > high:
            low, high = high, low
        close = random.uniform(low, high)
    else:
        # Invalid: high < low
        high = open_price - random.uniform(0.5, 2)
        low = open_price + random.uniform(0.5, 2)
        close = open_price

    return {
        "S": symbol,
        "t": timestamp.isoformat(),
        "o": round(open_price, 2),
        "h": round(high, 2),
        "l": round(low, 2),
        "c": round(close, 2),
        "v": random.randint(10000, 1000000),
        "vw": round((open_price + close) / 2, 4),
        "n": random.randint(100, 5000),
    }


def generate_quote(
    symbol: str,
    timestamp: datetime,
    base_price: float = 100.0,
    crossed: bool = False,
) -> dict[str, Any]:
    """Generate a single quote record."""
    mid = base_price + random.uniform(-2, 2)
    spread = random.uniform(0.01, 0.10)

    if crossed:
        # Invalid: bid > ask
        bid = mid + spread / 2
        ask = mid - spread / 2
    else:
        bid = mid - spread / 2
        ask = mid + spread / 2

    return {
        "S": symbol,
        "t": timestamp.isoformat(),
        "bp": round(bid, 2),
        "ap": round(ask, 2),
        "bs": random.randint(100, 5000),
        "as": random.randint(100, 5000),
        "bx": "XNYS",
        "ax": "XNAS",
    }


def generate_trade(
    symbol: str,
    timestamp: datetime,
    base_price: float = 100.0,
    zero_size: bool = False,
) -> dict[str, Any]:
    """Generate a single trade record."""
    return {
        "S": symbol,
        "t": timestamp.isoformat(),
        "p": round(base_price + random.uniform(-1, 1), 2),
        "s": 0 if zero_size else random.randint(1, 10000),
        "x": random.choice(["XNYS", "XNAS", "XASE", "ARCX"]),
        "c": ["@"],
    }


def generate_option_contract(
    underlying: str,
    expiration: datetime,
    strike: float,
    is_call: bool,
) -> dict[str, Any]:
    """Generate an option contract."""
    option_type = "C" if is_call else "P"
    exp_str = expiration.strftime("%y%m%d")
    strike_str = f"{int(strike * 1000):08d}"
    occ_symbol = f"{underlying}{exp_str}{option_type}{strike_str}"

    return {
        "symbol": occ_symbol,
        "underlying": underlying,
        "expiration": expiration.strftime("%Y-%m-%d"),
        "strike": strike,
        "option_type": "call" if is_call else "put",
        "bid": round(random.uniform(0.5, 10), 2),
        "ask": round(random.uniform(0.5, 10) + 0.05, 2),
        "last": round(random.uniform(0.5, 10), 2),
        "volume": random.randint(10, 5000),
        "open_interest": random.randint(100, 50000),
        "implied_volatility": round(random.uniform(0.15, 0.80), 4),
        "delta": round(random.uniform(-1, 1), 4),
        "gamma": round(random.uniform(0, 0.1), 4),
        "theta": round(random.uniform(-0.5, 0), 4),
        "vega": round(random.uniform(0, 0.5), 4),
    }


def generate_uw_flow(timestamp: datetime) -> dict[str, Any]:
    """Generate a UW flow record."""
    symbols = ["AAPL", "TSLA", "NVDA", "SPY", "QQQ", "MSFT", "AMD", "META"]
    symbol = random.choice(symbols)

    return {
        "timestamp": timestamp.isoformat(),
        "symbol": symbol,
        "type": random.choice(["call", "put"]),
        "premium": random.randint(10000, 5000000),
        "size": random.randint(1, 1000),
        "strike": round(random.uniform(50, 500), 0),
        "expiration": (timestamp + timedelta(days=random.randint(1, 90))).strftime("%Y-%m-%d"),
        "spot": round(random.uniform(100, 500), 2),
        "sentiment": random.choice(["bullish", "bearish", "neutral"]),
        "unusual_score": round(random.uniform(0, 10), 2),
    }


def generate_fixtures() -> None:
    """Generate all test fixtures as specified in PRD."""
    FIXTURES_DIR.mkdir(exist_ok=True)

    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "AMD", "SPY", "QQQ"]
    base_time = datetime(2026, 1, 14, 14, 30, 0, tzinfo=UTC)

    # 1. alpaca_bars_valid.json (1000 records)
    print("Generating alpaca_bars_valid.json...")
    bars_valid = []
    for i in range(1000):
        symbol = symbols[i % len(symbols)]
        ts = base_time - timedelta(minutes=i)
        bars_valid.append(generate_bar(symbol, ts, base_price=150 + random.uniform(-20, 20)))

    with open(FIXTURES_DIR / "alpaca_bars_valid.json", "w") as f:
        json.dump(bars_valid, f, indent=2)

    # 2. alpaca_bars_invalid.json (50 records)
    print("Generating alpaca_bars_invalid.json...")
    bars_invalid = []
    for i in range(50):
        symbol = symbols[i % len(symbols)]
        ts = base_time - timedelta(minutes=i)
        bars_invalid.append(generate_bar(symbol, ts, valid=False))

    with open(FIXTURES_DIR / "alpaca_bars_invalid.json", "w") as f:
        json.dump(bars_invalid, f, indent=2)

    # 3. alpaca_quotes.json (500 records)
    print("Generating alpaca_quotes.json...")
    quotes = []
    for i in range(500):
        symbol = symbols[i % len(symbols)]
        ts = base_time - timedelta(seconds=i)
        quotes.append(generate_quote(symbol, ts, base_price=150 + random.uniform(-20, 20)))

    with open(FIXTURES_DIR / "alpaca_quotes.json", "w") as f:
        json.dump(quotes, f, indent=2)

    # 4. alpaca_trades.json (1000 records)
    print("Generating alpaca_trades.json...")
    trades = []
    for i in range(1000):
        symbol = symbols[i % len(symbols)]
        ts = base_time - timedelta(seconds=i)
        trades.append(generate_trade(symbol, ts, base_price=150 + random.uniform(-20, 20)))

    with open(FIXTURES_DIR / "alpaca_trades.json", "w") as f:
        json.dump(trades, f, indent=2)

    # 5. option_chains.json (500 contracts)
    print("Generating option_chains.json...")
    options = []
    underlyings = ["AAPL", "MSFT", "SPY", "QQQ", "TSLA"]
    for underlying in underlyings:
        for dte in [7, 14, 30, 45, 60]:
            exp = base_time + timedelta(days=dte)
            base_strike = 150
            for strike_offset in range(-10, 11, 2):
                strike = base_strike + strike_offset
                options.append(generate_option_contract(underlying, exp, strike, is_call=True))
                options.append(generate_option_contract(underlying, exp, strike, is_call=False))

    with open(FIXTURES_DIR / "option_chains.json", "w") as f:
        json.dump(options[:500], f, indent=2)

    # 6. uw_flow_samples.json (200 records)
    print("Generating uw_flow_samples.json...")
    flows = []
    for i in range(200):
        ts = base_time - timedelta(minutes=i * 5)
        flows.append(generate_uw_flow(ts))

    with open(FIXTURES_DIR / "uw_flow_samples.json", "w") as f:
        json.dump(flows, f, indent=2)

    # 7. symbols_1000.txt (load testing)
    print("Generating symbols_1000.txt...")
    all_symbols = [f"SYM{i:04d}" for i in range(1, 1001)]
    with open(FIXTURES_DIR / "symbols_1000.txt", "w") as f:
        f.write("\n".join(all_symbols))

    # 8. symbols_5000.txt (stress testing)
    print("Generating symbols_5000.txt...")
    stress_symbols = [f"SYM{i:05d}" for i in range(1, 5001)]
    with open(FIXTURES_DIR / "symbols_5000.txt", "w") as f:
        f.write("\n".join(stress_symbols))

    # Edge case files
    print("Generating edge case fixtures...")

    # invalid_high_low.json
    with open(FIXTURES_DIR / "invalid_high_low.json", "w") as f:
        json.dump(generate_bar("AAPL", base_time, valid=False), f, indent=2)

    # crossed_quote.json
    with open(FIXTURES_DIR / "crossed_quote.json", "w") as f:
        json.dump(generate_quote("AAPL", base_time, crossed=True), f, indent=2)

    # zero_volume.json
    with open(FIXTURES_DIR / "zero_volume.json", "w") as f:
        json.dump(generate_trade("AAPL", base_time, zero_size=True), f, indent=2)

    # future_timestamp.json
    future_bar = generate_bar("AAPL", datetime.now(UTC) + timedelta(days=1))
    with open(FIXTURES_DIR / "future_timestamp.json", "w") as f:
        json.dump(future_bar, f, indent=2)

    # extreme_prices.json
    extreme_cases = [
        {"S": "TEST", "t": base_time.isoformat(), "o": 0, "h": 0, "l": 0, "c": 0, "v": 100},
        {"S": "TEST", "t": base_time.isoformat(), "o": -10, "h": -5, "l": -15, "c": -8, "v": 100},
        {
            "S": "TEST",
            "t": base_time.isoformat(),
            "o": 999999,
            "h": 1000000,
            "l": 999990,
            "c": 999995,
            "v": 1,
        },
    ]
    with open(FIXTURES_DIR / "extreme_prices.json", "w") as f:
        json.dump(extreme_cases, f, indent=2)

    print(f"All fixtures generated in {FIXTURES_DIR}")


if __name__ == "__main__":
    generate_fixtures()
