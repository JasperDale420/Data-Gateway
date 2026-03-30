#!/usr/bin/env python3
"""
Hourly check-in script for Jarvis to work on trading bot.
Run this during hourly wake-ups to see status and decide what to work on.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def print_header():
    """Print check-in header."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 70)
    print("TRADING BOT - HOURLY CHECK-IN")
    print(f"Time: {timestamp}")
    print("=" * 70)


def check_system_status():
    """Check system and connectivity status."""
    print("\n1. SYSTEM STATUS")
    print("-" * 40)

    # Check Data-Gateway
    try:
        import requests

        response = requests.get("http://localhost:8080/health", timeout=5)
        if response.status_code == 200:
            print("✅ Data-Gateway: Running")
        else:
            print(f"⚠️  Data-Gateway: Status {response.status_code}")
    except Exception as exc:
        print(f"❌ Data-Gateway: Not reachable - {exc}")

    # Check Alpaca
    try:
        from core.trading_client import TradingClientWrapper

        client = TradingClientWrapper(paper=True)
        account = client.get_account()
        print("✅ Alpaca Paper Trading: Connected")
        print(f"   Equity: ${account['equity']:,.2f}")
        print(f"   Buying Power: ${account['buying_power']:,.2f}")
        print(f"   Cash: ${account['cash']:,.2f}")

        positions = client.get_positions()
        print(f"   Positions: {len(positions)}")
        for pos in positions:
            print(f"     {pos['symbol']}: {pos['qty']} shares (${pos['market_value']:,.2f})")

    except Exception as e:
        print(f"❌ Alpaca: Connection failed - {e}")


def check_recent_activity():
    """Check recent logs and activity."""
    print("\n2. RECENT ACTIVITY")
    print("-" * 40)

    log_dir = Path(__file__).parent / "logs"

    # Check for recent trade logs
    trade_log = log_dir / "trades.jsonl"
    if trade_log.exists():
        with open(trade_log) as f:
            trades = f.readlines()
        print(f"📊 Recent trades: {len(trades)}")
        if trades:
            # Show last 3 trades
            for trade in trades[-3:]:
                import json

                data = json.loads(trade)
                print(
                    f"   {data['timestamp'][:16]}: {data['signal']} {data['symbol']} "
                    f"{data['shares']} shares @ ${data['price']:.2f}"
                )
    else:
        print("📊 No trades recorded yet")

    # Check performance log
    perf_log = log_dir / "performance.csv"
    if perf_log.exists():
        import pandas as pd

        try:
            df = pd.read_csv(perf_log)
            if len(df) > 0:
                latest = df.iloc[-1]
                print(f"📈 Latest equity: ${latest['equity']:,.2f}")
                print(f"📈 Positions: {latest['positions_count']}")
                if len(df) > 1:
                    prev = df.iloc[-2]
                    pnl = latest["equity"] - prev["equity"]
                    print(f"📈 Last P&L: ${pnl:,.2f}")
        except Exception as exc:
            print(f"📈 Performance data available (parse error: {exc})")
    else:
        print("📈 No performance data yet")


def suggest_tasks():
    """Suggest tasks for this hour based on current status."""
    print("\n3. SUGGESTED TASKS FOR THIS HOUR")
    print("-" * 40)

    log_dir = Path(__file__).parent / "logs"
    trade_log = log_dir / "trades.jsonl"

    # Priority 1: First test trade
    trade_count = 0
    if trade_log.exists():
        with trade_log.open() as trade_file:
            trade_count = len(trade_file.readlines())

    if trade_count == 0:
        print("🚀 PRIORITY: Execute first test trade")
        print("   Command: ./run_with_deps.sh src/core/trading_bot.py")
        print("   This will run the bot and potentially execute a trade")
        return

    # Priority 2: Analyze recent trades
    print("📊 Analyze recent trades and performance")
    print("   Check logs/trades.jsonl and logs/performance.csv")

    # Priority 3: Run backtest
    print("🧪 Run backtest on historical data")
    print("   Need to implement backtest framework")

    # Priority 4: Improve strategy
    print("⚙️  Improve strategy parameters")
    print("   Adjust Bollinger Bands, RSI thresholds, etc.")

    # Priority 5: Add features
    print("✨ Add new features")
    print("   - Stop loss/take profit automation")
    print("   - Additional technical indicators")
    print("   - Multiple symbol support")


def run_quick_test():
    """Run quick test to verify everything works."""
    print("\n4. QUICK SYSTEM TEST")
    print("-" * 40)

    try:
        # Import and test strategy
        import numpy as np
        import pandas as pd
        from strategies.mean_reversion import MeanReversionStrategy

        # Create test data
        np.random.seed(42)
        dates = pd.date_range("2026-01-01", periods=50, freq="D")
        prices = 100 + np.cumsum(np.random.randn(50) * 2)

        df = pd.DataFrame(
            {
                "open": prices * 0.99,
                "high": prices * 1.01,
                "low": prices * 0.98,
                "close": prices,
                "volume": np.random.randint(1000000, 5000000, 50),
            },
            index=dates,
        )

        strategy = MeanReversionStrategy()
        df_with_indicators = strategy.calculate_indicators(df)
        signal = strategy.generate_signal(df_with_indicators, "SPY")

        if signal:
            print(f"✅ Strategy signal: {signal.signal.value}")
            print(f"   Confidence: {signal.confidence:.2%}")
            print(f"   Price: ${signal.price:.2f}")
        else:
            print("✅ Strategy: No signal (HOLD)")

        print("✅ All components working")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()


def main():
    """Main check-in function."""
    print_header()
    check_system_status()
    check_recent_activity()
    suggest_tasks()
    run_quick_test()

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print("1. Review suggested tasks above")
    print("2. Run trading bot: ./run_with_deps.sh src/core/trading_bot.py")
    print("3. Check logs in: logs/")
    print("4. Update artifact trail: ARTIFACT_TRAIL.md")
    print("\nUseful commands:")
    print("  ./run_with_deps.sh quick_test.py      # Run quick test")
    print("  ./run_with_deps.sh test_integration.py # Full integration test")
    print("  ./setup_hourly_wakeup.sh              # Setup hourly wake-ups")
    print("=" * 70)


if __name__ == "__main__":
    main()
